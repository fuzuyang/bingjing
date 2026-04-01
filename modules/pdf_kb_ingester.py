import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
from openai import OpenAI
from pypdf import PdfReader
from sqlalchemy.orm import Session

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from core import KBChunk, KBDocument, SessionLocal, init_db  # noqa: E402

load_dotenv()


@dataclass
class PDFIngestResult:
    file_path: str
    document_title: str
    status: str
    message: str
    document_id: int = 0
    page_count: int = 0
    chunk_count: int = 0
    llm_refined_count: int = 0


class PDFKBIngestor:
    """
    kb_chunk target:
      section_no       -> Arabic number string, e.g. "1"
      section_title    -> "第X章 XXX"
      subsection_no    -> Arabic number string, e.g. "3"
      subsection_title -> "第X条"
    """

    CHAPTER_RE = re.compile(r"^第\s*([一二三四五六七八九十百千万零〇两\d]+)\s*章(?:[：:\s、．.]*)?(.*)$")
    ARTICLE_RE = re.compile(r"^第\s*([一二三四五六七八九十百千万零〇两\d]+)\s*条(?:[：:\s、．.]*(.*))?$")
    PAGE_NOISE_RE = re.compile(r"^(第?\s*\d+\s*页\s*/\s*共?\s*\d+\s*页|\d+\s*/\s*\d+)$")
    SENTENCE_PUNC_RE = re.compile(r"[。；;，,：:！!?]$")
    LEAD_ARTICLE_HEADING_RE = re.compile(r"^[（(【\[\《][^）)】\]》]{1,24}[）)】\]》][：:、，,\s]*(.*)$")
    ARTICLE_LEAD_RE = re.compile(r"^第\s*([一二三四五六七八九十百千万零〇两\d]+)\s*条")

    CN_NUM_MAP = {
        "零": 0,
        "〇": 0,
        "一": 1,
        "二": 2,
        "两": 2,
        "三": 3,
        "四": 4,
        "五": 5,
        "六": 6,
        "七": 7,
        "八": 8,
        "九": 9,
    }
    CN_UNIT_MAP = {"十": 10, "百": 100, "千": 1000, "万": 10000}

    DOC_TITLE_HINTS = ("制度", "办法", "规范", "手册", "管理", "指引", "细则", "章程")

    def __init__(self, raw_folder: Optional[str] = None):
        self.raw_folder = raw_folder or os.path.join(PROJECT_ROOT, "raw_file")
        self.api_key = os.getenv("SILICONFLOW_API_KEY")
        self.enable_llm_refine = str(os.getenv("ENABLE_PDF_LLM_REFINE", "1")).strip() == "1" and bool(self.api_key)
        self.client = (
            OpenAI(api_key=self.api_key, base_url="https://api.siliconflow.cn/v1")
            if self.enable_llm_refine
            else None
        )
        self.model = os.getenv("PDF_STRUCTURE_LLM_MODEL", "deepseek-ai/DeepSeek-V3")
        self.low_conf_threshold = float(os.getenv("PDF_LOW_CONF_THRESHOLD", "0.72"))
        self.max_refine_chunks = int(os.getenv("PDF_MAX_REFINE_CHUNKS", "18"))
        self.refine_batch_size = int(os.getenv("PDF_REFINE_BATCH_SIZE", "8"))
        self.enable_vector_sync = str(os.getenv("ENABLE_KB_VECTOR_SYNC_ON_INGEST", "1")).strip() == "1"

        data_dir = os.path.join(PROJECT_ROOT, "data")
        os.makedirs(data_dir, exist_ok=True)
        self.manifest_path = os.path.join(data_dir, "pdf_ingest_manifest.json")

    @staticmethod
    def _clean_text(text: str) -> str:
        raw = str(text or "").replace("\u3000", " ")
        raw = re.sub(r"[ \t]+", " ", raw)
        raw = re.sub(r"\n{3,}", "\n\n", raw)
        return raw.strip()

    @staticmethod
    def _normalize_cmp_text(text: str) -> str:
        return re.sub(r"[^\u4e00-\u9fffa-z0-9]+", "", str(text or "").lower())

    @staticmethod
    def _is_empty_body(content: str) -> bool:
        raw = str(content or "").strip()
        return (not raw) or raw in {".", "。", "-", "—"}

    @staticmethod
    def _file_sha256(file_path: str) -> str:
        h = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()

    def _load_manifest(self) -> Dict:
        if not os.path.exists(self.manifest_path):
            return {}
        try:
            with open(self.manifest_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _save_manifest(self, manifest: Dict):
        with open(self.manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)

    def _list_pdf_files(self, folder_path: str) -> List[str]:
        if not os.path.isdir(folder_path):
            return []
        out = []
        for name in os.listdir(folder_path):
            path = os.path.join(folder_path, name)
            if os.path.isfile(path) and name.lower().endswith(".pdf"):
                out.append(path)
        return sorted(out)

    def _extract_pdf_pages(self, file_path: str) -> List[Dict]:
        pages: List[Dict] = []
        reader = PdfReader(file_path)
        for i, page in enumerate(reader.pages, start=1):
            text = page.extract_text() or ""
            lines = []
            for raw in text.splitlines():
                line = self._clean_text(raw)
                if not line or self.PAGE_NOISE_RE.match(line):
                    continue
                lines.append(line)
            pages.append({"page_no": i, "lines": lines})
        return pages

    @classmethod
    def _cn_to_int(cls, token: str) -> int:
        text = str(token or "").strip()
        if not text:
            return 0
        if text.isdigit():
            return int(text)

        total = 0
        section = 0
        number = 0
        has_value = False
        for ch in text:
            if ch in cls.CN_NUM_MAP:
                number = cls.CN_NUM_MAP[ch]
                has_value = True
                continue
            unit = cls.CN_UNIT_MAP.get(ch)
            if not unit:
                return 0
            has_value = True
            if unit == 10000:
                section = section + number
                if section == 0:
                    section = 1
                total += section * unit
                section = 0
                number = 0
            else:
                if number == 0:
                    number = 1
                section += number * unit
                number = 0
        if not has_value:
            return 0
        return total + section + number

    @staticmethod
    def _int_to_cn(num: int) -> str:
        if num <= 0:
            return ""
        digits = "零一二三四五六七八九"
        units = ["", "十", "百", "千"]
        n = int(num)
        parts: List[str] = []
        pos = 0
        while n > 0:
            n, rem = divmod(n, 10)
            if rem == 0:
                parts.append("零")
            else:
                parts.append(f"{digits[rem]}{units[pos]}")
            pos += 1
        text = "".join(reversed(parts))
        text = re.sub(r"零+", "零", text).strip("零")
        text = text.replace("一十", "十", 1)
        return text or "零"

    @staticmethod
    def _extract_digits(text: str) -> str:
        m = re.search(r"\d+", str(text or ""))
        return str(int(m.group(0))) if m else ""

    def _looks_like_doc_title_line(self, line: str, doc_title_norm: str) -> bool:
        text = self._clean_text(line)
        if not text:
            return False
        # Never treat structural chapter/article headings as document title noise.
        if self.CHAPTER_RE.match(text) or self.ARTICLE_RE.match(text):
            return False
        cmp_text = self._normalize_cmp_text(text)
        if doc_title_norm and cmp_text:
            if cmp_text == doc_title_norm:
                return True
            # Allow compact variants that are clearly the same title line.
            if len(cmp_text) >= 6 and (cmp_text in doc_title_norm or doc_title_norm in cmp_text):
                return True
        return False

    def _strip_article_lead_heading(self, rest: str) -> str:
        text = self._clean_text(rest)
        if not text:
            return ""
        m = self.LEAD_ARTICLE_HEADING_RE.match(text)
        if m:
            return self._clean_text(m.group(1))
        return text

    def _normalize_section_title(self, section_no: str, section_title: str) -> str:
        sec = str(section_no or "").strip()
        title = self._clean_text(section_title)
        sec_cn = self._int_to_cn(int(sec)) if sec.isdigit() else sec
        if not sec:
            return title
        if not title:
            return f"第{sec_cn}章"
        if "章" in title:
            suffix = self._clean_text(title.split("章", 1)[1])
            return f"第{sec_cn}章" + (f" {suffix}" if suffix else "")
        return f"第{sec_cn}章 {title}"

    def _normalize_subsection_title(self, subsection_no: str) -> str:
        sub = str(subsection_no or "").strip()
        if not sub:
            return ""
        sub_cn = self._int_to_cn(int(sub)) if sub.isdigit() else sub
        return f"第{sub_cn}条"

    def _match_chapter(self, line: str) -> Optional[Dict]:
        m = self.CHAPTER_RE.match(line)
        if not m:
            return None
        raw_num = self._clean_text(m.group(1))
        chapter_no = self._cn_to_int(raw_num)
        if chapter_no <= 0:
            return None
        tail = self._clean_text(m.group(2))
        section_no = str(chapter_no)
        section_title = self._normalize_section_title(section_no, f"第{raw_num}章 {tail}".strip())
        return {"section_no": section_no, "section_title": section_title}

    def _match_article(self, line: str) -> Optional[Dict]:
        m = self.ARTICLE_RE.match(line)
        if not m:
            return None
        raw_num = self._clean_text(m.group(1))
        article_no = self._cn_to_int(raw_num)
        if article_no <= 0:
            return None
        subsection_no = str(article_no)
        subsection_title = self._normalize_subsection_title(subsection_no)
        rest = self._strip_article_lead_heading(self._clean_text(m.group(2)))
        return {"subsection_no": subsection_no, "subsection_title": subsection_title, "rest": rest}

    def _rule_build_chunks(self, pages: List[Dict], doc_title: str) -> List[Dict]:
        chunks: List[Dict] = []
        body: List[Dict] = []
        pre_structure_body: List[Dict] = []
        section_auto = 0
        current_section_no = ""
        current_section_title = ""
        current_subsection_no = ""
        current_subsection_title = ""
        seen_structure = False
        doc_title_norm = self._normalize_cmp_text(doc_title)

        def ensure_section_context():
            nonlocal current_section_no, current_section_title, section_auto
            if not current_section_no:
                section_auto += 1
                current_section_no = str(section_auto)
                current_section_title = self._normalize_section_title(current_section_no, "")

        def flush_body():
            nonlocal body
            if not body:
                return
            content = self._clean_text("\n".join([x["text"] for x in body]))
            if self._is_empty_body(content):
                body = []
                return
            ensure_section_context()
            chunks.append(
                {
                    "chunk_no": len(chunks) + 1,
                    "section_no": current_section_no,
                    "section_title": current_section_title,
                    "subsection_no": current_subsection_no,
                    "subsection_title": current_subsection_title,
                    "content": content,
                    "page_no": int(body[0]["page_no"]),
                    "_confidence": min(float(x.get("confidence", 0.86)) for x in body),
                }
            )
            body = []

        for page in pages:
            page_no = int(page.get("page_no") or 1)
            for raw_line in page.get("lines", []):
                line = self._clean_text(raw_line)
                if not line:
                    continue

                chapter = self._match_chapter(line)
                if chapter:
                    if not seen_structure:
                        pre_structure_body = []
                    seen_structure = True
                    flush_body()
                    current_section_no = chapter["section_no"]
                    current_section_title = chapter["section_title"][:255]
                    section_auto = max(section_auto, int(current_section_no))
                    current_subsection_no = ""
                    current_subsection_title = ""
                    continue

                article = self._match_article(line)
                if article:
                    if not seen_structure:
                        pre_structure_body = []
                    seen_structure = True
                    flush_body()
                    ensure_section_context()
                    current_subsection_no = article["subsection_no"]
                    current_subsection_title = article["subsection_title"]
                    rest = self._clean_text(article["rest"])
                    if rest and not self._looks_like_doc_title_line(rest, doc_title_norm):
                        body.append({"text": rest, "page_no": page_no, "confidence": 0.92})
                    continue

                if self._looks_like_doc_title_line(line, doc_title_norm):
                    continue

                if not seen_structure:
                    if not self._looks_like_doc_title_line(line, doc_title_norm):
                        pre_structure_body.append({"text": line, "page_no": page_no, "confidence": 0.84})
                    continue
                body.append({"text": line, "page_no": page_no, "confidence": 0.88})

        if not seen_structure and pre_structure_body:
            body = pre_structure_body
        flush_body()
        return chunks

    def _chunk_confidence(self, row: Dict) -> float:
        score = float(row.get("_confidence", 0.7))
        if not self._extract_digits(row.get("section_no")):
            score -= 0.35
        subsection_no = self._extract_digits(row.get("subsection_no"))
        content = self._clean_text(row.get("content") or "")
        if (not subsection_no) and self.ARTICLE_LEAD_RE.match(content):
            score -= 0.25
        if self._is_empty_body(content):
            score -= 0.45
        return max(0.0, min(1.0, score))

    def _sanitize_row(self, row: Dict, fallback: Dict) -> Dict:
        def pick(field: str, default):
            if field in row:
                return row.get(field)
            return fallback.get(field, default)

        out = dict(fallback)
        out["chunk_no"] = max(1, int(pick("chunk_no", 1) or 1))
        out["section_no"] = self._extract_digits(pick("section_no", "")) or self._extract_digits(fallback.get("section_no", ""))
        out["subsection_no"] = self._extract_digits(pick("subsection_no", "")) or self._extract_digits(
            fallback.get("subsection_no", "")
        )
        out["content"] = self._clean_text(pick("content", fallback.get("content", "")))
        out["page_no"] = max(1, int(pick("page_no", fallback.get("page_no", 1)) or 1))
        out["_confidence"] = float(pick("_confidence", fallback.get("_confidence", 0.7)) or 0.7)

        out["section_title"] = self._normalize_section_title(out["section_no"], pick("section_title", fallback.get("section_title", "")))[:255]
        out["subsection_title"] = self._normalize_subsection_title(out["subsection_no"])[:255]
        return out

    def _llm_refine_chunks(self, doc_title: str, chunks: List[Dict]) -> Tuple[List[Dict], int]:
        if not self.enable_llm_refine or not self.client:
            return chunks, 0
        candidates = [x for x in chunks if self._chunk_confidence(x) < self.low_conf_threshold][: self.max_refine_chunks]
        if not candidates:
            return chunks, 0
        by_id = {int(x["chunk_no"]): dict(x) for x in chunks}
        refined_count = 0
        for i in range(0, len(candidates), self.refine_batch_size):
            batch = candidates[i : i + self.refine_batch_size]
            prompt = (
                "Return JSON only: {\"chunks\":[...]}.\n"
                "Fields: chunk_no, section_no, section_title, subsection_no, subsection_title, content, page_no.\n"
                "section_no must be Arabic digits.\n"
                "subsection_no can be empty only for chapter-intro text (between chapter title and first article);\n"
                "otherwise subsection_no must be Arabic digits.\n"
                "section_title format must be exactly like: 第一章 总则.\n"
                "subsection_title must be empty when subsection_no is empty.\n"
                "subsection_title otherwise must be exactly like: 第一条 (do not write article heading text).\n"
                f"Document: {doc_title}\n"
                f"Candidates: {json.dumps(batch, ensure_ascii=False)}"
            )
            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[{"role": "system", "content": "You are a strict JSON emitter."}, {"role": "user", "content": prompt}],
                    response_format={"type": "json_object"},
                    temperature=0.1,
                    max_tokens=1600,
                )
                payload = json.loads(response.choices[0].message.content or "{}")
                fixed_rows = payload.get("chunks", []) if isinstance(payload, dict) else []
                fixed_map = {int(x.get("chunk_no") or 0): x for x in fixed_rows if isinstance(x, dict)}
            except Exception:
                fixed_map = {}

            for old in batch:
                key = int(old["chunk_no"])
                normalized = self._sanitize_row(fixed_map.get(key, old), by_id.get(key, old))
                if normalized != by_id.get(key):
                    refined_count += 1
                by_id[key] = normalized
        return [by_id[k] for k in sorted(by_id.keys())], refined_count

    def _finalize_chunks(self, chunks: List[Dict]) -> List[Dict]:
        out: List[Dict] = []
        section_auto = 0
        for row in chunks:
            fixed = self._sanitize_row(row, row)
            if not fixed["section_no"]:
                section_auto += 1
                fixed["section_no"] = str(section_auto)
            else:
                section_auto = max(section_auto, int(fixed["section_no"]))
            fixed["section_title"] = self._normalize_section_title(fixed["section_no"], fixed.get("section_title", ""))
            if fixed["subsection_no"]:
                fixed["subsection_title"] = self._normalize_subsection_title(fixed["subsection_no"])
            else:
                fixed["subsection_title"] = ""

            if self._is_empty_body(fixed["content"]):
                continue
            out.append(fixed)

        for idx, row in enumerate(out, start=1):
            row["chunk_no"] = idx
        return out

    @staticmethod
    def _doc_title_from_file(file_path: str) -> str:
        return os.path.splitext(os.path.basename(file_path))[0][:255]

    def _ensure_document_record(self, db: Session, doc_title: str) -> int:
        doc = db.query(KBDocument).filter(KBDocument.title == doc_title).first()
        if not doc:
            doc = KBDocument(title=doc_title, doc_type="pdf", status="有效")
            db.add(doc)
            db.flush()
        else:
            doc.doc_type = "pdf"
            doc.status = "有效"
            db.flush()
        return int(doc.id)

    def _write_to_db(self, db: Session, doc_title: str, chunks: List[Dict]) -> int:
        doc_id = self._ensure_document_record(db, doc_title)
        db.query(KBChunk).filter(KBChunk.document_id == doc_id).delete(synchronize_session=False)
        for row in chunks:
            subsection_no = str(row.get("subsection_no") or "").strip()
            subsection_title = str(row.get("subsection_title") or "").strip()
            db.add(
                KBChunk(
                    document_id=doc_id,
                    chunk_no=int(row["chunk_no"]),
                    section_no=str(row["section_no"]),
                    section_title=str(row["section_title"])[:255],
                    subsection_no=subsection_no or None,
                    subsection_title=(subsection_title[:255] if subsection_title else None),
                    content=str(row["content"]),
                    page_no=int(row["page_no"]),
                )
            )
        db.flush()
        return int(doc_id)

    def _sync_vector_index(self) -> Tuple[bool, str]:
        if not self.enable_vector_sync:
            return True, "向量同步已关闭"
        try:
            from modules.evaluator.vector_kb_retriever import VectorKBRetriever

            result = VectorKBRetriever().rebuild_index_now()
            if result.get("ok"):
                return True, "向量库同步成功"
            return False, f"向量库同步失败({result.get('reason', 'unknown')})"
        except Exception as exc:
            return False, f"向量同步器加载失败: {exc}"

    def ingest_pdf(self, file_path: str, sync_vector_index: bool = True) -> PDFIngestResult:
        init_db()
        if not os.path.isfile(file_path) or not file_path.lower().endswith(".pdf"):
            return PDFIngestResult(file_path, self._doc_title_from_file(file_path), "failed", "文件不存在或不是 PDF")

        doc_title = self._doc_title_from_file(file_path)
        file_hash = self._file_sha256(file_path)
        manifest = self._load_manifest()

        db: Session = SessionLocal()
        try:
            doc_id = self._ensure_document_record(db, doc_title)
            db.commit()
        finally:
            db.close()

        old = manifest.get(file_path, {})
        if old.get("sha256") == file_hash:
            return PDFIngestResult(file_path, doc_title, "skipped", "文件未变化，跳过入库", doc_id)

        try:
            pages = self._extract_pdf_pages(file_path)
        except Exception as e:
            return PDFIngestResult(file_path, doc_title, "failed", f"PDF 解析失败: {e}")
        if not pages:
            return PDFIngestResult(file_path, doc_title, "failed", "PDF 无可用文本页")

        chunks = self._finalize_chunks(self._rule_build_chunks(pages, doc_title=doc_title))
        if not chunks:
            return PDFIngestResult(file_path, doc_title, "failed", "规则切块失败，未生成 chunk", page_count=len(pages))

        refined, refined_count = self._llm_refine_chunks(doc_title, chunks)
        final_chunks = self._finalize_chunks(refined)

        db = SessionLocal()
        try:
            doc_id = self._write_to_db(db, doc_title, final_chunks)
            db.commit()
        except Exception as e:
            db.rollback()
            return PDFIngestResult(
                file_path=file_path,
                document_title=doc_title,
                status="failed",
                message=f"写入关系库失败: {e}",
                page_count=len(pages),
                chunk_count=len(final_chunks),
                llm_refined_count=refined_count,
            )
        finally:
            db.close()

        manifest[file_path] = {"sha256": file_hash, "document_id": doc_id, "title": doc_title}
        self._save_manifest(manifest)

        msg = "入库成功"
        if sync_vector_index:
            _, vmsg = self._sync_vector_index()
            msg = f"{msg}; {vmsg}"
        return PDFIngestResult(file_path, doc_title, "ok", msg, doc_id, len(pages), len(final_chunks), refined_count)

    def ingest_folder(self, folder_path: Optional[str] = None) -> List[PDFIngestResult]:
        files = self._list_pdf_files(folder_path or self.raw_folder)
        if not files:
            return []
        rows = [self.ingest_pdf(path, sync_vector_index=False) for path in files]
        if any(x.status == "ok" for x in rows):
            _, vmsg = self._sync_vector_index()
            for row in rows:
                if row.status == "ok":
                    row.message = f"{row.message}; {vmsg}"
        return rows


if __name__ == "__main__":
    ingestor = PDFKBIngestor()
    results = ingestor.ingest_folder()
    if not results:
        print(f"未发现可入库 PDF 文件: {ingestor.raw_folder}")
    for row in results:
        print(
            f"[{row.status}] {os.path.basename(row.file_path)} -> {row.message}; "
            f"doc_id={row.document_id}, pages={row.page_count}, chunks={row.chunk_count}, llm_refined={row.llm_refined_count}"
        )
