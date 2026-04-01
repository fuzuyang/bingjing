import os
import pickle
import re
import sys
import time
from typing import Dict, List, Tuple

import faiss
import numpy as np
from dotenv import load_dotenv
from openai import OpenAI
from sqlalchemy import func
from sqlalchemy.orm import Session

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from core import KBChunk, KBDocument, SessionLocal  # noqa: E402

load_dotenv()


class VectorKBRetriever:
    """
    Vector-first retrieval for kb_document/kb_chunk:
    1) vector recall chunk ids
    2) relational fetch by chunk ids
    3) structure supplements (parent section)
    """
    ARTICLE_SECTION_RE = re.compile(r"^\u7b2c[\u4e00-\u9fa5\d]+\u6761$")
    CHAPTER_SECTION_RE = re.compile(r"^\u7b2c[\u4e00-\u9fa5\d]+\u7ae0$")
    LIST_SECTION_RE = re.compile(r"^(?:[\uff08\(][\u4e00-\u9fa5\d]{1,4}[\uff09\)]|[\u4e00-\u9fa5\d]{1,4}[\.、])")
    LEAD_CUE_RE = re.compile(r"(?:\u4ee5\u4e0b|\u5982\u4e0b|\u539f\u5219|\u8981\u70b9|\u8def\u5f84|\u7a0b\u5e8f|\u804c\u8d23)")
    LEAD_CONTINUATION_RE = re.compile(r"(?:\u4e3b\u8981|\u5c65\u884c|\u804c\u8d23|[\u4ee5\u5982]\u4e0b)")
    SECTION_REF_RE = re.compile(r"\u7b2c\s*[\u4e00-\u9fa5\d]{1,10}?\s*[\u7ae0\u6761\u6b3e\u76ee]")
    SECTION_INDEX_RE = re.compile(r"^第([\u4e00-\u9fa5\d〇零两]{1,10})([章节条款目])$")
    ROLE_ANCHOR_RE = re.compile(r"[\u4e00-\u9fff]{2,12}(?:\u5c42|\u4f1a|\u90e8\u95e8|\u673a\u6784|\u59d4\u5458\u4f1a|\u8d1f\u8d23\u4eba|\u5b98)")
    ROLE_TERMS = (
        "\u7ecf\u7406\u5c42",
        "\u8463\u4e8b\u4f1a",
        "\u515a\u59d4",
        "\u515a\u7ec4",
        "\u4e1a\u52a1\u53ca\u804c\u80fd\u90e8\u95e8",
        "\u5408\u89c4\u7ba1\u7406\u90e8\u95e8",
        "\u4e3b\u8981\u8d1f\u8d23\u4eba",
        "\u9996\u5e2d\u5408\u89c4\u5b98",
        "\u5408\u89c4\u7ba1\u7406\u59d4\u5458\u4f1a",
        "\u7eaa\u68c0\u76d1\u5bdf\u673a\u6784",
        "\u5ba1\u8ba1\u90e8\u95e8",
        "\u5de1\u5bdf\u90e8\u95e8",
        "\u76d1\u7763\u8ffd\u8d23\u90e8\u95e8",
    )
    DUTY_PRINCIPLE_TERMS = (
        "\u804c\u8d23",
        "\u5c65\u884c",
        "\u8d1f\u8d23",
        "\u4e3b\u8981\u8d1f\u8d23",
        "\u4e3b\u8981\u5c65\u884c",
        "\u539f\u5219",
        "\u9075\u5faa",
    )
    OBJECT_ROLE_CUES = (
        "\u534f\u52a9",
        "\u914d\u5408",
        "\u6307\u5bfc",
        "\u652f\u6301",
        "\u670d\u52a1",
    )
    LEXICAL_STOP_TERMS = {
        "\u4ec0\u4e48",
        "\u5462",
        "\u5417",
        "\u554a",
        "\u5440",
        "\u600e\u4e48",
        "\u5982\u4f55",
        "\u4e3b\u8981",
        "\u5de5\u4f5c",
        "\u4e8b\u9879",
        "\u8981\u6c42",
        "\u5904\u7406",
        "\u5904\u7f6e",
        "\u5e94\u5bf9",
    }
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

    def __init__(self):
        self.api_key = os.getenv("SILICONFLOW_API_KEY")
        self.embedding_model = os.getenv("VECTOR_EMBEDDING_MODEL", "baai/bge-m3")
        self.top_k = int(os.getenv("VECTOR_TOP_K", "14"))
        self.min_score = float(os.getenv("VECTOR_MIN_SCORE", "0.13"))
        self.search_expand_factor = max(3, int(os.getenv("VECTOR_SEARCH_EXPAND_FACTOR", "4")))
        self.rerank_candidate_factor = max(3, int(os.getenv("VECTOR_RERANK_CANDIDATE_FACTOR", "5")))
        self.lexical_weight = float(os.getenv("VECTOR_LEXICAL_WEIGHT", "0.35"))
        self.auto_rebuild = str(os.getenv("VECTOR_AUTO_REBUILD", "1")).strip() == "1"
        self.meta_version = "kb_chunk_v4_section_subsection"

        data_dir = os.path.join(BASE_DIR, "data")
        os.makedirs(data_dir, exist_ok=True)
        self.index_path = os.path.join(data_dir, "kb_chunk_vector.index")
        self.meta_path = os.path.join(data_dir, "kb_chunk_vector_meta.pkl")

        self.client = (
            OpenAI(
                api_key="sk-dzniguzacgbipgsjjunqbbnbrwfmnodxlyvsswjcyrnbmdfl",
                base_url="https://api.siliconflow.cn/v1",
            )
            if self.api_key
            else None
        )

        self._index = None
        self._meta_records: List[Dict] = []
        self._meta_stats: Dict = {}
        self._faiss_id_to_meta: Dict[int, Dict] = {}

    @staticmethod
    def _safe_load_faiss_index(index_path: str):
        try:
            return faiss.read_index(index_path)
        except Exception:
            with open(index_path, "rb") as f:
                raw = f.read()
            return faiss.deserialize_index(np.frombuffer(raw, dtype="uint8"))

    @staticmethod
    def _safe_save_faiss_index(index, index_path: str):
        try:
            faiss.write_index(index, index_path)
        except Exception:
            raw = faiss.serialize_index(index)
            with open(index_path, "wb") as f:
                f.write(raw.tobytes())

    @staticmethod
    def _clean_text(text: str) -> str:
        return re.sub(r"\s+", " ", str(text or "")).strip()

    @staticmethod
    def _trim_text(text: str, limit: int = 220) -> str:
        raw = VectorKBRetriever._clean_text(text)
        if len(raw) <= limit:
            return raw
        return raw[:limit] + "..."

    @staticmethod
    def _is_empty_chunk_content(content: str) -> bool:
        raw = str(content or "").strip()
        return (not raw) or raw in {".", "。", "-", "…"}

    @staticmethod
    def _normalize_cmp_text(text: str) -> str:
        return re.sub(r"[^\u4e00-\u9fffa-z0-9_]+", "", str(text or "").lower())

    @classmethod
    def _is_article_section(cls, section_no: str) -> bool:
        return bool(cls.ARTICLE_SECTION_RE.match(str(section_no or "").strip()))

    @classmethod
    def _is_chapter_section(cls, section_no: str) -> bool:
        return bool(cls.CHAPTER_SECTION_RE.match(str(section_no or "").strip()))

    @classmethod
    def _is_list_section(cls, section_no: str) -> bool:
        sec = str(section_no or "").strip()
        if not sec:
            return False
        base = sec.split(".p", 1)[0].split(".s", 1)[0]
        return bool(cls.LIST_SECTION_RE.match(base))

    @classmethod
    def _is_enumeration_lead(cls, content: str) -> bool:
        text = re.sub(r"\s+", " ", str(content or "")).strip()
        if not text:
            return False
        if text.endswith(("：", ":")):
            return True
        if len(text) <= 80 and cls.LEAD_CUE_RE.search(text):
            return True
        return False

    @classmethod
    def _is_continuation_title(cls, text: str) -> bool:
        body = re.sub(r"\s+", " ", str(text or "")).strip()
        if not body:
            return False
        if len(body) > 36:
            return False
        if body.startswith(("用，", "主要", "职责", "原则", "如下", "以下")):
            return True
        return bool(cls.LEAD_CONTINUATION_RE.search(body))

    def _collect_lead_continuations(self, chunk: KBChunk, sibling_chunks: List[KBChunk]) -> List[str]:
        if not sibling_chunks:
            return []
        ordered = sorted(sibling_chunks, key=lambda x: int(x.chunk_no or 0))
        idx = -1
        for i, row in enumerate(ordered):
            if int(row.id) == int(chunk.id):
                idx = i
                break
        if idx < 0:
            return []

        base_chunk_no = int(chunk.chunk_no or 0)
        extra_parts: List[str] = []
        for row in ordered[idx + 1 :]:
            row_chunk_no = int(row.chunk_no or 0)
            if base_chunk_no > 0 and row_chunk_no - base_chunk_no > 6:
                break
            if int(row.is_title or 0) != 1:
                if extra_parts:
                    break
                continue

            text = re.sub(r"\s+", " ", str(row.section_title or row.content or "")).strip()
            if self._is_empty_chunk_content(text):
                continue
            if not self._is_continuation_title(text):
                break
            extra_parts.append(text)
            if len(extra_parts) >= 2:
                break
        return extra_parts

    def _collect_following_list_contents(self, chunk: KBChunk, sibling_chunks: List[KBChunk]) -> List[str]:
        if not sibling_chunks:
            return []

        ordered = sorted(sibling_chunks, key=lambda x: int(x.chunk_no or 0))
        idx = -1
        for i, row in enumerate(ordered):
            if int(row.id) == int(chunk.id):
                idx = i
                break
        if idx < 0:
            return []

        section_no = str(chunk.section_no or "").strip()
        base_chunk_no = int(chunk.chunk_no or 0)
        collected: List[str] = []
        seen_sections = set()
        total_chars = 0

        for row in ordered[idx + 1 :]:
            row_chunk_no = int(row.chunk_no or 0)
            if base_chunk_no > 0 and row_chunk_no - base_chunk_no > 20:
                break
            if int(row.is_title or 0) == 1:
                title_text = re.sub(r"\s+", " ", str(row.section_title or row.content or "")).strip()
                if not collected and self._is_continuation_title(title_text):
                    continue
                break

            row_section = str(row.section_no or "").strip()
            row_parent = str(row.parent_section or "").strip()
            if row_section and self._is_article_section(row_section):
                break

            include = False
            section_base = row_section.split(".p", 1)[0].split(".s", 1)[0] if row_section else ""
            if row_parent == section_no:
                include = True
            elif self._is_list_section(row_section):
                include = True
                if section_base:
                    seen_sections.add(section_base)
            elif row_parent and self._is_list_section(row_parent):
                include = True
            elif row_parent and row_parent in seen_sections:
                include = True

            if not include:
                if collected:
                    break
                continue

            row_text = re.sub(r"\s+", " ", str(row.content or "")).strip()
            if self._is_empty_chunk_content(row_text):
                continue

            collected.append(row_text)
            total_chars += len(row_text)
            if len(collected) >= 12 or total_chars >= 1500:
                break

        return collected

    def _infer_parent_section(self, chunk: KBChunk, sibling_chunks: List[KBChunk]) -> str:
        explicit_parent = str(chunk.parent_section or "").strip()
        if explicit_parent:
            return explicit_parent

        section_no = str(chunk.section_no or "").strip()
        if not section_no or self._is_chapter_section(section_no):
            return ""

        ordered = sorted(sibling_chunks or [], key=lambda x: int(x.chunk_no or 0))
        if not ordered:
            return ""

        idx = -1
        for i, row in enumerate(ordered):
            if int(row.id) == int(chunk.id):
                idx = i
                break
        if idx <= 0:
            return ""

        base_chunk_no = int(chunk.chunk_no or 0)
        for row in reversed(ordered[:idx]):
            row_section = str(row.section_no or "").strip()
            if not row_section:
                continue
            if self._is_chapter_section(row_section):
                row_chunk_no = int(row.chunk_no or 0)
                if base_chunk_no > 0 and row_chunk_no > 0 and base_chunk_no - row_chunk_no > 120:
                    return ""
                return row_section
        return ""

    def _build_effective_chunk_content(self, chunk: KBChunk, sibling_chunks: List[KBChunk]) -> str:
        content = str(chunk.content or "").strip()
        section_no = str(chunk.section_no or "").strip()
        children: List[KBChunk] = []
        if section_no:
            for row in sibling_chunks:
                if str(row.parent_section or "").strip() == section_no and not self._is_empty_chunk_content(row.content):
                    children.append(row)
        children.sort(key=lambda x: int(x.chunk_no or 0))
        if not self._is_empty_chunk_content(content):
            lead_content = content
            continuation_parts: List[str] = []
            if self._is_article_section(section_no):
                continuation_parts = self._collect_lead_continuations(chunk, sibling_chunks)
            if continuation_parts:
                lead_content = " ".join([content, *continuation_parts]).strip()

            if self._is_enumeration_lead(lead_content):
                stitched_parts: List[str] = [lead_content]
                seen_text = {self._normalize_cmp_text(lead_content)}
                for row_text in self._collect_following_list_contents(chunk, sibling_chunks):
                    norm = self._normalize_cmp_text(row_text)
                    if not norm or norm in seen_text:
                        continue
                    stitched_parts.append(row_text)
                    seen_text.add(norm)
                if len(stitched_parts) > 1:
                    return " ".join(stitched_parts).strip()
            return lead_content
        if children:
            return " ".join(re.sub(r"\s+", " ", str(c.content or "")).strip() for c in children[:6]).strip()
        return ""

    def _collect_chunk_rows(self, db: Session) -> List[Tuple[KBDocument, KBChunk, str]]:
        rows = (
            db.query(KBDocument, KBChunk)
            .join(KBChunk, KBDocument.id == KBChunk.document_id)
            .order_by(KBDocument.id.asc(), KBChunk.chunk_no.asc())
            .all()
        )
        if not rows:
            return []

        siblings_map: Dict[int, List[KBChunk]] = {}
        for doc, chunk in rows:
            siblings_map.setdefault(int(doc.id), []).append(chunk)

        result = []
        for doc, chunk in rows:
            effective_content = self._build_effective_chunk_content(chunk, siblings_map.get(int(doc.id), []))
            if self._is_empty_chunk_content(effective_content):
                continue
            result.append((doc, chunk, effective_content))
        return result

    def _chunk_stats(self, db: Session) -> Dict:
        count, max_id, max_updated = db.query(
            func.count(KBChunk.id),
            func.max(KBChunk.id),
            func.max(KBChunk.created_at),
        ).one()
        return {
            "count": int(count or 0),
            "max_id": int(max_id or 0),
            "max_updated": str(max_updated or ""),
        }

    def _load_meta(self) -> Dict:
        if not os.path.exists(self.meta_path):
            return {}
        try:
            with open(self.meta_path, "rb") as f:
                data = pickle.load(f)
            if isinstance(data, dict):
                return data
        except Exception:
            return {}
        return {}

    def _save_meta(self, meta: Dict):
        with open(self.meta_path, "wb") as f:
            pickle.dump(meta, f)

    @staticmethod
    def _meta_matches_stats(meta: Dict, current_stats: Dict, required_version: str = "") -> bool:
        if not isinstance(meta, dict):
            return False
        if required_version and str(meta.get("version", "")) != str(required_version):
            return False
        meta_stats = meta.get("stats", {})
        return (
            int(meta_stats.get("count", -1)) == int(current_stats.get("count", 0))
            and int(meta_stats.get("max_id", -1)) == int(current_stats.get("max_id", 0))
            and str(meta_stats.get("max_updated", "")) == str(current_stats.get("max_updated", ""))
        )

    def _build_embeddings(self, texts: List[str]) -> np.ndarray:
        if not self.client or not texts:
            return np.array([]).astype("float32")

        max_batch_size = 64
        all_embeddings = []
        for i in range(0, len(texts), max_batch_size):
            batch = texts[i : i + max_batch_size]
            retry_count = 3
            while retry_count > 0:
                try:
                    response = self.client.embeddings.create(
                        model=self.embedding_model,
                        input=batch,
                    )
                    all_embeddings.extend([item.embedding for item in response.data])
                    break
                except Exception:
                    retry_count -= 1
                    if retry_count == 0:
                        raise
                    time.sleep(1.5)

        return np.array(all_embeddings).astype("float32")

    def _rebuild_index(self, db: Session) -> bool:
        rows = self._collect_chunk_rows(db)
        if not rows:
            self._index = None
            self._meta_records = []
            self._meta_stats = self._chunk_stats(db)
            self._faiss_id_to_meta = {}
            return False

        texts: List[str] = []
        records: List[Dict] = []
        for doc, chunk, content in rows:
            text = " ".join(
                [
                    str(doc.title or ""),
                    str(chunk.section_no or ""),
                    str(chunk.section_title or ""),
                    str(chunk.subsection_no or ""),
                    str(chunk.subsection_title or ""),
                    str(content or ""),
                ]
            ).strip()
            if len(text) < 2:
                continue
            texts.append(text)
            records.append(
                {
                    "chunk_id": int(chunk.id),
                    "document_id": int(doc.id),
                }
            )

        if not texts:
            return False

        vectors = self._build_embeddings(texts)
        if vectors.size == 0:
            return False

        faiss.normalize_L2(vectors)
        index = faiss.IndexFlatIP(vectors.shape[1])
        index.add(vectors)

        meta_records: List[Dict] = []
        for idx, row in enumerate(records):
            meta_records.append(
                {
                    "faiss_id": idx,
                    "chunk_id": row["chunk_id"],
                    "document_id": row["document_id"],
                }
            )

        current_stats = self._chunk_stats(db)
        meta = {
            "version": self.meta_version,
            "stats": current_stats,
            "records": meta_records,
        }

        self._safe_save_faiss_index(index, self.index_path)
        self._save_meta(meta)

        self._index = index
        self._meta_records = meta_records
        self._meta_stats = current_stats
        self._faiss_id_to_meta = {int(x["faiss_id"]): x for x in meta_records}
        return True

    def rebuild_index_now(self, db: Session = None) -> Dict:
        """
        Force rebuild vector index from current kb_document/kb_chunk.
        Returns a lightweight status dict for ingestion pipeline logging.
        """
        own_session = False
        session = db
        if session is None:
            session = SessionLocal()
            own_session = True

        try:
            stats_before = self._chunk_stats(session)
            if int(stats_before.get("count", 0)) <= 0:
                # Keep local cache/meta coherent even when KB is empty.
                self._index = None
                self._meta_records = []
                self._meta_stats = stats_before
                self._faiss_id_to_meta = {}
                return {
                    "ok": True,
                    "rebuilt": False,
                    "reason": "empty_kb",
                    "chunk_count": 0,
                    "index_total": 0,
                }

            if not self.client:
                return {
                    "ok": False,
                    "rebuilt": False,
                    "reason": "embedding_client_unavailable",
                    "chunk_count": int(stats_before.get("count", 0)),
                    "index_total": int(getattr(self._index, "ntotal", 0) if self._index is not None else 0),
                }

            rebuilt = self._rebuild_index(session)
            index_total = int(getattr(self._index, "ntotal", 0) if self._index is not None else 0)
            return {
                "ok": bool(rebuilt or index_total > 0),
                "rebuilt": bool(rebuilt),
                "reason": "ok" if rebuilt else "no_vectors_built",
                "chunk_count": int(stats_before.get("count", 0)),
                "index_total": index_total,
            }
        except Exception as exc:
            return {
                "ok": False,
                "rebuilt": False,
                "reason": f"exception: {exc}",
                "chunk_count": 0,
                "index_total": int(getattr(self._index, "ntotal", 0) if self._index is not None else 0),
            }
        finally:
            if own_session and session is not None:
                session.close()

    def _ensure_index(self, db: Session) -> bool:
        if not self.client:
            return False

        current_stats = self._chunk_stats(db)
        if current_stats.get("count", 0) <= 0:
            return False

        if (
            self._index is not None
            and self._meta_records
            and self._meta_stats
            and self._meta_matches_stats({"version": self.meta_version, "stats": self._meta_stats}, current_stats, self.meta_version)
        ):
            return True

        if os.path.exists(self.index_path) and os.path.exists(self.meta_path):
            meta = self._load_meta()
            if self._meta_matches_stats(meta, current_stats, self.meta_version):
                try:
                    index = self._safe_load_faiss_index(self.index_path)
                    records = list(meta.get("records", []))
                    if index is not None and records:
                        self._index = index
                        self._meta_records = records
                        self._meta_stats = current_stats
                        self._faiss_id_to_meta = {int(x["faiss_id"]): x for x in records}
                        return True
                except Exception:
                    pass

        if not self.auto_rebuild:
            return False
        return self._rebuild_index(db)

    @staticmethod
    def _compose_query_text(user_query: str, keywords: List[str]) -> str:
        seen = set()
        parts = []
        for raw in [user_query] + list(keywords or [])[:12]:
            value = str(raw or "").strip()
            if not value or value in seen:
                continue
            seen.add(value)
            parts.append(value)
        return " ".join(parts).strip()

    def _vector_search_chunk_ids(self, db: Session, query_text: str, top_k: int) -> List[Tuple[int, float]]:
        if not query_text:
            return []
        if not self._ensure_index(db):
            return []

        query_vec = self._build_embeddings([query_text])
        if query_vec.size == 0:
            return []
        faiss.normalize_L2(query_vec)

        k = max(top_k * self.search_expand_factor, top_k + 10)
        scores, ids = self._index.search(query_vec, k)

        dedup: Dict[int, float] = {}
        for score, faiss_id in zip(scores[0], ids[0]):
            if int(faiss_id) < 0:
                continue
            meta = self._faiss_id_to_meta.get(int(faiss_id))
            if not meta:
                continue
            chunk_id = int(meta.get("chunk_id") or 0)
            if chunk_id <= 0:
                continue
            score_val = float(score)
            if score_val < self.min_score:
                continue
            if (chunk_id not in dedup) or (score_val > dedup[chunk_id]):
                dedup[chunk_id] = score_val

        ranked = sorted(dedup.items(), key=lambda x: x[1], reverse=True)
        return ranked[:top_k]

    def _fetch_chunk_rows(self, db: Session, chunk_ids: List[int]) -> Dict[int, Tuple[KBDocument, KBChunk]]:
        if not chunk_ids:
            return {}
        rows = (
            db.query(KBDocument, KBChunk)
            .join(KBChunk, KBDocument.id == KBChunk.document_id)
            .filter(KBChunk.id.in_(chunk_ids))
            .all()
        )
        return {int(chunk.id): (doc, chunk) for doc, chunk in rows}

    def _build_siblings_map(self, db: Session, doc_ids: List[int]) -> Dict[int, List[KBChunk]]:
        if not doc_ids:
            return {}
        rows = (
            db.query(KBChunk)
            .filter(KBChunk.document_id.in_(doc_ids))
            .order_by(KBChunk.chunk_no.asc())
            .all()
        )
        siblings_map: Dict[int, List[KBChunk]] = {}
        for row in rows:
            siblings_map.setdefault(int(row.document_id), []).append(row)
        return siblings_map

    def _to_item(
        self,
        doc: KBDocument,
        chunk: KBChunk,
        effective_content: str,
        score: float,
        stage: str,
        sibling_chunks: List[KBChunk] | None = None,
    ) -> Dict:
        section_no = str(chunk.section_no or "").strip()
        section_title = str(chunk.section_title or "").strip()
        subsection_no = str(chunk.subsection_no or "").strip()
        subsection_title = str(chunk.subsection_title or "").strip()
        parent_section = self._infer_parent_section(chunk, sibling_chunks or [])
        title_parts = [p for p in [section_no, section_title, subsection_no, subsection_title] if p]
        item_name = " ".join(title_parts).strip() or f"{doc.title} chunk-{chunk.chunk_no}"
        ref = f"kb://document/{doc.id}/chunk/{chunk.id}"
        if chunk.page_no:
            ref += f"?page={int(chunk.page_no)}"

        content = self._clean_text(effective_content)
        rewrite = f"{item_name}主要说明：{content}" if content else ""

        return {
            "knowledge_id": int(chunk.id),
            "name": item_name,
            "description": self._trim_text(content, 180),
            "golden_quote": content,
            "original_text": content,
            "domain": str(doc.doc_type or ""),
            "source_doc_id": int(doc.id),
            "source_title": doc.title or "",
            "source_file_name": "",
            "source_heading": doc.title or "",
            "source_ref": ref,
            "source_excerpt": self._trim_text(content, 320),
            "rewrite": rewrite,
            "section_no": section_no,
            "section_title": section_title,
            "subsection_no": subsection_no,
            "subsection_title": subsection_title,
            "page_no": chunk.page_no,
            "parent_section": parent_section,
            "vector_score": float(score),
            "hybrid_score": float(score),
            "retrieval_stage": stage,
        }

    def _expand_parent_sections(
        self,
        db: Session,
        items: List[Dict],
        siblings_map: Dict[int, List[KBChunk]],
        top_k: int,
    ) -> List[Dict]:
        if not items:
            return items

        existing_ids = {int(x.get("knowledge_id") or 0) for x in items}
        parent_needs: List[Tuple[int, str, float]] = []
        for item in items:
            doc_id = int(item.get("source_doc_id") or 0)
            parent = str(item.get("parent_section") or "").strip()
            if doc_id > 0 and parent:
                parent_needs.append((doc_id, parent, float(item.get("vector_score", 0.0))))

        if not parent_needs:
            return items

        for doc_id, parent_section, base_score in parent_needs:
            row = (
                db.query(KBDocument, KBChunk)
                .join(KBChunk, KBDocument.id == KBChunk.document_id)
                .filter(KBDocument.id == doc_id, KBChunk.section_no == parent_section)
                .first()
            )
            if not row:
                continue
            doc, chunk = row
            if int(chunk.id) in existing_ids:
                continue

            effective_content = self._build_effective_chunk_content(chunk, siblings_map.get(doc_id, []))
            if self._is_empty_chunk_content(effective_content):
                continue

            items.append(
                self._to_item(
                    doc=doc,
                    chunk=chunk,
                    effective_content=effective_content,
                    score=max(base_score * 0.9, self.min_score),
                    stage="structure-parent",
                    sibling_chunks=siblings_map.get(doc_id, []),
                )
            )
            existing_ids.add(int(chunk.id))
            if len(items) >= max(top_k * 2, top_k):
                break
        return items

    @classmethod
    def _extract_section_refs(cls, text: str) -> List[str]:
        refs: List[str] = []
        seen = set()
        raw = str(text or "")
        for match in cls.SECTION_REF_RE.finditer(raw):
            ref = re.sub(r"\s+", "", str(match.group(0) or "")).strip()
            if len(ref) < 3:
                continue
            if ref in seen:
                continue
            seen.add(ref)
            refs.append(ref)
        return refs

    @classmethod
    def _section_index_to_int(cls, raw_value: str) -> int:
        token = re.sub(r"\s+", "", str(raw_value or "")).strip()
        if not token:
            return 0
        if token.isdigit():
            return int(token)

        total = 0
        section = 0
        number = 0
        has_value = False
        for char in token:
            if char in cls.CN_NUM_MAP:
                number = cls.CN_NUM_MAP[char]
                has_value = True
                continue
            unit = cls.CN_UNIT_MAP.get(char)
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
    def _int_to_cn_index(number: int) -> str:
        if number <= 0:
            return ""
        digits = "零一二三四五六七八九"
        units = ["", "十", "百", "千"]
        raw = []
        n = int(number)
        pos = 0
        while n > 0:
            n, rem = divmod(n, 10)
            if rem == 0:
                raw.append("零")
            else:
                raw.append(f"{digits[rem]}{units[pos]}")
            pos += 1
        text = "".join(reversed(raw))
        text = re.sub(r"零+", "零", text).strip("零")
        text = text.replace("一十", "十", 1)
        return text

    @classmethod
    def _expand_section_ref_equivalents(cls, raw_ref: str) -> List[str]:
        text = re.sub(r"\s+", "", str(raw_ref or "")).strip()
        if not text:
            return []

        variants: List[str] = []
        seen = set()

        def put(value: str):
            v = str(value or "").strip()
            if not v or v in seen:
                return
            seen.add(v)
            variants.append(v)

        put(text)
        match = cls.SECTION_INDEX_RE.match(text)
        if not match:
            return variants

        index_raw = str(match.group(1) or "").strip()
        suffix = str(match.group(2) or "").strip()
        index_val = cls._section_index_to_int(index_raw)
        if index_val <= 0:
            return variants

        put(f"第{index_val}{suffix}")
        index_cn = cls._int_to_cn_index(index_val)
        if index_cn:
            put(f"第{index_cn}{suffix}")
        return variants

    @classmethod
    def _section_ref_variants(cls, raw_value: str) -> List[str]:
        text = re.sub(r"\s+", "", str(raw_value or "")).strip()
        if not text:
            return []

        variants: List[str] = []
        seen = set()

        def put(value: str):
            for item in cls._expand_section_ref_equivalents(value):
                if item in seen:
                    continue
                seen.add(item)
                variants.append(item)

        put(text)
        for sep in (".p", ".s", ".h", "~"):
            if sep in text:
                put(text.split(sep, 1)[0])
        return variants

    @classmethod
    def _query_terms(cls, user_query: str, keywords: List[str]) -> List[str]:
        terms: List[str] = []
        seen = set()

        def put(raw_value: str):
            token = cls._normalize_cmp_text(raw_value)
            if len(token) < 2:
                return
            if token in cls.LEXICAL_STOP_TERMS:
                return
            if token in seen:
                return
            seen.add(token)
            terms.append(token)

        for seed in [user_query] + list(keywords or [])[:16]:
            raw = str(seed or "").strip()
            if not raw:
                continue
            put(raw)
            for block in re.findall(r"[\u4e00-\u9fff]{2,14}|[a-z0-9_]{3,24}", raw.lower()):
                put(block)
            for ref in cls._extract_section_refs(raw):
                put(ref)
        return terms[:40]

    @classmethod
    def _extract_role_anchors(cls, user_query: str, keywords: List[str]) -> List[str]:
        anchors: List[str] = []
        seen = set()
        seeds = [str(user_query or "").strip(), *[str(x or "").strip() for x in (keywords or [])[:8]]]

        def put(raw_value: str):
            token = cls._normalize_cmp_text(raw_value)
            if len(token) < 2:
                return
            if token in seen:
                return
            seen.add(token)
            anchors.append(token)

        for seed in seeds:
            if not seed:
                continue
            for role in cls.ROLE_TERMS:
                if role in seed:
                    put(role)
            for block in cls.ROLE_ANCHOR_RE.findall(seed):
                put(block)
        # Prefer more specific anchors (e.g. "业务及职能部门" over "职能部门"/"部门").
        kept: List[str] = []
        for token in sorted(anchors, key=len, reverse=True):
            if any(token in existed for existed in kept):
                continue
            kept.append(token)
        return kept[:8]

    @classmethod
    def _is_duty_or_principle_query(cls, user_query: str, keywords: List[str]) -> bool:
        seeds = [str(user_query or "")]
        seeds.extend(str(x or "") for x in (keywords or [])[:12])
        merged = cls._normalize_cmp_text(" ".join(seeds))
        if not merged:
            return False
        for term in cls.DUTY_PRINCIPLE_TERMS:
            token = cls._normalize_cmp_text(term)
            if token and token in merged:
                return True
        return False

    @classmethod
    def _role_relevance_score(cls, item: Dict, role_anchors: List[str], duty_query: bool = False) -> float:
        if not role_anchors:
            return 0.0

        joined_raw = cls._clean_text(
            " ".join(
                [
                    str(item.get("name") or ""),
                    str(item.get("section_no") or ""),
                    str(item.get("section_title") or ""),
                    str(item.get("original_text") or ""),
                ]
            )
        )
        joined_cmp = cls._normalize_cmp_text(joined_raw)
        if not joined_cmp:
            return 0.0

        object_cues = [cls._normalize_cmp_text(x) for x in cls.OBJECT_ROLE_CUES]
        duty_pattern_tokens = (
            "\u8d1f\u8d23",
            "\u5c65\u884c",
            "\u4e3b\u8981\u5c65\u884c",
            "\u4e3b\u8981\u804c\u8d23",
            "\u804c\u8d23",
            "\u539f\u5219",
            "\u9075\u5faa",
            "\u627f\u62c5",
        )

        score = 0.0
        anchor_hit = False
        duty_patterns = ("负责", "履行", "主要履行", "主要职责")
        for anchor in role_anchors:
            token = cls._normalize_cmp_text(anchor)
            if len(token) < 2:
                continue
            pos = joined_cmp.find(token)
            if pos < 0:
                continue
            anchor_hit = True
            if pos <= 20:
                score += 0.45
            elif pos <= 80:
                score += 0.22
            else:
                score += 0.06

            subject_hit = False
            for duty in duty_pattern_tokens:
                if re.search(re.escape(token) + r".{0,12}" + duty, joined_cmp):
                    score += 0.55
                    subject_hit = True
                    break

            object_like = any(re.search(cue + r".{0,10}" + re.escape(token), joined_cmp) for cue in object_cues)
            if object_like and not subject_hit:
                score -= 0.55
            elif duty_query and not subject_hit:
                score -= 0.18

        if not anchor_hit:
            score -= 0.50 if duty_query else 0.30

        # Penalize when leading role subject is clearly another role.
        lead_role = ""
        for role in sorted(cls.ROLE_TERMS, key=len, reverse=True):
            role_token = cls._normalize_cmp_text(role)
            idx = joined_cmp.find(role_token)
            if 0 <= idx <= 16:
                lead_role = role_token
                break

        if lead_role:
            if any(lead_role == cls._normalize_cmp_text(anchor) for anchor in role_anchors):
                score += 0.35
            else:
                score -= 0.45

        return score

    @classmethod
    def _lexical_score_item(cls, item: Dict, terms: List[str]) -> float:
        if not terms:
            return 0.0
        section_text = cls._normalize_cmp_text(
            " ".join(
                [
                    str(item.get("section_no") or ""),
                    str(item.get("section_title") or ""),
                    str(item.get("subsection_no") or ""),
                    str(item.get("subsection_title") or ""),
                    str(item.get("name") or ""),
                ]
            )
        )
        body_text = cls._normalize_cmp_text(
            " ".join(
                [
                    str(item.get("original_text") or ""),
                    str(item.get("description") or ""),
                    str(item.get("source_title") or ""),
                ]
            )
        )
        score = 0.0
        for term in terms:
            if len(term) < 2:
                continue
            if term in section_text:
                score += 3.2 + min(len(term), 4) * 0.15
            if term in body_text:
                score += 1.8 + min(len(term), 5) * 0.1
        return score

    def _rerank_items(self, items: List[Dict], user_query: str, keywords: List[str], top_k: int) -> List[Dict]:
        if not items:
            return items
        terms = self._query_terms(user_query, keywords)
        if not terms:
            return sorted(items, key=lambda x: float(x.get("vector_score", 0.0)), reverse=True)[:top_k]

        section_refs = self._extract_section_refs(user_query)
        expanded_section_refs = set()
        for ref in section_refs:
            for item in self._section_ref_variants(ref):
                expanded_section_refs.add(item)
        role_anchors = self._extract_role_anchors(user_query, keywords)
        duty_query = self._is_duty_or_principle_query(user_query, keywords)
        lexical_scores = [self._lexical_score_item(item, terms) for item in items]
        max_lex = max(lexical_scores) if lexical_scores else 0.0
        ranked: List[Dict] = []
        for item, lex in zip(items, lexical_scores):
            lex_norm = (lex / max_lex) if max_lex > 0 else 0.0
            hybrid = float(item.get("vector_score", 0.0)) + self.lexical_weight * lex_norm
            sec_variants = self._section_ref_variants(item.get("section_no") or "")
            subsection_variants = self._section_ref_variants(item.get("subsection_no") or "")
            parent_variants = self._section_ref_variants(item.get("parent_section") or "")
            if expanded_section_refs and any(ref in subsection_variants for ref in expanded_section_refs):
                hybrid += 0.5
            elif expanded_section_refs and any(ref in sec_variants for ref in expanded_section_refs):
                hybrid += 0.45
            elif expanded_section_refs and any(ref in parent_variants for ref in expanded_section_refs):
                hybrid += 0.30

            if role_anchors:
                role_score = self._role_relevance_score(item, role_anchors, duty_query=duty_query)
                if duty_query and len(role_anchors) == 1:
                    role_score *= 1.2
                hybrid += role_score

            enriched = dict(item)
            enriched["hybrid_score"] = hybrid
            ranked.append(enriched)

        ranked.sort(key=lambda x: float(x.get("hybrid_score", x.get("vector_score", 0.0))), reverse=True)
        return ranked[: max(top_k, 1)]

    @staticmethod
    def _dedupe_items(items: List[Dict], top_k: int, score_key: str = "vector_score") -> List[Dict]:
        dedup: Dict[int, Dict] = {}
        for item in items:
            kid = int(item.get("knowledge_id") or 0)
            if kid <= 0:
                continue
            cur_score = float(item.get(score_key, item.get("vector_score", 0.0)))
            old = dedup.get(kid)
            old_score = float(old.get(score_key, old.get("vector_score", 0.0))) if old else -1e9
            if old is None or cur_score > old_score:
                dedup[kid] = item
        ranked = sorted(
            dedup.values(),
            key=lambda x: float(x.get(score_key, x.get("vector_score", 0.0))),
            reverse=True,
        )
        return ranked[:top_k]

    def search(self, db: Session, user_query: str, keywords: List[str], top_k: int = 12) -> List[Dict]:
        if not self.client:
            return []

        use_top_k = top_k or self.top_k
        query_text = self._compose_query_text(user_query, keywords)
        candidate_k = max(use_top_k * self.rerank_candidate_factor, use_top_k + 20)
        chunk_hits = self._vector_search_chunk_ids(db, query_text, candidate_k)
        if not chunk_hits:
            return []

        chunk_ids = [cid for cid, _ in chunk_hits]
        chunk_map = self._fetch_chunk_rows(db, chunk_ids)
        if not chunk_map:
            return []

        doc_ids = sorted({int(row[0].id) for row in chunk_map.values()})
        siblings_map = self._build_siblings_map(db, doc_ids)

        items: List[Dict] = []
        for chunk_id, score in chunk_hits:
            row = chunk_map.get(int(chunk_id))
            if not row:
                continue
            doc, chunk = row
            effective_content = self._build_effective_chunk_content(chunk, siblings_map.get(int(doc.id), []))
            if self._is_empty_chunk_content(effective_content):
                continue
            items.append(
                self._to_item(
                    doc=doc,
                    chunk=chunk,
                    effective_content=effective_content,
                    score=float(score),
                    stage="vector",
                    sibling_chunks=siblings_map.get(int(doc.id), []),
                )
            )

        items = self._expand_parent_sections(db, items, siblings_map, use_top_k)
        items = self._rerank_items(items, user_query, keywords, use_top_k)
        return self._dedupe_items(items, use_top_k, score_key="hybrid_score")
