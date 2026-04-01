import logging
import os
import re
import sys
from typing import Dict, List, Optional, Tuple

from sqlalchemy import inspect, or_
from sqlalchemy.orm import Session

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from core import (  # noqa: E402
    BusinessClauseMap,
    BusinessType,
    KBChunk,
    KBDocument,
    PolicyClause,
    PolicyDocument,
    ProcedureStep,
    RequiredMaterial,
    SessionLocal,
)
from modules.evaluator.vector_kb_retriever import VectorKBRetriever  # noqa: E402

logger = logging.getLogger(__name__)


class PolicyRetriever:
    STRUCTURED_TABLES = {
        "biz_business_type",
        "biz_business_clause_map",
        "biz_policy_clause",
        "biz_policy_document",
        "biz_procedure_step",
        "biz_required_material",
    }
    ACTIVE_DOC_STATUS = {"有效", "active", "ACTIVE"}
    CHAPTER_REF_RE = re.compile(r"第\s*([一二三四五六七八九十百千万零〇两\d]+)\s*章")
    ARTICLE_REF_RE = re.compile(r"第\s*([一二三四五六七八九十百千万零〇两\d]+)\s*条")
    CHAPTER_OVERVIEW_HINTS = (
        "总则",
        "都有什么",
        "有哪些",
        "主要内容",
        "内容",
        "概述",
        "介绍",
        "讲解",
        "讲讲",
        "说明",
        "体系",
        "章节",
    )
    CHAPTER_EXCLUDE_HINTS = ("流程", "步骤", "材料", "审批", "报批", "提交", "办理", "时限")
    DOC_SUFFIX_PATTERNS = (
        "集团股份有限公司",
        "股份有限公司",
        "有限责任公司",
        "集团有限公司",
        "有限公司",
        "集团公司",
        "集团",
    )
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
        self.vector_kb = VectorKBRetriever()

    @staticmethod
    def _normalize_text(text: str) -> str:
        return re.sub(r"\s+", " ", str(text or "")).strip()

    @staticmethod
    def _normalize_match_text(text: str) -> str:
        return re.sub(r"[^\u4e00-\u9fffa-z0-9_]+", "", str(text or "").lower())

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

    @classmethod
    def _chapter_title_body(cls, section_title: str) -> str:
        text = cls._normalize_text(section_title)
        if not text:
            return ""
        return re.sub(r"^第\s*[一二三四五六七八九十百千万零〇两\d]+\s*章\s*", "", text).strip()

    @classmethod
    def _build_doc_aliases(cls, title: str) -> List[str]:
        raw_title = cls._normalize_text(title)
        if not raw_title:
            return []
        base = re.split(r"[-—－_]", raw_title, maxsplit=1)[0].strip()
        base_norm = cls._normalize_match_text(base)
        aliases = set()
        if len(base_norm) >= 2:
            aliases.add(base_norm)

        reduced = base_norm
        for suffix in cls.DOC_SUFFIX_PATTERNS:
            suffix_norm = cls._normalize_match_text(suffix)
            if not suffix_norm:
                continue
            while reduced.endswith(suffix_norm) and len(reduced) > len(suffix_norm):
                reduced = reduced[: -len(suffix_norm)]
                if len(reduced) >= 2:
                    aliases.add(reduced)

        compact = reduced
        for token in ("科技", "股份", "集团", "公司"):
            compact = compact.replace(cls._normalize_match_text(token), "")
        if len(compact) >= 2:
            aliases.add(compact)

        for alias in list(aliases):
            if len(alias) >= 4:
                aliases.add(alias[:4])
        return sorted([x for x in aliases if len(x) >= 2], key=len, reverse=True)

    def _is_chapter_overview_query(self, query_mode: str, query_text: str, keywords: List[str]) -> bool:
        text = self._normalize_text(query_text)
        if not text:
            return False
        if self.ARTICLE_REF_RE.search(text):
            return False
        if self.CHAPTER_REF_RE.search(text):
            return True

        has_overview_hint = any(word in text for word in self.CHAPTER_OVERVIEW_HINTS)
        if not has_overview_hint:
            return False

        merged_kw = " ".join([self._normalize_text(x) for x in keywords[:12]])
        has_section_target = any(word in text for word in ("章", "总则", "体系")) or any(
            word in merged_kw for word in ("总则", "体系", "章节")
        )
        if not has_section_target:
            return False

        if any(word in text for word in self.CHAPTER_EXCLUDE_HINTS):
            return False

        mode = str(query_mode or "").strip().lower()
        if mode == "knowledge_explain":
            return True
        return len(text) <= 48

    def _pick_target_document(self, db: Session, query_text: str, keywords: List[str]) -> Optional[KBDocument]:
        query_cmp = self._normalize_match_text(query_text)
        if not query_cmp:
            return None

        docs = (
            db.query(KBDocument)
            .filter(or_(KBDocument.status.is_(None), KBDocument.status.in_(list(self.ACTIVE_DOC_STATUS))))
            .order_by(KBDocument.id.asc())
            .all()
        )
        if not docs:
            return None

        keyword_tokens = [self._normalize_match_text(x) for x in [query_text] + list(keywords or [])[:16]]
        keyword_tokens = [x for x in keyword_tokens if len(x) >= 2]

        scored: List[Tuple[int, KBDocument]] = []
        for doc in docs:
            doc_norm = self._normalize_match_text(doc.title)
            alias_score = 0
            for alias in self._build_doc_aliases(doc.title):
                if alias and alias in query_cmp:
                    alias_score = max(alias_score, 20 + len(alias))
            if alias_score <= 0:
                continue

            score = alias_score
            for token in keyword_tokens:
                if token in doc_norm:
                    score += min(len(token), 8)
            scored.append((score, doc))

        if not scored:
            return None
        scored.sort(key=lambda x: (x[0], x[1].id), reverse=True)
        if len(scored) >= 2 and scored[0][0] <= scored[1][0] + 2:
            return None
        return scored[0][1]

    def _pick_target_section(self, db: Session, doc_id: int, query_text: str, keywords: List[str]) -> Optional[Tuple[str, str]]:
        rows = (
            db.query(KBChunk)
            .filter(KBChunk.document_id == doc_id)
            .order_by(KBChunk.chunk_no.asc())
            .all()
        )
        if not rows:
            return None

        section_map: Dict[str, str] = {}
        for row in rows:
            sec_no = str(row.section_no or "").strip()
            if not sec_no.isdigit():
                continue
            if sec_no not in section_map:
                section_map[sec_no] = str(row.section_title or "").strip()
        if not section_map:
            return None

        m = self.CHAPTER_REF_RE.search(query_text or "")
        if m:
            ref_no = self._cn_to_int(m.group(1))
            if ref_no > 0 and str(ref_no) in section_map:
                return str(ref_no), section_map[str(ref_no)]

        query_cmp = self._normalize_match_text(query_text)
        kw_terms = [self._normalize_match_text(x) for x in [query_text] + list(keywords or [])[:18]]
        kw_terms = [x for x in kw_terms if len(x) >= 2]

        scored_sections: List[Tuple[int, str, str]] = []
        for sec_no, sec_title in section_map.items():
            title_body = self._chapter_title_body(sec_title)
            title_body_cmp = self._normalize_match_text(title_body)
            if not title_body_cmp:
                continue

            score = 0
            if title_body_cmp in query_cmp:
                score += 30 + min(len(title_body_cmp), 12)
            for token in kw_terms:
                if token in title_body_cmp:
                    score += min(len(token), 6)
            if score > 0:
                scored_sections.append((score, sec_no, sec_title))

        if not scored_sections:
            return None
        scored_sections.sort(key=lambda x: (x[0], int(x[1])), reverse=True)
        if len(scored_sections) >= 2 and scored_sections[0][0] <= scored_sections[1][0] + 1:
            return None
        return scored_sections[0][1], scored_sections[0][2]

    def _kb_chunk_to_knowledge_item(self, doc: KBDocument, chunk: KBChunk) -> Optional[Dict]:
        content = self._normalize_text(chunk.content)
        if not content:
            return None
        ref = f"kb://document/{doc.id}/chunk/{chunk.id}"
        if chunk.page_no:
            ref += f"?page={int(chunk.page_no)}"
        title_parts = [
            str(chunk.section_no or "").strip(),
            str(chunk.section_title or "").strip(),
            str(chunk.subsection_no or "").strip(),
            str(chunk.subsection_title or "").strip(),
        ]
        item_name = " ".join([x for x in title_parts if x]).strip() or f"{doc.title} chunk-{chunk.chunk_no}"
        return {
            "knowledge_id": int(chunk.id),
            "name": item_name,
            "description": content[:180] + ("..." if len(content) > 180 else ""),
            "golden_quote": content,
            "original_text": content,
            "domain": str(doc.doc_type or ""),
            "source_doc_id": int(doc.id),
            "source_title": doc.title or "",
            "source_file_name": "",
            "source_heading": doc.title or "",
            "source_ref": ref,
            "source_excerpt": content[:320] + ("..." if len(content) > 320 else ""),
            "rewrite": content,
            "section_no": str(chunk.section_no or "").strip(),
            "section_title": str(chunk.section_title or "").strip(),
            "subsection_no": str(chunk.subsection_no or "").strip(),
            "subsection_title": str(chunk.subsection_title or "").strip(),
            "parent_section": str(chunk.parent_section or "").strip(),
            "page_no": chunk.page_no,
        }

    def _chapter_full_knowledge(
        self,
        db: Session,
        query_mode: str,
        query_text: str,
        keywords: List[str],
    ) -> Optional[Dict]:
        if not self._is_chapter_overview_query(query_mode, query_text, keywords):
            return None

        doc = self._pick_target_document(db, query_text, keywords)
        if not doc:
            return None

        section_match = self._pick_target_section(db, int(doc.id), query_text, keywords)
        if not section_match:
            return None
        section_no, section_title = section_match

        rows = (
            db.query(KBChunk)
            .filter(KBChunk.document_id == int(doc.id), KBChunk.section_no == str(section_no))
            .order_by(KBChunk.chunk_no.asc())
            .all()
        )
        if not rows:
            return None

        items: List[Dict] = []
        for chunk in rows:
            item = self._kb_chunk_to_knowledge_item(doc, chunk)
            if item:
                items.append(item)
        if not items:
            return None
        return {
            "items": items,
            "scope": f"chapter_full:doc={int(doc.id)}:section={section_no}",
            "section_title": section_title,
        }

    @classmethod
    def _tokenize(cls, text: str) -> List[str]:
        raw = cls._normalize_text(text)
        if not raw:
            return []
        tokens: List[str] = []
        seen = set()
        for part in re.findall(r"[\u4e00-\u9fff]{2,14}|[a-z0-9_]{3,24}", raw.lower()):
            token = cls._normalize_match_text(part)
            if len(token) < 2 or token in seen:
                continue
            seen.add(token)
            tokens.append(token)
        return tokens

    def _extract_keywords(self, intent_data: Dict) -> List[str]:
        seeds = [
            str(intent_data.get("user_event") or ""),
            str(intent_data.get("event_summary") or ""),
            str(intent_data.get("llm_rewritten_question") or ""),
            str(intent_data.get("query_text") or ""),
        ]
        for key in ["keywords", "entities", "risk_points"]:
            value = intent_data.get(key, [])
            if isinstance(value, list):
                seeds.extend([str(x or "") for x in value[:20]])
        out: List[str] = []
        seen = set()
        for seed in seeds:
            for token in self._tokenize(seed):
                if token in seen:
                    continue
                seen.add(token)
                out.append(token)
        return out[:30]

    def _resolve_business_types(self, db: Session, keywords: List[str], limit: int = 8) -> List[BusinessType]:
        if not keywords:
            return []
        rows = db.query(BusinessType).filter(BusinessType.status == 1).all()
        scored = []
        for row in rows:
            name = self._normalize_match_text(row.type_name)
            desc = self._normalize_match_text(row.description)
            score = 0
            for kw in keywords:
                if kw in name:
                    score += 8 + min(len(kw), 4)
                if kw and kw in desc:
                    score += 3
            if score > 0:
                scored.append((score, row))
        scored.sort(key=lambda x: (x[0], x[1].id), reverse=True)
        return [row for _, row in scored[:limit]]

    def _query_clauses(self, db: Session, biz_ids: List[int]) -> List[Dict]:
        if not biz_ids:
            return []
        rows = (
            db.query(BusinessClauseMap, PolicyClause, PolicyDocument)
            .join(PolicyClause, BusinessClauseMap.clause_id == PolicyClause.id)
            .join(PolicyDocument, PolicyClause.policy_doc_id == PolicyDocument.id)
            .filter(BusinessClauseMap.business_type_id.in_(biz_ids), BusinessClauseMap.status == 1, PolicyClause.status == 1)
            .all()
        )
        out = []
        for m, c, d in rows:
            out.append(
                {
                    "business_type_id": m.business_type_id,
                    "business_type_code": "",
                    "business_type_name": "",
                    "clause_id": c.id,
                    "clause_no": c.clause_no or "",
                    "clause_title": c.clause_title or "",
                    "clause_text": c.clause_text or "",
                    "mandatory_level": int(m.mandatory_level or 1),
                    "relevance_weight": float(m.relevance_weight or 1.0),
                    "policy_doc_id": d.id,
                    "policy_doc_code": d.doc_code or "",
                    "policy_doc_name": d.doc_name or "",
                    "policy_doc_version": d.version_no or "",
                    "policy_doc_category": d.doc_category or "",
                    "trace_link": f"policy://biz_policy_clause/{c.id}",
                    "page_no": c.page_no,
                    "parent_section": "",
                    "subsection_no": "",
                    "subsection_title": "",
                }
            )
        return out

    def _query_procedures(self, db: Session, biz_ids: List[int]) -> List[Dict]:
        if not biz_ids:
            return []
        rows = db.query(ProcedureStep).filter(ProcedureStep.business_type_id.in_(biz_ids), ProcedureStep.status == 1).all()
        out = []
        for row in rows:
            out.append(
                {
                    "step_id": row.id,
                    "business_type_id": row.business_type_id,
                    "step_no": row.step_no,
                    "step_name": row.step_name or "",
                    "step_desc": row.step_desc or "",
                    "responsible_role": row.responsible_role or "",
                    "due_rule": row.due_rule or "",
                    "output_deliverable": row.output_deliverable or "",
                    "clause_no": "",
                    "policy_doc_name": "",
                    "trace_link": f"policy://biz_procedure_step/{row.id}",
                }
            )
        return out

    def _query_materials(self, db: Session, biz_ids: List[int]) -> List[Dict]:
        if not biz_ids:
            return []
        rows = db.query(RequiredMaterial).filter(RequiredMaterial.business_type_id.in_(biz_ids), RequiredMaterial.status == 1).all()
        out = []
        for row in rows:
            out.append(
                {
                    "material_id": row.id,
                    "business_type_id": row.business_type_id,
                    "material_code": row.material_code or "",
                    "material_name": row.material_name or "",
                    "required_level": int(row.required_level or 1),
                    "format_rule": row.format_rule or "",
                    "validator_rule": row.validator_rule or "",
                    "clause_no": "",
                    "policy_doc_name": "",
                    "trace_link": f"policy://biz_required_material/{row.id}",
                }
            )
        return out

    def _score_kb_chunk_item(self, doc: KBDocument, chunk: KBChunk, keywords: List[str]) -> int:
        title = self._normalize_match_text(doc.title)
        sec_no = self._normalize_match_text(chunk.section_no)
        sec_title = self._normalize_match_text(chunk.section_title)
        sub_no = self._normalize_match_text(chunk.subsection_no)
        sub_title = self._normalize_match_text(chunk.subsection_title)
        body = self._normalize_match_text(chunk.content)
        if not body:
            return 0

        score = 0
        for kw in keywords:
            if kw in title:
                score += 12
            if kw in sec_no:
                score += 8
            if kw in sec_title:
                score += 10
            if kw in sub_no:
                score += 9
            if kw in sub_title:
                score += 11
            if kw in body:
                score += 14
        if len(chunk.content or "") <= 10:
            score -= 6
        return max(score, 0)

    def _fallback_kb_chunk_knowledge(self, db: Session, keywords: List[str], top_n: int = 12) -> List[Dict]:
        rows = (
            db.query(KBDocument, KBChunk)
            .join(KBChunk, KBDocument.id == KBChunk.document_id)
            .filter(or_(KBDocument.status.is_(None), KBDocument.status.in_(list(self.ACTIVE_DOC_STATUS))))
            .order_by(KBDocument.id.asc(), KBChunk.chunk_no.asc())
            .all()
        )

        scored = []
        for doc, chunk in rows:
            score = self._score_kb_chunk_item(doc, chunk, keywords)
            if score > 0:
                scored.append((score, doc, chunk))
        scored.sort(key=lambda x: (x[0], x[2].chunk_no), reverse=True)

        out: List[Dict] = []
        for _, doc, chunk in scored[:top_n]:
            item = self._kb_chunk_to_knowledge_item(doc, chunk)
            if item:
                out.append(item)
        return out

    @staticmethod
    def _kb_hits_to_clauses(kb_hits: List[Dict]) -> List[Dict]:
        clauses: List[Dict] = []
        for item in kb_hits:
            clauses.append(
                {
                    "business_type_id": None,
                    "business_type_code": "",
                    "business_type_name": "",
                    "clause_id": item.get("knowledge_id"),
                    "clause_no": item.get("subsection_no") or item.get("section_no") or "",
                    "clause_title": item.get("subsection_title") or item.get("section_title") or item.get("name", ""),
                    "clause_text": item.get("original_text", ""),
                    "mandatory_level": 1,
                    "relevance_weight": 1.0,
                    "policy_doc_id": item.get("source_doc_id"),
                    "policy_doc_code": "",
                    "policy_doc_name": item.get("source_heading") or item.get("source_title") or "",
                    "policy_doc_version": "",
                    "policy_doc_category": "",
                    "trace_link": item.get("source_ref", ""),
                    "page_no": item.get("page_no"),
                    "parent_section": item.get("parent_section", ""),
                    "subsection_no": item.get("subsection_no", ""),
                    "subsection_title": item.get("subsection_title", ""),
                }
            )
        return clauses

    @staticmethod
    def _guess_query_text(intent_data: Dict) -> str:
        for key in ["llm_rewritten_question", "query_text", "event_summary", "user_event"]:
            value = str(intent_data.get(key) or "").strip()
            if value:
                return value
        return ""

    def retrieve(self, intent_data: Dict) -> Dict:
        if not intent_data:
            return {
                "business_types": [],
                "clauses": [],
                "procedures": [],
                "materials": [],
                "fallback_knowledge": [],
                "summary": {"matched_business_types": 0, "clause_count": 0, "procedure_count": 0, "material_count": 0},
            }

        db: Session = SessionLocal()
        try:
            keywords = self._extract_keywords(intent_data)
            query_text = self._guess_query_text(intent_data)
            query_mode = str(intent_data.get("query_mode", "standard")).strip().lower()

            table_names = set(inspect(db.bind).get_table_names())
            has_structured = self.STRUCTURED_TABLES.issubset(table_names)

            business_types: List[BusinessType] = []
            clauses: List[Dict] = []
            procedures: List[Dict] = []
            materials: List[Dict] = []
            if has_structured:
                business_types = self._resolve_business_types(db, keywords)
                biz_ids = [x.id for x in business_types]
                clauses = self._query_clauses(db, biz_ids)
                procedures = self._query_procedures(db, biz_ids)
                materials = self._query_materials(db, biz_ids)

            retrieval_scope = ""
            chapter_full = self._chapter_full_knowledge(
                db=db,
                query_mode=query_mode,
                query_text=query_text,
                keywords=keywords,
            )
            if chapter_full:
                fallback_knowledge = chapter_full["items"]
                retrieval_path = "chapter-full"
                keyword_tier = "chapter-full"
                retrieval_scope = chapter_full["scope"]
            else:
                fallback_knowledge = self.vector_kb.search(db=db, user_query=query_text, keywords=keywords, top_k=14)
                retrieval_path = "vector->relation->structure" if fallback_knowledge else "none"
                keyword_tier = "vector-kb" if fallback_knowledge else "none"

                if not fallback_knowledge:
                    fallback_knowledge = self._fallback_kb_chunk_knowledge(db, keywords, top_n=12)
                    if fallback_knowledge:
                        retrieval_path = "keyword-kb"
                        keyword_tier = "keyword-kb"

            if fallback_knowledge and (not clauses or retrieval_scope.startswith("chapter_full")):
                clauses = self._kb_hits_to_clauses(fallback_knowledge)

            return {
                "business_types": [
                    {"id": bt.id, "type_code": bt.type_code, "type_name": bt.type_name, "description": bt.description or ""}
                    for bt in business_types
                ],
                "clauses": clauses,
                "procedures": procedures,
                "materials": materials,
                "fallback_knowledge": fallback_knowledge,
                "summary": {
                    "query_mode": str(intent_data.get("query_mode", "standard")),
                    "kb_only_mode": not has_structured,
                    "keyword_tier": keyword_tier,
                    "retrieval_path": retrieval_path,
                    "retrieval_scope": retrieval_scope,
                    "vector_hit_count": len(fallback_knowledge),
                    "keyword_count": len(keywords),
                    "matched_business_types": len(business_types),
                    "clause_count": len(clauses),
                    "procedure_count": len(procedures),
                    "material_count": len(materials),
                    "fallback_knowledge_count": len(fallback_knowledge),
                },
            }
        except Exception as e:
            logger.warning("[Retriever] retrieval error: %s", e)
            return {
                "business_types": [],
                "clauses": [],
                "procedures": [],
                "materials": [],
                "fallback_knowledge": [],
                "summary": {"matched_business_types": 0, "clause_count": 0, "procedure_count": 0, "material_count": 0},
                "error": str(e),
            }
        finally:
            db.close()
