import os
import re
import sys
import logging
from typing import Dict, List

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

logger = logging.getLogger(__name__)


class PolicySpiritAnalyzer:
    """
    合规比对分析器。
    - 事项咨询：输出应遵循流程与应提供材料
    - 请示审查：输出缺漏项（缺流程/缺材料/缺授权）
    """

    @staticmethod
    def _normalize_text(text: str) -> str:
        return re.sub(r"\s+", " ", str(text or "")).strip().lower()

    @staticmethod
    def _tokenize_reference(text: str) -> List[str]:
        raw = re.split(r"[，。；、,.;:\s]+", str(text or ""))
        return [x.strip().lower() for x in raw if len(x.strip()) >= 2][:8]

    @staticmethod
    def _is_hit(user_text: str, candidates: List[str]) -> bool:
        if not user_text or not candidates:
            return False
        for c in candidates:
            if c and c in user_text:
                return True
        return False

    def _check_procedures(self, user_text: str, procedures: List[Dict]) -> List[Dict]:
        results = []
        for p in procedures:
            refs = [p.get("step_name", ""), p.get("step_desc", ""), p.get("output_deliverable", "")]
            tokens = []
            for r in refs:
                tokens.extend(self._tokenize_reference(r))
            hit = self._is_hit(user_text, tokens)
            status = "已覆盖" if hit else "待补充"
            results.append(
                {
                    "item_type": "procedure",
                    "item_id": p.get("step_id"),
                    "item_name": p.get("step_name", ""),
                    "required_level": 1,
                    "status": status,
                    "expected": p.get("step_desc", ""),
                    "detected": "文本中已体现相关内容" if hit else "",
                    "trace_link": p.get("trace_link", ""),
                    "clause_no": p.get("clause_no", ""),
                    "policy_doc_name": p.get("policy_doc_name", ""),
                    "suggestion": "" if hit else f"请补充步骤：{p.get('step_name', '')}",
                }
            )
        return results

    def _check_materials(self, user_text: str, materials: List[Dict]) -> List[Dict]:
        results = []
        for m in materials:
            refs = [m.get("material_name", ""), m.get("material_code", ""), m.get("format_rule", "")]
            tokens = []
            for r in refs:
                tokens.extend(self._tokenize_reference(r))
            hit = self._is_hit(user_text, tokens)

            required_level = int(m.get("required_level", 1) or 1)
            status = "已提供" if hit else ("待补充" if required_level in (1, 2) else "可选未提供")
            results.append(
                {
                    "item_type": "material",
                    "item_id": m.get("material_id"),
                    "item_name": m.get("material_name", ""),
                    "required_level": required_level,
                    "status": status,
                    "expected": f"材料代码: {m.get('material_code', '')}; 格式要求: {m.get('format_rule', '')}",
                    "detected": "文本中已体现相关材料" if hit else "",
                    "trace_link": m.get("trace_link", ""),
                    "clause_no": m.get("clause_no", ""),
                    "policy_doc_name": m.get("policy_doc_name", ""),
                    "suggestion": "" if hit else f"请补充材料：{m.get('material_name', '')}",
                }
            )
        return results

    @staticmethod
    def _build_gaps(procedure_checks: List[Dict], material_checks: List[Dict], intent_type: str) -> List[Dict]:
        gaps = []
        need_review = intent_type in {"请示审查", "混合"}
        if not need_review:
            return gaps

        for p in procedure_checks:
            if p["status"] == "待补充":
                gaps.append(
                    {
                        "gap_type": "缺程序",
                        "gap_item": p["item_name"],
                        "expected_req": p["expected"],
                        "detected_content": p["detected"],
                        "severity": 3,
                        "fix_suggestion": p["suggestion"],
                        "trace_link": p["trace_link"],
                        "clause_no": p["clause_no"],
                        "policy_doc_name": p["policy_doc_name"],
                    }
                )

        for m in material_checks:
            if m["status"] == "待补充":
                severity = 3 if m.get("required_level", 1) == 1 else 2
                gaps.append(
                    {
                        "gap_type": "缺材料",
                        "gap_item": m["item_name"],
                        "expected_req": m["expected"],
                        "detected_content": m["detected"],
                        "severity": severity,
                        "fix_suggestion": m["suggestion"],
                        "trace_link": m["trace_link"],
                        "clause_no": m["clause_no"],
                        "policy_doc_name": m["policy_doc_name"],
                    }
                )
        return gaps

    @staticmethod
    def _trim_text(text: str, limit: int = 260) -> str:
        cleaned = re.sub(r"\s+", " ", str(text or "")).strip()
        if len(cleaned) <= limit:
            return cleaned
        return cleaned[:limit] + "..."

    @staticmethod
    def _context_text_limit(text: str) -> int:
        cleaned = re.sub(r"\s+", " ", str(text or "")).strip()
        if not cleaned:
            return 360
        lead = cleaned[:120]
        if lead.endswith(("：", ":")):
            return 1400
        if ("以下" in lead or "如下" in lead) and any(
            token in lead for token in ("原则", "要求", "要点", "步骤", "流程", "清单", "情形")
        ):
            return 1400
        if len(cleaned) <= 520:
            return 520
        return 620

    @staticmethod
    def _normalize_section_fields(section_no: str, section_title: str) -> tuple[str, str]:
        sec_no = re.sub(r"\s+", " ", str(section_no or "")).strip()
        sec_title = re.sub(r"\s+", " ", str(section_title or "")).strip()
        if not sec_no or not sec_title:
            return sec_no, sec_title

        no_compact = re.sub(r"\s+", "", sec_no)
        title_compact = re.sub(r"\s+", "", sec_title)
        if no_compact == title_compact:
            return sec_no, ""
        if title_compact.startswith(no_compact):
            remain = sec_title[len(sec_no):].strip()
            return sec_no, remain
        if no_compact.startswith(title_compact):
            return sec_no, ""
        return sec_no, sec_title

    @staticmethod
    def _build_source_text(
        source_title: str,
        section_no: str,
        section_title: str,
        parent_section: str = "",
        page_no=None,
    ) -> str:
        source = str(source_title or "").strip() or "未知来源"
        sec_no, sec_title = PolicySpiritAnalyzer._normalize_section_fields(section_no, section_title)
        parent = re.sub(r"\s+", "", str(parent_section or "")).strip()
        sec_no_compact = re.sub(r"\s+", "", sec_no)
        sec_title_compact = re.sub(r"\s+", "", sec_title)

        chapter_label = ""
        clause_label = ""
        if parent.endswith("章"):
            chapter_label = parent
        if sec_no_compact.endswith("章"):
            chapter_label = sec_no_compact
        elif sec_title_compact.endswith("章"):
            chapter_label = sec_title

        if sec_title:
            clause_label = sec_title
        elif sec_no_compact.endswith("条"):
            clause_label = sec_no_compact
        elif sec_no:
            clause_label = sec_no

        parts = [source]
        if chapter_label:
            parts.append(f"章节 {chapter_label}")
        if clause_label and clause_label != chapter_label:
            parts.append(f"条款 {clause_label}")
        if page_no:
            parts.append(f"第{page_no}页")
        return "，".join(parts)

    def _build_llm_context(
        self,
        user_event: str,
        fallback_knowledge: List[Dict],
        clauses: List[Dict],
        procedures: List[Dict],
        materials: List[Dict],
        retrieval_scope: str = "",
    ) -> Dict:
        context_items: List[Dict] = []
        chapter_full_mode = str(retrieval_scope or "").startswith("chapter_full")
        fallback_limit = len(fallback_knowledge) if chapter_full_mode else 8

        if fallback_knowledge:
            for item in fallback_knowledge[:fallback_limit]:
                original_text = (
                    item.get("original_text")
                    or item.get("golden_quote")
                    or item.get("description")
                    or ""
                )
                if not str(original_text).strip():
                    continue
                sec_no, sec_title = self._normalize_section_fields(
                    str(item.get("section_no") or ""),
                    str(item.get("section_title") or item.get("name") or ""),
                )
                context_items.append(
                    {
                        "section_no": sec_no,
                        "section_title": sec_title,
                        "subsection_no": str(item.get("subsection_no") or ""),
                        "subsection_title": str(item.get("subsection_title") or ""),
                        "parent_section": str(item.get("parent_section") or ""),
                        "original_text": self._trim_text(original_text, self._context_text_limit(original_text)),
                        "source_title": str(item.get("source_heading") or item.get("source_title") or ""),
                        "source_ref": str(item.get("source_ref") or ""),
                        "page_no": item.get("page_no"),
                    }
                )
        elif clauses:
            for c in clauses[:8]:
                clause_text = str(c.get("clause_text") or "").strip()
                if not clause_text:
                    continue
                sec_no, sec_title = self._normalize_section_fields(
                    str(c.get("clause_no") or ""),
                    str(c.get("clause_title") or ""),
                )
                context_items.append(
                    {
                        "section_no": sec_no,
                        "section_title": sec_title,
                        "subsection_no": str(c.get("subsection_no") or ""),
                        "subsection_title": str(c.get("subsection_title") or ""),
                        "parent_section": str(c.get("parent_section") or ""),
                        "original_text": self._trim_text(clause_text, self._context_text_limit(clause_text)),
                        "source_title": str(c.get("policy_doc_name") or ""),
                        "source_ref": str(c.get("trace_link") or ""),
                        "page_no": c.get("page_no"),
                    }
                )
        else:
            for p in procedures[:6]:
                pieces = [
                    str(p.get("step_name") or "").strip(),
                    str(p.get("step_desc") or "").strip(),
                    str(p.get("output_deliverable") or "").strip(),
                ]
                text = "；".join([x for x in pieces if x])
                if not text:
                    continue
                context_items.append(
                    {
                        "section_no": str(p.get("clause_no") or ""),
                        "section_title": str(p.get("step_name") or ""),
                        "subsection_no": "",
                        "subsection_title": "",
                        "parent_section": "",
                        "original_text": self._trim_text(text, self._context_text_limit(text)),
                        "source_title": str(p.get("policy_doc_name") or ""),
                        "source_ref": str(p.get("trace_link") or ""),
                        "page_no": None,
                    }
                )

            for m in materials[:6]:
                text = "；".join(
                    [
                        f"材料名称：{str(m.get('material_name') or '').strip()}",
                        f"材料编码：{str(m.get('material_code') or '').strip()}",
                        f"格式要求：{str(m.get('format_rule') or '').strip()}",
                    ]
                )
                if not text.strip("； "):
                    continue
                context_items.append(
                    {
                        "section_no": str(m.get("clause_no") or ""),
                        "section_title": str(m.get("material_name") or ""),
                        "subsection_no": "",
                        "subsection_title": "",
                        "parent_section": "",
                        "original_text": self._trim_text(text, self._context_text_limit(text)),
                        "source_title": str(m.get("policy_doc_name") or ""),
                        "source_ref": str(m.get("trace_link") or ""),
                        "page_no": None,
                    }
                )

        numbered_items: List[Dict] = []
        context_lines: List[str] = []
        for idx, item in enumerate(context_items, start=1):
            source_text = self._build_source_text(
                source_title=item.get("source_title", ""),
                section_no=item.get("subsection_no", "") or item.get("section_no", ""),
                section_title=item.get("subsection_title", "") or item.get("section_title", ""),
                parent_section=item.get("parent_section", ""),
                page_no=item.get("page_no"),
            )
            row = {
                "ctx_id": idx,
                "section_no": item.get("section_no", ""),
                "section_title": item.get("section_title", ""),
                "subsection_no": item.get("subsection_no", ""),
                "subsection_title": item.get("subsection_title", ""),
                "parent_section": item.get("parent_section", ""),
                "original_text": item.get("original_text", ""),
                "source_title": item.get("source_title", ""),
                "source_ref": item.get("source_ref", ""),
                "source_text": source_text,
            }
            numbered_items.append(row)
            context_lines.extend(
                [
                    f"[{idx}] 条款：{' '.join([x for x in [row['section_no'], row['section_title']] if x]).strip()}",
                    f"原文：{row['original_text']}",
                    f"出处：{row['source_text']}",
                    f"链接：{row['source_ref']}" if row["source_ref"] else "链接：无",
                    "",
                ]
            )

        context_text = "\n".join(
            [
                f"用户问题：{str(user_event or '').strip()}",
                "",
                "可用证据：",
                *context_lines,
            ]
        ).strip()
        return {
            "context_items": numbered_items,
            "context_text": context_text,
            "context_count": len(numbered_items),
            "context_chars": len(context_text),
        }

    def analyze_compliance(self, user_event: str, intent_data: Dict, retrieved: Dict) -> Dict:
        logger.info("[Analyzer] 正在执行合规清单匹配与缺漏诊断...")
        user_text = self._normalize_text(user_event)
        intent_type = str(intent_data.get("intent_type", "事项咨询"))
        query_mode = str(intent_data.get("query_mode", "standard")).strip().lower()

        procedures = retrieved.get("procedures", [])
        materials = retrieved.get("materials", [])
        fallback_knowledge = retrieved.get("fallback_knowledge", [])

        procedure_checks = self._check_procedures(user_text, procedures)
        material_checks = self._check_materials(user_text, materials)
        gaps = self._build_gaps(procedure_checks, material_checks, intent_type)
        retrieval_trace = retrieved.get("summary", {}) or {}

        clause_refs = []
        for c in retrieved.get("clauses", []):
            clause_refs.append(
                {
                    "policy_doc_name": c.get("policy_doc_name", ""),
                    "policy_doc_version": c.get("policy_doc_version", ""),
                    "clause_no": c.get("clause_no", ""),
                    "clause_title": c.get("clause_title", ""),
                    "trace_link": c.get("trace_link", ""),
                    "mandatory_level": c.get("mandatory_level", 1),
                }
            )

        if intent_type in {"请示审查", "混合"} and not (procedures or materials):
            gaps.append(
                {
                    "gap_type": "制度映射缺失",
                    "gap_item": "未命中可执行的流程与材料清单",
                    "expected_req": "需匹配到业务类型对应的流程步骤和必需材料后，才能完成请示完整性审查。",
                    "detected_content": "当前仅命中条款/知识片段，未形成结构化办理清单。",
                    "severity": 3,
                    "fix_suggestion": "请先补齐业务类型与制度条款映射，或补录该事项的流程模板与材料模板。",
                    "trace_link": "",
                    "clause_no": "",
                    "policy_doc_name": "",
                }
            )

        llm_context = self._build_llm_context(
            user_event=user_event,
            fallback_knowledge=fallback_knowledge,
            clauses=retrieved.get("clauses", []),
            procedures=procedures,
            materials=materials,
            retrieval_scope=str(retrieval_trace.get("retrieval_scope") or ""),
        )

        has_structured_hits = bool(procedures or materials or retrieved.get("clauses"))
        has_checklist = bool(procedures or materials)
        retrieval_path = str(retrieval_trace.get("retrieval_path") or "")
        kb_clause_only = bool(retrieved.get("clauses")) and not has_checklist and retrieval_path != "structured"
        if query_mode == "knowledge_explain" and retrieved.get("clauses"):
            summary = f"已命中 {len(retrieved.get('clauses', []))} 条原文片段，可直接基于原文答复。"
        elif kb_clause_only and fallback_knowledge:
            summary = (
                f"已命中 {len(fallback_knowledge)} 条知识片段，"
                "可提供原文依据与条款链接；当前无流程/材料模板匹配。"
            )
        elif intent_type in {"请示审查", "混合"} and not has_checklist and fallback_knowledge:
            summary = (
                f"当前仅命中 {len(fallback_knowledge)} 条知识片段，"
                "未形成结构化流程/材料清单，无法完成完整请示审查。"
            )
        elif intent_type in {"请示审查", "混合"} and not has_checklist:
            summary = "未命中结构化制度关系，无法完成请示审查。建议先完善业务类型与制度条款映射数据。"
        elif not has_structured_hits and fallback_knowledge:
            summary = f"未命中结构化制度关系，但命中 {len(fallback_knowledge)} 条知识点，可用于讲解与初步答复。"
        elif not has_structured_hits:
            summary = "未命中结构化制度关系，建议先完善业务类型与制度条款映射数据。"
        elif intent_type in {"请示审查", "混合"}:
            summary = f"已完成请示审查，发现 {len(gaps)} 项需关注缺漏。"
        else:
            summary = f"已生成合规办理建议，共 {len(procedure_checks)} 个程序步骤、{len(material_checks)} 项材料要求。"

        return {
            "intent_type": intent_type,
            "query_mode": intent_data.get("query_mode", "standard"),
            "matched_business_types": retrieved.get("business_types", []),
            "clause_refs": clause_refs,
            "procedure_checks": procedure_checks,
            "material_checks": material_checks,
            "gaps": gaps,
            "fallback_knowledge": fallback_knowledge,
            "llm_context": llm_context,
            "retrieval_trace": retrieval_trace,
            "summary": summary,
        }

    # 兼容旧调用
    def summarize_spirit(self, user_event: str, retrieved_materials: Dict) -> Dict:
        return {
            "dynamic_principles": [],
            "tensions_with_fixed": [],
            "summary": "该方法已切换为合规比对流程，请使用 analyze_compliance。",
            "raw": retrieved_materials,
        }
