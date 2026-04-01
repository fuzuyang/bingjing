import re
from datetime import datetime
from typing import Dict, List, Set


class ReportGenerator:
    """
    合规自查与制度推荐报告生成器。
    """

    @staticmethod
    def _render_business_types(types: List[Dict]) -> List[str]:
        if not types:
            return ["- 未识别到明确业务类型（建议完善业务类型映射配置）"]
        lines = []
        for t in types:
            code = t.get("type_code", "")
            name = t.get("type_name", "")
            conf = t.get("confidence", None)
            if conf is None:
                lines.append(f"- {name} (`{code}`)".rstrip())
            else:
                lines.append(f"- {name} (`{code}`), 置信度: {conf}")
        return lines

    def _render_clause_refs(self, clause_refs: List[Dict], limit: int = 20, exclude_keys: Set[str] | None = None) -> List[str]:
        if not clause_refs:
            return ["- 暂无命中的制度条款。"]

        exclude_keys = exclude_keys or set()
        lines: List[str] = []
        seen: Set[str] = set()
        skipped = 0

        for c in clause_refs:
            row_key = self._make_ref_key(c.get("trace_link", ""), c.get("policy_doc_name", ""), c.get("clause_no", ""))
            if row_key in exclude_keys:
                skipped += 1
                continue
            if row_key in seen:
                continue
            seen.add(row_key)

            doc = c.get("policy_doc_name", "未知制度")
            ver = c.get("policy_doc_version", "")
            no = c.get("clause_no", "")
            title = c.get("clause_title", "")
            link = c.get("trace_link", "")
            clause_label = self._format_clause_label(str(no or ""), str(title or ""), fallback="")
            doc_text = self._escape_markdown_text(doc)
            ver_text = self._escape_markdown_text(ver)
            clause_text = self._escape_markdown_text(clause_label)
            text = f"- {' '.join([x for x in [doc_text, ver_text, clause_text] if x]).strip()}".strip()
            if link:
                text += f"（[查看原文片段]({link})）"
            lines.append(text)
            if len(lines) >= limit:
                break

        if not lines:
            return ["- 上方已展示核心证据，无重复列出。"]
        if skipped > 0:
            lines.append(f"- 已去重 {skipped} 条与上方重复的证据。")
        return lines

    @staticmethod
    def _render_procedures(procedure_checks: List[Dict]) -> List[str]:
        if not procedure_checks:
            return ["- 暂无流程模板命中。"]
        lines = []
        for p in procedure_checks:
            link = p.get("trace_link", "")
            trace = f" | 依据: {p.get('policy_doc_name', '')} {p.get('clause_no', '')}" if p.get("clause_no") else ""
            if link:
                trace += f" | 链接: {link}"
            lines.append(
                f"- [{p.get('status')}] 步骤{p.get('item_id', '')}: {p.get('item_name', '')}{trace}".strip()
            )
        return lines

    @staticmethod
    def _render_materials(material_checks: List[Dict]) -> List[str]:
        if not material_checks:
            return ["- 暂无材料模板命中。"]

        level_label = {1: "必须", 2: "条件必需", 3: "可选"}
        lines = []
        for m in material_checks:
            level = int(m.get("required_level", 1) or 1)
            trace = f" | 依据: {m.get('policy_doc_name', '')} {m.get('clause_no', '')}" if m.get("clause_no") else ""
            if m.get("trace_link"):
                trace += f" | 链接: {m.get('trace_link')}"
            lines.append(
                f"- [{m.get('status')}] ({level_label.get(level, '必须')}) {m.get('item_name', '')}{trace}".strip()
            )
        return lines

    @staticmethod
    def _render_gaps(gaps: List[Dict]) -> List[str]:
        if not gaps:
            return ["- 未发现明显缺漏。"]
        lines = []
        for g in gaps:
            sev = int(g.get("severity", 2))
            sev_label = {1: "提示", 2: "一般", 3: "严重"}.get(sev, "一般")
            trace = ""
            if g.get("policy_doc_name") or g.get("clause_no"):
                trace += f" | 依据: {g.get('policy_doc_name', '')} {g.get('clause_no', '')}"
            if g.get("trace_link"):
                trace += f" | 链接: {g.get('trace_link')}"
            lines.append(
                f"- [{sev_label}] {g.get('gap_type')} - {g.get('gap_item')}：{g.get('fix_suggestion', '')}{trace}"
            )
        return lines

    @staticmethod
    def _clean_text(text: str) -> str:
        return re.sub(r"\s+", " ", str(text or "")).strip()

    @staticmethod
    def _normalize_clause_fields(section_no: str, section_title: str) -> tuple[str, str]:
        no = ReportGenerator._clean_text(section_no)
        title = ReportGenerator._clean_text(section_title)
        if not no:
            return "", title
        if not title:
            return no, ""

        compact_no = re.sub(r"\s+", "", no)
        compact_title = re.sub(r"\s+", "", title)
        if compact_no == compact_title:
            return no, ""
        if compact_title.startswith(compact_no):
            remain = title[len(no):].strip()
            return no, remain
        if compact_no.startswith(compact_title):
            return no, ""
        return no, title

    @staticmethod
    def _format_clause_label(section_no: str, section_title: str, fallback: str = "") -> str:
        no, title = ReportGenerator._normalize_clause_fields(section_no, section_title)
        label = " ".join([x for x in [no, title] if x]).strip()
        return label or fallback

    @staticmethod
    def _escape_markdown_text(text: str) -> str:
        raw = ReportGenerator._clean_text(text)
        if not raw:
            return ""
        return re.sub(r"([\\`*_{}\[\]()|~])", r"\\\1", raw)

    @staticmethod
    def _extract_page_no(item: Dict) -> int | None:
        page_no = item.get("page_no")
        try:
            if page_no:
                return int(page_no)
        except Exception:
            pass

        source_ref = str(item.get("source_ref") or "")
        m = re.search(r"[?&]page=(\d+)", source_ref)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                return None

        source_text = str(item.get("source_text") or "")
        m = re.search(r"\u7b2c\s*(\d+)\s*\u9875", source_text)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                return None
        return None

    @staticmethod
    def _normalize_answer_markdown(text: str) -> str:
        raw = str(text or "").replace("\r\n", "\n").replace("\r", "\n").strip()
        if not raw:
            return ""

        lines: List[str] = []
        for line in raw.split("\n"):
            cleaned = re.sub(r"[ \t]+", " ", line).strip()
            if cleaned:
                lines.append(cleaned)
        if not lines:
            return ""

        merged = "\n".join(lines)

        # Inline "1. ... 2. ..." -> multi-line ordered list for clearer rendering.
        flat = re.sub(r"\s+", " ", merged).strip()
        if "\n" not in merged and len(re.findall(r"\d+\.\s*", flat)) >= 2:
            parts = re.findall(r"\d+\.\s*(.+?)(?=(?:\s+\d+\.\s*)|$)", flat)
            items: List[str] = []
            for part in parts:
                item = re.sub(r"^(?:\d+\.\s*)+", "", str(part or "")).strip(" ;；。")
                if len(item) >= 2:
                    items.append(item)
            if len(items) >= 2:
                merged = "\n".join(f"{idx}. {item}" for idx, item in enumerate(items, 1))

        return merged

    @staticmethod
    def _trim_text(text: str, limit: int = 180) -> str:
        cleaned = ReportGenerator._clean_text(text)
        if len(cleaned) <= limit:
            return cleaned
        return cleaned[:limit] + "..."

    @staticmethod
    def _compact_text(text: str) -> str:
        return re.sub(r"[^\w\u4e00-\u9fff]", "", ReportGenerator._clean_text(text))

    @staticmethod
    def _clause_depth(clause_no: str) -> int:
        parts = [p for p in str(clause_no or "").split(".") if p]
        return len(parts)

    @staticmethod
    def _is_parent_child_clause(a: str, b: str) -> bool:
        a = str(a or "").strip()
        b = str(b or "").strip()
        if not a or not b or a == b:
            return False
        return a.startswith(f"{b}.") or b.startswith(f"{a}.")

    @staticmethod
    def _make_ref_key(link: str, doc_name: str, clause_no: str) -> str:
        if link:
            return f"link::{link}"
        return f"doc::{doc_name}::clause::{clause_no}"

    def _is_similar_text(self, a: str, b: str) -> bool:
        ta = self._compact_text(a)
        tb = self._compact_text(b)
        if not ta or not tb:
            return False
        if ta == tb:
            return True
        short, long_ = (ta, tb) if len(ta) <= len(tb) else (tb, ta)
        if short in long_ and len(short) / max(len(long_), 1) >= 0.75:
            return True
        return False

    def _prefer_knowledge_item(self, left: Dict, right: Dict) -> Dict:
        def score(item: Dict) -> float:
            clause_no = item.get("section_no") or str(item.get("name", "")).split(" ")[0]
            text = item.get("original_text") or item.get("golden_quote") or item.get("description") or ""
            depth = self._clause_depth(str(clause_no))
            has_title = 1 if str(item.get("section_title") or "").strip() else 0
            return depth * 10 + has_title * 2 + min(len(self._compact_text(text)), 400) / 100.0

        return left if score(left) >= score(right) else right

    def _dedupe_knowledge_hits(self, fallback_knowledge: List[Dict], limit: int = 6) -> List[Dict]:
        deduped: List[Dict] = []
        for item in fallback_knowledge:
            if len(deduped) >= limit:
                break

            current_text = item.get("original_text") or item.get("golden_quote") or item.get("description") or ""
            current_doc = str(item.get("source_title") or item.get("source_heading") or "")
            current_no = str(item.get("section_no") or "").strip()
            merged = False

            for idx, existing in enumerate(deduped):
                exist_text = existing.get("original_text") or existing.get("golden_quote") or existing.get("description") or ""
                exist_doc = str(existing.get("source_title") or existing.get("source_heading") or "")
                exist_no = str(existing.get("section_no") or "").strip()
                same_doc = current_doc == exist_doc
                if not same_doc:
                    continue
                same_text = self._is_similar_text(current_text, exist_text)
                parent_child = self._is_parent_child_clause(current_no, exist_no)
                if same_text or parent_child:
                    deduped[idx] = self._prefer_knowledge_item(item, existing)
                    merged = True
                    break

            if not merged:
                deduped.append(item)
        return deduped

    @staticmethod
    def _is_rewrite_useful(original_text: str, rewritten: str) -> bool:
        original = ReportGenerator._clean_text(original_text)
        rewrite = ReportGenerator._clean_text(rewritten)
        if not rewrite:
            return False
        if not original:
            return True

        norm_original = re.sub(r"[^\w\u4e00-\u9fff]", "", original)
        norm_rewrite = re.sub(r"[^\w\u4e00-\u9fff]", "", rewrite)
        if not norm_original or not norm_rewrite:
            return rewrite != original
        if norm_original == norm_rewrite:
            return False

        # 高度重叠时不再展示“改写”，避免重复噪音。
        shorter = min(len(norm_original), len(norm_rewrite))
        longer = max(len(norm_original), len(norm_rewrite))
        if shorter / longer >= 0.75 and (norm_original in norm_rewrite or norm_rewrite in norm_original):
            return False
        return True

    @staticmethod
    def _friendly_source_text_legacy(item: Dict) -> str:
        source_name = (
            item.get("source_heading")
            or item.get("source_title")
            or item.get("source_file_name")
            or "未知来源"
        )
        section_no = str(item.get("section_no") or "").strip()
        if not section_no:
            name = str(item.get("name") or "").strip()
            section_no = name.split(" ")[0] if name else ""

        page_no = item.get("page_no")
        if not page_no:
            source_ref = str(item.get("source_ref") or "")
            m = re.search(r"[?&]page=(\d+)", source_ref)
            if m:
                try:
                    page_no = int(m.group(1))
                except Exception:
                    page_no = None

        parts = [source_name]
        if section_no:
            parts.append(f"章节 {section_no}")
        if page_no:
            parts.append(f"第{page_no}页")
        return "，".join(parts)

    @staticmethod
    def _normalize_section_token(raw: str) -> str:
        return re.sub(r"\s+", "", str(raw or "")).strip()

    @staticmethod
    def _extract_chapter_clause(item: Dict) -> tuple[str, str]:
        chapter = ""
        clause = ""

        candidates = [
            str(item.get("parent_section") or ""),
            str(item.get("section_no") or ""),
            str(item.get("section_title") or ""),
            str(item.get("name") or ""),
            str(item.get("source_text") or ""),
        ]

        for raw in candidates:
            text = str(raw or "")
            if not text:
                continue
            if not chapter:
                m = re.search(r"第[一二三四五六七八九十百千万零〇\d]+\s*章", text)
                if m:
                    chapter = ReportGenerator._normalize_section_token(m.group(0))
            if not clause:
                m = re.search(r"第[一二三四五六七八九十百千万零〇\d]+\s*条", text)
                if m:
                    clause = ReportGenerator._normalize_section_token(m.group(0))
            if chapter and clause:
                break

        section_no = ReportGenerator._normalize_section_token(item.get("section_no") or "")
        parent_section = ReportGenerator._normalize_section_token(item.get("parent_section") or "")
        if not chapter and parent_section.endswith("章"):
            chapter = parent_section
        if not clause and section_no.endswith("条"):
            clause = section_no
        if not chapter and section_no.endswith("章"):
            chapter = section_no
        if not clause and parent_section.endswith("条"):
            clause = parent_section

        if chapter and clause and chapter == clause:
            clause = ""
        return chapter, clause

    @staticmethod
    def _friendly_source_text(item: Dict) -> str:
        source_name = ReportGenerator._clean_text(
            item.get("source_heading") or item.get("source_title") or item.get("source_file_name") or ""
        )
        if not source_name:
            source_text = ReportGenerator._clean_text(item.get("source_text") or "")
            source_name = re.split(r"[,，]\s*\u7ae0\u8282\s*", source_text, maxsplit=1)[0].strip() if source_text else ""
        if not source_name:
            source_name = "未知来源"

        section_no = str(item.get("section_no") or "").strip()
        section_title = str(item.get("section_title") or "").strip()
        if not section_no and not section_title:
            name = str(item.get("name") or "").strip()
            if name:
                section_no = name.split(" ")[0]
        chapter_label, clause_label = ReportGenerator._extract_chapter_clause(item)
        if not clause_label:
            fallback_clause = ReportGenerator._format_clause_label(section_no, section_title, fallback="")
            fallback_clause = ReportGenerator._normalize_section_token(fallback_clause)
            if fallback_clause and fallback_clause != chapter_label:
                clause_label = fallback_clause
        page_no = ReportGenerator._extract_page_no(item)

        parts = [source_name]
        if chapter_label:
            parts.append(f"章节 {chapter_label}")
        if clause_label:
            parts.append(f"条款 {clause_label}")
        if page_no:
            parts.append(f"第{page_no}页")
        return "，".join(parts)

    @staticmethod
    def _best_answer_text(item: Dict) -> str:
        original_text = item.get("original_text") or item.get("golden_quote") or item.get("description") or ""
        rewritten = item.get("rewrite") or ""
        candidate = rewritten if ReportGenerator._is_rewrite_useful(original_text, rewritten) else original_text
        normalized = ReportGenerator._normalize_answer_markdown(candidate)
        if not normalized:
            return ""
        if "\n" in normalized:
            return normalized
        return ReportGenerator._trim_text(normalized, 220)

    def _render_llm_answer(self, llm_answer: Dict) -> Dict:
        answer = self._normalize_answer_markdown(llm_answer.get("answer") or "")
        citations = llm_answer.get("citations") or []
        if not answer:
            return {"lines": [], "used_ref_keys": set()}

        lines: List[str] = [
            "## 知识点回答（大模型生成）",
            "",
            "### 最终回答",
            "",
            answer,
            "",
            "### 引用依据",
        ]

        used_ref_keys: Set[str] = set()
        if citations:
            for idx, item in enumerate(citations[:5], start=1):
                section_no = str(item.get("section_no") or "").strip()
                source_ref = str(item.get("source_ref") or "").strip()
                quote = self._trim_text(item.get("quote") or "", 120)

                source_title = self._friendly_source_text(item)
                source_key = source_title
                source_title = self._escape_markdown_text(source_title)
                quote = self._escape_markdown_text(quote)
                detail_parts: List[str] = [
                    f"**来源**：{source_title}",
                ]
                if quote:
                    detail_parts.append(f"**原文摘录**：{quote}")
                if source_ref:
                    detail_parts.append(f"**原文链接**：[定位到原文片段]({source_ref})")
                lines.append(f"{idx}. " + "<br>".join(detail_parts))
                used_ref_keys.add(self._make_ref_key(source_ref, source_key, section_no))
        else:
            lines.append("- 未返回可用引用。")
        lines.append("")
        return {"lines": lines, "used_ref_keys": used_ref_keys}

    def _render_knowledge_answer(self, fallback_knowledge: List[Dict]) -> Dict:
        if not fallback_knowledge:
            return {"lines": [], "used_ref_keys": set()}

        picked = self._dedupe_knowledge_hits(fallback_knowledge, limit=6)
        if not picked:
            return {"lines": [], "used_ref_keys": set()}

        top = picked[0]
        top_name = str(top.get("name") or top.get("section_no") or "未命名条款").strip()
        top_original = self._clean_text(top.get("original_text") or top.get("golden_quote") or top.get("description") or "")
        top_rewrite = self._clean_text(top.get("rewrite") or "")
        top_source = self._friendly_source_text(top)
        top_source_ref = str(top.get("source_ref") or "").strip()

        lines: List[str] = [
            "## 知识点原文答复（优先）",
            "",
            "### 最终回答",
            self._best_answer_text(top) or "暂无可直接回答的原文内容。",
            "",
            "### 依据原文（Top1）",
            f"- 条款：{top_name}",
            f"- 原文：{self._trim_text(top_original, 260)}",
            f"- 出处：{top_source}",
        ]
        if top_source_ref:
            lines.append(f"- 查看原文链接：[定位到原文片段]({top_source_ref})")
        if self._is_rewrite_useful(top_original, top_rewrite):
            lines.append(f"- 解释改写：{self._trim_text(top_rewrite, 260)}")

        supplements = picked[1:2]
        if supplements:
            lines.extend(["", "### 补充命中（Top1）"])
            for idx, item in enumerate(supplements, start=1):
                name = str(item.get("name") or item.get("section_no") or "未命名条款").strip()
                original_text = item.get("original_text") or item.get("golden_quote") or item.get("description") or ""
                source_text = self._friendly_source_text(item)
                lines.append(f"{idx}. {name}：{self._trim_text(original_text, 120)}")
                lines.append(f"   出处：{source_text}")
        lines.append("")
        used_ref_keys = {
            self._make_ref_key(top.get("source_ref", ""), top.get("source_title", ""), top.get("section_no", "")),
        }
        for item in supplements:
            used_ref_keys.add(
                self._make_ref_key(item.get("source_ref", ""), item.get("source_title", ""), item.get("section_no", ""))
            )
        return {"lines": lines, "used_ref_keys": used_ref_keys}

    def generate(self, intent: Dict, analysis: Dict, evaluation: Dict) -> str:
        overall = evaluation.get("overall_conclusion", {})
        compliance_rating = evaluation.get("compliance_rating", {})
        recommendations = evaluation.get("recommendations", {})
        fallback_knowledge = analysis.get("fallback_knowledge", [])
        llm_answer = analysis.get("llm_answer", {})
        clause_ref_exclude_keys: Set[str] = set()

        if llm_answer and llm_answer.get("answer"):
            knowledge_block = self._render_llm_answer(llm_answer)
            clause_ref_exclude_keys = knowledge_block.get("used_ref_keys", set())
            knowledge_lines = [""] + knowledge_block.get("lines", [])
        elif fallback_knowledge:
            knowledge_block = self._render_knowledge_answer(fallback_knowledge)
            clause_ref_exclude_keys = knowledge_block.get("used_ref_keys", set())
            knowledge_lines = [""] + knowledge_block.get("lines", [])
        else:
            knowledge_lines = []

        lines = [
            "# 场景化合规自查与制度推荐报告",
            f"> 生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "## 一、事项识别",
            f"- 事项摘要：{intent.get('event_summary', '')}",
            f"- 意图类型：{intent.get('intent_type', '')}",
            f"- 核心问题：{intent.get('core_issue', '')}",
            f"- 结论状态：**{overall.get('evaluation', '未判定')}**",
            f"- 结论说明：{overall.get('core_reason', '')}",
            "",
            "### 识别的业务类型",
            *self._render_business_types(analysis.get("matched_business_types", [])),
            *knowledge_lines,
            "",
            "## 二、更多证据（可展开）",
            *self._render_clause_refs(analysis.get("clause_refs", []), exclude_keys=clause_ref_exclude_keys),
            "",
            "## 三、应遵循程序",
            *self._render_procedures(analysis.get("procedure_checks", [])),
            "",
            "## 四、应提供材料",
            *self._render_materials(analysis.get("material_checks", [])),
            "",
            "## 五、请示缺漏诊断",
            *self._render_gaps(analysis.get("gaps", [])),
            "",
            "## 六、综合评分",
            f"- 合规状态：**{compliance_rating.get('status', '未判定')}**",
            f"- 完整度评分：**{compliance_rating.get('completeness_score', 0)} / 100**",
            f"- 必需项总数：{compliance_rating.get('required_total', 0)}",
            f"- 已满足项：{compliance_rating.get('completed', 0)}",
            f"- 未满足必需项：{compliance_rating.get('missing_required', 0)}",
            "",
            "## 七、建议动作",
            "### 对申请方",
        ]

        for r in recommendations.get("for_applicant", []):
            lines.append(f"- {r}")
        if not recommendations.get("for_applicant"):
            lines.append("- 暂无。")

        lines.append("")
        lines.append("### 对审核方")
        for r in recommendations.get("for_reviewer", []):
            lines.append(f"- {r}")
        if not recommendations.get("for_reviewer"):
            lines.append("- 暂无。")

        lines.extend(
            [
                "",
                "---",
                "注：本报告用于合规自查与制度执行辅助，不构成法律意见。",
            ]
        )
        return "\n".join(lines)
