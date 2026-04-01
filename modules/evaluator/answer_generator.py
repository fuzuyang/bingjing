import json
import os
import re
import sys
from typing import Dict, Iterator, List

from dotenv import load_dotenv
from openai import OpenAI

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

load_dotenv()


class GroundedAnswerGenerator:
    """
    Generate final answer strictly grounded on retrieved context.
    """
    STRONG_LIST_HINT_TERMS = (
        "哪些",
        "哪几",
        "原则",
        "职责",
        "步骤",
        "流程",
        "材料",
        "清单",
        "要点",
        "分别列出",
    )
    NO_EVIDENCE_HINT_TERMS = (
        "未在已命中原文中找到",
        "未命中可用原文",
        "无法基于原文生成答复",
    )

    def __init__(self):
        self.api_key = os.getenv("SILICONFLOW_API_KEY")
        self.enable_llm = str(os.getenv("ENABLE_LLM_ANSWER", "1")).strip() == "1" and bool(self.api_key)
        self.model = os.getenv("ANSWER_LLM_MODEL", "deepseek-chat")
        self.client = (
            OpenAI(
                api_key=self.api_key,
                base_url="https://api.deepseek.com/v1",
            )
            if self.enable_llm
            else None
        )

    @staticmethod
    def _trim_text(text: str, limit: int = 220) -> str:
        cleaned = re.sub(r"\s+", " ", str(text or "")).strip()
        if len(cleaned) <= limit:
            return cleaned
        return cleaned[:limit] + "..."

    @staticmethod
    def _safe_int_list(values) -> List[int]:
        result = []
        if not isinstance(values, list):
            return result
        for x in values:
            try:
                result.append(int(x))
            except Exception:
                continue
        return result

    @staticmethod
    def _normalize_numbered_answer(text: str) -> str:
        body = re.sub(r"\s+", " ", str(text or "")).strip()
        if not body:
            return ""
        pieces = re.findall(r"\d+\.\s*(.+?)(?=(?:\s+\d+\.\s*)|$)", body)
        if len(pieces) < 2:
            return ""
        items: List[str] = []
        seen = set()
        for piece in pieces:
            cleaned = re.sub(r"^(?:\d+\.\s*)+", "", str(piece or "")).strip(" ;；。")
            if len(cleaned) < 2:
                continue
            if cleaned in seen:
                continue
            seen.add(cleaned)
            items.append(cleaned)
        items = GroundedAnswerGenerator._expand_list_items(items)
        if len(items) < 2:
            return ""
        return "\n".join(f"{idx}. {item}" for idx, item in enumerate(items, 1))

    @staticmethod
    def _expand_list_items(items: List[str]) -> List[str]:
        splitter = re.compile(r"(?<=[。；;])\s*(?=(?:此外|另外|同时|另需|另应|另须|补充说明|补充))")
        expanded: List[str] = []
        seen = set()
        for raw in items or []:
            text = re.sub(r"\s+", " ", str(raw or "")).strip().strip("。；; ")
            if len(text) < 2:
                continue
            parts = splitter.split(text)
            for part in parts:
                cleaned = re.sub(r"\s+", " ", str(part or "")).strip().strip("。；; ")
                if len(cleaned) < 2:
                    continue
                if cleaned in seen:
                    continue
                seen.add(cleaned)
                expanded.append(cleaned)
        return expanded

    @staticmethod
    def _is_multi_question(text: str) -> bool:
        question = str(text or "")
        if question.count("？") + question.count("?") >= 2:
            return True
        if any(term in question for term in ("以及", "并且", "同时")) and any(
            term in question for term in ("什么", "哪些", "哪种", "哪两种", "包括")
        ):
            return True
        return False

    @staticmethod
    def _parse_numbered_items(text: str) -> List[str]:
        items: List[str] = []
        for line in str(text or "").split("\n"):
            m = re.match(r"^\s*\d+\.\s*(.+)$", line.strip())
            if not m:
                continue
            item = re.sub(r"\s+", " ", m.group(1)).strip().strip("；;")
            if len(item) >= 2:
                items.append(item)
        return items

    @staticmethod
    def _split_mixed_heading_item(item: str) -> tuple[str, str]:
        text = re.sub(r"\s+", " ", str(item or "")).strip().strip("；;")
        if not text:
            return "", ""
        m = re.match(
            r"^(?P<left>.+?)\s*(?P<label>(?:主要|重点|核心)?(?:评价|考核|管理|工作)?(?:内容|事项|要点)[^：:]{0,10}(?:包括|如下|为)[:：]?)$",
            text,
        )
        if not m:
            return "", ""
        left = m.group("left").strip(" ：:;；")
        label = m.group("label").strip()
        if len(left) < 2 or len(label) < 2:
            return "", ""
        return left, label

    def _refine_multi_question_numbered_answer(self, numbered_text: str, user_question: str) -> str:
        if not self._is_multi_question(user_question):
            return numbered_text

        items = self._parse_numbered_items(numbered_text)
        if len(items) < 3:
            return numbered_text

        split_idx = -1
        first_part = ""
        heading = ""
        for idx, item in enumerate(items):
            left, label = self._split_mixed_heading_item(item)
            if left and label and idx < len(items) - 1:
                split_idx = idx
                first_part = left
                heading = label.rstrip("：: ")
                break
        if split_idx <= 0:
            return numbered_text

        prefix_items = items[:split_idx] + [first_part]
        detail_items = items[split_idx + 1 :]
        if not prefix_items or len(detail_items) < 2:
            return numbered_text

        prefix_text = "、".join(prefix_items)
        prefix_label = "考评方式" if ("方式" in prefix_text or "分为" in str(user_question or "")) else "第一部分"
        detail_block = "\n".join(f"{idx}. {item}" for idx, item in enumerate(detail_items, 1))
        return f"{prefix_label}：{prefix_text}\n{heading}：\n{detail_block}"

    def _prefer_list_format(self, user_question: str, context_items: List[Dict]) -> bool:
        question = str(user_question or "")
        if any(term in question for term in self.STRONG_LIST_HINT_TERMS):
            return True
        if self._is_multi_question(question) and ("分为" in question or "包括" in question):
            return False
        top_text = str((context_items[0].get("original_text") if context_items else "") or "")
        if re.search(r"（[一二三四五六七八九十\d]+）", top_text):
            return True
        if top_text.count("；") >= 2:
            return True
        return False

    @staticmethod
    def _split_list_items(text: str) -> List[str]:
        body = re.sub(r"\s+", " ", str(text or "")).strip().strip("。；;")
        if not body:
            return []
        if "；" in body or ";" in body:
            raw_items = re.split(r"[；;]", body)
        elif body.count("。") >= 2:
            raw_items = body.split("。")
        elif body.count("、") >= 2:
            raw_items = body.split("、")
        else:
            return []

        items: List[str] = []
        seen = set()
        for item in raw_items:
            cleaned = str(item or "").strip("：:，,。；; ")
            cleaned = re.sub(r"^\s*(?:\d+[\.、\)]|[\(\uff08]?[一二三四五六七八九十]{1,3}[、\.\)\uff09])\s*", "", cleaned)
            cleaned = re.sub(r"^(?:\d+\.\s*)+", "", cleaned)
            if len(cleaned) < 2:
                continue
            if cleaned in seen:
                continue
            seen.add(cleaned)
            items.append(cleaned)
        return items

    def _format_answer_output(self, answer: str, user_question: str, context_items: List[Dict]) -> str:
        text = re.sub(r"\s+", " ", str(answer or "")).strip()
        if not text:
            return text
        normalized_numbered = self._normalize_numbered_answer(text)
        if normalized_numbered:
            return self._refine_multi_question_numbered_answer(normalized_numbered, user_question)
        return text

    @classmethod
    def _is_no_evidence_answer(cls, answer: str) -> bool:
        text = str(answer or "").strip()
        if not text:
            return True
        return any(term in text for term in cls.NO_EVIDENCE_HINT_TERMS)

    def _fallback_answer(self, context_items: List[Dict], mode: str = "extractive-fallback", user_question: str = "") -> Dict:
        if not context_items:
            return {
                "mode": "no-context",
                "answer": "未命中可用原文，无法基于原文生成答复。",
                "citations": [],
            }

        top = context_items[0]
        rewrite_text = str(top.get("rewrite") or "").strip()
        answer_text = rewrite_text or str(top.get("original_text") or "").strip()
        answer_text = self._format_answer_output(answer_text, str(user_question or ""), context_items)
        if self._is_no_evidence_answer(answer_text):
            return {
                "mode": mode,
                "answer": answer_text or "未命中可用原文，无法基于原文生成答复。",
                "citations": [],
            }
        return {
            "mode": mode,
            "answer": answer_text or "未命中可用原文，无法基于原文生成答复。",
            "citations": [
                {
                    "ctx_id": int(top.get("ctx_id") or 1),
                    "section_no": str(top.get("section_no") or ""),
                    "section_title": str(top.get("section_title") or ""),
                    "subsection_no": str(top.get("subsection_no") or ""),
                    "subsection_title": str(top.get("subsection_title") or ""),
                    "parent_section": str(top.get("parent_section") or ""),
                    "source_title": str(top.get("source_title") or ""),
                    "source_ref": str(top.get("source_ref") or ""),
                    "source_text": str(top.get("source_text") or ""),
                    "quote": self._trim_text(top.get("original_text") or "", 220),
                }
            ],
        }

    @staticmethod
    def _build_system_prompt() -> str:
        return (
            "你是企业制度问答助手。必须严格基于“可用证据”作答，"
            "不得补充证据外信息。若证据不足，直接说明“未在已命中原文中找到”。"
            "请优先使用自己的话进行改写转述，适当扩写，不要大段照抄原文。"
            "若问题包含两个或多个子问，请先按子问分段作答，再在必要处列点，不要把全部信息机械编号。"
            "输出 JSON："
            '{"answer":"", "used_ids":[1,2], "confidence":"high|medium|low"}。'
        )

    @staticmethod
    def _build_user_prompt(user_question: str, context_text: str) -> str:
        return (
            f"用户问题：{user_question}\n\n"
            "请仅使用下面证据回答，并给出 used_ids：\n"
            f"{context_text}\n"
        )

    def _request_llm_answer(self, user_question: str, context_text: str) -> Dict:
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": self._build_system_prompt()},
                {"role": "user", "content": self._build_user_prompt(user_question, context_text)},
            ],
            response_format={"type": "json_object"},
            temperature=0.2,
            max_tokens=1000,
        )
        content = response.choices[0].message.content
        return json.loads(content)

    @staticmethod
    def _build_stream_system_prompt() -> str:
        return (
            "你是企业制度问答助手。必须严格基于“可用证据”作答，"
            "不得补充证据外信息。若证据不足，直接说明“未在已命中原文中找到”。"
            "请只输出最终答案正文，不要输出 JSON，不要输出额外解释。"
        )

    @staticmethod
    def _build_stream_user_prompt(user_question: str, context_text: str) -> str:
        return (
            f"用户问题：{user_question}\n\n"
            "请仅使用下面证据回答：\n"
            f"{context_text}\n"
        )

    def _stream_llm_answer_text(self, user_question: str, context_text: str) -> Iterator[str]:
        stream = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": self._build_stream_system_prompt()},
                {"role": "user", "content": self._build_stream_user_prompt(user_question, context_text)},
            ],
            temperature=0.2,
            max_tokens=1000,
            stream=True,
        )
        for chunk in stream:
            try:
                delta = (chunk.choices[0].delta.content or "")
            except Exception:
                delta = ""
            if delta:
                yield str(delta)

    def _default_citations_from_context(self, context_items: List[Dict], limit: int = 3) -> List[Dict]:
        citations = []
        for item in (context_items or [])[: max(1, int(limit))]:
            try:
                ctx_id = int(item.get("ctx_id") or 0)
            except Exception:
                ctx_id = 0
            citations.append(
                {
                    "ctx_id": ctx_id,
                    "section_no": str(item.get("section_no") or ""),
                    "section_title": str(item.get("section_title") or ""),
                    "subsection_no": str(item.get("subsection_no") or ""),
                    "subsection_title": str(item.get("subsection_title") or ""),
                    "parent_section": str(item.get("parent_section") or ""),
                    "source_title": str(item.get("source_title") or ""),
                    "source_ref": str(item.get("source_ref") or ""),
                    "source_text": str(item.get("source_text") or ""),
                    "quote": self._trim_text(item.get("original_text") or "", 220),
                }
            )
        return citations

    def generate_stream(self, user_event: str, intent_data: Dict, analysis: Dict) -> Iterator[Dict]:
        llm_context = analysis.get("llm_context", {}) or {}
        context_items = llm_context.get("context_items", []) or []
        question = str(user_event or "")

        if not context_items:
            fallback = self._fallback_answer([], mode="no-context", user_question=question)
            for ch in str(fallback.get("answer") or ""):
                yield {"type": "token", "delta": ch}
            yield {"type": "final", "payload": fallback}
            return

        if not self.enable_llm or not self.client:
            fallback = self._fallback_answer(context_items, mode="rule-fallback", user_question=question)
            for ch in str(fallback.get("answer") or ""):
                yield {"type": "token", "delta": ch}
            yield {"type": "final", "payload": fallback}
            return

        raw_answer_parts: List[str] = []
        try:
            for delta in self._stream_llm_answer_text(
                user_question=question,
                context_text=str(llm_context.get("context_text") or ""),
            ):
                raw_answer_parts.append(delta)
                yield {"type": "token", "delta": delta}

            raw_answer = "".join(raw_answer_parts).strip()
            if not raw_answer:
                fallback = self._fallback_answer(context_items, mode="llm-empty-fallback", user_question=question)
                yield {"type": "final", "payload": fallback}
                return

            formatted_answer = self._format_answer_output(raw_answer, question, context_items)
            citations = self._default_citations_from_context(context_items, limit=3)
            if not citations:
                citations = self._fallback_answer(
                    context_items,
                    mode="llm-citation-fallback",
                    user_question=question,
                ).get("citations", [])
            if self._is_no_evidence_answer(formatted_answer):
                citations = []

            yield {
                "type": "final",
                "payload": {
                    "mode": "llm-stream",
                    "answer": formatted_answer,
                    "citations": citations,
                    "confidence": "unknown",
                },
            }
        except Exception as e:
            fallback = self._fallback_answer(context_items, mode="llm-error-fallback", user_question=question)
            fallback["error"] = str(e)
            if not raw_answer_parts:
                for ch in str(fallback.get("answer") or ""):
                    yield {"type": "token", "delta": ch}
            yield {"type": "final", "payload": fallback}

    def generate(self, user_event: str, intent_data: Dict, analysis: Dict) -> Dict:
        llm_context = analysis.get("llm_context", {}) or {}
        context_items = llm_context.get("context_items", []) or []
        if not context_items:
            return self._fallback_answer([], mode="no-context", user_question=str(user_event or ""))

        if not self.enable_llm or not self.client:
            return self._fallback_answer(context_items, mode="rule-fallback", user_question=str(user_event or ""))

        try:
            payload = self._request_llm_answer(
                user_question=str(user_event or ""),
                context_text=str(llm_context.get("context_text") or ""),
            )
            answer = str(payload.get("answer") or "").strip()
            used_ids = self._safe_int_list(payload.get("used_ids", []))
            if not answer:
                return self._fallback_answer(context_items, mode="llm-empty-fallback", user_question=str(user_event or ""))
            answer = self._format_answer_output(answer, str(user_event or ""), context_items)

            if not used_ids:
                used_ids = [int(context_items[0].get("ctx_id") or 1)]

            item_map = {int(x.get("ctx_id") or 0): x for x in context_items}
            citations = []
            for uid in used_ids[:5]:
                item = item_map.get(uid)
                if not item:
                    continue
                citations.append(
                    {
                        "ctx_id": uid,
                        "section_no": str(item.get("section_no") or ""),
                        "section_title": str(item.get("section_title") or ""),
                        "subsection_no": str(item.get("subsection_no") or ""),
                        "subsection_title": str(item.get("subsection_title") or ""),
                        "parent_section": str(item.get("parent_section") or ""),
                        "source_title": str(item.get("source_title") or ""),
                        "source_ref": str(item.get("source_ref") or ""),
                        "source_text": str(item.get("source_text") or ""),
                        "quote": self._trim_text(item.get("original_text") or "", 220),
                    }
                )
            if not citations:
                citations = self._fallback_answer(
                    context_items,
                    mode="llm-citation-fallback",
                    user_question=str(user_event or ""),
                ).get("citations", [])
            if self._is_no_evidence_answer(answer):
                citations = []

            return {
                "mode": "llm",
                "answer": answer,
                "citations": citations,
                "confidence": str(payload.get("confidence") or "unknown"),
            }
        except Exception as e:
            fallback = self._fallback_answer(context_items, mode="llm-error-fallback", user_question=str(user_event or ""))
            fallback["error"] = str(e)
            return fallback
