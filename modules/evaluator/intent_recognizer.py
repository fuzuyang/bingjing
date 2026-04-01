import json
import os
import re
import sys
import logging
from typing import Dict, List

from dotenv import load_dotenv
from openai import OpenAI

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

load_dotenv()

logger = logging.getLogger(__name__)


class IntentRecognizer:
    """
    意图识别与检索指令生成（提示词层）。
    目标：将用户输入转成稳定可执行的检索指令，避免“问句动词污染关键词”。
    """

    QUERY_PREFIXES = (
        "请问",
        "请帮我",
        "请帮忙",
        "麻烦",
        "帮我",
        "帮忙",
        "请",
        "请讲解",
        "讲解",
        "请解释",
        "解释",
        "请介绍",
        "介绍",
        "请说明",
        "说明",
        "请分析",
        "分析",
        "阐述",
        "简述",
        "聊聊",
        "谈谈",
        "说说",
        "告诉我",
        "想了解",
        "讲述",
        "讲述一下",
        "讲一下",
        "说一下",
        "介绍一下",
    )
    QUESTION_SUFFIX_RE = re.compile(r"(是什么|是啥|有哪些|有何|有什么|吗|呢|吧)$")
    QUESTION_TAIL_PATTERNS = (
        re.compile(r"(都)?包括哪些(?:步骤|内容|环节|方面)?$"),
        re.compile(r"(都)?包括什么(?:步骤|内容|环节|方面)?$"),
        re.compile(r"(都)?包含哪些(?:步骤|内容|环节|方面)?$"),
        re.compile(r"(都)?包含什么(?:步骤|内容|环节|方面)?$"),
        re.compile(r"(都)?有哪些(?:步骤|内容|环节|方面)?$"),
        re.compile(r"(的)?步骤有哪些$"),
        re.compile(r"(的)?内容有哪些$"),
        re.compile(r"(的)?流程有哪些$"),
    )
    GENERIC_KEYWORDS = {
        "合法",
        "合规",
        "原则",
        "制度",
        "规范",
        "管理",
        "流程",
        "材料",
        "要求",
        "事项",
        "审查",
    }
    KNOWLEDGE_HINT_WORDS = {"原则", "概念", "定义", "内涵", "含义", "框架", "要义", "是什么", "指导思想", "目的"}
    ACTION_HINT_WORDS = {
        "流程",
        "步骤",
        "材料",
        "办理",
        "申请",
        "提交",
        "审批",
        "审查",
        "请示",
        "注销",
        "担保",
        "采购",
        "并购",
        "投资",
        "合同",
        "授权",
        "资产处置",
        "税务",
    }
    PHRASE_SPLIT_RE = re.compile(r"(?:以及|并且|或者|有关|关于|针对|对|之|及|与|和|并|或|、|/)")
    KEYWORD_STOP_WORDS = {
        "是什么",
        "是啥",
        "有什么",
        "有何",
        "有哪些",
        "什么",
        "吗",
        "呢",
        "吧",
        "请问",
        "请讲解",
        "讲解",
        "解释",
        "说明",
    }
    PROTECTED_TERMS = {
        "目的",
        "原则",
        "定义",
        "概念",
        "范围",
        "职责",
        "条件",
        "依据",
        "流程",
        "步骤",
        "指导思想",
    }
    FOCUS_SUFFIXES = (
        "思想",
        "原则",
        "目的",
        "范围",
        "条件",
        "职责",
        "依据",
        "流程",
        "步骤",
        "要求",
        "定义",
        "概念",
        "内涵",
        "要点",
        "要义",
        "标准",
        "权限",
        "机制",
        "办法",
        "制度",
        "规则",
        "条款",
    )
    NOISE_PARTS = {"一下", "讲述", "讲解", "解释", "说明", "介绍", "说说", "聊聊", "谈谈", "公司的", "企业的"}
    ACTION_PREFIXES = (
        "创建",
        "制定",
        "建立",
        "新增",
        "编写",
        "起草",
        "设立",
    )

    def __init__(self):
        self.api_key = os.getenv("SILICONFLOW_API_KEY")
        self.enable_llm = str(os.getenv("ENABLE_LLM_INTENT", "1")).strip() == "1" and bool(self.api_key)
        self.enable_llm_rescue = str(os.getenv("ENABLE_LLM_INTENT_RESCUE", "1")).strip() == "1" and bool(self.api_key)
        self.client = (
            OpenAI(
                api_key=self.api_key,
                base_url="https://api.siliconflow.cn/v1",
            )
            if self.api_key
            else None
        )
        self.model = "deepseek-ai/DeepSeek-V3"

    def _get_system_prompt(self) -> str:
        return """
你是“场景化合规自查与制度推荐”的意图识别专家。
请把用户输入转成检索指令 JSON。仅输出 JSON，不要输出解释文本。
要求：must_keywords 必须保留用户问题中的焦点词（如“目的/原则/范围/条件”），不要只输出“是什么/如何”等问句词。

必须包含字段：
{
  "event_summary": "一句话摘要",
  "intent_type": "事项咨询/请示审查/混合",
  "domain": "行政执法/企业退出/财税金融/合同纠纷/劳动用工/知识产权/市场准入/其他",
  "core_issue": "核心问题",
  "business_types": [{"type_code": "", "type_name": "", "confidence": 0.0}],
  "key_elements": {"subject": "", "action": "", "object": "", "context": ""},
  "compliance_focus": {
    "procedure_topics": [],
    "required_material_topics": [],
    "authorization_topics": [],
    "risk_points": []
  },
  "search_criteria": {
    "text_search": {
      "fields": ["name", "description", "golden_quote", "source_title", "content_text"],
      "must_keywords": [],
      "should_keywords": [],
      "exclude_keywords": []
    },
    "structured_filters": {
      "source_tables": ["policy", "case", "speech"],
      "policy_categories": [],
      "relation_tables": [
        "kb_document",
        "kb_chunk"
      ],
      "recency_years": 3
    }
  },
  "traceability_plan": {
    "clause_trace_required": true,
    "target_entities": ["制度条款", "流程步骤", "材料清单"],
    "output_requirements": ["引用条款编号", "标注制度名称及版本"]
  },
  "priority_suggestion": ""
}
"""

    @staticmethod
    def _clean_text(text: str) -> str:
        text = re.sub(r"\s+", " ", str(text or "")).strip()
        text = re.sub(r"[。！？!?]+$", "", text).strip()
        return text

    def _strip_instruction_prefix(self, text: str) -> str:
        cleaned = self._clean_text(text)
        changed = True
        while cleaned and changed:
            changed = False
            for prefix in self.QUERY_PREFIXES:
                if cleaned.startswith(prefix) and len(cleaned) > len(prefix):
                    cleaned = cleaned[len(prefix):].strip()
                    changed = True
        return cleaned

    def _strip_action_prefix(self, text: str) -> str:
        cleaned = self._clean_text(text)
        for prefix in self.ACTION_PREFIXES:
            if cleaned.startswith(prefix) and len(cleaned) > len(prefix) + 1:
                return cleaned[len(prefix):].strip()
        return cleaned

    def _strip_question_tail(self, text: str) -> str:
        cleaned = self.QUESTION_SUFFIX_RE.sub("", self._clean_text(text)).strip()
        changed = True
        while cleaned and changed:
            changed = False
            for pattern in self.QUESTION_TAIL_PATTERNS:
                updated = pattern.sub("", cleaned).strip()
                if updated != cleaned:
                    cleaned = updated
                    changed = True
        return cleaned

    def _expand_topic_keywords(self, text: str) -> List[str]:
        cleaned = self._strip_question_tail(text)
        if not cleaned:
            return []

        variants: List[str] = []
        seen = set()

        def put(value: str):
            kw = self._clean_text(value)
            if not kw or len(kw) < 2 or kw in seen:
                return
            seen.add(kw)
            variants.append(kw)

        put(cleaned)
        if cleaned.endswith("工作") and len(cleaned) > 2:
            put(cleaned[:-2].strip())
        if cleaned.endswith("事项") and len(cleaned) > 2:
            put(cleaned[:-2].strip())
        if cleaned.endswith("流程") and len(cleaned) > 2:
            put(cleaned[:-2].strip())
        return variants

    def _is_noise_keyword(self, text: str) -> bool:
        kw = self._clean_text(text)
        return (not kw) or kw in self.GENERIC_KEYWORDS or kw in self.KEYWORD_STOP_WORDS

    def _extract_protected_terms(self, text: str) -> List[str]:
        raw = self._clean_text(text)
        return [term for term in self.PROTECTED_TERMS if term in raw]

    def _extract_focus_phrases(self, text: str) -> List[str]:
        raw = self._clean_text(text)
        if not raw:
            return []

        candidates: List[str] = []
        seen = set()

        def put(value: str):
            kw = self._clean_text(value)
            if not kw or len(kw) < 2:
                return
            if kw in self.KEYWORD_STOP_WORDS:
                return
            if kw in seen:
                return
            seen.add(kw)
            candidates.append(kw)

        def normalize_piece(piece: str) -> str:
            cleaned = self._clean_text(piece)
            cleaned = self.QUESTION_SUFFIX_RE.sub("", cleaned).strip()
            cleaned = re.sub(r"^(请|请你|帮我|麻烦|讲述一下|讲述|讲一下|说一下|介绍一下|介绍|解释一下|解释|说明一下|说明)", "", cleaned).strip()
            for token in self.NOISE_PARTS:
                cleaned = cleaned.replace(token, "")
            return cleaned.strip()

        for block in re.findall(r"[\u4e00-\u9fff]{2,24}", raw):
            block_norm = normalize_piece(block)
            if not block_norm:
                continue
            parts = [p for p in re.split(r"[的之关于针对对于]", block_norm) if p]
            tails = parts[-2:] if parts else [block_norm]
            for part in [block_norm] + tails:
                p = normalize_piece(part)
                if len(p) < 2:
                    continue
                if p in self.PROTECTED_TERMS:
                    put(p)
                    continue
                if any(p.endswith(suf) for suf in self.FOCUS_SUFFIXES):
                    put(p)

        for term in self._extract_protected_terms(raw):
            put(term)
        return candidates[:10]

    def _is_low_quality_keywords(self, keywords: List[str]) -> bool:
        useful = []
        for kw in keywords:
            cleaned = self._clean_text(kw)
            if not cleaned or len(cleaned) < 2:
                continue
            if self._is_noise_keyword(cleaned):
                continue
            useful.append(cleaned)
        if not useful:
            return True
        semantic_hits = 0
        for kw in useful:
            if kw in self.PROTECTED_TERMS or any(kw.endswith(suf) for suf in self.FOCUS_SUFFIXES):
                semantic_hits += 1
        if semantic_hits == 0 and len(useful) <= 2:
            avg_len = sum(len(x) for x in useful) / max(len(useful), 1)
            if avg_len <= 4:
                return True
            # 长短语关键词可能是精确提问，不应误判为低质量
            question_like_tokens = ("请问", "如何", "怎么办", "是什么", "有什么", "哪些", "说明")
            if any(any(tok in kw for tok in question_like_tokens) for kw in useful):
                return True
        if len(useful) == 1 and useful[0] in {"合规审查", "制度要求", "政策依据"}:
            return True
        if len(useful) == 1 and len(useful[0]) <= 3:
            return True
        return False

    def _request_llm_intent(self, user_input: str) -> Dict:
        if not self.client:
            return {}
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": self._get_system_prompt()},
                {"role": "user", "content": f"请基于以下输入生成检索指令：\n{str(user_input)}"},
            ],
            response_format={"type": "json_object"},
            temperature=0.2,
            max_tokens=1400,
        )
        return json.loads(response.choices[0].message.content)

    def _try_llm_keyword_rescue(self, user_input: str, local_intent: Dict) -> Dict:
        if not self.enable_llm_rescue:
            return local_intent
        try:
            llm_intent = self._request_llm_intent(user_input)
            if not isinstance(llm_intent, dict):
                return local_intent
            self._validate_intent_data(llm_intent, str(user_input))

            local_keywords = local_intent.get("search_criteria", {}).get("text_search", {}).get("must_keywords", [])
            llm_keywords = llm_intent.get("search_criteria", {}).get("text_search", {}).get("must_keywords", [])
            if self._is_low_quality_keywords(llm_keywords):
                return local_intent

            merged_keywords = self._normalize_keyword_pool(str(user_input), list(llm_keywords) + list(local_keywords))
            for kw in local_keywords:
                cleaned = self._clean_text(kw)
                if cleaned and not self._is_noise_keyword(cleaned) and cleaned not in merged_keywords:
                    merged_keywords.append(cleaned)
            llm_intent.setdefault("search_criteria", {}).setdefault("text_search", {})["must_keywords"] = merged_keywords
            llm_intent["intent_rescue"] = "llm"
            return llm_intent
        except Exception as e:
            logger.warning("关键词纠错(LLM)失败: %s", str(e))
            return local_intent

    def _fallback_keywords(self, raw_input: str) -> List[str]:
        text = self._clean_text(raw_input)
        if not text:
            return ["合规审查"]

        candidates: List[str] = []
        seen = set()

        def add_keyword(value: str):
            kw = self._clean_text(value)
            if not kw or len(kw) < 2:
                return
            if self._is_noise_keyword(kw):
                return
            if kw in seen:
                return
            seen.add(kw)
            candidates.append(kw)

        def add_phrase_parts(value: str):
            text_value = self._strip_question_tail(value)
            if not text_value:
                return
            for part in self.PHRASE_SPLIT_RE.split(text_value):
                cleaned = self._strip_question_tail(part)
                if len(cleaned) >= 2:
                    add_keyword(cleaned)

        stripped = self._strip_instruction_prefix(text)
        stripped_action = self._strip_action_prefix(stripped)
        shortened = self._strip_question_tail(stripped)
        shortened_action = self._strip_question_tail(stripped_action)

        add_keyword(text)
        add_keyword(stripped)
        add_keyword(shortened)
        add_keyword(stripped_action)
        add_keyword(shortened_action)
        for topic in self._expand_topic_keywords(text):
            add_keyword(topic)
        for topic in self._expand_topic_keywords(stripped):
            add_keyword(topic)
        for topic in self._expand_topic_keywords(stripped_action):
            add_keyword(topic)
        add_phrase_parts(stripped)
        add_phrase_parts(stripped_action)
        for phrase in self._extract_focus_phrases(text):
            add_keyword(phrase)
        for phrase in self._extract_focus_phrases(stripped):
            add_keyword(phrase)
        for phrase in self._extract_focus_phrases(stripped_action):
            add_keyword(phrase)

        for term in self._extract_protected_terms(stripped):
            add_keyword(term)
        for term in self._extract_protected_terms(stripped_action):
            add_keyword(term)

        for seg in re.split(r"[，,。；;、!?！？\s]+", stripped):
            seg = self._strip_question_tail(seg)
            add_keyword(seg)
            add_phrase_parts(seg)
            for phrase in self._extract_focus_phrases(seg):
                add_keyword(phrase)
            for term in self._extract_protected_terms(seg):
                add_keyword(term)

        for sep in ("与", "和", "及"):
            if sep in stripped:
                for part in stripped.split(sep):
                    add_keyword(part.strip())

        if not candidates:
            fallback = stripped[:12] if stripped else text[:12]
            if fallback:
                add_keyword(fallback)
        if not candidates:
            candidates.append("合规审查")
        return candidates[:8]

    @staticmethod
    def _infer_business_types(raw_input: str) -> List[Dict]:
        text = str(raw_input or "")
        mappings = [
            (["注销", "清算", "吊销转注销", "deregistration", "liquidation", "cancel"], "COMPANY_DEREG", "公司注销"),
            (["担保", "保证人", "反担保", "guarantee"], "EXTERNAL_GUARANTEE", "对外担保"),
            (["采购", "招标", "供应商", "procurement", "purchase"], "PROCUREMENT", "采购管理"),
            (["投资", "并购", "股权收购", "investment", "merger", "acquisition"], "INVESTMENT", "投资并购"),
            (["授权", "审批权限", "签字权限", "authorization", "approval"], "LEGAL_AUTH", "法律授权"),
            (["资产处置", "固定资产", "资产转让", "asset disposal"], "ASSET_DISPOSAL", "资产处置"),
            (["合同", "补充协议", "违约", "contract"], "CONTRACT_REVIEW", "合同审查"),
        ]

        results = []
        for keys, code, name in mappings:
            if any(k in text for k in keys):
                results.append({"type_code": code, "type_name": name, "confidence": 0.85})
        return results

    @staticmethod
    def _infer_intent_type(raw_input: str) -> str:
        text = str(raw_input or "")
        lower_text = text.lower()
        if any(k in text for k in ["请示", "报批", "审查", "审批"]) or any(
            k in lower_text for k in ["review", "approval", "submission", "for approval"]
        ):
            if any(k in text for k in ["同时", "并", "并且", "另外"]):
                return "混合"
            return "请示审查"
        return "事项咨询"

    @staticmethod
    def _infer_domain(raw_input: str) -> str:
        text = str(raw_input or "")
        if any(k in text for k in ["注销", "破产", "清算"]):
            return "企业退出"
        if any(k in text for k in ["税", "发票", "财务", "融资"]):
            return "财税金融"
        if any(k in text for k in ["合同", "违约"]):
            return "合同纠纷"
        if any(k in text for k in ["知识产权", "专利", "商标", "著作权"]):
            return "知识产权"
        if any(k in text for k in ["执法", "处罚", "监管"]):
            return "行政执法"
        return "其他"

    def _infer_query_mode(self, raw_input: str, business_types: List[Dict]) -> str:
        # 明确业务类型优先走结构化检索，不降级为知识讲解模式。
        if business_types:
            return "standard"

        text = self._clean_text(raw_input)
        stripped = self._strip_instruction_prefix(text)
        has_knowledge_hint = any(word in stripped for word in self.KNOWLEDGE_HINT_WORDS)
        has_action_hint = any(word in stripped for word in self.ACTION_HINT_WORDS)
        has_explain_prefix = stripped != text

        if (has_knowledge_hint and not has_action_hint) or (has_explain_prefix and not has_action_hint):
            return "knowledge_explain"
        return "standard"

    def _normalize_keyword_pool(self, raw_input: str, incoming_keywords: List[str]) -> List[str]:
        merged: List[str] = []
        seen = set()

        def put(values: List[str]):
            for kw in values:
                cleaned = self._clean_text(kw)
                if not cleaned or cleaned in seen:
                    continue
                if self._is_noise_keyword(cleaned):
                    continue
                seen.add(cleaned)
                merged.append(cleaned)

        for kw in incoming_keywords:
            put(self._fallback_keywords(kw))
            put(self._extract_focus_phrases(kw))
        put(self._fallback_keywords(raw_input))
        put(self._extract_focus_phrases(raw_input))
        put(self._extract_protected_terms(raw_input))

        if not merged:
            merged = self._fallback_keywords(raw_input)
        return merged[:12]

    def _build_local_intent(self, raw_input: str) -> Dict:
        business_types = self._infer_business_types(raw_input)
        must_keywords = self._fallback_keywords(raw_input)
        for bt in business_types:
            for word in [bt.get("type_name", ""), bt.get("type_code", "")]:
                if word and word not in must_keywords:
                    must_keywords.append(word)

        return {
            "event_summary": raw_input[:80],
            "intent_type": self._infer_intent_type(raw_input),
            "domain": self._infer_domain(raw_input),
            "core_issue": raw_input[:40],
            "query_mode": self._infer_query_mode(raw_input, business_types),
            "business_types": business_types,
            "key_elements": {"subject": "", "action": "", "object": "", "context": ""},
            "compliance_focus": {
                "procedure_topics": [],
                "required_material_topics": [],
                "authorization_topics": [],
                "risk_points": [],
            },
            "search_criteria": {
                "text_search": {
                    "fields": ["name", "description", "golden_quote", "source_title", "content_text"],
                    "must_keywords": must_keywords,
                    "should_keywords": [],
                    "exclude_keywords": [],
                },
                "structured_filters": {
                    "source_tables": ["policy", "case", "speech"],
                    "policy_categories": [],
                    "relation_tables": [
                        "kb_document",
                        "kb_chunk",
                    ],
                    "recency_years": 3,
                },
            },
            "traceability_plan": {
                "clause_trace_required": True,
                "target_entities": ["制度条款", "流程步骤", "材料清单"],
                "output_requirements": ["引用条款编号", "标注制度名称及版本"],
            },
            "priority_suggestion": "优先匹配业务类型，再按条款、流程、材料进行分层检索",
        }

    @staticmethod
    def _normalize_source_tables(source_tables) -> List[str]:
        allowed = {"policy", "case", "speech"}
        if not isinstance(source_tables, list):
            return ["policy", "case", "speech"]

        normalized = []
        for item in source_tables:
            v = str(item).strip().lower()
            if v == "article":
                v = "case"
            if v in allowed and v not in normalized:
                normalized.append(v)
        return normalized or ["policy", "case", "speech"]

    def recognize(self, user_input: str) -> Dict:
        logger.info("[Agent A] 正在执行意图识别与检索指令生成...")

        if not user_input or not str(user_input).strip():
            logger.info("输入为空，无法识别。")
            return {}

        if not self.enable_llm:
            intent_data = self._build_local_intent(str(user_input))
            self._validate_intent_data(intent_data, str(user_input))
            local_keywords = intent_data["search_criteria"]["text_search"]["must_keywords"]
            if self._is_low_quality_keywords(local_keywords):
                intent_data = self._try_llm_keyword_rescue(str(user_input), intent_data)
                self._validate_intent_data(intent_data, str(user_input))
            keywords = intent_data["search_criteria"]["text_search"]["must_keywords"]
            rescue_mode = intent_data.get("intent_rescue", "local")
            logger.info("意图识别完成(本地规则+纠错:%s)，must_keywords=%s", rescue_mode, keywords)
            return intent_data

        try:
            intent_data = self._request_llm_intent(str(user_input))
            self._validate_intent_data(intent_data, str(user_input))
            if self._is_low_quality_keywords(intent_data["search_criteria"]["text_search"]["must_keywords"]):
                local_intent = self._build_local_intent(str(user_input))
                self._validate_intent_data(local_intent, str(user_input))
                intent_data = self._try_llm_keyword_rescue(str(user_input), local_intent)
                self._validate_intent_data(intent_data, str(user_input))

            keywords = intent_data["search_criteria"]["text_search"]["must_keywords"]
            logger.info("意图识别完成，must_keywords=%s", keywords)
            return intent_data
        except Exception as e:
            logger.warning("意图识别异常: %s", str(e))
            fallback = self._build_local_intent(str(user_input))
            fallback["error"] = str(e)
            self._validate_intent_data(fallback, str(user_input))
            return fallback

    def _validate_intent_data(self, data: Dict, raw_input: str):
        if not data.get("event_summary"):
            data["event_summary"] = raw_input[:80]
        if not data.get("intent_type"):
            data["intent_type"] = self._infer_intent_type(raw_input)
        if not data.get("domain"):
            data["domain"] = self._infer_domain(raw_input)
        if not data.get("core_issue"):
            data["core_issue"] = raw_input[:40]

        if not isinstance(data.get("business_types"), list):
            data["business_types"] = []

        cleaned_biz_types = []
        for item in data["business_types"]:
            if not isinstance(item, dict):
                continue
            code = str(item.get("type_code", "")).strip()
            name = str(item.get("type_name", "")).strip()
            raw = f"{code} {name}".lower()
            if not code and not name:
                continue
            if "unknown" in raw or "未知" in raw or raw in {"n/a", "null"}:
                continue
            cleaned_biz_types.append(
                {
                    "type_code": code,
                    "type_name": name,
                    "confidence": float(item.get("confidence", 0.7) or 0.7),
                }
            )
        data["business_types"] = cleaned_biz_types

        if not data["business_types"]:
            data["business_types"] = self._infer_business_types(raw_input)

        query_mode = str(data.get("query_mode", "")).strip().lower()
        if query_mode not in {"standard", "knowledge_explain"}:
            query_mode = self._infer_query_mode(raw_input, data["business_types"])
        data["query_mode"] = query_mode

        if "key_elements" not in data or not isinstance(data["key_elements"], dict):
            data["key_elements"] = {}
        for k in ("subject", "action", "object", "context"):
            data["key_elements"].setdefault(k, "")

        if "compliance_focus" not in data or not isinstance(data["compliance_focus"], dict):
            data["compliance_focus"] = {}
        for k in ("procedure_topics", "required_material_topics", "authorization_topics", "risk_points"):
            if not isinstance(data["compliance_focus"].get(k), list):
                data["compliance_focus"][k] = []

        if "search_criteria" not in data or not isinstance(data["search_criteria"], dict):
            data["search_criteria"] = {}

        text_search = data["search_criteria"].get("text_search")
        if not isinstance(text_search, dict):
            text_search = {}
            data["search_criteria"]["text_search"] = text_search

        if not isinstance(text_search.get("fields"), list) or not text_search.get("fields"):
            text_search["fields"] = ["name", "description", "golden_quote", "source_title", "content_text"]
        if not isinstance(text_search.get("must_keywords"), list):
            text_search["must_keywords"] = []
        if not isinstance(text_search.get("should_keywords"), list):
            text_search["should_keywords"] = []
        if not isinstance(text_search.get("exclude_keywords"), list):
            text_search["exclude_keywords"] = []

        text_search["must_keywords"] = [str(k).strip() for k in text_search["must_keywords"] if str(k).strip()]
        text_search["must_keywords"] = self._normalize_keyword_pool(raw_input, text_search["must_keywords"])
        for term in self._extract_protected_terms(raw_input):
            if term not in text_search["must_keywords"]:
                text_search["must_keywords"].append(term)
        for phrase in self._extract_focus_phrases(raw_input):
            if phrase not in text_search["must_keywords"]:
                text_search["must_keywords"].append(phrase)

        # 已识别业务类型时，强制注入业务词，提高结构化关系命中率。
        for bt in data.get("business_types", []):
            for word in [str(bt.get("type_name", "")).strip(), str(bt.get("type_code", "")).strip()]:
                if word and word not in text_search["must_keywords"]:
                    text_search["must_keywords"].append(word)
        text_search["must_keywords"] = [
            kw for kw in text_search["must_keywords"] if not self._is_noise_keyword(kw)
        ][:12]

        structured_filters = data["search_criteria"].get("structured_filters")
        if not isinstance(structured_filters, dict):
            structured_filters = {}
            data["search_criteria"]["structured_filters"] = structured_filters

        structured_filters["source_tables"] = self._normalize_source_tables(structured_filters.get("source_tables"))
        if not isinstance(structured_filters.get("policy_categories"), list):
            structured_filters["policy_categories"] = []
        if not isinstance(structured_filters.get("relation_tables"), list):
            structured_filters["relation_tables"] = [
                "kb_document",
                "kb_chunk",
            ]
        if not isinstance(structured_filters.get("recency_years"), int):
            structured_filters["recency_years"] = 3

        if "traceability_plan" not in data or not isinstance(data["traceability_plan"], dict):
            data["traceability_plan"] = {}
        data["traceability_plan"].setdefault("clause_trace_required", True)
        if not isinstance(data["traceability_plan"].get("target_entities"), list):
            data["traceability_plan"]["target_entities"] = ["制度条款", "流程步骤", "材料清单"]
        if not isinstance(data["traceability_plan"].get("output_requirements"), list):
            data["traceability_plan"]["output_requirements"] = ["引用条款编号", "标注制度名称及版本"]

        if not data.get("priority_suggestion"):
            data["priority_suggestion"] = "优先匹配业务类型，再按条款、流程、材料进行分层检索"
