import json
import sys

from modules.evaluator.analyzer import PolicySpiritAnalyzer
from modules.evaluator.answer_generator import GroundedAnswerGenerator
from modules.evaluator.intent_recognizer import IntentRecognizer
from modules.evaluator.retriever import PolicyRetriever


def _print_json(title: str, data):
    print(f"\n=== {title} ===")
    print(json.dumps(data, ensure_ascii=False, indent=2))


def run_debug(user_input: str):
    recognizer = IntentRecognizer()
    retriever = PolicyRetriever()
    analyzer = PolicySpiritAnalyzer()
    generator = GroundedAnswerGenerator()

    # 1) 问题改写
    intent = recognizer.recognize(user_input)
    rewrite_result = {
        "query_mode": intent.get("query_mode"),
        "core_issue": intent.get("core_issue"),
        "must_keywords": intent.get("search_criteria", {}).get("text_search", {}).get("must_keywords", []),
        "business_types": intent.get("business_types", []),
    }
    _print_json("1. 问题改写", rewrite_result)

    # 2) 向量召回 + 关系补结构
    retrieved = retriever.retrieve(intent)
    retrieval_result = {
        "summary": retrieved.get("summary", {}),
        "clauses_head": retrieved.get("clauses", [])[:3],
        "fallback_head": retrieved.get("fallback_knowledge", [])[:3],
    }
    _print_json("2. 检索召回", retrieval_result)

    # 3) 拼上下文
    analysis = analyzer.analyze_compliance(user_input, intent, retrieved)
    context_result = {
        "summary": analysis.get("summary"),
        "query_mode": analysis.get("query_mode"),
        "context_meta": {
            "context_count": analysis.get("llm_context", {}).get("context_count", 0),
            "context_chars": analysis.get("llm_context", {}).get("context_chars", 0),
        },
        "context_preview": (analysis.get("llm_context", {}).get("context_text", "") or "")[:800],
    }
    _print_json("3. 拼上下文", context_result)

    # 4) 大模型生成
    generated = generator.generate(user_input, intent, analysis)
    gen_result = {
        "mode": generated.get("mode"),
        "answer": generated.get("answer"),
        "citations": generated.get("citations", []),
        "confidence": generated.get("confidence"),
        "error": generated.get("error"),
    }
    _print_json("4. 大模型生成", gen_result)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        question = " ".join(sys.argv[1:])
    else:
        question = "讲述一下公司的指导思想"
    run_debug(question)
