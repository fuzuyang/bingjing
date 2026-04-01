import os
import sys

# 1. 路径补丁：确保从 D:\sjtu 目录下能找到 core
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from core import SessionLocal, LegalKnowledge, SourceDocument


def diagnose_database():
    """
    通过显式查询获取知识点及其对应文档标题，检查库内法理储备
    """
    db = SessionLocal()

    # 针对“后备箱集市”和“政策变动”案例设计的核心法理关键词
    test_keywords = ["诚信", "预期", "补偿", "信赖", "裁量", "过渡", "小微", "个体"]

    print("\n" + "=" * 80)
    print(f"🔍 营商环境知识库存量诊断 (当前库内总数: {db.query(LegalKnowledge).count()} 条)")
    print("=" * 80)

    found_any = False

    for kw in test_keywords:
        print(f"\n👉 正在检索关键词: [{kw}]")

        # 核心修复：直接在 query 中指定返回对象和标题字段
        results = db.query(LegalKnowledge, SourceDocument.title).join(
            SourceDocument, LegalKnowledge.source_doc_id == SourceDocument.id
        ).filter(
            (LegalKnowledge.name.contains(kw)) |
            (LegalKnowledge.description.contains(kw))
        ).limit(3).all()

        if not results:
            print(f"   ❌ 未发现包含 '{kw}' 的相关原则。")
        else:
            found_any = True
            # 这里的 r 是 LegalKnowledge 对象，title 是字符串
            for r, title in results:
                print(f"   ✅ [ID: {r.id}] 原则名: {r.name}")
                print(f"      - 描述: {r.description[:60]}...")
                print(f"      - 出处: 《{title[:20]}...》")

    print("\n" + "=" * 80)
    if not found_any:
        print("💡 诊断结论：库内严重缺乏相关数据。")
    else:
        print("💡 诊断结论：数据库里有货！报错前的‘善意文明执行原则’说明库里存了高质量法理。")
    print("=" * 80 + "\n")

    db.close()


if __name__ == "__main__":
    diagnose_database()