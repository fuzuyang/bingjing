import sys
import os

# 1. 路径补丁：确保能找到 core 文件夹
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from sqlalchemy import text
from core.database import SessionLocal

def reset_database():
    """
    全量重置数据库：清空原始文档表和关联的知识点表。
    使用 TRUNCATE 而非 DELETE，以重置自增 ID 并提升速度。
    """
    print("\n" + "!" * 60)
    print("  警告：此操作将清空所有已导入的文档及提炼的知识点！")
    print("  涉及表：biz_source_documents, biz_legal_knowledge")
    print("!" * 60 + "\n")

    confirm = input("确定要执行全量重置吗？确认请输入 'RESET': ")
    if confirm != 'RESET':
        print("操作已取消。")
        return

    db = SessionLocal()
    try:
        # 1. 关闭外键检查，防止删除失败
        db.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))

        # 2. 清空知识点表（必须先于文档表清理，因为有外键关联）
        print("正在清理知识库表 (biz_legal_knowledge)...")
        db.execute(text("TRUNCATE TABLE biz_legal_knowledge;"))

        # 3. 清空原始文档表
        print("正在清理原始文档表 (biz_source_documents)...")
        db.execute(text("TRUNCATE TABLE biz_source_documents;"))

        # 4. 恢复外键检查
        db.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))

        db.commit()
        print("\n✅ 数据库已清理干净。")
        print("现在你可以开始执行更高标准的‘数据清洗入库’流程了。")

    except Exception as e:
        db.rollback()
        print(f"❌ 重置失败: {str(e)}")
    finally:
        db.close()

if __name__ == "__main__":
    reset_database()