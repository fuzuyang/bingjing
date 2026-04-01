import sys
import os

# 确保能找到项目根目录下的 core
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from core.database import engine, SessionLocal


def clean_database():
    """
    清空数据库中的原始文档表，以便重新开始干净的同步
    """
    print(" 正在准备清空数据库...")

    # 增加一个人机交互确认，防止手抖误删
    confirm = input("此操作将删除 biz_source_documents 表中的所有数据，不可恢复！确认请输入 'yes': ")
    if confirm.lower() != 'yes':
        print(" 操作已取消。")
        return

    db = SessionLocal()
    try:
        # 1. 关闭外键检查（如果有其他表关联了它，防止报错）
        db.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))

        # 2. 使用 TRUNCATE 清空表并重置自增 ID (比 DELETE 快且彻底)
        print(" 正在清空 biz_source_documents...")
        db.execute(text("TRUNCATE TABLE biz_source_documents;"))

        # 3. 如果你想连带清空知识库表，可以取消下面这行的注释
        # db.execute(text("TRUNCATE TABLE biz_legal_knowledge;"))

        # 4. 恢复外键检查
        db.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))

        db.commit()
        print(" 数据库已清理干净，现在你可以重新运行 ingester.py 了。")

    except Exception as e:
        db.rollback()
        print(f" 清理失败: {e}")
    finally:
        db.close()


if __name__ == "__main__":
    clean_database()