import sys
import os

# 确保 Python 能找到根目录下的 core 文件夹
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from core.database import engine, SessionLocal


def drop_specific_table():
    """
    专门删除 biz_source_documents 表的脚本
    """
    print(" 正在连接数据库并准备删除操作...")

    # 安全第一：增加手动确认
    confirm = input(" 警告：这将彻底删除 biz_source_documents 表及其所有数据！确认删除请输入 'YES': ")

    if confirm != 'YES':
        print(" 操作取消。")
        return

    db = SessionLocal()
    try:
        # 1. 关闭外键约束，防止有关联数据时删除失败
        db.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))

        # 2. 执行删除指令
        print(" 正在执行 DROP TABLE...")
        db.execute(text("DROP TABLE IF EXISTS biz_source_documents;"))

        # 3. 提交更改
        db.commit()
        print(" biz_source_documents 表已成功删除。")
        print(" 现在你可以运行 ingester.py，它会自动按 LONGTEXT 规格重建此表。")

    except Exception as e:
        db.rollback()
        print(f" 删除失败: {e}")
    finally:
        db.close()
        db.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))


if __name__ == "__main__":
    drop_specific_table()