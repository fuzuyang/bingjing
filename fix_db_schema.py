# D:\sjtu\fix_db_schema.py
import sys
import os
from sqlalchemy import text

# 1. 确保能找到 core.py
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from core import engine


def fix_risk_level_column():
    """
    修复 biz_risk_assessments 表中 risk_level 字段长度过短导致的截断错误
    """
    print("🛠️  正在检查并修复数据库字段...")

    # 使用原始 SQL 执行 ALTER TABLE 操作
    # 将 risk_level 修改为 VARCHAR(20)，确保能存下 "高风险"、"中风险" 等中文字符
    alter_sql = text("ALTER TABLE biz_risk_assessments MODIFY COLUMN risk_level VARCHAR(20);")

    try:
        with engine.connect() as connection:
            # 执行修改命令
            connection.execute(alter_sql)
            # 提交更改
            connection.commit()
            print(" 修复成功：'risk_level' 字段长度已扩展至 20 个字符。")

    except Exception as e:
        if "Unknown column" in str(e):
            print(" 错误：找不到该表或字段，请确认你已经运行过核心初始化脚本。")
        else:
            print(f" 修复过程中发生意外错误: {e}")


if __name__ == "__main__":
    fix_risk_level_column()