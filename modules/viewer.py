# D:\sjtu\modules\viewer.py
import sys
import os
import pandas as pd
from sqlalchemy import text

# 路径补丁：确保能找到 core 文件夹
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from core.database import engine


def view_knowledge():
    """
    使用 SQL 关联查询，将提取出的知识点与原始文章标题匹配展示
    """
    # SQL 逻辑：连接 (JOIN) 知识表和原始文件表
    query = """
    SELECT 
        k.id as '编号',
        d.title as '原始出处',
        k.name as '核心原则',
        k.domain as '所属领域',
        k.description as '原则描述',
        k.golden_quote as '金句支撑'
    FROM biz_legal_knowledge k
    JOIN biz_source_documents d ON k.source_doc_id = d.id
    ORDER BY k.id DESC;
    """

    try:
        # 使用 Pandas 优雅地展示表格
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)

        # 设置 Pandas 显示参数，确保在控制台不被折断
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_colwidth', 40)
        pd.set_option('display.width', 1000)

        print("\n" + "=" * 100)
        print("  营商环境法理知识库 - 提取成果预览")
        print("=" * 100)

        if df.empty:
            print("  知识库目前为空，请先运行 extractor.py。")
        else:
            print(df)

        print("=" * 100 + "\n")
        print(f" 当前知识库总条数: {len(df)}")

    except Exception as e:
        print(f" 读取失败: {e}")


if __name__ == "__main__":
    view_knowledge()