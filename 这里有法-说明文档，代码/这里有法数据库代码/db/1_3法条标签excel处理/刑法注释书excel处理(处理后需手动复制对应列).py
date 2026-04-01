import pandas as pd
import pymysql
from pymysql import OperationalError
import os
#%%
# 数据库配置
mysql_config={
              'host': '211.149.136.21',
              'port': 3307,
              'user': 'root',
              'password': 'zjcx111ch9',
              'database': 'LAWdatabase',
              'charset': 'utf8mb4'}
def connect_database():
    """连接MySQL数据库"""
    try:
        conn = pymysql.connect(**mysql_config)
        cursor = conn.cursor()
        print("数据库连接成功!")
        return conn, cursor
    except OperationalError as e:
        print(f"MySQL连接失败: {e}")
        return None, None
#%%
def get_mapping_dict(conn, cursor, db_table, db_num_col, db_provision_col):
    """从数据库获取映射字典"""
    try:
        query = f"SELECT {db_provision_col}, {db_num_col} FROM {db_table} WHERE Law_ID = 2"
        cursor.execute(query)
        results = cursor.fetchall()
        mapping_dict = dict(results)
        print(f"成功读取 {len(mapping_dict)} 条映射关系")
        return mapping_dict
    except Exception as e:
        print(f"读取数据库失败: {e}")
        return None

def add_number_column_to_first(excel_file, db_table, db_num_col, db_provision_col, excel_provision_col, new_num_col='对应数字'):
    """
    将新列插入到Excel第一列
    """
    # 连接数据库
    conn, cursor = connect_database()
    if conn is None:
        return None
    
    try:
        # 1. 读取Excel文件
        df = pd.read_excel(excel_file)
        print(f"成功读取Excel: {excel_file}")
        print(f"原始列名: {list(df.columns)}")
        
        # 2. 获取映射字典
        mapping_dict = get_mapping_dict(conn, cursor, db_table, db_num_col, db_provision_col)
        if mapping_dict is None:
            return None
        
        # 3. 数据清洗和映射
        df[excel_provision_col] = df[excel_provision_col].astype(str).str.strip()
        
        # 4. 创建新列数据
        new_column_data = df[excel_provision_col].map(mapping_dict)
        
        # 5. 将新列插入到第一列位置
        df.insert(0, new_num_col, new_column_data)
        
        # 6. 统计匹配情况
        matched_count = new_column_data.notna().sum()
        print(f"匹配结果: {matched_count}/{len(df)} 条记录")
        
        # 7. 保存到新文件
        output_file = excel_file.replace('.xlsx', '_带数字列.xlsx')
        df.to_excel(output_file, index=False)
        
        # 显示文件位置和结果
        full_path = os.path.abspath(output_file)
        print(f"\n=== 处理完成 ===")
        print(f"输出文件: {full_path}")
        print(f"新增列位置: 第一列")
        print(f"新增列名: {new_num_col}")
        print(f"文件大小: {len(df)} 行")
        print(f"\n处理后列名: {list(df.columns)}")
        
        return df
        
    except Exception as e:
        print(f"处理过程中出错: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# 交互式版本
def add_number_column_interactive():
    """
    交互式版本：自动检测列名并插入第一列
    """
    # 输入Excel文件路径
    excel_file = input("请输入Excel文件路径: ").strip()
    
    if not os.path.exists(excel_file):
        print("文件不存在!")
        return
    
    # 读取Excel并显示列名
    df = pd.read_excel(excel_file)
    print("\nExcel列名:")
    for i, col in enumerate(df.columns, 1):
        print(f"{i}. {col}")
    
    # 选择条文列
    try:
        choice = int(input("\n请选择条文列(输入序号): "))
        excel_provision_col = df.columns[choice-1]
        print(f"已选择: {excel_provision_col}")
    except:
        print("选择无效!")
        return
    
    # 数据库信息
    db_table = input("数据库表名: ")
    db_num_col = input("数据库数字列名: ")
    db_provision_col = input("数据库条文列名: ")
    new_num_col = input("新增列名(默认:对应数字): ") or "对应数字"
    
    # 执行处理
    result = add_number_column_to_first(
        excel_file=excel_file,
        db_table=db_table,
        db_num_col=db_num_col,
        db_provision_col=db_provision_col,
        excel_provision_col=excel_provision_col,
        new_num_col=new_num_col
    )
    
    if result is not None:
        print("\n前5行结果:")
        print(result.head())

# 直接调用版本
def quick_run(excel_file, db_table, db_num_col, db_provision_col, excel_provision_col, new_num_col='对应数字'):
    """
    快速运行版本
    """
    result = add_number_column_to_first(
        excel_file=excel_file,
        db_table=db_table,
        db_num_col=db_num_col,
        db_provision_col=db_provision_col,
        excel_provision_col=excel_provision_col,
        new_num_col=new_num_col
    )
    return result

#%%
# 使用示例
if __name__ == "__main__":
    # 直接使用quick_run
    result_df = quick_run(
        db_table='1_2Provision_content',           # 数据库表名
        excel_file=r"C:\Users\29944\Desktop\2;3刑法注释书.xlsx",          # Excel文件路径
        db_num_col='Provision_ID',             # 数据库中数字列的列名
        db_provision_col='Provision_number',  # 数据库中条文列的列名
        excel_provision_col='条文序号',       # Excel中条文列的列名
        new_num_col='条文ID'                 # 要新增的数字列的列名
    )
    
    if result_df is not None:
        print("\n处理完成！")
        print("前5行结果:")
        print(result_df.head())