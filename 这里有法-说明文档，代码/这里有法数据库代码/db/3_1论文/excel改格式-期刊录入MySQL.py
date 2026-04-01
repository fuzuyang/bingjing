import pandas as pd
import pymysql
import os
from pymysql.err import OperationalError
#%% 
mysql_config = {
    'host': '211.149.136.21',
    'port': 3406,
    'user': 'root', 
    'password': '@#Zxli*313100',
    'database': 'LAWdatabase',
    'charset': 'utf8mb4'
}
#%%  def process_excel_file(file_path)
# 读取Excel文件
def process_excel_file(file_path):
    df = pd.read_excel(file_path, 
                       sheet_name="文献列表", 
                       dtype=str)
    # 1. 删除特定的列
    columns_to_delete = ['序号', '数据库', '引用次数','下载次数','分类号','页数','URL']  # 修改为你要删除的列名
    df = df.drop(columns=columns_to_delete, errors='ignore')  # errors='ignore'防止列不存在时报错
   
    # 2. 将其他列名改为特定的名称
    remaining_columns = list(df.columns)
    new_column_names = ['Paper_title', 'Author', 'Affiliation', 'Keywords', 'Abstract','Issue','Publication_date']  # 根据实际列数调整
    
    # 检查列数是否匹配
    if len(remaining_columns) == len(new_column_names):
        df.columns = new_column_names
    else:
        print(f"列数不匹配！当前有{len(remaining_columns)}列，但提供了{len(new_column_names)}个新列名")
        # 自动生成列名
    # 3. 新增列名为F的第六列（初始值为空）
    df.insert(5, 'Journal', '')
    df.insert(8, 'Official_account', '')
    df.insert(9, 'Link', '')
    
    # 4. 删除列名为keyword中值为"无关键词"的行
    # 先检查是否有keyword列（可能是重命名后的列）
    keyword_column = None
    for col in df.columns:
        if 'Keywords' in col.lower() or col == 'Keywords':
            keyword_column = col
            break

    if keyword_column:
        # 删除值为"无关键词"的行
        before_count = len(df)
        df = df[df[keyword_column] != "无关键词"]
        after_count = len(df)
        print(f"删除了 {before_count - after_count} 行包含'无关键词'的数据")
    else:
        print("未找到keyword列，跳过删除操作")
    
    #删除Abstract中“无摘要”的行
    abstract_column = None
    for col in df.columns:
        if 'Abstract' in col.lower() or col == 'Abstract':
            abstract_column = col
            break

    if abstract_column:
        # 删除值为"无摘要"的行
        before_count = len(df)
        df = df[df[abstract_column] != "无摘要"]
        after_count = len(df)
        print(f"删除了 {before_count - after_count} 行包含'无摘要'的数据")
    else:
        print("未找到abstract列，跳过删除操作")
    # 显示处理后的数据信息
    print(f"\n处理完成！")
    print(f"最终数据形状: {df.shape}")
    print(f"列名: {list(df.columns)}")
    return df

# 保存处理后的数据（可选）
# df.to_excel('处理后的数据.xlsx', index=False)

#%% def insert_to_mysql(df, table_name)
def insert_to_mysql(df, table_name):
    """将DataFrame插入MySQL"""
    try:
        conn = pymysql.connect(**mysql_config)
        cursor = conn.cursor()
        
        # 构建INSERT语句
        columns = "`,`".join(df.columns)
        placeholders = ",".join(["%s"] * len(df.columns))
        insert_sql = f"INSERT INTO `{table_name}` (`{columns}`) VALUES ({placeholders})"
        
        # 转换数据：将NaN替换为None
        data_list = []
        for row in df.values:
            # 将numpy的nan转换为None
            converted_row = []
            for value in row:
                # 检查是否是NaN值（包括numpy.nan和pandas.na）
                if pd.isna(value):
                    converted_row.append(None)
                else:
                    converted_row.append(value)
            data_list.append(tuple(converted_row))
        
        # 批量插入
        cursor.executemany(insert_sql, data_list)
        conn.commit()
        
        # 获取插入的ID范围
        cursor.execute("SELECT LAST_INSERT_ID()")
        first_id = cursor.fetchone()[0]
        last_id = first_id + len(data_list) - 1 if len(data_list) > 1 else first_id
        
        print(f"  成功入库 {len(data_list)} 条数据")
        print(f"  ID范围: {first_id} - {last_id}")
        
        cursor.close()
        conn.close()
        return True
    
    except Exception as e:
        print(f"  入库失败: {e}")
        return False
#%% def process_folder(folder_path, mysql_table_name)
def process_folder(folder_path, mysql_table_name):
    """处理文件夹中的所有Excel文件"""
    # 获取所有Excel文件
    excel_files = []
    for file in os.listdir(folder_path):
        if file.endswith(('.xls', '.xlsx')):
            excel_files.append(os.path.join(folder_path, file))
    
    if not excel_files:
        print("文件夹中没有找到Excel文件")
        return
    
    print(f"找到 {len(excel_files)} 个Excel文件")
    print("=" * 50)
    
    total_success = 0
    total_records = 0
    
    for file_path in excel_files:
        # 处理Excel文件
        df = process_excel_file(file_path)
        
        if df is not None and not df.empty:
            # 插入MySQL
            if insert_to_mysql(df, mysql_table_name):
                total_success += 1
                total_records += len(df)
    
    
        print("-" * 30)
    
    print("=" * 50)
    print(f"处理完成！成功处理 {total_success}/{len(excel_files)} 个文件")
    print(f"总共入库 {total_records} 条记录")
#%%

process_folder(r"D:\这里有法_资料\文书资料\3-2019十六本法学期刊摘要(已入库,剔除无关键词无摘要)\2019十六本法学期刊摘要","3_1Paper")
