# 需要修改的为excel表文件路径、录入的MySQL表
# 需要确认合并基准列
#%%
import pandas as pd
import pymysql
from pymysql.err import OperationalError, ProgrammingError
import os

#读取excel数据
file=r"C:\Users\29944\Desktop\136;32.xlsx"  #要录入的excel表
#%%
def merge_labelcontent(excel_file, provision_id_col='Provision_ID', label_content_col='Label_content'):
    """
    合并相同Provision_ID和Label_type的行的Label_content，其他列保留第一行的内容
    """
    try:
        # 读取Excel文件
        df = pd.read_excel(excel_file,
                           dtype={0:int,1:int,2:int})
        df = df.fillna("")
        print(f"成功读取Excel: {excel_file}")
        print(f"原始数据行数: {len(df)}")
        
        # 检查必要的列是否存在
        if provision_id_col not in df.columns:
            print(f"错误：缺少 {provision_id_col} 列")
            return None
        if label_content_col not in df.columns:
            print(f"错误：缺少 {label_content_col} 列")
            return None
        if 'Label_type' not in df.columns:
            print(f"错误：缺少 Label_type 列")
            return None
        
        # 按Provision_ID和Label_type分组，合并Label_content
        def merge_group(group):
            # 其他列取第一行的值
            result = group.iloc[0].copy()
            # 合并Label_content
            result[label_content_col] = ''.join(str(x) for x in group[label_content_col] if pd.notna(x))
            return result
        
        # 修复分组警告：使用 include_groups=False
        merged_df = df.groupby([provision_id_col, 'Label_type'], as_index=False).apply(merge_group, include_groups=False).reset_index(drop=True)
        
        print(f"合并后数据行数: {len(merged_df)}")
        print(f"合并了 {len(df) - len(merged_df)} 行数据")
        
        # 显示统计信息
        print(f"\n=== 统计信息 ===")
        print(f"唯一的Provision_ID数量: {merged_df[provision_id_col].nunique()}")
        print(f"Label_content平均长度: {merged_df[label_content_col].str.len().mean():.1f} 字符")
        
        return merged_df
        
    except Exception as e:
        print(f"处理过程中出错: {e}")
        import traceback
        traceback.print_exc()
        return None

df = merge_labelcontent(excel_file=file)
#%%
#连接MySQL
mysql_config={
              'host': '211.149.136.21',
              'port': 3307,
              'user': 'root',
              'password': 'zjcx111ch9',
              'database': 'LAWdatabase',
              'charset': 'utf8mb4'}
#%%
try:
    conn=pymysql.connect(**mysql_config)  #字典解包
    cursor=conn.cursor()

except OperationalError as e:
    print(f"MySQL连接失败:{e}")
    exit()
    
#插入数据
columns = ",".join(df.columns)  #获取excel列名
placeholders=",".join(["%s"] *len(df.columns))  #占位符,适配pymysql
insert_sql=f"INSERT INTO 1_3Provision_label({columns}) VALUES ({placeholders})"   #表名在此

#转换数据格式(dataframe→元组列表，pymysql要求)
data_list=[tuple(row) for row in df.values]

# 执行分批插入
try:
    batch_size = 1000  # 每批插入1000条数据
    total_rows = len(data_list)
    success_count = 0
    
    for i in range(0, total_rows, batch_size):
        batch = data_list[i:i + batch_size]
        cursor.executemany(insert_sql, batch)
        conn.commit()
        success_count += len(batch)
        print(f"已成功插入 {success_count}/{total_rows} 条数据")
    
    print(f"全部数据入库完成！共插入 {success_count} 条数据")
    
except Exception as e:
    conn.rollback()  # 失败回滚
    print(f"入库失败：{e}")
    print("生成的SQL语句：", insert_sql)
    
finally:  # 关闭连接
    cursor.close()
    conn.close()