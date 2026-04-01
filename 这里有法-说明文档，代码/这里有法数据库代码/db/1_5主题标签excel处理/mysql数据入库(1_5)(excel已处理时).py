# 需要修改的为excel表文件路径、录入的MySQL表
#%%
import pandas as pd
import pymysql
from pymysql.err import OperationalError, ProgrammingError

#读取excel数据
df = pd.read_excel(r"C:\Users\29944\Desktop\编.xlsx",  #要录入的excel表
                 dtype={2: int, 3: int})  
df = df.fillna("")

print(df.dtypes)
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
insert_sql=f"INSERT INTO 1_5Topic_label({columns}) VALUES ({placeholders})"   #表名在此

#转换数据格式(dataframe→元组列表，pymysql要求)
data_list=[tuple(row) for row in df.values]

#执行插入
try:
    cursor.executemany(insert_sql, data_list)  #批量插入
    conn.commit()
    print(f"成功入库 {cursor.rowcount} 条数据")
except Exception as e:
    conn.rollback()  #失败回滚
    print(f"入库失败：{e}")
    print("生成的SQL语句：", insert_sql)
    
finally:  #关闭连接
    cursor.close()
    conn.close
