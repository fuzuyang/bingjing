import pandas as pd
import cn2an

def int_to_chinese(row):
    """将整数转换为中文数字"""
    try:
        return cn2an.an2cn(row['Topic_title'])
    except:
        return str(row['Topic_title'])

# 读取Excel文件
df = pd.read_excel(r"C:\Users\29944\Desktop\10001(需修改)海商法(不是逐条解释).xlsx")

# 确保Topic_title列存在且为整数类型
df['Topic_title'] = df['Topic_title'].astype(int)

# 创建新的中文数字列
df['Topic_title_中文'] = df.apply(int_to_chinese, axis=1)

# 保存到新文件
df.to_excel(r"C:\Users\29944\Desktop\10001海商法.xlsx", index=False)