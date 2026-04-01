import pandas as pd
import re
import cn2an

# 需修改的为excel文件路径、输出文件路径：第6、24行
df=pd.read_excel(r"C:\Users\29944\Desktop\26劳动法司法解释_副本2025-11-20.xlsx")
first_column_name=df.columns[0]

def extract_first_number(text):
    chinese_number=u"零一二三四五六七八九十百千万拾〇"
    match=re.search(r'第(['+chinese_number+r']+)条',str(text))
    if match:
        chinese_num=match.group(1)
        try:
            return int(cn2an.cn2an(chinese_num,"smart"))
        except:
            return 0
    return 0

provision_ids=df[first_column_name].apply(extract_first_number)
df.insert(0,'Provision_ID',provision_ids)

df.to_excel(r"C:\Users\29944\Desktop\88;31.xlsx",index=False)
print("处理完成")
   