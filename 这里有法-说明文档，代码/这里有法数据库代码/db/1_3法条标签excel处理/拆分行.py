#%%
import pandas as pd

def split_excel_rows(input_file, output_file):
    """
    将Excel的每行拆分
    """
    # 读取文件，不读取标题行
    df = pd.read_excel(input_file, header=None)
    df = df.fillna('')  # 将NaN替换为空字符串
    
    print(f"原数据有 {len(df)} 行，{len(df.columns)} 列")
    
    # 提取数据部分（去掉第一行标题）
    data = df.iloc[1:]  # 从第二行开始（索引1）
    
    # 创建第分组数据
    df1 = data.iloc[:, [0, 1, 2]].copy()  
    df2 = data.iloc[:, [0, 3, 4]].copy()  
    df3 = data.iloc[:, [0, 5, 6]].copy()
    df4 = data.iloc[:, [0, 7, 8]].copy()
    
    # 重命名列，确保都是3列
    df1.columns = range(3)
    df2.columns = range(3)
    df3.columns = range(3)
    df4.columns = range(3)
    
    # 合并两组数据，重置索引
    result = pd.concat([df1, df2, df3, df4], ignore_index=True)
    
    # 在第三列插入新列，内容为最后一列的第一句话
    def get_first_sentence(text):
        if not text or pd.isna(text):
            return ""
        text = str(text)
        # 简单的句子分割：按句号、问号、感叹号分割
        sentences = text.split('。')[0]  # 取第一句话
        return sentences.strip()
    
    # 在第三列位置插入新列
    result.insert(2, 'new_column', result.iloc[:, -1].apply(get_first_sentence))
    
    # 保存结果，不包含标题
    result.to_excel(output_file, index=False, header=False)
    print(f"处理完成！新数据有 {len(result)} 行")
    
    return result
#%%
# 使用示例
if __name__ == "__main__":
    input_file = r"C:\Users\29944\Desktop\4;5刑事诉讼法-法条注释.xlsx"    # 你的输入文件名
    output_file = r"C:\Users\29944\Desktop\4;5.xlsx"   # 输出文件名
    
    split_excel_rows(input_file, output_file)