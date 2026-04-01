import pandas as pd

def add_sequence_column(input_file, output_file, new_col_name='Topic_ID'):
    """
    在Excel中新增一列为第一列，当原始第一列变动时，新增列数值加1
    
    参数:
    input_file: 输入Excel文件路径
    output_file: 输出Excel文件路径
    new_col_name: 新增列的列名，默认为'序号'
    """
    # 读取Excel文件
    df = pd.read_excel(input_file)
    
    if len(df) == 0:
        print("Excel文件为空")
        return df
    
    # 获取原始第一列的列名
    original_first_col = df.columns[0]
    
    # 计算序号：当值变化时序号加1
    # 1. 检查每一行是否与上一行不同
    # 2. 将变化标记累积求和
    # 3. 序号从1开始，所以+1
    sequence = (df[original_first_col] != df[original_first_col].shift()).cumsum()
    
    # 将新列插入到第一列位置
    df.insert(0, new_col_name, sequence)
    
    # 保存到Excel
    df.to_excel(output_file, index=False)
    
    # 输出处理信息
    print("处理完成！")
    # 显示前几行结果供验证
    
    return df

# 使用示例
if __name__ == "__main__":
    add_sequence_column(
        input_file=r"C:\Users\29944\Desktop\10009刑事证据2025-11-22_22-53-14.xlsx",
        output_file=r"C:\Users\29944\Desktop\10009_有序号.xlsx"
    )
