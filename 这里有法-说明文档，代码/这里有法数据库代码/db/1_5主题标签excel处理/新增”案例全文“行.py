import pandas as pd

def add_full_text_rows(input_file, output_file):
    """
    为每个案例编号添加案例全文行
    
    参数:
    input_file: 输入Excel文件路径
    output_file: 输出Excel文件路径
    """
    # 读取Excel文件
    df = pd.read_excel(input_file)
    
    # 确保列名正确
    required_columns = ['案例编号', '案例标题', '案例全文', '标签类型', '标签标题', '标签内容']
    if not all(col in df.columns for col in required_columns):
        print("列名不匹配，请检查文件列名")
        return
    
    # 用于存储新DataFrame的列表
    result_rows = []
    
    # 记录上一个案例编号，用于检测案例是否变化
    prev_case_id = None
    
    # 遍历每一行
    for index, row in df.iterrows():
        current_case_id = row['案例编号']
        
        # 如果案例编号变化（或者是第一行），添加案例全文行
        if current_case_id != prev_case_id or prev_case_id is None:
            # 添加案例全文行
            new_row = {
                '案例编号': current_case_id,
                '案例标题': row['案例标题'],
                '案例全文': row['案例全文'],
                '标签类型': '案例全文',
                '标签标题': '案例全文',
                '标签内容': row['案例全文']
            }
            result_rows.append(new_row)
            
            # 更新上一个案例编号
            prev_case_id = current_case_id
        
        # 添加原始行
        result_rows.append(row.to_dict())
    
    # 将结果转换为DataFrame
    result_df = pd.DataFrame(result_rows)
    
    # 保存到Excel
    result_df.to_excel(output_file, index=False)
    
    print(f"处理完成！")
    print(f"原数据行数: {len(df)}")
    print(f"新增行数: {len(result_df) - len(df)}")
    print(f"总行数: {len(result_df)}")
    
    # 调试信息：检查案例编号变化次数
    print(f"\n案例编号变化次数: {len(result_rows) - len(df)}")
    
    return result_df

# 使用示例
add_full_text_rows(r"C:\Users\29944\Desktop\民一庭观点六合一 - 副本 (6).xlsx", r"C:\Users\29944\Desktop\民一庭观点.xlsx")