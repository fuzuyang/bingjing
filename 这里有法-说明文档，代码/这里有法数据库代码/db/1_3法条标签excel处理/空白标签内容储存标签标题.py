import pandas as pd
import os

def fill_label_content(excel_file):
    """
    当标签内容列为空白时，将标签标题列的内容存入标签内容列
    """
    try:
        # 读取Excel文件
        df = pd.read_excel(excel_file)
        print(f"成功读取Excel: {excel_file}")
        
        # 检查必要的列是否存在
        if '标签内容' not in df.columns or '标签标题' not in df.columns:
            print("错误：缺少必要的列（标签内容 或 标签标题）")
            return None
        
        # 当标签内容列为空时，用标签标题列的内容填充
        mask = df['标签内容'].isna() | (df['标签内容'] == '') | (df['标签内容'].str.strip() == '')
        fill_count = mask.sum()
        
        df.loc[mask, '标签内容'] = df.loc[mask, '标签标题']
        
        # 保存到新文件
        output_file = excel_file.replace('.xlsx', '_填充标签内容.xlsx')
        df.to_excel(output_file, index=False)
        
        print(f"\n=== 处理完成 ===")
        print(f"填充了 {fill_count} 个空白标签内容")
        print(f"输出文件: {output_file}")
        
        return df
        
    except Exception as e:
        print(f"处理过程中出错: {e}")
        return None

# 使用示例
if __name__ == "__main__":
    result_df = fill_label_content(r"C:\Users\29944\Desktop\2;3刑法注释书.xlsx")