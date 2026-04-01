import pandas as pd

def fix_excel_topic_id(input_file, output_file):
    """
    修复Excel文件中的Topic_ID列并保存为新文件
    
    参数:
    input_file: 输入Excel文件路径
    output_file: 输出Excel文件路径（你自己指定的位置）
    """
    try:
        print(f"正在读取: {input_file}")
        
        # 读取Excel文件
        df = pd.read_excel(input_file)
        print(f"读取成功: {len(df)} 行, {len(df.columns)} 列")
        
        # 显示原始Topic_ID信息
        if 'Topic_ID' in df.columns:
            print(f"\n原始Topic_ID信息:")
            print(f"  数据类型: {df['Topic_ID'].dtype}")
            print(f"  唯一值数量: {df['Topic_ID'].nunique()}")
            
            # 检查非数字值
            non_numeric = []
            for idx, value in df['Topic_ID'].items():
                try:
                    float(str(value).strip())
                except:
                    non_numeric.append((idx, value))
            
            if non_numeric:
                print(f"  发现 {len(non_numeric)} 个非数字值")
                for idx, value in non_numeric[:5]:  # 显示前5个
                    print(f"    行{idx+1}: {repr(value)}")
        
        # 修复Topic_ID
        def fix_value(x):
            """修复单个值"""
            try:
                if pd.isna(x):
                    return 0
                
                str_val = str(x).strip()
                if not str_val:
                    return 0
                
                # 尝试转换为数字
                num = float(str_val)
                return int(num) if num.is_integer() else int(round(num))
            except:
                return -1  # 非数字值设为-1
        
        if 'Topic_ID' in df.columns:
            original_topic_id = df['Topic_ID'].copy()  # 备份原始值
            df['Topic_ID'] = df['Topic_ID'].apply(fix_value)
            
            # 统计修复结果
            special_count = (df['Topic_ID'] == -1).sum()
            zero_count = (df['Topic_ID'] == 0).sum()
            
            print(f"\n修复完成:")
            print(f"  非数字值设为-1: {special_count} 行")
            print(f"  空值设为0: {zero_count} 行")
            print(f"  最小值: {df['Topic_ID'].min()}")
            print(f"  最大值: {df['Topic_ID'].max()}")
            print(f"  新数据类型: {df['Topic_ID'].dtype}")
        
        # 保存为新文件
        df.to_excel(output_file, index=False)
        print(f"\n已保存为: {output_file}")
        
        # 验证保存结果
        saved_df = pd.read_excel(output_file)
        print(f"验证: 新文件有 {len(saved_df)} 行, {len(saved_df.columns)} 列")
        
        return True
        
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()
        return False

# ==================== 使用示例 ====================
if __name__ == "__main__":
    # 原始文件（你自己修改这个路径）
    input_file = r"C:\Users\29944\Desktop\10002民事司法观点集成.xlsx"
    
    # 新文件保存位置（你自己指定）
    output_file = r"C:\Users\29944\Desktop\10002民事司法观点集成_修复后.xlsx"
    
    print("="*60)
    print("Excel文件修复工具")
    print("="*60)
    
    # 执行修复
    success = fix_excel_topic_id(input_file, output_file)
    
    if success:
        print("\n✓ 修复完成！")
        print(f"原始文件: {input_file}")
        print(f"修复后文件: {output_file}")
    else:
        print("\n✗ 修复失败")
    
    print("="*60)