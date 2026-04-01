import pandas as pd
import pymysql
import os
from pymysql.err import OperationalError
import re

#%% 
mysql_config = {
    'host': '211.149.136.21',
    'port': 3406,
    'user': 'root', 
    'password': '@#Zxli*313100',
    'database': 'LAWdatabase',
    'charset': 'utf8mb4'
}

#%% 录入原创为“是”的公众号数据
#  含其他筛选条件，存在不完全筛选情况
def process_folder(folder_path, mysql_table_name):
    """处理文件夹中的所有CSV文件"""
    
    # 获取所有CSV文件
    csv_files = []
    for file in os.listdir(folder_path):
        if file.lower().endswith('.csv'):
            csv_files.append(os.path.join(folder_path, file))
    
    if not csv_files:
        print("文件夹中没有找到CSV文件")
        return
    
    print(f"找到 {len(csv_files)} 个CSV文件")
    print("=" * 50)
    
    total_success = 0
    total_records = 0
    
    # 定义需要排除的文章标题字符串列表（精简版）
    excluded_strings = [
        "目录","要目", "内容提要", "摘要", "正文", "注释体例", "节快乐", "创作谈","寄语",
        "征稿启事", "征文启事", "欢迎订阅", "征订单", "请假帖", "暂停更新", "征文", "征稿",
        "恭祝", "正式通知", "盘点 |", "成功举办", "举行", "喜讯", "作者推介", "召开",
        "收取版面费等费用", "喜讯", "【","合集","公告", "汇总"
    ]
    
    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        print(f"正在处理: {file_name}")
        
        try:
            # 读取CSV文件
            df = pd.read_csv(file_path, dtype=str)
            
            # 1. 只保留"原创"列为"是"的行
            if '原创' in df.columns:
                df = df[df['原创'] == '是']
                print(f"  原创为'是'的行数: {len(df)}")
            else:
                print(f"  警告: {file_name} 中没有'原创'列")
                continue
            
            if df.empty:
                print(f"  跳过: 没有原创为'是'的数据")
                continue
            
            # 2. 只处理需要的列
            needed_columns = ['公众号', '发表期号', '文章标题', '作者', '作者单位', 
                            '内容提要', '关键词', '发布时间', '文章摘要', '期刊名称', '文章链接']
            existing_columns = [col for col in needed_columns if col in df.columns]
            
            if len(existing_columns) < len(needed_columns):
                missing_cols = set(needed_columns) - set(existing_columns)
                print(f"  警告: 缺少列 {missing_cols}")
            
            df = df[existing_columns].copy()
            
            # 3. 处理关键词列 - 将空格分隔改为英文分号分隔
            if '关键词' in df.columns:
                df['关键词'] = df['关键词'].apply(
                    lambda x: '; '.join([kw.strip() for kw in str(x).split() if kw.strip()]) 
                    if pd.notna(x) and str(x).strip() != '' else None
                )
            
            # 4. 修复作者单位字段过长问题 - 截断过长的作者单位
            if '作者单位' in df.columns:
                # 检测并截断过长的作者单位（假设正常作者单位不超过200字符）
                def truncate_affiliation(x):
                    if pd.isna(x):
                        return None
                    x_str = str(x)
                    if len(x_str) > 200:
                        # 如果太长，可能是错误数据，只取前200字符
                        return x_str[:200] + "..."
                    return x_str
                
                df['作者单位'] = df['作者单位'].apply(truncate_affiliation)
                print(f"  已处理过长的作者单位字段")
            
            # 5. 改进的作者提取功能（针对华政法学等文件）
            if '文章标题' in df.columns and '作者' in df.columns:
                def improved_extract_author(row):
                    """改进的作者提取函数"""
                    title = str(row['文章标题']) if pd.notna(row['文章标题']) else ''
                    author = str(row['作者']) if pd.notna(row['作者']) else ''
                    
                    # 如果作者不为空，直接返回
                    if author.strip() and author.lower() != 'nan':
                        return author
                    
                    # 尝试从标题中提取作者（更全面的提取逻辑）
                    patterns = [
                        # 格式: 作者：标题
                        r'^([^：:]+?)[：:]\s*(.+)',
                        # 格式: 作者｜标题  
                        r'^([^\|]+?)\s*\|\s*(.+)',
                        # 格式: 作者-标题
                        r'^(.+?)\s*-\s*(.+)',
                        # 格式: 作者 标题（空格分隔）
                        r'^(\S+?\s+\S+?)\s+(.+)',  # 至少两个词作为作者名
                    ]
                    
                    for pattern in patterns:
                        match = re.match(pattern, title.strip())
                        if match:
                            extracted_author = match.group(1).strip()
                            # 清理作者名
                            extracted_author = extracted_author.replace('《', '').replace('》', '')
                            # 移除常见的非作者部分
                            if '：' in extracted_author or ':' in extracted_author or '|' in extracted_author:
                                continue
                            if extracted_author and len(extracted_author) < 50:  # 防止提取到过长内容
                                return extracted_author
                    
                    return author  # 返回原值（空值）
                
                # 应用提取函数
                mask = (df['作者'].isna()) | (df['作者'].astype(str).str.strip() == '') | (df['作者'].astype(str).str.lower() == 'nan')
                if mask.sum() > 0:
                    print(f"  需要提取作者的行数: {mask.sum()}")
                    df.loc[mask, '作者'] = df[mask].apply(improved_extract_author, axis=1)
                    
                    # 检查提取效果
                    after_extract_mask = (df['作者'].isna()) | (df['作者'].astype(str).str.strip() == '') | (df['作者'].astype(str).str.lower() == 'nan')
                    extracted_count = mask.sum() - after_extract_mask.sum()
                    print(f"  成功提取作者: {extracted_count} 条")
            
            # 6. 过滤文章标题
            if '文章标题' in df.columns:
                before_filter_count = len(df)
                
                # 创建总过滤掩码
                total_mask = pd.Series(False, index=df.index)
                
                # 6.1 检查所有排除字符串
                for exclude_str in excluded_strings:
                    mask = df['文章标题'].astype(str).str.contains(
                        re.escape(exclude_str), case=False, na=False, regex=True
                    )
                    total_mask = total_mask | mask
                
                # 6.2 删除完全匹配"《xx》xx年第xx期"格式的行
                exact_pattern = r'^\s*《[^》]*》\s*[^》]*年\s*第\s*[^期]*期\s*$'
                pattern_mask = df['文章标题'].astype(str).str.match(exact_pattern, na=False)
                total_mask = total_mask | pattern_mask
                
                # 一次性应用所有过滤
                df = df[~total_mask]
                
                after_filter_count = len(df)
                filtered_count = before_filter_count - after_filter_count
                print(f"  过滤文章标题后保留: {after_filter_count} 行 (删除: {filtered_count} 行)")
            
            if df.empty:
                print(f"  跳过: 过滤后无有效数据")
                continue
            
            # 7. 重命名列名
            column_mapping = {
                '公众号': 'Official_account',
                '发表期号': 'Issue',
                '文章标题': 'Paper_title', 
                '作者': 'Author',
                '作者单位': 'Affiliation',
                '内容提要': 'Abstract',
                '关键词': 'Keywords',
                '发布时间': 'Publication_date',
                '期刊名称': 'Journal',
                '文章链接': 'Link'
            }
            
            # 只重命名存在的列
            rename_dict = {old: new for old, new in column_mapping.items() 
                          if old in df.columns}
            df.rename(columns=rename_dict, inplace=True)
                
            # 8. 处理Abstract列（如果Abstract为空则用文章摘要）
            if '文章摘要' in df.columns and 'Abstract' in df.columns:
                df['Abstract'] = df.apply(
                    lambda row: row['文章摘要'] if pd.isna(row['Abstract']) or str(row['Abstract']).strip() == '' else row['Abstract'], 
                    axis=1
                )
            
            # 9. 删除文章标题或作者为空的行
            before_count = len(df)
            if 'Paper_title' in df.columns and 'Author' in df.columns:
                # 先替换'nan'字符串为真正的NaN
                df['Author'] = df['Author'].replace('nan', None)
                df['Paper_title'] = df['Paper_title'].replace('nan', None)
                
                df = df[~((df['Paper_title'].isna()) | (df['Paper_title'].astype(str).str.strip() == ''))]
                df = df[~((df['Author'].isna()) | (df['Author'].astype(str).str.strip() == ''))]
            after_count = len(df)
            print(f"  删除空值后保留: {after_count} 行 (删除: {before_count - after_count} 行)")
            
            # 10. 选择最终需要的列（修复：添加Link列）
            final_columns = ['Paper_title', 'Author', 'Affiliation', 'Keywords', 
                           'Abstract', 'Journal', 'Issue', 'Publication_date', 'Official_account', 'Link']
            df = df[[col for col in final_columns if col in df.columns]]
            
            # 调试：显示列信息
            print(f"  最终列: {list(df.columns)}")
            
            if df.empty:
                print(f"  跳过: 处理后无有效数据")
                continue
            
            # 11. 插入MySQL - 修复NaN问题
            try:
                conn = pymysql.connect(**mysql_config)
                cursor = conn.cursor()
                
                columns = "`,`".join(df.columns)
                placeholders = ",".join(["%s"] * len(df.columns))
                insert_sql = f"INSERT INTO `{mysql_table_name}` (`{columns}`) VALUES ({placeholders})"
                
                # 转换数据时处理NaN值
                data_list = []
                for row in df.values:
                    # 将numpy.nan转换为None
                    row_tuple = tuple(None if pd.isna(val) else val for val in row)
                    data_list.append(row_tuple)
                
                # 批量插入
                cursor.executemany(insert_sql, data_list)
                conn.commit()
                
                print(f"  成功入库 {len(data_list)} 条数据")
                total_success += 1
                total_records += len(data_list)
                
                cursor.close()
                conn.close()
                
            except Exception as e:
                print(f"  入库失败: {e}")
                continue
            
        except Exception as e:
            print(f"  处理文件失败: {e}")
            continue
        
        print("-" * 30)
    
    print("=" * 50)
    print(f"处理完成！成功处理 {total_success}/{len(csv_files)} 个文件")
    print(f"总共入库 {total_records} 条记录")

#%% 使用示例
process_folder(r"D:\这里有法_资料\已入库\3_1论文基础信息", "3_1Paper")