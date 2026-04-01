import pandas as pd
import pymysql
import json
import os
import warnings
warnings.filterwarnings('ignore')

# ============= 配置参数 =============
MYSQL_CONFIG = {
    'host': '211.149.136.21',
    'port': 3406,
    'user': 'root', 
    'password': '@#Zxli*313100',
    'database': 'LAWdatabase',
    'charset': 'utf8mb4'
}

# 修改：改为文件夹路径
FOLDER_PATH = r"D:\这里有法_资料\文书分段第二次\2018年"
PROGRESS_FILE = "processing_progress.json"

# 文件名模式（用于识别文件类型）
MAIN_FILE_PATTERN = "分段表_1"
DETAIL_FILE_PATTERN = "细节表_"

# 工作表名配置
MAIN_SHEET_NAME = '分段表_1'  # 主表的工作表名
DETAIL_SHEET_PREFIX = '细节表_'  # 细节表工作表名前缀

# 批次大小配置
MAIN_BATCH_SIZE = 20000
DETAIL_BATCH_SIZE = 20000
INSERT_CHUNK_SIZE = 2500
COMMIT_FREQUENCY = 20000

# ============= 辅助函数 =============
def clean_string(value, allow_null=False):
    if pd.isna(value):
        return None if allow_null else ""
    result = str(value).strip()
    return result if result else (None if allow_null else "")

def get_current_max_ids():
    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(Document_ID) FROM 2_1Criminal_document_2_2")
        max_doc_id = cursor.fetchone()[0]
        cursor.execute("SELECT MAX(Label_ID) FROM 2_2Document_label_2_2")
        max_label_id = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return max_doc_id, max_label_id
    except Exception as e:
        print(f"获取当前最大ID时出错: {e}")
        return None, None

def save_progress(data):
    try:
        with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"保存进度失败: {e}")

def load_progress():
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {}
    return {}

def clear_progress():
    if os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)

def find_files_in_folder(folder_path):
    """查找文件夹中的Excel文件并分类"""
    if not os.path.exists(folder_path):
        print(f"错误: 文件夹不存在 {folder_path}")
        return None, []
    
    all_files = [f for f in os.listdir(folder_path) if f.endswith('.xlsx')]
    print(f"在文件夹中找到 {len(all_files)} 个Excel文件")
    
    # 查找主表文件
    main_files = [f for f in all_files if MAIN_FILE_PATTERN in f]
    if not main_files:
        print(f"错误: 未找到包含 '{MAIN_FILE_PATTERN}' 的主表文件")
        return None, []
    elif len(main_files) > 1:
        print(f"警告: 找到多个包含 '{MAIN_FILE_PATTERN}' 的文件，将使用第一个: {main_files[0]}")
    
    main_file_path = os.path.join(folder_path, main_files[0])
    
    # 查找所有细节表文件
    detail_files = [f for f in all_files if DETAIL_FILE_PATTERN in f and MAIN_FILE_PATTERN not in f]
    
    # 按数字排序（细节表_1, 细节表_2, ...）
    detail_files.sort(key=lambda x: int(x.split('细节表_')[-1].split('.')[0]) if '细节表_' in x else 0)
    
    detail_file_paths = [os.path.join(folder_path, f) for f in detail_files]
    
    print(f"主表文件: {main_files[0]}")
    print(f"找到 {len(detail_files)} 个细节表文件: {detail_files}")
    
    return main_file_path, detail_file_paths

# ============= 主处理函数 =============
def process_main_sheet_with_resume(main_file_path, progress_data):
    print(f"开始处理主表文件: {os.path.basename(main_file_path)}")
    
    try:
        print(f"正在读取分段表_1工作表...")
        df = pd.read_excel(main_file_path, sheet_name=MAIN_SHEET_NAME, dtype=str)
        total_rows = len(df)
        print(f"分段表_1共读取 {total_rows} 行")
        
        conn = pymysql.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        
        index_to_doc_id = {}
        inserted_count = 0
        start_row = 0
        
        # 从进度恢复
        if progress_data.get('main_sheet'):
            index_to_doc_id = progress_data['main_sheet'].get('index_to_doc_id', {})
            inserted_count = progress_data['main_sheet'].get('inserted_count', 0)
            start_row = progress_data['main_sheet'].get('processed_rows', 0)
            print(f"从进度点恢复: 已处理 {start_row} 行，插入 {inserted_count} 条记录")
        
        # 分批处理
        for start_idx in range(start_row, total_rows, MAIN_BATCH_SIZE):
            end_idx = min(start_idx + MAIN_BATCH_SIZE, total_rows)
            batch_df = df.iloc[start_idx:end_idx]
            
            print(f"处理行 {start_idx+1} 到 {end_idx}...")
            
            batch_data = []
            batch_indices = []
            
            for _, row in batch_df.iterrows():
                original_index = row['原始索引']
                
                if pd.isna(original_index):
                    continue
                
                original_index_str = str(original_index)
                if original_index_str in index_to_doc_id:
                    continue
                
                case_number = clean_string(row['案号'])
                if not case_number:
                    continue
                
                parties_info = clean_string(row['当事人基本信息'], allow_null=True)
                litigation_process = clean_string(row['诉讼经过'], allow_null=True)
                prosecution = clean_string(row['起诉书段'], allow_null=True)
                defense = clean_string(row['被告和辩护人意见段'], allow_null=True)
                facts = clean_string(row['基本事实段'], allow_null=True)
                evidence = clean_string(row['证据段'], allow_null=True)
                reasoning_point = clean_string(row['分点说理段'], allow_null=True)
                reasoning_concluding = clean_string(row['总结说理段'], allow_null=True)
                disposition = clean_string(row['判决主文'], allow_null=True)
                concluding = clean_string(row['判决落款'], allow_null=True)
                original_text = clean_string(row['清洗后全文'], allow_null=True)
                
                batch_data.append((
                    case_number, '判决书', '刑初', original_text, parties_info,
                    litigation_process, prosecution, defense, facts, evidence,
                    reasoning_point, reasoning_concluding, disposition, concluding
                ))
                batch_indices.append(original_index_str)
            
            if not batch_data:
                # 保存进度
                progress_data['main_sheet'] = {
                    'processed_rows': end_idx,
                    'inserted_count': inserted_count,
                    'index_to_doc_id': index_to_doc_id
                }
                save_progress(progress_data)
                continue
            
            # 分批插入
            sql = """
            INSERT INTO 2_1Criminal_document_2_2 
            (Case_number, Document_type, Instance, Original_text, Parties_information, 
             Litigation_process, Prosecution, Defense, Facts, Evidence, 
             Reasoning_point, Reasoning_concluding, Disposition, Concluding)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            for i in range(0, len(batch_data), INSERT_CHUNK_SIZE):
                chunk = batch_data[i:i + INSERT_CHUNK_SIZE]
                cursor.executemany(sql, chunk)
                
                # 获取刚插入的Document_ID
                first_id = cursor.lastrowid - len(chunk) + 1
                for j in range(len(chunk)):
                    doc_id = first_id + j
                    index_to_doc_id[batch_indices[i + j]] = doc_id
            
            inserted_count += len(batch_data)
            
            # 定期提交和保存进度
            if inserted_count % COMMIT_FREQUENCY == 0:
                conn.commit()
                print(f"已提交事务，累计插入 {inserted_count} 条记录")
                
                # 保存进度
                progress_data['main_sheet'] = {
                    'processed_rows': end_idx,
                    'inserted_count': inserted_count,
                    'index_to_doc_id': index_to_doc_id
                }
                save_progress(progress_data)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # 主表处理完成
        if 'main_sheet' in progress_data:
            del progress_data['main_sheet']
            save_progress(progress_data)
        
        print(f"主表处理完成: 插入 {inserted_count} 条记录")
        return index_to_doc_id
        
    except Exception as e:
        print(f"处理主表时出错: {e}")
        import traceback
        traceback.print_exc()
        return {}

def process_detail_file_with_resume(detail_file_path, index_to_doc_id, progress_data):
    """处理单个细节表文件"""
    file_name = os.path.basename(detail_file_path)
    print(f"开始处理细节表文件: {file_name}")
    
    try:
        # 获取该文件中的所有工作表
        xl = pd.ExcelFile(detail_file_path)
        all_sheet_names = xl.sheet_names
        
        # 筛选出细节表工作表（以'细节表_'开头）
        detail_sheet_names = [name for name in all_sheet_names if name.startswith(DETAIL_SHEET_PREFIX)]
        
        if not detail_sheet_names:
            print(f"警告: 文件 {file_name} 中没有找到以 '{DETAIL_SHEET_PREFIX}' 开头的工作表")
            return 0
        
        print(f"在文件 {file_name} 中找到 {len(detail_sheet_names)} 个细节表工作表: {detail_sheet_names}")
        
        total_labels_for_file = 0
        
        # 处理该文件中的每个细节表工作表
        for sheet_name in detail_sheet_names:
            labels_count = process_detail_sheet_with_resume(detail_file_path, sheet_name, index_to_doc_id, progress_data, file_name)
            total_labels_for_file += labels_count
        
        print(f"细节表文件 {file_name} 处理完成: 共插入 {total_labels_for_file} 条标签记录")
        return total_labels_for_file
        
    except Exception as e:
        print(f"处理细节表文件 {file_name} 时出错: {e}")
        import traceback
        traceback.print_exc()
        return 0

def process_detail_sheet_with_resume(excel_path, sheet_name, index_to_doc_id, progress_data, file_name):
    """处理单个细节表工作表"""
    print(f"开始处理工作表: {sheet_name} (来自文件: {os.path.basename(excel_path)})")
    
    try:
        print(f"正在读取 {sheet_name}...")
        df = pd.read_excel(excel_path, sheet_name=sheet_name, dtype=str)
        total_rows = len(df)
        print(f"{sheet_name} 共读取 {total_rows} 行")
        
        conn = pymysql.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        
        inserted_count = 0
        start_row = 0
        
        # 从进度恢复 - 使用复合键：文件名+工作表名
        sheet_key = f"{file_name}_{sheet_name}"
        if progress_data.get('detail_sheets', {}).get(sheet_key):
            start_row = progress_data['detail_sheets'][sheet_key].get('processed_rows', 0)
            inserted_count = progress_data['detail_sheets'][sheet_key].get('inserted_count', 0)
            print(f"从进度点恢复: 已处理 {start_row} 行，插入 {inserted_count} 条记录")
        
        # 分批处理
        for start_idx in range(start_row, total_rows, DETAIL_BATCH_SIZE):
            end_idx = min(start_idx + DETAIL_BATCH_SIZE, total_rows)
            batch_df = df.iloc[start_idx:end_idx]
            
            print(f"处理行 {start_idx+1} 到 {end_idx}...")
            
            batch_data = []
            
            for _, row in batch_df.iterrows():
                original_index = row['原始索引']
                
                if pd.isna(original_index):
                    continue
                
                original_index_str = str(original_index)
                if original_index_str not in index_to_doc_id:
                    continue
                
                case_number = clean_string(row['案号'])
                if not case_number:
                    continue
                
                doc_id = index_to_doc_id[original_index_str]
                label_type = clean_string(row['类型'], allow_null=True)
                label_content = clean_string(row['内容'], allow_null=True)
                
                batch_data.append((doc_id, case_number, label_type, label_content))
            
            if not batch_data:
                # 保存进度
                if 'detail_sheets' not in progress_data:
                    progress_data['detail_sheets'] = {}
                progress_data['detail_sheets'][sheet_key] = {
                    'processed_rows': end_idx,
                    'inserted_count': inserted_count
                }
                save_progress(progress_data)
                continue
            
            # 分批插入
            sql = """
            INSERT INTO 2_2Document_label_2_2 
            (Document_ID, Case_Number, Label_type, Label_content)
            VALUES (%s, %s, %s, %s)
            """
            
            for i in range(0, len(batch_data), INSERT_CHUNK_SIZE):
                chunk = batch_data[i:i + INSERT_CHUNK_SIZE]
                cursor.executemany(sql, chunk)
            
            inserted_count += len(batch_data)
            
            # 定期提交和保存进度
            if inserted_count % COMMIT_FREQUENCY == 0:
                conn.commit()
                print(f"已提交事务，本表累计插入 {inserted_count} 条记录")
                
                # 保存进度
                if 'detail_sheets' not in progress_data:
                    progress_data['detail_sheets'] = {}
                progress_data['detail_sheets'][sheet_key] = {
                    'processed_rows': end_idx,
                    'inserted_count': inserted_count
                }
                save_progress(progress_data)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # 当前工作表处理完成
        if progress_data.get('detail_sheets', {}).get(sheet_key):
            del progress_data['detail_sheets'][sheet_key]
            save_progress(progress_data)
        
        print(f"工作表 {sheet_name} 处理完成: 插入 {inserted_count} 条标签记录")
        return inserted_count
        
    except Exception as e:
        print(f"处理工作表 {sheet_name} 时出错: {e}")
        import traceback
        traceback.print_exc()
        return 0

def process_folder_to_mysql_with_resume():
    """处理整个文件夹到MySQL（带断点续传）"""
    try:
        print("=" * 60)
        print("开始处理文件夹中的Excel文件 (带断点续传)...")
        print(f"文件夹路径: {FOLDER_PATH}")
        print("=" * 60)
        
        if not os.path.exists(FOLDER_PATH):
            print(f"错误: 找不到文件夹 {FOLDER_PATH}")
            return
        
        # 查找文件
        main_file_path, detail_file_paths = find_files_in_folder(FOLDER_PATH)
        if not main_file_path:
            print("错误: 未找到主表文件")
            return
        
        # 加载进度
        progress_data = load_progress()
        if progress_data:
            print("检测到未完成的进度，恢复处理...")
        
        print("获取当前数据库最大ID...")
        max_doc_id, max_label_id = get_current_max_ids()
        print(f"当前最大Document_ID: {max_doc_id or 0}")
        print(f"当前最大Label_ID: {max_label_id or 0}")
        
        # 处理主表
        print("\n" + "=" * 40)
        index_to_doc_id = process_main_sheet_with_resume(main_file_path, progress_data)
        
        if not index_to_doc_id:
            print("错误: 主表处理失败或没有数据")
            return
        
        print(f"成功建立 {len(index_to_doc_id)} 个原始索引到Document_ID的映射")
        
        # 处理细节表
        print("\n" + "=" * 40)
        print("开始处理细节表...")
        total_labels = 0
        
        for detail_file_path in detail_file_paths:
            try:
                # 重新加载进度（可能已被更新）
                current_progress = load_progress()
                
                labels_count = process_detail_file_with_resume(detail_file_path, index_to_doc_id, current_progress)
                total_labels += labels_count
                
            except Exception as e:
                print(f"处理文件 {detail_file_path} 时出错: {e}")
                continue
        
        # 所有处理完成，清除进度文件
        clear_progress()
        
        print("\n" + "=" * 60)
        print("数据处理完成!")
        print(f"总计插入 {len(index_to_doc_id)} 条主表记录")
        print(f"总计插入 {total_labels} 条标签记录")
        print("=" * 60)
        
    except Exception as e:
        print(f"处理过程中出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    process_folder_to_mysql_with_resume()