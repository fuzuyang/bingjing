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

EXCEL_FILE_PATH = r"D:\这里有法_资料\文书分段第二次\cleaned_2021_full_汇总.xlsx"
PROGRESS_FILE = "processing_progress.json"

# 修改：只指定主表名，细节表动态获取
MAIN_SHEET_NAME = '分段表_1'
# 删除固定的 DETAIL_SHEET_NAMES 配置

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
        cursor.execute("SELECT MAX(Document_ID) FROM 2_1Criminal_document_2")
        max_doc_id = cursor.fetchone()[0]
        cursor.execute("SELECT MAX(Label_ID) FROM 2_2Document_label_2")
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

# ============= 主处理函数 =============
def process_main_sheet_with_resume(excel_path, progress_data):
    print("开始处理分段表_1...")
    
    try:
        print("正在读取分段表_1...")
        df = pd.read_excel(excel_path, sheet_name=MAIN_SHEET_NAME, dtype=str)
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
            INSERT INTO 2_1Criminal_document_2 
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
        
        print(f"分段表_1处理完成: 插入 {inserted_count} 条记录")
        return index_to_doc_id
        
    except Exception as e:
        print(f"处理分段表_1时出错: {e}")
        import traceback
        traceback.print_exc()
        return {}

def process_detail_sheet_with_resume(excel_path, sheet_name, index_to_doc_id, progress_data):
    print(f"开始处理{sheet_name}...")
    
    try:
        print(f"正在读取{sheet_name}...")
        df = pd.read_excel(excel_path, sheet_name=sheet_name, dtype=str)
        total_rows = len(df)
        print(f"{sheet_name}共读取 {total_rows} 行")
        
        conn = pymysql.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        
        inserted_count = 0
        total_labels = 0
        start_row = 0
        
        # 从进度恢复
        if progress_data.get('detail_sheets', {}).get(sheet_name):
            start_row = progress_data['detail_sheets'][sheet_name].get('processed_rows', 0)
            inserted_count = progress_data['detail_sheets'][sheet_name].get('inserted_count', 0)
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
                progress_data['detail_sheets'][sheet_name] = {
                    'processed_rows': end_idx,
                    'inserted_count': inserted_count
                }
                save_progress(progress_data)
                continue
            
            # 分批插入
            sql = """
            INSERT INTO 2_2Document_label_2 
            (Document_ID, Case_Number, Label_type, Label_content)
            VALUES (%s, %s, %s, %s)
            """
            
            for i in range(0, len(batch_data), INSERT_CHUNK_SIZE):
                chunk = batch_data[i:i + INSERT_CHUNK_SIZE]
                cursor.executemany(sql, chunk)
            
            inserted_count += len(batch_data)
            total_labels += len(batch_data)
            
            # 定期提交和保存进度
            if inserted_count % COMMIT_FREQUENCY == 0:
                conn.commit()
                print(f"已提交事务，本表累计插入 {inserted_count} 条记录")
                
                # 保存进度
                if 'detail_sheets' not in progress_data:
                    progress_data['detail_sheets'] = {}
                progress_data['detail_sheets'][sheet_name] = {
                    'processed_rows': end_idx,
                    'inserted_count': inserted_count
                }
                save_progress(progress_data)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # 当前表处理完成
        if progress_data.get('detail_sheets', {}).get(sheet_name):
            del progress_data['detail_sheets'][sheet_name]
            save_progress(progress_data)
        
        print(f"{sheet_name}处理完成: 插入 {total_labels} 条标签记录")
        return total_labels
        
    except Exception as e:
        print(f"处理{sheet_name}时出错: {e}")
        return 0

def process_excel_to_mysql_with_resume():
    try:
        print("=" * 60)
        print("开始处理Excel文件 (带断点续传)...")
        print(f"文件路径: {EXCEL_FILE_PATH}")
        print("=" * 60)
        
        if not os.path.exists(EXCEL_FILE_PATH):
            print(f"错误: 找不到文件 {EXCEL_FILE_PATH}")
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
        index_to_doc_id = process_main_sheet_with_resume(EXCEL_FILE_PATH, progress_data)
        
        if not index_to_doc_id:
            print("错误: 主表处理失败或没有数据")
            return
        
        print(f"成功建立 {len(index_to_doc_id)} 个原始索引到Document_ID的映射")
        
        # 处理细节表 - 动态获取所有工作表名
        print("\n" + "=" * 40)
        print("开始处理细节表...")
        total_labels = 0
        
        # 获取Excel文件中的所有工作表名
        xl = pd.ExcelFile(EXCEL_FILE_PATH)
        all_sheet_names = xl.sheet_names
        print(f"Excel文件中包含的工作表: {all_sheet_names}")
        
        # 排除主表，剩下的都是细节表
        detail_sheet_names = [name for name in all_sheet_names if name != MAIN_SHEET_NAME]
        print(f"将处理以下细节表: {detail_sheet_names}")
        
        for sheet_name in detail_sheet_names:
            try:
                print(f"\n处理工作表: {sheet_name}")
                
                # 重新加载进度（可能已被更新）
                current_progress = load_progress()
                
                labels_count = process_detail_sheet_with_resume(EXCEL_FILE_PATH, sheet_name, index_to_doc_id, current_progress)
                total_labels += labels_count
                
            except Exception as e:
                print(f"处理工作表 {sheet_name} 时出错: {e}")
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
    process_excel_to_mysql_with_resume()

