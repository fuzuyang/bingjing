import os
import pandas as pd
import numpy as np
import pymysql
import logging
import warnings
from datetime import datetime
from tqdm import tqdm
import gc
import psutil
import time

mysql_config = {
    'host': '211.149.136.21',
    'port': 3406,
    'user': 'root', 
    'password': '@#Zxli*313100',
    'database': 'LAWdatabase',
    'charset': 'utf8mb4'
}
# ==================== 日志配置 ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('正式处理.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
warnings.filterwarnings('ignore')

# ==================== 优化配置 ====================
OPTIMAL_BATCH_SIZE = 2500
OPTIMAL_CHUNK_SIZE = 15000
PROVISIONS_MAX_LENGTH = 5000

# ==================== 辅助函数 ====================
def get_excel_files(folder_path):
    """获取Excel文件列表"""
    excel_files = []
    extensions = ['.xlsx', '.xls']
    
    for file in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file)
        if (os.path.isfile(file_path) and 
            not file.startswith('.') and
            not file.startswith('~$')):
            file_ext = os.path.splitext(file)[1].lower()
            if file_ext in extensions and os.path.getsize(file_path) > 1024:
                excel_files.append(file_path)
    
    return sorted(excel_files)

def clean_value(value):
    """清理NaN和空值"""
    if pd.isna(value):
        return None
    if isinstance(value, float) and np.isnan(value):
        return None
    if isinstance(value, str) and value.strip().lower() in ['nan', 'null', 'none']:
        return None
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned == '':
            return None
        return cleaned
    return str(value) if value is not None else None

def clean_provisions_value(value):
    """专门清理Provisions字段"""
    if pd.isna(value):
        return None
    if isinstance(value, float) and np.isnan(value):
        return None
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned == '':
            return None
        
        # 去除末尾的空白字符
        while cleaned and cleaned[-1] in [' ', '\n', '\r', '\t', '　']:
            cleaned = cleaned[:-1]
        
        cleaned = cleaned.rstrip('\n\r')
        return cleaned
    return str(value) if value is not None else None

def check_provisions_length(provisions_value):
    """检查Provisions字段长度"""
    if provisions_value is None:
        return True
    
    provisions_str = str(provisions_value)
    if len(provisions_str) > PROVISIONS_MAX_LENGTH:
        logger.warning(f"Provisions字段超长: {len(provisions_str)} > {PROVISIONS_MAX_LENGTH}")
        return False
    return True

def get_mysql_connection():
    """获取MySQL连接"""
    config = mysql_config.copy()
    config['connect_timeout'] = 60
    config['read_timeout'] = 600
    config['write_timeout'] = 600
    config['autocommit'] = False
    config['max_allowed_packet'] = 1024 * 1024 * 32
    return pymysql.connect(**config)

def get_max_ids_safe(cursor):
    """安全获取最大ID"""
    try:
        cursor.execute("SELECT COALESCE(MAX(Document_ID), 0) FROM `2_1Criminal_document`")
        max_doc_id = cursor.fetchone()[0]
        
        cursor.execute("SELECT COALESCE(MAX(Label_ID), 0) FROM `2_2Document_label`")
        max_label_id = cursor.fetchone()[0]
        
        if max_doc_id < 0:
            max_doc_id = 0
        if max_label_id < 0:
            max_label_id = 0
            
        logger.info(f"MySQL现有最大ID: Document_ID={max_doc_id}, Label_ID={max_label_id}")
        return max_doc_id + 1, max_label_id + 1
        
    except Exception as e:
        logger.warning(f"获取最大ID时出错，使用默认值1: {str(e)}")
        return 1, 1

# ==================== 数据库插入函数 ====================
def insert_batch_or_skip(conn, cursor, df1_batch, df2_batch):
    """批量插入数据"""
    if not df1_batch:
        return True, 0
    
    try:
        conn.begin()
        
        if df1_batch:
            sql_df1 = """
            INSERT INTO `2_1Criminal_document` 
            (Document_ID, Case_number, Document_type, Law_type, Charge, Instance, Court, 
             Provisions, Original_text, Parties_information, Litigation_process, Defense, 
             Facts, Evidence, Reasoning_point, Reasoning_concluding, Disposition, Concluding)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            df1_values = []
            for record in df1_batch:
                provisions_value = clean_provisions_value(record.get('Provisions'))
                
                if not check_provisions_length(provisions_value):
                    raise ValueError("Provisions字段超长，放弃批次")
                
                df1_values.append((
                    int(record['Document_ID']),
                    clean_value(record.get('Case_number')),
                    record.get('Document_type', '判决'),
                    record.get('Law_type', '刑事'),
                    clean_value(record.get('Charge')),
                    clean_value(record.get('Instance')),
                    clean_value(record.get('Court')),
                    provisions_value,
                    clean_value(record.get('Original_text')),
                    clean_value(record.get('Parties_information')),
                    clean_value(record.get('Litigation_process')),
                    clean_value(record.get('Defense')),
                    clean_value(record.get('Facts')),
                    clean_value(record.get('Evidence')),
                    clean_value(record.get('Reasoning_point')),
                    clean_value(record.get('Reasoning_concluding')),
                    clean_value(record.get('Disposition')),
                    clean_value(record.get('Concluding'))
                ))
            
            cursor.executemany(sql_df1, df1_values)
        
        if df2_batch:
            sql_df2 = """
            INSERT INTO `2_2Document_label` 
            (Label_ID, Document_ID, Case_number, Label_type, Label_content)
            VALUES (%s, %s, %s, %s, %s)
            """
            
            df2_values = []
            for record in df2_batch:
                df2_values.append((
                    int(record['Label_ID']),
                    int(record['Document_ID']),
                    clean_value(record.get('Case_number')),
                    clean_value(record.get('Label_type')),
                    clean_value(record.get('Label_content'))
                ))
            
            cursor.executemany(sql_df2, df2_values)
        
        conn.commit()
        logger.info(f"✓ 批次插入成功: {len(df1_batch)} 条文档记录")
        return True, 0
        
    except ValueError as e:
        conn.rollback()
        logger.warning(f"放弃当前批次 ({len(df1_batch)} 条文档记录): {str(e)}")
        return False, len(df1_batch)
        
    except Exception as e:
        try:
            conn.rollback()
        except:
            pass
        logger.warning(f"放弃当前批次 ({len(df1_batch)} 条文档记录): {str(e)[:200]}")
        return False, len(df1_batch)

# ==================== 核心处理函数 ====================
def process_excel_file_with_skip(excel_file):
    """处理Excel文件"""
    file_name = os.path.basename(excel_file)
    file_size_gb = os.path.getsize(excel_file) / (1024 ** 3)
    
    logger.info(f"开始处理: {file_name} ({file_size_gb:.2f} GB)")
    
    start_time = datetime.now()
    
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()
        cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        
        document_counter, label_counter = get_max_ids_safe(cursor)
        logger.info(f"起始ID: Document_ID={document_counter}, Label_ID={label_counter}")
        
        if document_counter < 1:
            document_counter = 1
        if label_counter < 1:
            label_counter = 1
            
        df1_batch = []
        df2_batch = []
        total_processed = 0
        total_skipped = 0
        chunk_start = 0
        
        # 读取列名
        try:
            df_sample1 = pd.read_excel(excel_file, sheet_name=0, nrows=1, engine='openpyxl')
            df_sample2 = pd.read_excel(excel_file, sheet_name=1, nrows=1, engine='openpyxl')
            df_sample3 = pd.read_excel(excel_file, sheet_name=2, nrows=1, engine='openpyxl')
            
            sheet1_columns = df_sample1.columns.tolist()
            sheet2_columns = df_sample2.columns.tolist()
            sheet3_columns = df_sample3.columns.tolist()
            
            if len(sheet1_columns) > 0:
                sheet1_columns[0] = "索引"
            if len(sheet2_columns) > 0:
                sheet2_columns[0] = "索引"
            if len(sheet3_columns) > 0:
                sheet3_columns[0] = "索引"
                
        except Exception as e:
            logger.error(f"读取列名时出错: {str(e)}")
            cursor.close()
            conn.close()
            return False, 0, 0, 0
        
        # 估计总行数
        try:
            df_test = pd.read_excel(excel_file, sheet_name=0, usecols=[0], engine='openpyxl')
            total_rows = len(df_test)
            total_chunks = max(1, total_rows // OPTIMAL_CHUNK_SIZE)
            logger.info(f"估计总行数: {total_rows}")
        except:
            total_chunks = None
        
        # 使用进度条
        with tqdm(total=total_chunks, desc=f"处理 {file_name}", unit="块") as pbar:
            
            while True:
                try:
                    read_params = {
                        'engine': 'openpyxl',
                        'dtype': object,
                        'skiprows': chunk_start,
                        'nrows': OPTIMAL_CHUNK_SIZE,
                        'header': None if chunk_start > 0 else 0
                    }
                    
                    df_sheet1 = pd.read_excel(excel_file, sheet_name=0, **read_params)
                    if df_sheet1.empty:
                        break
                        
                    df_sheet2 = pd.read_excel(excel_file, sheet_name=1, **read_params)
                    if df_sheet2.empty:
                        break
                        
                    df_sheet3 = pd.read_excel(excel_file, sheet_name=2, **read_params)
                    if df_sheet3.empty:
                        break
                    
                    if chunk_start == 0:
                        df_sheet1.columns = sheet1_columns[:len(df_sheet1.columns)]
                        df_sheet2.columns = sheet2_columns[:len(df_sheet2.columns)]
                        df_sheet3.columns = sheet3_columns[:len(df_sheet3.columns)]
                    else:
                        df_sheet1.columns = [f"col_{i}" for i in range(len(df_sheet1.columns))]
                        df_sheet2.columns = [f"col_{i}" for i in range(len(df_sheet2.columns))]
                        df_sheet3.columns = [f"col_{i}" for i in range(len(df_sheet3.columns))]
                        
                        if len(df_sheet1.columns) > 0:
                            df_sheet1.rename(columns={'col_0': '索引'}, inplace=True)
                        if len(df_sheet2.columns) > 0:
                            df_sheet2.rename(columns={'col_0': '索引'}, inplace=True)
                        if len(df_sheet3.columns) > 0:
                            df_sheet3.rename(columns={'col_0': '索引'}, inplace=True)
                    
                    chunk_processed = 0
                    
                    for idx in range(len(df_sheet3)):
                        try:
                            row_sheet3 = df_sheet3.iloc[idx]
                            index_val = row_sheet3['索引'] if '索引' in row_sheet3 else idx
                            
                            try:
                                row_sheet1 = df_sheet1[df_sheet1['索引'] == index_val].iloc[0]
                                row_sheet2 = df_sheet2[df_sheet2['索引'] == index_val].iloc[0]
                            except:
                                continue
                            
                            case_number = None
                            if len(row_sheet3) > 1:
                                case_number = clean_value(row_sheet3.iloc[1])
                            
                            if case_number is None:
                                continue
                            
                            current_doc_id = document_counter
                            
                            provisions_raw = row_sheet3.iloc[27] if len(row_sheet3) > 27 else None
                            provisions_cleaned = clean_provisions_value(provisions_raw)
                            
                            df1_record = {
                                'Document_ID': current_doc_id,
                                'Case_number': case_number,
                                'Document_type': '判决',
                                'Law_type': '刑事',
                                'Charge': clean_value(row_sheet3.iloc[7] if len(row_sheet3) > 7 else None),
                                'Instance': clean_value(row_sheet3.iloc[4] if len(row_sheet3) > 4 else None),
                                'Court': clean_value(row_sheet3.iloc[2] if len(row_sheet3) > 2 else None),
                                'Provisions': provisions_cleaned,
                                'Original_text': clean_value(row_sheet1.iloc[1] if len(row_sheet1) > 1 else None),
                                'Parties_information': clean_value(row_sheet2.iloc[2] if len(row_sheet2) > 2 else None),
                                'Litigation_process': clean_value(row_sheet2.iloc[3] if len(row_sheet2) > 3 else None),
                                'Defense': clean_value(row_sheet2.iloc[4] if len(row_sheet2) > 4 else None),
                                'Facts': clean_value(row_sheet2.iloc[5] if len(row_sheet2) > 5 else None),
                                'Evidence': clean_value(row_sheet2.iloc[6] if len(row_sheet2) > 6 else None),
                                'Reasoning_point': clean_value(row_sheet2.iloc[7] if len(row_sheet2) > 7 else None),
                                'Reasoning_concluding': clean_value(row_sheet2.iloc[8] if len(row_sheet2) > 8 else None),
                                'Disposition': clean_value(row_sheet2.iloc[9] if len(row_sheet2) > 9 else None),
                                'Concluding': clean_value(row_sheet2.iloc[10] if len(row_sheet2) > 10 else None)
                            }
                            
                            df1_batch.append(df1_record)
                            
                            for col_idx in range(2, min(len(row_sheet3), 30)):
                                col_value = row_sheet3.iloc[col_idx] if col_idx < len(row_sheet3) else None
                                clean_col_value = clean_value(col_value)
                                
                                if clean_col_value is not None:
                                    col_name = f"标签_{col_idx}"
                                    
                                    df2_record = {
                                        'Label_ID': label_counter,
                                        'Document_ID': current_doc_id,
                                        'Case_number': case_number,
                                        'Label_type': col_name,
                                        'Label_content': clean_col_value
                                    }
                                    df2_batch.append(df2_record)
                                    label_counter += 1
                            
                            document_counter += 1
                            chunk_processed += 1
                            
                            if len(df1_batch) >= OPTIMAL_BATCH_SIZE:
                                success, skipped = insert_batch_or_skip(conn, cursor, df1_batch, df2_batch)
                                if success:
                                    total_processed += len(df1_batch)
                                    df1_batch = []
                                    df2_batch = []
                                else:
                                    total_skipped += skipped
                                    df1_batch = []
                                    df2_batch = []
                            
                        except Exception as e:
                            continue
                    
                    memory_usage = psutil.Process().memory_info().rss / (1024 ** 2)
                    pbar.update(1)
                    pbar.set_postfix({
                        '成功': total_processed,
                        '跳过': total_skipped,
                        '内存': f'{memory_usage:.1f}MB'
                    })
                    
                    chunk_start += OPTIMAL_CHUNK_SIZE
                    
                    if chunk_start % (OPTIMAL_CHUNK_SIZE * 10) == 0:
                        gc.collect()
                    
                except Exception as e:
                    logger.error(f"读取块时出错: {str(e)}")
                    break
        
        if df1_batch:
            success, skipped = insert_batch_or_skip(conn, cursor, df1_batch, df2_batch)
            if success:
                total_processed += len(df1_batch)
            else:
                total_skipped += skipped
        
        cursor.close()
        conn.close()
        
        end_time = datetime.now()
        time_used = (end_time - start_time).total_seconds()
        
        logger.info(f"✓ 处理完成: {file_name}")
        logger.info(f"  成功插入: {total_processed} 条文档记录")
        logger.info(f"  跳过文档记录: {total_skipped} 条 (Provisions超长)")
        logger.info(f"  耗时: {time_used:.2f} 秒")
        
        return True, total_processed, total_skipped, time_used
        
    except Exception as e:
        logger.error(f"处理文件时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False, 0, 0, 0

# ==================== 主运行函数 ====================
def main():
    """主函数"""
    folder_path = r"D:\这里有法_资料\文书分段\刑事文书一审-所有年份处理结果"
    
    print("\n" + "="*80)
    print("批量插入处理开始")
    print("="*80)
    print(f"参数: chunk_size={OPTIMAL_CHUNK_SIZE}, batch_size={OPTIMAL_BATCH_SIZE}")
    print("="*80 + "\n")
    
    files = get_excel_files(folder_path)
    if not files:
        logger.error("没有找到Excel文件")
        return
    
    file_sizes = [(f, os.path.getsize(f) / (1024**3)) for f in files]
    file_sizes.sort(key=lambda x: x[1])
    
    logger.info(f"找到 {len(files)} 个文件:")
    for i, (file, size_gb) in enumerate(file_sizes, 1):
        logger.info(f"  {i}. {os.path.basename(file)} ({size_gb:.2f} GB)")
    
    # 只获取初始文档数量
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM `2_1Criminal_document`")
        initial_doc_count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        logger.info(f"数据库初始记录数: {initial_doc_count} 条")
    except Exception as e:
        logger.warning(f"获取数据库初始状态失败: {str(e)}")
        initial_doc_count = 0
    
    total_processed = 0
    total_skipped = 0
    total_time = 0
    
    for i, (excel_file, file_size_gb) in enumerate(file_sizes, 1):
        logger.info(f"\n{'='*60}")
        logger.info(f"处理文件 {i}/{len(files)}: {os.path.basename(excel_file)}")
        
        success, processed, skipped, time_used = process_excel_file_with_skip(excel_file)
        
        if success:
            total_processed += processed
            total_skipped += skipped
            total_time += time_used
            
            if skipped == 0:
                logger.info(f"✓ 成功: {processed} 条文档记录")
            else:
                logger.info(f"✓ 部分成功: {processed} 条文档成功，{skipped} 条文档跳过")
        else:
            logger.error("✗ 处理失败")
        
        gc.collect()
        if i < len(files):
            time.sleep(3)
    
    logger.info(f"\n{'='*60}")
    logger.info("所有文件处理完成!")
    logger.info(f"总成功文档记录: {total_processed} 条")
    logger.info(f"总跳过文档记录: {total_skipped} 条 (Provisions超长)")
    logger.info(f"总耗时: {total_time:.2f} 秒")
    
    # 只获取最终文档数量
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM `2_1Criminal_document`")
        final_doc_count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        
        logger.info(f"\n数据库最终状态:")
        logger.info(f"  2_1Criminal_document表: {final_doc_count} 条记录")
        logger.info(f"  实际新增: {final_doc_count - initial_doc_count} 条")
        
        # 简单验证文档数量
        expected_total_docs = initial_doc_count + total_processed
        if final_doc_count != expected_total_docs:
            logger.warning(f"文档数量不一致! 数据库={final_doc_count}, 预期={expected_total_docs}")
        else:
            logger.info("✓ 文档数量验证通过")
            
    except Exception as e:
        logger.warning(f"获取数据库最终状态失败: {str(e)}")
    
    if total_skipped > 0:
        logger.warning(f"注意: 跳过了 {total_skipped} 条文档记录，原因是Provisions字段超过{PROVISIONS_MAX_LENGTH}字符")

# ==================== 运行代码 ====================
if __name__ == "__main__":
    main()