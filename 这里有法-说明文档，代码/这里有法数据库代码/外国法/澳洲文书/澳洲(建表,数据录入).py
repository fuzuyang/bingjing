import pandas as pd
import pymysql
import re
import math
from typing import Dict, List, Tuple, Optional
import logging
import os

# ============ 配置部分 ============
MYSQL_CONFIG = {
    'host': '211.149.136.21',
    'port': 3307,
    'user': 'root',
    'password': 'zjcx111ch9',
    'database': 'LAWdatabase',
    'charset': 'utf8mb4'
}

# 批量处理参数
BATCH_SIZE = 20000
INSERT_CHUNK_SIZE = 2500
COMMIT_FREQUENCY = 20000

# 表名
TABLE_NAME = 'Australia'

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('excel_to_mysql.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============ 工具函数 ============
def clean_file_path(file_path: str) -> str:
    """清理文件路径：去除首尾的引号和空格"""
    cleaned = file_path.strip().strip('"').strip("'").strip()
    if os.name == 'nt':  # Windows
        cleaned = cleaned.replace('/', '\\')
    else:
        cleaned = cleaned.replace('\\', '/')
    return cleaned

def validate_file_path(file_path: str) -> bool:
    """验证文件路径是否有效"""
    if not os.path.exists(file_path):
        logger.error(f"文件不存在: {file_path}")
        return False
    if not file_path.lower().endswith(('.xlsx', '.xls')):
        logger.error(f"文件不是Excel格式: {file_path}")
        return False
    return True

def clean_column_name(col_name: str) -> str:
    """清洗列名：去除"/"后面部分，非字母符号转下划线，字母小写"""
    if pd.isna(col_name):
        return "unknown_column"
    
    col_str = str(col_name)
    
    # 处理"/"符号：只取"/"之前的部分
    if '/' in col_str:
        col_str = col_str.split('/')[0]
    
    # 将所有非字母数字字符替换为下划线
    cleaned = re.sub(r'[^a-zA-Z0-9]', '_', col_str)
    cleaned = re.sub(r'_+', '_', cleaned)
    cleaned = cleaned.strip('_')
    
    if not cleaned:
        cleaned = "column"
    
    return cleaned.lower()

def clean_cell_content(value) -> str:
    """
    清理单元格内容：去除末尾的空白字符（空格、制表符、换行符等）
    但保留中间的字符
    """
    if pd.isna(value):
        return ""
    
    # 转换为字符串
    str_value = str(value)
    
    # 去除字符串末尾的空白字符（包括空格、制表符、换行符等）
    # 使用正则表达式去除所有末尾空白字符
    cleaned = re.sub(r'[\s\t\n\r\f\v]+$', '', str_value)
    
    return cleaned

def calculate_field_length(max_length: int) -> Tuple[str, str]:
    """
    根据最大长度计算字段类型和长度
    严格遵循原规则，但对于较长的字段使用longtext
    """
    # 如果所有单元格都为空，设置为varchar(50)
    if max_length == 0:
        return "varchar", "varchar(50)"
    
    # 根据原规则计算字段长度
    if max_length < 20:
        field_length = 50
    elif max_length < 100:
        field_length = 200
    else:
        # [最长长度 * (1 + 1/2)] 取整
        field_length = int(math.ceil(max_length * 1.5))
    
    # 优化：对于varchar长度超过1000的字段，直接使用longtext
    # 这样可以避免MySQL行大小超限问题
    if field_length > 1000:
        return "longtext", "longtext"
    # 检查是否超过varchar最大长度（utf8mb4下最大16383）
    elif field_length > 16383:
        return "longtext", "longtext"
    else:
        return "varchar", f"varchar({field_length})"

def process_cell_value(value) -> Optional[str]:
    """处理单元格值：去除末尾空格，空值转为None"""
    if pd.isna(value):
        return None
    
    # 使用clean_cell_content函数清理内容
    cleaned_value = clean_cell_content(value)
    
    # 如果清理后为空，返回None
    if not cleaned_value:
        return None
    
    return cleaned_value

# ============ 建表阶段 ============
def create_table_from_excel(excel_file_path: str) -> bool:
    """根据Excel文件创建MySQL表"""
    try:
        cleaned_path = clean_file_path(excel_file_path)
        logger.info(f"开始建表流程，读取Excel文件: {cleaned_path}")
        
        if not validate_file_path(cleaned_path):
            return False
        
        df = pd.read_excel(cleaned_path, dtype=str)
        logger.info(f"Excel文件读取成功，共{len(df)}行，{len(df.columns)}列")
        
        if len(df.columns) < 2:
            logger.error("Excel文件至少需要2列")
            return False
        
        excel_columns = list(df.columns)[1:]  # 跳过第一列
        logger.info(f"需要处理的Excel列数: {len(excel_columns)}")
        
        # 处理列名
        mysql_columns = []
        column_mapping = {}
        column_max_lengths = {}
        
        for i, excel_col in enumerate(excel_columns, 1):
            mysql_col = clean_column_name(excel_col)
            original_mysql_col = mysql_col
            counter = 1
            while mysql_col in column_mapping.values():
                mysql_col = f"{original_mysql_col}_{counter}"
                counter += 1
            
            mysql_columns.append(mysql_col)
            column_mapping[excel_col] = mysql_col
            logger.info(f"Excel列 {i}: '{excel_col}' -> MySQL字段 '{mysql_col}'")
        
        # 计算每列的最大长度（在清理末尾空白字符后）
        for excel_col, mysql_col in column_mapping.items():
            column_data = df[excel_col].iloc[1:] if len(df) > 1 else pd.Series(dtype=str)
            
            if column_data.empty:
                max_len = 0
            else:
                # 清理每个单元格内容后计算长度
                lengths = column_data.apply(
                    lambda x: len(clean_cell_content(x)) if pd.notna(x) and clean_cell_content(x) else 0
                )
                max_len = int(lengths.max()) if not lengths.empty else 0
            
            column_max_lengths[mysql_col] = max_len
            logger.info(f"字段 '{mysql_col}' 最大长度（清理后）: {max_len}")
        
        # 连接MySQL数据库
        logger.info("连接MySQL数据库...")
        conn = pymysql.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        
        # 检查表是否已存在
        cursor.execute(f"SHOW TABLES LIKE '{TABLE_NAME}'")
        if cursor.fetchone():
            logger.warning(f"表 '{TABLE_NAME}' 已存在")
            response = input(f"表 '{TABLE_NAME}' 已存在，是否删除并重建？(y/n): ").strip().lower()
            if response == 'y':
                cursor.execute(f"DROP TABLE {TABLE_NAME}")
                logger.info(f"表 '{TABLE_NAME}' 已删除")
            else:
                logger.info("取消建表操作")
                cursor.close()
                conn.close()
                return False
        
        # 生成字段定义
        field_definitions = []
        longtext_count = 0
        varchar_count = 0
        
        for mysql_col in mysql_columns:
            max_len = column_max_lengths[mysql_col]
            field_type, field_def = calculate_field_length(max_len)
            
            if 'longtext' in field_def.lower():
                longtext_count += 1
            else:
                varchar_count += 1
            
            field_definitions.append((mysql_col, field_def, max_len, field_type))
        
        # 生成建表SQL
        create_table_sql = f"CREATE TABLE {TABLE_NAME} (\n"
        create_table_sql += "    id INT AUTO_INCREMENT PRIMARY KEY,\n"
        
        field_sql_defs = []
        for mysql_col, field_def, max_len, field_type in field_definitions:
            field_sql_defs.append(f"    {mysql_col} {field_def}")
        
        create_table_sql += ",\n".join(field_sql_defs)
        create_table_sql += "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        
        # 执行建表语句
        logger.info("创建MySQL表...")
        logger.info(f"建表SQL:\n{create_table_sql}")
        cursor.execute(create_table_sql)
        
        # 创建索引
        if mysql_columns:
            first_field = mysql_columns[0]
            cursor.execute(f"CREATE INDEX idx_{first_field} ON {TABLE_NAME}({first_field})")
            logger.info(f"在字段 '{first_field}' 上创建索引")
        
        conn.commit()
        logger.info(f"表 '{TABLE_NAME}' 创建成功！")
        
        # 保存列映射信息
        import json
        mapping_info = {
            'excel_columns': list(column_mapping.keys()),
            'mysql_columns': list(column_mapping.values()),
            'column_mapping': column_mapping,
            'table_name': TABLE_NAME,
            'created_time': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
            'source_file': cleaned_path
        }
        
        with open('column_mapping.json', 'w', encoding='utf-8') as f:
            json.dump(mapping_info, f, ensure_ascii=False, indent=2)
        
        logger.info("列映射信息已保存到 column_mapping.json")
        
        # 显示表结构
        cursor.execute(f"DESCRIBE {TABLE_NAME}")
        table_structure = cursor.fetchall()
        logger.info("\n表结构:")
        logger.info("=" * 90)
        logger.info(f"{'字段名':<30} {'类型':<25} {'允许空':<10}")
        logger.info("=" * 90)
        for row in table_structure:
            logger.info(f"{row[0]:<30} {row[1]:<25} {row[2]:<10}")
        
        # 显示字段统计
        logger.info("\n字段统计:")
        logger.info("=" * 70)
        logger.info(f"{'字段名':<30} {'最大长度':<10} {'字段类型':<15}")
        logger.info("=" * 70)
        
        for mysql_col, field_def, max_len, field_type in field_definitions:
            logger.info(f"{mysql_col:<30} {max_len:<10} {field_type:<15}")
        
        logger.info(f"\n字段类型统计: varchar={varchar_count}, longtext={longtext_count}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"建表过程中出错: {e}", exc_info=True)
        return False

# ============ 数据录入阶段 ============
def insert_data_from_excel(excel_file_path: str, use_existing_mapping: bool = True) -> Tuple[int, int, List[str]]:
    """将Excel数据插入到已存在的MySQL表中"""
    success_count = 0
    skip_count = 0
    error_list = []
    
    try:
        cleaned_path = clean_file_path(excel_file_path)
        logger.info(f"开始数据录入流程，读取Excel文件: {cleaned_path}")
        
        if not validate_file_path(cleaned_path):
            return 0, 0, ["文件路径无效"]
        
        column_mapping = {}
        if use_existing_mapping:
            try:
                import json
                with open('column_mapping.json', 'r', encoding='utf-8') as f:
                    mapping_info = json.load(f)
                column_mapping = mapping_info.get('column_mapping', {})
                logger.info(f"使用已有的列映射信息，共{len(column_mapping)}个字段")
            except FileNotFoundError:
                logger.warning("未找到列映射文件，将根据Excel列名自动生成")
                use_existing_mapping = False
        
        logger.info("连接MySQL数据库...")
        conn = pymysql.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        
        # 检查表是否存在
        cursor.execute(f"SHOW TABLES LIKE '{TABLE_NAME}'")
        if not cursor.fetchone():
            logger.error(f"表 '{TABLE_NAME}' 不存在，请先创建表")
            return 0, 0, ["目标表不存在"]
        
        # 获取表结构
        cursor.execute(f"DESCRIBE {TABLE_NAME}")
        table_structure = cursor.fetchall()
        mysql_fields = [row[0] for row in table_structure if row[0].lower() != 'id']
        logger.info(f"MySQL表字段: {mysql_fields}")
        
        # 检查字段数量是否匹配
        expected_field_count = len(mysql_fields)
        logger.info(f"期望的字段数量: {expected_field_count}")
        
        logger.info("开始分批次读取Excel文件...")
        total_processed = 0
        batch_num = 0
        
        # 使用自定义的分批读取函数
        for df in read_excel_in_batches(cleaned_path, BATCH_SIZE):
            batch_num += 1
            logger.info(f"处理第 {batch_num} 批数据，共 {len(df)} 行")
            
            if batch_num == 1 and not use_existing_mapping:
                # 自动生成列映射
                excel_columns = list(df.columns)[1:]  # 跳过第一列
                for excel_col in excel_columns:
                    mysql_col = clean_column_name(excel_col)
                    column_mapping[excel_col] = mysql_col
                logger.info("自动生成的列映射:")
                for excel_col, mysql_col in column_mapping.items():
                    logger.info(f"  {excel_col} -> {mysql_col}")
            
            if not column_mapping:
                logger.error("无法确定列映射关系")
                return success_count, skip_count, error_list
            
            # 检查Excel列数是否与MySQL字段数匹配
            excel_data_cols = len(df.columns) - 1  # 减去第一列
            if excel_data_cols != expected_field_count:
                logger.warning(f"Excel数据列数 ({excel_data_cols}) 与MySQL字段数 ({expected_field_count}) 不匹配")
                logger.warning("将尝试按位置对应，但可能会导致数据错位")
            
            insert_data = []
            batch_errors = 0
            
            for row_idx, row in df.iterrows():
                try:
                    total_processed += 1
                    
                    # 【修改】不再检查第二列是否为空，所有行都录入
                    
                    # 处理每一列的数据
                    row_data = []
                    # 只取与MySQL字段数量相同的数据
                    for i in range(1, min(len(df.columns), expected_field_count + 1)):
                        if i < len(row):
                            cell_value = row.iloc[i]
                            processed_value = process_cell_value(cell_value)
                            row_data.append(processed_value)
                        else:
                            row_data.append(None)
                    
                    # 如果数据列数少于MySQL字段数，补充None
                    while len(row_data) < expected_field_count:
                        row_data.append(None)
                    
                    insert_data.append(tuple(row_data))
                    
                except Exception as e:
                    batch_errors += 1
                    error_msg = f"第{total_processed}行处理失败: {str(e)}"
                    error_list.append(error_msg)
                    if len(error_list) <= 10:
                        logger.error(error_msg)
                    continue
            
            # 批量插入数据
            if insert_data:
                # 使用参数化查询，避免%符号冲突
                placeholders = ', '.join(['%s'] * expected_field_count)
                insert_sql = f"INSERT INTO {TABLE_NAME} ({', '.join(mysql_fields)}) VALUES ({placeholders})"
                
                logger.info(f"准备插入 {len(insert_data)} 行数据（本批次处理失败 {batch_errors} 行）")
                
                for i in range(0, len(insert_data), INSERT_CHUNK_SIZE):
                    chunk = insert_data[i:i + INSERT_CHUNK_SIZE]
                    
                    try:
                        # 方法1：使用executemany（如果数据中没有特殊字符）
                        cursor.executemany(insert_sql, chunk)
                        success_count += len(chunk)
                        
                        if success_count % COMMIT_FREQUENCY == 0:
                            conn.commit()
                            logger.info(f"已提交 {success_count} 行数据")
                            
                    except pymysql.Error as e:
                        conn.rollback()
                        error_msg = f"批量插入失败: {e}"
                        error_list.append(error_msg)
                        logger.error(error_msg)
                        
                        # 方法2：如果批量插入失败，尝试逐行插入
                        logger.info("尝试逐行插入...")
                        for single_row in chunk:
                            try:
                                # 对每个值进行转义处理
                                escaped_row = []
                                for value in single_row:
                                    if value is None:
                                        escaped_row.append(None)
                                    elif isinstance(value, str) and '%' in value:
                                        # 如果字符串中包含%，需要进行转义
                                        escaped_row.append(value.replace('%', '%%'))
                                    else:
                                        escaped_row.append(value)
                                
                                cursor.execute(insert_sql, tuple(escaped_row))
                                success_count += 1
                                
                                if success_count % 100 == 0:
                                    conn.commit()
                                    logger.info(f"已逐行插入 {success_count} 行")
                                    
                            except Exception as single_err:
                                error_msg = f"单行插入失败: {single_err}"
                                if len(error_list) < 100:
                                    error_list.append(error_msg)
                                logger.error(error_msg)
                                conn.rollback()
                                continue
        
        # 最终提交
        conn.commit()
        logger.info(f"数据录入完成，成功插入 {success_count} 行，处理失败 {len(error_list)} 行")
        
        # 获取插入后的总行数
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        total_rows = cursor.fetchone()[0]
        logger.info(f"表 '{TABLE_NAME}' 现在共有 {total_rows} 行数据")
        
        cursor.close()
        conn.close()
        
        # 统计错误信息
        if error_list:
            logger.warning(f"共有 {len(error_list)} 个错误")
            
        return success_count, len(error_list), error_list
        
    except Exception as e:
        logger.error(f"数据录入过程中出错: {e}", exc_info=True)
        return success_count, skip_count, error_list + [f"处理过程失败: {str(e)}"]
def read_excel_in_batches(file_path: str, batch_size: int = BATCH_SIZE):
    """
    分批次读取Excel文件
    由于pandas的read_excel不支持chunksize，我们手动实现
    """
    # 首先读取整个文件获取总行数
    logger.info(f"读取Excel文件: {file_path}")
    df_all = pd.read_excel(file_path, dtype=str)
    total_rows = len(df_all)
    logger.info(f"Excel文件总行数: {total_rows}")
    
    # 分批读取
    for start_idx in range(0, total_rows, batch_size):
        end_idx = min(start_idx + batch_size, total_rows)
        chunk = df_all.iloc[start_idx:end_idx].copy()
        logger.info(f"读取第 {start_idx+1} 到 {end_idx} 行，共 {len(chunk)} 行")
        yield chunk

# ============ 主程序 ============
def main():
    """主程序：选择执行建表或数据录入"""
    print("=" * 60)
    print("Excel数据导入MySQL工具")
    print("=" * 60)
    print("1. 建表阶段：根据Excel文件创建MySQL表")
    print("2. 数据录入阶段：将Excel数据插入到现有表中")
    print("3. 查看表结构")
    print("4. 退出")
    print("=" * 60)
    
    while True:
        try:
            choice = input("请选择操作 (1-4): ").strip()
            
            if choice == "1":
                excel_file = input("请输入Excel文件路径: ").strip()
                if not excel_file:
                    print("错误：请输入有效的文件路径")
                    continue
                
                if create_table_from_excel(excel_file):
                    print("建表成功！")
                else:
                    print("建表失败，请查看日志文件获取详细信息")
                break
                
            elif choice == "2":
                excel_file = input("请输入Excel文件路径: ").strip()
                if not excel_file:
                    print("错误：请输入有效的文件路径")
                    continue
                
                use_mapping_input = input("是否使用已有的列映射文件? (y/n): ").strip().lower()
                use_mapping = use_mapping_input == 'y' if use_mapping_input else True
                
                print("开始数据录入...")
                success, skipped, errors = insert_data_from_excel(excel_file, use_mapping)
                
                print(f"\n数据录入完成:")
                print(f"成功插入: {success} 行")
                print(f"跳过: {skipped} 行")
                print(f"错误数: {len(errors)}")
                
                if errors:
                    print("\n前10个错误:")
                    for i, error in enumerate(errors[:10]):
                        print(f"{i+1}. {error}")
                break
                
            elif choice == "3":
                try:
                    conn = pymysql.connect(**MYSQL_CONFIG)
                    cursor = conn.cursor()
                    
                    cursor.execute(f"SHOW TABLES LIKE '{TABLE_NAME}'")
                    if cursor.fetchone():
                        cursor.execute(f"DESCRIBE {TABLE_NAME}")
                        table_structure = cursor.fetchall()
                        
                        print(f"\n表 '{TABLE_NAME}' 结构:")
                        print("-" * 90)
                        print(f"{'字段名':<30} {'类型':<25} {'允许空':<10}")
                        print("-" * 90)
                        
                        for row in table_structure:
                            print(f"{row[0]:<30} {row[1]:<25} {row[2]:<10}")
                        
                        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
                        row_count = cursor.fetchone()[0]
                        print(f"\n当前表中有 {row_count} 行数据")
                    else:
                        print(f"表 '{TABLE_NAME}' 不存在")
                    
                    cursor.close()
                    conn.close()
                    
                except Exception as e:
                    print(f"查看表结构时出错: {e}")
                break
                
            elif choice == "4":
                print("退出程序")
                break
                
            else:
                print("无效的选择，请重新输入")
                
        except KeyboardInterrupt:
            print("\n程序被用户中断")
            break
        except Exception as e:
            print(f"程序出错: {e}")
            break

if __name__ == "__main__":
    main()