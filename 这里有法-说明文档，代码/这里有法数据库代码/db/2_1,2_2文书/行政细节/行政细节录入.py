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
    'port': 3406,
    'user': 'root', 
    'password': '@#Zxli*313100',
    'database': 'LAWdatabase',
    'charset': 'utf8mb4'
}

# 批量处理参数
BATCH_SIZE = 20000
INSERT_CHUNK_SIZE = 2500
COMMIT_FREQUENCY = 20000

# 表名
TABLE_NAME = '2_2Admin_label'

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('csv_to_mysql.log', encoding='utf-8'),
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
    # 修改：支持CSV格式
    if not file_path.lower().endswith(('.csv')):
        logger.error(f"文件不是CSV格式: {file_path}")
        return False
    return True

def clean_column_name(col_name: str) -> str:
    """清洗列名：保留原始字符（包括越南文字），将各种符号（包括/）转为下划线"""
    if pd.isna(col_name):
        return "unknown_column"
    
    col_str = str(col_name)
    
    # 将所有非字母数字字符（包括空格、/、特殊符号等）替换为下划线
    cleaned = re.sub(r'[^\w\s]', '_', col_str)  # 非单词字符转下划线
    cleaned = re.sub(r'\s+', '_', cleaned)      # 空白字符转下划线
    cleaned = re.sub(r'_+', '_', cleaned)       # 多个下划线合并为一个
    cleaned = cleaned.strip('_')                # 去除首尾下划线
    
    if not cleaned:
        cleaned = "column"
    
    # 保留原始大小写和字符集（包括越南文字）
    return cleaned

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
def create_table_from_csv(csv_file_path: str) -> bool:
    """根据CSV文件创建MySQL表（所有CSV列都对应MySQL字段，无自增ID）"""
    try:
        cleaned_path = clean_file_path(csv_file_path)
        logger.info(f"开始建表流程，读取CSV文件: {cleaned_path}")
        
        if not validate_file_path(cleaned_path):
            return False
        
        # 修改：使用pd.read_csv读取CSV文件
        # 添加encoding参数以支持不同编码的CSV文件
        df = pd.read_csv(cleaned_path, dtype=str, encoding='utf-8')
        logger.info(f"CSV文件读取成功，共{len(df)}行，{len(df.columns)}列")
        
        # 处理所有CSV列，包括第一列
        csv_columns = list(df.columns)
        logger.info(f"需要处理的CSV列数: {len(csv_columns)}")
        
        # 处理列名
        mysql_columns = []
        column_mapping = {}
        column_max_lengths = {}
        
        for i, csv_col in enumerate(csv_columns, 1):
            mysql_col = clean_column_name(csv_col)
            original_mysql_col = mysql_col
            counter = 1
            while mysql_col in column_mapping.values():
                mysql_col = f"{original_mysql_col}_{counter}"
                counter += 1
            
            mysql_columns.append(mysql_col)
            column_mapping[csv_col] = mysql_col
            logger.info(f"CSV列 {i}: '{csv_col}' -> MySQL字段 '{mysql_col}'")
        
        # 计算每列的最大长度（在清理末尾空白字符后）
        for csv_col, mysql_col in column_mapping.items():
            column_data = df[csv_col].iloc[1:] if len(df) > 1 else pd.Series(dtype=str)
            
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
        
        # 生成建表SQL（移除自增ID列）
        create_table_sql = f"CREATE TABLE {TABLE_NAME} (\n"
        
        field_sql_defs = []
        for mysql_col, field_def, max_len, field_type in field_definitions:
            field_sql_defs.append(f"    {mysql_col} {field_def}")
        
        create_table_sql += ",\n".join(field_sql_defs)
        create_table_sql += "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        
        # 执行建表语句
        logger.info("创建MySQL表...")
        logger.info(f"建表SQL:\n{create_table_sql}")
        cursor.execute(create_table_sql)
        
        # 创建索引（在第一个字段上创建索引）
        if mysql_columns:
            first_field = mysql_columns[0]
            cursor.execute(f"CREATE INDEX idx_{first_field} ON {TABLE_NAME}({first_field})")
            logger.info(f"在字段 '{first_field}' 上创建索引")
        
        conn.commit()
        logger.info(f"表 '{TABLE_NAME}' 创建成功！")
        
        # 保存列映射信息
        import json
        mapping_info = {
            'csv_columns': list(column_mapping.keys()),
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
def insert_data_from_csv(csv_file_path: str, use_existing_mapping: bool = True) -> Tuple[int, int, List[str]]:
    """将CSV数据插入到已存在的MySQL表中（所有CSV列都对应MySQL字段）"""
    success_count = 0
    skip_count = 0
    error_list = []
    
    try:
        cleaned_path = clean_file_path(csv_file_path)
        logger.info(f"开始数据录入流程，读取CSV文件: {cleaned_path}")
        
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
                logger.warning("未找到列映射文件，将根据CSV列名自动生成")
                use_existing_mapping = False
        
        logger.info("连接MySQL数据库...")
        conn = pymysql.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute(f"SHOW TABLES LIKE '{TABLE_NAME}'")
        if not cursor.fetchone():
            logger.error(f"表 '{TABLE_NAME}' 不存在，请先创建表")
            return 0, 0, ["目标表不存在"]
        
        cursor.execute(f"DESCRIBE {TABLE_NAME}")
        table_structure = cursor.fetchall()
        mysql_fields = [row[0] for row in table_structure]  # 获取所有字段，因为不再有id列
        
        logger.info("开始分批次读取CSV文件...")
        total_processed = 0
        batch_num = 0
        
        # 使用自定义的分批读取函数
        for df in read_csv_in_batches(cleaned_path, BATCH_SIZE):
            batch_num += 1
            logger.info(f"处理第 {batch_num} 批数据，共 {len(df)} 行")
            
            if batch_num == 1 and not use_existing_mapping:
                # 处理所有CSV列，包括第一列
                csv_columns = list(df.columns)
                for csv_col in csv_columns:
                    mysql_col = clean_column_name(csv_col)
                    column_mapping[csv_col] = mysql_col
                logger.info("自动生成的列映射:")
                for csv_col, mysql_col in column_mapping.items():
                    logger.info(f"  {csv_col} -> {mysql_col}")
            
            if not column_mapping:
                logger.error("无法确定列映射关系")
                return success_count, skip_count, error_list
            
            insert_data = []
            
            for row_idx, row in df.iterrows():
                try:
                    total_processed += 1
                    
                    # 处理所有列的数据
                    row_data = []
                    for i in range(len(df.columns)):  # 从0开始，处理所有列
                        if i < len(row):
                            csv_col = df.columns[i]
                            cell_value = row.iloc[i]
                            processed_value = process_cell_value(cell_value)
                            row_data.append(processed_value)
                        else:
                            row_data.append(None)
                    
                    insert_data.append(tuple(row_data))
                    
                except Exception as e:
                    error_msg = f"第{total_processed}行处理失败: {str(e)}"
                    error_list.append(error_msg)
                    if len(error_list) <= 10:
                        logger.error(error_msg)
                    skip_count += 1
                    continue
            
            if insert_data:
                placeholders = ', '.join(['%s'] * len(mysql_fields))
                insert_sql = f"INSERT INTO {TABLE_NAME} ({', '.join(mysql_fields)}) VALUES ({placeholders})"
                
                for i in range(0, len(insert_data), INSERT_CHUNK_SIZE):
                    chunk = insert_data[i:i + INSERT_CHUNK_SIZE]
                    
                    try:
                        cursor.executemany(insert_sql, chunk)
                        success_count += len(chunk)
                        
                        if success_count % COMMIT_FREQUENCY == 0:
                            conn.commit()
                            logger.info(f"已提交 {success_count} 行数据")
                            
                    except pymysql.Error as e:
                        conn.rollback()
                        error_msg = f"插入数据失败: {e}"
                        error_list.append(error_msg)
                        logger.error(error_msg)
                        
                        # 尝试单行插入
                        for single_row in chunk:
                            try:
                                cursor.execute(insert_sql, single_row)
                                success_count += 1
                            except Exception as single_err:
                                skip_count += 1
                                continue
        
        # 最终提交
        conn.commit()
        logger.info(f"数据录入完成，成功插入 {success_count} 行，跳过 {skip_count} 行")
        
        # 获取插入后的总行数
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        total_rows = cursor.fetchone()[0]
        logger.info(f"表 '{TABLE_NAME}' 现在共有 {total_rows} 行数据")
        
        cursor.close()
        conn.close()
        return success_count, skip_count, error_list
        
    except Exception as e:
        logger.error(f"数据录入过程中出错: {e}", exc_info=True)
        return success_count, skip_count, error_list + [f"处理过程失败: {str(e)}"]

def read_csv_in_batches(file_path: str, batch_size: int = BATCH_SIZE):
    """
    分批次读取CSV文件
    使用pandas的chunksize参数实现高效分批读取
    """
    logger.info(f"开始分批读取CSV文件: {file_path}")
    
    # 使用chunksize参数分批读取CSV文件
    # 这样可以避免一次性加载整个文件到内存
    chunk_iter = pd.read_csv(file_path, dtype=str, encoding='utf-8', chunksize=batch_size)
    
    chunk_num = 0
    for chunk in chunk_iter:
        chunk_num += 1
        logger.info(f"读取第 {chunk_num} 批数据，共 {len(chunk)} 行")
        yield chunk

# ============ 主程序 ============
def main():
    """主程序：选择执行建表或数据录入"""
    print("=" * 60)
    print("CSV数据导入MySQL工具")
    print("=" * 60)
    print("1. 建表阶段：根据CSV文件创建MySQL表")
    print("2. 数据录入阶段：将CSV数据插入到现有表中")
    print("3. 查看表结构")
    print("4. 退出")
    print("=" * 60)
    
    while True:
        try:
            choice = input("请选择操作 (1-4): ").strip()
            
            if choice == "1":
                csv_file = input("请输入CSV文件路径: ").strip()
                if not csv_file:
                    print("错误：请输入有效的文件路径")
                    continue
                
                if create_table_from_csv(csv_file):
                    print("建表成功！")
                else:
                    print("建表失败，请查看日志文件获取详细信息")
                break
                
            elif choice == "2":
                csv_file = input("请输入CSV文件路径: ").strip()
                if not csv_file:
                    print("错误：请输入有效的文件路径")
                    continue
                
                use_mapping_input = input("是否使用已有的列映射文件? (y/n): ").strip().lower()
                use_mapping = use_mapping_input == 'y' if use_mapping_input else True
                
                print("开始数据录入...")
                success, skipped, errors = insert_data_from_csv(csv_file, use_mapping)
                
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