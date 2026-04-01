import pandas as pd
import pymysql
from sqlalchemy import create_engine, text
import math

class CSVToMySQL:
    def __init__(self):
        # 数据库配置
        self.mysql_config = {
            'host': '211.149.136.21',
            'port': 3307,
            'user': 'root',
            'password': 'zjcx111ch9',
            'database': 'LAWdatabase',
            'charset': 'utf8mb4'
        }
        self.engine = None
        self.connection = None
    
    def connect_mysql(self):
        """连接MySQL数据库"""
        print("连接数据库中...")
        try:
            self.engine = create_engine(
                f"mysql+pymysql://{self.mysql_config['user']}:{self.mysql_config['password']}@{self.mysql_config['host']}:{self.mysql_config['port']}/{self.mysql_config['database']}?charset={self.mysql_config['charset']}"
            )
            self.connection = self.engine.connect()
            print("数据库连接成功")
            return True
        except Exception as e:
            print(f"数据库连接失败: {e}")
            return False

    def detect_encoding(self, csv_file):
        """检测CSV文件编码"""
        encodings = ['utf-8-sig', 'utf-8', 'latin-1', 'cp1258', 'iso-8859-1', 'windows-1258']
        
        for encoding in encodings:
            try:
                # 尝试读取前5行来检测编码
                df_test = pd.read_csv(csv_file, encoding=encoding, nrows=5)
                # 检查是否包含越南语字符
                sample_text = df_test.to_string()
                # 越南语常见字符范围
                vietnamese_chars = ['á', 'à', 'ả', 'ã', 'ạ', 'ă', 'ắ', 'ằ', 'ẳ', 'ẵ', 'ặ', 
                                  'â', 'ấ', 'ầ', 'ẩ', 'ẫ', 'ậ', 'é', 'è', 'ẻ', 'ẽ', 'ẹ', 
                                  'ê', 'ế', 'ề', 'ể', 'ễ', 'ệ', 'í', 'ì', 'ỉ', 'ĩ', 'ị', 
                                  'ó', 'ò', 'ỏ', 'õ', 'ọ', 'ô', 'ố', 'ồ', 'ổ', 'ỗ', 'ộ', 
                                  'ơ', 'ớ', 'ờ', 'ở', 'ỡ', 'ợ', 'ú', 'ù', 'ủ', 'ũ', 'ụ', 
                                  'ư', 'ứ', 'ừ', 'ử', 'ữ', 'ự', 'ý', 'ỳ', 'ỷ', 'ỹ', 'ỵ', 
                                  'đ']
                
                # 如果包含越南语字符或能正常读取，认为编码正确
                if any(char in sample_text for char in vietnamese_chars) or len(df_test) > 0:
                    print(f"检测到编码: {encoding}")
                    return encoding
            except Exception as e:
                continue
        
        print("无法自动检测编码，使用默认编码: utf-8-sig")
        return 'utf-8-sig'

    def analyze_csv_structure(self, csv_file):
        """分析CSV文件结构并生成建表语句"""
        try:
            # 检测编码
            encoding = self.detect_encoding(csv_file)
            
            # 读取前20行分析数据结构
            df_sample = pd.read_csv(csv_file, encoding=encoding, nrows=20)
            
            print("CSV文件分析结果:")
            print(f"总列数: {len(df_sample.columns)}")
            print(f"列名: {df_sample.columns.tolist()}")
            print(f"文件编码: {encoding}")
            
            # 显示越南语内容样本
            print("\n越南语内容样本:")
            for i, col in enumerate(df_sample.columns):
                non_null_data = df_sample[col].dropna()
                if len(non_null_data) > 0:
                    sample = str(non_null_data.iloc[0])
                    if any(char in sample for char in ['á', 'à', 'ả', 'ã', 'ạ', 'đ']):  # 越南语特征字符
                        print(f"  列 '{col}': {sample[:50]}...")
            
            # 生成建表语句
            create_table_sql = "CREATE TABLE IF NOT EXISTS `越南法案例` (\n"
            create_table_sql += "    `id` INT AUTO_INCREMENT PRIMARY KEY,\n"
            
            for i, col_name in enumerate(df_sample.columns):
                # 分析列的数据类型和长度
                col_data = df_sample[col_name].dropna()
                
                if len(col_data) > 0:
                    # 检查是否为数值类型
                    if pd.api.types.is_numeric_dtype(df_sample[col_name]):
                        data_type = "DECIMAL(15,4)"
                    else:
                        # 文本类型，根据最大长度选择VARCHAR或TEXT
                        max_length = df_sample[col_name].astype(str).str.len().max()
                        if max_length <= 200:
                            data_type = "VARCHAR(500)"
                        elif max_length <= 1000:
                            data_type = "VARCHAR(2000)"
                        elif max_length <= 5000:
                            data_type = "TEXT"
                        else:
                            data_type = "LONGTEXT"
                else:
                    data_type = "VARCHAR(1000)"  # 默认类型
                
                create_table_sql += f"    `{col_name}` {data_type}"
                if i < len(df_sample.columns) - 1:
                    create_table_sql += ",\n"
                else:
                    create_table_sql += "\n"
            
            create_table_sql += ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
            
            print("\n=== 生成的建表语句 ===")
            print(create_table_sql)
            
            return create_table_sql, df_sample.columns.tolist(), encoding
            
        except Exception as e:
            print(f"分析CSV文件失败: {e}")
            return None, None, None
    
    def create_table(self, create_table_sql):
        """执行建表语句"""
        try:
            # 先删除已存在的表（可选）
            with self.engine.begin() as conn:
                conn.execute(text("DROP TABLE IF EXISTS `越南法案例`"))
                conn.execute(text(create_table_sql))
            print("表创建成功")
            return True
        except Exception as e:
            print(f"建表失败: {e}")
            return False
    
    def chunked_insert_to_mysql(self, csv_file, encoding, chunk_size=100):
        """分块插入数据到MySQL"""
        try:
            # 获取总行数
            with open(csv_file, 'r', encoding=encoding) as f:
                total_rows = sum(1 for line in f) - 1
            
            print(f"CSV文件总行数: {total_rows}")
            
            # 计算总块数
            total_chunks = math.ceil(total_rows / chunk_size)
            print(f"分块大小: {chunk_size} 行，总块数: {total_chunks}")
            
            # 分块读取和插入
            inserted_rows = 0
            chunk_iterator = pd.read_csv(csv_file, encoding=encoding, chunksize=chunk_size)
            
            for chunk_num, chunk in enumerate(chunk_iterator, 1):
                try:
                    # 处理数据：将空字符串转换为None
                    chunk = chunk.replace({pd.NA: None, '': None, ' ': None})
                    chunk = chunk.where(pd.notnull(chunk), None)
                    
                    # 插入数据
                    chunk.to_sql(
                        name='越南法案例',
                        con=self.engine,
                        if_exists='append',
                        index=False,
                        method='multi',
                        chunksize=1000
                    )
                    
                    inserted_rows += len(chunk)
                    print(f"第 {chunk_num}/{total_chunks} 块插入成功，本次插入 {len(chunk)} 行，累计 {inserted_rows} 行")
                    
                except Exception as e:
                    print(f"第 {chunk_num} 块插入失败: {e}")
                    continue
            
            print(f"\n数据插入完成！总共插入 {inserted_rows} 行数据")
            return inserted_rows
            
        except Exception as e:
            print(f"分块插入数据失败: {e}")
            return 0
    
    def verify_data(self):
        """验证插入的数据"""
        try:
            # 检查总行数
            count_query = "SELECT COUNT(*) as total FROM `越南法案例`"
            result = self.connection.execute(text(count_query))
            total_rows = result.fetchone()[0]
            
            print(f"\n=== 数据验证 ===")
            print(f"MySQL表中的总行数: {total_rows}")
            
            # 检查越南语内容是否正确保存
            vietnamese_query = """
            SELECT * FROM `越南法案例` 
            WHERE `标题` LIKE '%á%' OR `标题` LIKE '%à%' OR `标题` LIKE '%đ%'
            LIMIT 3
            """
            result = self.connection.execute(text(vietnamese_query))
            columns = result.keys()
            rows = result.fetchall()
            
            print(f"\n越南语内容验证 (前3条):")
            for i, row in enumerate(rows):
                row_dict = dict(zip(columns, row))
                preview = {k: (str(v)[:100] + '...' if v and len(str(v)) > 100 else v) 
                          for k, v in row_dict.items()}
                print(f"第{i+1}行: {preview}")
                
            return total_rows
            
        except Exception as e:
            print(f"数据验证失败: {e}")
            return 0
    
    def process_csv_to_mysql(self, csv_file, chunk_size=100):
        """完整的CSV到MySQL处理流程"""
        print("开始处理CSV文件到MySQL...")
        
        # 1. 连接MySQL
        if not self.connect_mysql():
            return False
        
        try:
            # 2. 分析CSV结构并建表
            create_table_sql, columns, encoding = self.analyze_csv_structure(csv_file)
            if not create_table_sql:
                return False
            
            # 3. 创建表
            if not self.create_table(create_table_sql):
                return False
            
            # 4. 分块插入数据
            inserted_rows = self.chunked_insert_to_mysql(csv_file, encoding, chunk_size)
            
            # 5. 验证数据
            if inserted_rows > 0:
                self.verify_data()
            
            return True
            
        finally:
            # 关闭连接
            if self.connection:
                self.connection.close()
            if self.engine:
                self.engine.dispose()

# 使用示例
if __name__ == "__main__":
    csv_file_path = r"D:\这里有法_资料\外国法\越南\越南法案例.csv"
    
    # 创建处理器实例
    processor = CSVToMySQL()
    
    # 执行处理
    success = processor.process_csv_to_mysql(csv_file_path, chunk_size=50)
    
    if success:
        print("\n🎉 CSV文件成功导入MySQL！")
    else:
        print("\n❌ 导入过程出现问题")