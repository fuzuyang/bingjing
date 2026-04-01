#%% 库的导入
import pymysql
from pymysql.cursors import DictCursor
import logging
import traceback
import time

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#%% class queryprovision
class Queryprovision:
    def __init__(self):
        # 数据库配置
        self.mysql_config = {
            'host': '211.149.136.21',
            'port': 3406,
            'user': 'root',
            'password': '@#Zxli*313100',
            'database': 'LAWdatabase',
            'charset': 'utf8mb4'
        }

        self.mysql_conn = None    # 用于存储 MySQL 连接对象
        
        # 配置参数
        self.MIN_CASES_FOR_STAGE1 = 5  # 第一阶段最少案例数，触发第二阶段的阈值
        self.CASE_LIMIT = 20           # 每个阶段最多返回案例数
        
        self.connect_databases()  
        
    def connect_databases(self):
        """连接数据库"""
        print("连接数据库中...")
        try:
            self.mysql_conn = pymysql.connect(**self.mysql_config)
            print("数据库连接成功")
            return True
        except Exception as e:
            print(f"数据库连接失败: {e}")
            print(traceback.format_exc())
            return False
    
    #%% 新增：案例检索辅助方法
    def build_or_conditions(self, synonyms: list, column: str) -> str:
        """构建同义替换词的OR条件"""
        if not synonyms:
            return "1=0"  # 如果没有同义词，返回假条件
        
        conditions = []
        for word in synonyms:
            if word and word.strip():  # 确保词不为空
                # 转义关键词
                try:
                    escaped_word = pymysql.converters.escape_string(word.strip())
                    # 直接构建LIKE语句
                    conditions.append(f"{column} LIKE '%{escaped_word}%'")
                except:
                    # 如果转义失败，使用简单处理
                    simple_word = word.strip().replace('%', '\\%').replace('_', '\\_')
                    conditions.append(f"{column} LIKE '%{simple_word}%'")
        
        return "(" + " OR ".join(conditions) + ")" if conditions else "1=0"
    
    def build_general_word_order_sql(self, general_words: list, table_alias: str = "") -> str:
        """
        构建一般词的排序SQL
        
        Args:
            general_words: 一般词列表 [[B1,B2], [C1,C2], ...]
            table_alias: 表别名
            
        Returns:
            order_by_sql: 排序SQL语句
        """
        if not general_words:
            return ""
        
        order_by_parts = []
        
        for i, synonyms in enumerate(general_words, 1):
            if not synonyms:
                continue
                
            # 构建匹配条件
            column = "Label_content"
            if table_alias:
                column = f"{table_alias}.Label_content"
            
            condition = self.build_or_conditions(synonyms, column)
            
            # 用于排序的CASE WHEN语句
            order_by_parts.append(f"CASE WHEN {condition} THEN 1 ELSE 0 END")
        
        # 构建ORDER BY语句，优先级高的词在前面（需要反转顺序）
        order_by_sql = "ORDER BY " + ", ".join(reversed(order_by_parts)) if order_by_parts else ""
        
        return order_by_sql
    
    def execute_case_query(self, topic_synonyms: list, general_words: list, limit: int = 20) -> list:
        """
        执行案例查询的核心函数
        
        Args:
            topic_synonyms: 主题词的同义替换词列表
            general_words: 一般词列表 [[B1,B2], [C1,C2], ...]
            limit: 返回结果的最大数量
            
        Returns:
            包含匹配结果的字典列表
        """
        if not self.mysql_conn:
            print("数据库未连接，尝试重新连接...")
            if not self.connect_databases():
                print("数据库连接失败")
                return []
        
        if not topic_synonyms:
            print("主题词为空，返回空列表")
            return []
        
        cursor = None
        try:
            # 清理关键词
            topic_synonyms = [word.strip() for word in topic_synonyms if word and word.strip()]
            general_words = [[word.strip() for word in group if word and word.strip()] 
                           for group in general_words]
            
            # 构建主题词匹配条件
            topic_condition = self.build_or_conditions(topic_synonyms, "Label_content")
            
            # 构建一般词排序条件
            order_by_sql = self.build_general_word_order_sql(general_words)
            
            # 构建完整查询
            query = f"""
            SELECT *
            FROM 1_3Provision_label
            WHERE 
                Label_category = '案例类'
                AND {topic_condition}
            """
            
            # 添加排序
            if order_by_sql:
                query += f" {order_by_sql}"
            
            # 添加限制
            query += f" LIMIT {limit}"
            
            # 执行查询
            cursor = self.mysql_conn.cursor(DictCursor)
            cursor.execute(query)
            results = cursor.fetchall()
            
            return results
            
        except Exception as e:
            print(f"案例查询出错: {e}")
            traceback.print_exc()
            return []
        finally:
            if cursor:
                cursor.close()
    
    #%% 案例检索主函数（支持两阶段检索）- 替换原有的case_search方法
    def case_search(self, keyword_groups: list, limit: int = 20) -> tuple:
        """
        案例检索主函数 - 支持两阶段检索
        替换原有的case_search方法
        
        Args:
            keyword_groups: 关键词组 [[A1,A2,A3], [B1,B2], [C1,C2], ...]
                - 第一组: 主题词（含同义替换词）
                - 后续组: 优先级递减的一般词（含同义替换词）
            limit: 每个阶段最多返回案例数，默认20条
            
        Returns:
            (stage1_results, stage2_results): 第一阶段和第二阶段的结果
        """
        if not keyword_groups or len(keyword_groups) == 0:
            print("关键词组为空，返回空列表")
            return [], []
        
        # 第一阶段：使用原始关键词组检索
        print(f"开始第一阶段检索（阈值: {self.MIN_CASES_FOR_STAGE1}个案例）")
        
        # 分离主题词和一般词
        original_topic = keyword_groups[0]  # 主题词的同义替换词
        original_general_words = keyword_groups[1:]  # 一般词列表（优先级递减）
        
        # 执行第一阶段查询
        stage1_results = self.execute_case_query(
            topic_synonyms=original_topic,
            general_words=original_general_words,
            limit=limit
        )
        
        print(f"第一阶段检索完成，找到 {len(stage1_results)} 个案例")
        
        stage2_results = []
        
        # 检查是否需要第二阶段检索
        if len(stage1_results) < self.MIN_CASES_FOR_STAGE1 and len(keyword_groups) >= 2:
            print(f"第一阶段结果不足{self.MIN_CASES_FOR_STAGE1}个，触发第二阶段检索")
            
            # 第二阶段：以优先级最高的一般词作为主题词
            # keyword_groups[1] 是原优先级最高的一般词
            new_topic = keyword_groups[1]  # 原优先级最高的一般词作为新主题词
            
            # 剩余的一般词作为新的一般词（去掉原优先级最高的一般词）
            new_general_words = keyword_groups[2:] if len(keyword_groups) > 2 else []
            
            print(f"第二阶段主题词: {new_topic}")
            print(f"第二阶段一般词: {new_general_words}")
            
            # 执行第二阶段查询
            stage2_results = self.execute_case_query(
                topic_synonyms=new_topic,
                general_words=new_general_words,
                limit=limit
            )
            
            print(f"第二阶段检索完成，找到 {len(stage2_results)} 个案例")
        else:
            print(f"第一阶段结果已达到{self.MIN_CASES_FOR_STAGE1}个或以上，不触发第二阶段检索")
        
        return stage1_results, stage2_results
    
    #%% 法条查询 onlyprovision , provision_allcontent（保持不变）
    def mysql_query_provision(self, Law_name: str, Provision_number: str) -> dict:
        # 输入输出仅一个法条，查询法条相关所有内容
        """
        根据法律名称和法条编号查询法条内容及标签
        Args:
            Law_name: 法律名称
            Provision_number: 法条编号（字符串类型，对应数据库中的Provision_number字段）
        """
        if not self.mysql_conn:
            print("数据库未连接，尝试重新连接...")
            if not self.connect_databases():
                print("数据库连接失败")
                return {
                    "success": False,
                    "message": "数据库连接失败",
                    "data": None
                }
                
        try:
            # 第一步：查询1_2Provision_content表获取法条基本信息
            content_query = """
            SELECT 
                Law_ID,
                Provision_ID,        
                Chapter,
                Provision_number,    
                Provision_text,
                Law_name
            FROM 1_2Provision_content
            WHERE Law_name = %s 
                AND Provision_number = %s
            """
            cursor = self.mysql_conn.cursor(DictCursor)           
            cursor.execute(content_query, (Law_name, Provision_number))
            content_result = cursor.fetchone()
            cursor.close()
            
            if not content_result:
                return {
                    "success": False,
                    "message": f"未找到法律 '{Law_name}' 的第 {Provision_number} 条",
                    "data": None
                }
            
            # 第二步：使用获取到的Law_ID和Provision_ID查询1_3Provision_label表
            law_id = content_result['Law_ID']
            provision_id = content_result['Provision_ID']
            
            label_query = """
            SELECT 
                Label_category,
                Label_type,
                Label_content
            FROM 1_3Provision_label
            WHERE Law_ID = %s 
                AND Provision_ID = %s
            ORDER BY Label_category, Label_type
            """
            
            cursor = self.mysql_conn.cursor(DictCursor)
            cursor.execute(label_query, (law_id, provision_id))
            label_results = cursor.fetchall()
            cursor.close()
            
            # 构建返回结果
            result = {
                "data": {
                    "law_info": {
                        "Law_name": content_result['Law_name'],
                        "provision_number": Provision_number
                    },
                    "content": {
                        "chapter": content_result['Chapter'],
                        "provision_number": content_result['Provision_number'],
                        "provision_text": content_result['Provision_text']
                    },
                    "labels": label_results if label_results else []
                }
            }
            
            return result
            
        except Exception as e:
            print(f"法条查询失败: {e}")
            traceback.print_exc()
            return {
                "success": False,
                "message": f"查询失败: {str(e)}",
                "data": None
            }


    def query_provision_text(self, Law_name: str, Provision_number: str) -> dict:
        # 输入输出仅一个法条，仅检索法条正文
        """
        根据法律名称和法条编号查询法条内容
        Args:
            Law_name: 法律名称
            Provision_number: 法条编号（字符串类型）
        """
        if not self.mysql_conn:
            print("数据库未连接，尝试重新连接...")
            if not self.connect_databases():
                print("数据库连接失败")
                return {
                    "success": False,
                    "message": "数据库连接失败",
                    "data": None
                }
                
        try:
            # 查询1_2Provision_content表获取法条基本信息
            content_query = """
            SELECT 
                Law_ID,
                Chapter,
                Provision_number,
                Provision_text,
                Law_name
            FROM 1_2Provision_content
            WHERE Law_name = %s 
                AND Provision_number = %s
            """
            cursor = self.mysql_conn.cursor(DictCursor)           
            cursor.execute(content_query, (Law_name, Provision_number))
            content_result = cursor.fetchone()
            cursor.close()
            
            if not content_result:
                return {
                    "success": False,
                    "message": f"未找到法律 '{Law_name}' 的第 {Provision_number} 条",
                    "data": None
                }
            
            # 构建返回结果
            result = {
                "Law_name": content_result['Law_name'],
                "chapter": content_result['Chapter'],
                "Provision_number": content_result['Provision_number'],
                "provision_text": content_result['Provision_text']   
            }
            
            return result
            
        except Exception as e:
            print(f"法条正文查询失败: {e}")
            traceback.print_exc()
            return {
                "success": False,
                "message": f"查询失败: {str(e)}",
                "data": None
            }
        
    def provision_allcontent(self, Provision_Input: list) -> list:
        """
        查询法条相关全部内容,输入为一个例子涉及的所有法条索引
        
        Args:
            Provision_Input: 法条索引列表，如 [("法律名称", "法条编号"), ...]
            
        Returns:
            查询结果列表
        """
        result = []
        for law_name, provision_num in Provision_Input:
            query_result = self.mysql_query_provision(law_name, provision_num)
            result.append(query_result) 
        return result 
    
    def onlyprovision(self, Provision_Input: list) -> list:
        """
        仅查询法条,输入为一个例子涉及的所有法条索引
        
        Args:
            Provision_Input: 法条索引列表，如 [("法律名称", "法条编号"), ...]
            
        Returns:
            法条正文查询结果列表
        """
        result = []
        for law_name, provision_num in Provision_Input:
            query_result = self.query_provision_text(law_name, provision_num)
            result.append(query_result) 
        return result  

#%% 实际调用
if __name__ == "__main__":
    print("开始执行查询程序...")
    
    try:
        # 通过关键词检索案例
        searcher = Queryprovision()
        
        # 使用你提供的新的关键词组格式
        keyword_groups_list = [
            # 第一组：刑事合规相关
            [
                ["刑事合规", "合规不起诉", "合规整改", "合规计划有效性"],  # 主题词
                ["单位犯罪", "企业犯罪", "公司犯罪"],                    # 优先级1一般词
                ["认罪认罚从宽", "认罪认罚", "从宽处罚"],                # 优先级2一般词
                ["社会危险性评估", "再犯可能性", "社会危害性"],          # 优先级3一般词
                ["合规评估", "合规考察", "合规监督评估"]                 # 优先级4一般词
            ],
            
            # 第二组：打击错误相关
            [
                ["打击错误", "对象错误", "方法错误", "行为偏差"],         # 主题词
                ["故意杀人", "故意伤害", "过失致人死亡", "过失致人重伤"], # 优先级1一般词
                ["刑法因果关系", "具体符合说", "法定符合说", "错误论"],   # 优先级2一般词
                ["主观罪过", "故意", "过失", "意外事件"],                # 优先级3一般词
                ["刑事责任", "罪责刑相适应", "犯罪构成", "刑罚"]         # 优先级4一般词
            ],
            
            # 第三组：间接故意相关
            [
                ["间接故意", "放任故意", "未必故意"],                    # 主题词
                ["过失", "疏忽大意", "过于自信", "有认识过失"],           # 优先级1一般词
                ["犯罪故意", "故意犯罪"],                               # 优先级2一般词
                ["犯罪过失", "过失犯罪"],                               # 优先级3一般词
                ["主观要件", "主观方面", "罪过形式", "主观罪过"]         # 优先级4一般词
            ],
            
            # 第四组：非法持有枪支相关
            [
                ["非法持有、私藏枪支、弹药罪", "非法持有枪支罪", "非法持有枪支"],  # 主题词
                ["但书", "情节显著轻微危害不大", "第十三条但书", "不认为是犯罪"],  # 优先级1一般词
                ["压缩气体枪支", "仿真枪", "气枪", "玩具枪"],                # 优先级2一般词
                ["社会危害性", "危害性显著轻微", "情节轻微"],                 # 优先级3一般词
                ["刑事处罚必要性", "免予刑事处罚", "不起诉"]                  # 优先级4一般词
            ],
            
            # 第五组：严打相关
            [
                ["严打", "严厉打击刑事犯罪", "专项斗争"],                 # 主题词
                ["刑事政策", "司法政策", "犯罪治理政策"],                 # 优先级1一般词
                ["法经济学分析", "成本效益分析", "效益与成本"],            # 优先级2一般词
                ["量刑", "刑罚裁量", "从重处罚"],                        # 优先级3一般词
                ["犯罪预防", "威慑效应", "一般预防"]                      # 优先级4一般词
            ],
            
            # 第六组：认罪认罚从宽相关
            [
                ["认罪认罚从宽", "认罪认罚", "自愿认罪", "认罚从宽"],       # 主题词
                ["量刑建议", "从宽处罚", "从轻减轻处罚", "刑罚裁量"],       # 优先级1一般词
                ["辩护律师", "值班律师", "法律帮助", "律师辩护"],          # 优先级2一般词
                ["被告人同意", "自愿性审查", "权利告知", "认罪自愿"],       # 优先级3一般词
                ["刑事速裁程序", "简易程序", "速裁程序", "诉讼程序简化"]     # 优先级4一般词
            ],
            
            # 第七组：因果关系相关
            [
                ["因果关系", "客观归责", "相当因果关系", "结果归责"],       # 主题词
                ["过失犯罪", "疏忽大意", "过于自信", "业务过失"],           # 优先级1一般词
                ["注意义务", "结果避免可能性", "风险降低", "信赖原则"],      # 优先级2一般词
                ["风险升高理论", "规范保护目的", "允许的风险", "制造法所不容许的风险"],  # 优先级3一般词
                ["犯罪构成", "四要件", "阶层理论", "客观构成要件"]           # 优先级4一般词
            ]
        ]
        
        case_save_path = r"C:\Users\29944\Desktop\案例查询结果.txt"
        
        # 清空文件内容（如果文件已存在）
        with open(case_save_path, 'w', encoding='utf-8') as f:
            f.write("案例查询结果（两阶段检索）\n")
            f.write(f"查询时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}\n")
            f.write(f"第一阶段阈值: {searcher.MIN_CASES_FOR_STAGE1}个案例\n")
            f.write(f"每阶段最大案例数: {searcher.CASE_LIMIT}\n")
            f.write("="*60 + "\n\n")
        
        # 调用两阶段检索
        for n, keyword_groups in enumerate(keyword_groups_list, 1):
            print(f"\n{'='*60}")
            print(f"开始执行第{n}组关键词查询")
            
            try:
                # 记录开始时间
                start_time = time.time()
                
                # 执行两阶段检索 - 现在直接调用case_search方法
                stage1_results, stage2_results = searcher.case_search(keyword_groups, searcher.CASE_LIMIT)
                
                # 记录结束时间
                end_time = time.time()
                search_time = end_time - start_time
                
                print(f"查询耗时: {search_time:.2f}秒")
                print(f"第一阶段结果: {len(stage1_results)}个案例")
                print(f"第二阶段结果: {len(stage2_results)}个案例")
                
                # 将结果写入txt文件
                with open(case_save_path, 'a', encoding='utf-8') as f:
                    # 每组关键词之间的分隔
                    f.write("\n" + "="*60 + "\n")
                    f.write(f"第 {n} 组关键词组\n")
                    f.write(f"查询时间: {time.strftime('%H:%M:%S', time.localtime())} (耗时: {search_time:.2f}秒)\n")
                    
                    # 显示关键词组结构
                    f.write("关键词组结构:\n")
                    f.write(f"  主题词（同义替换）: {', '.join(keyword_groups[0])}\n")
                    for i, general_group in enumerate(keyword_groups[1:], 1):
                        f.write(f"  优先级{i}一般词: {', '.join(general_group)}\n")
                    
                    f.write(f"第一阶段找到 {len(stage1_results)} 个相关案例\n")
                    
                    # 第一阶段结果
                    if stage1_results:
                        f.write("\n=== 第一阶段检索结果 ===\n\n")
                        
                        for i, item in enumerate(stage1_results, 1):
                            f.write(f"【第一阶段案例 {i}】\n")
                            f.write(f"标签类别: {item.get('Label_category', 'N/A')}\n")
                            f.write(f"标签类型: {item.get('Label_type', 'N/A')}\n")
                            f.write(f"案例内容:\n")
                            
                            # 获取内容，确保是字符串
                            content = item.get('Label_content', '')
                            if isinstance(content, str):
                                # 按段落分割并格式化输出
                                paragraphs = content.split('\n')
                                for para in paragraphs:
                                    if para.strip():  # 只输出非空段落
                                        f.write(f"  {para}\n")
                            else:
                                f.write(f"  {str(content)}\n")
                            
                            f.write("-"*50 + "\n\n")
                    else:
                        f.write("\n第一阶段未找到相关案例\n\n")
                    
                    # 第二阶段结果
                    if stage2_results:
                        f.write("=== 第二阶段检索结果 ===\n\n")
                        f.write(f"第二阶段找到 {len(stage2_results)} 个相关案例\n")
                        f.write(f"第二阶段主题词: {', '.join(keyword_groups[1])}\n")
                        if len(keyword_groups) > 2:
                            f.write("第二阶段一般词:\n")
                            for i, general_group in enumerate(keyword_groups[2:], 1):
                                f.write(f"  优先级{i}一般词: {', '.join(general_group)}\n")
                        f.write("\n")
                        
                        for i, item in enumerate(stage2_results, 1):
                            f.write(f"【第二阶段案例 {i}】\n")
                            f.write(f"标签类别: {item.get('Label_category', 'N/A')}\n")
                            f.write(f"标签类型: {item.get('Label_type', 'N/A')}\n")
                            f.write(f"案例内容:\n")
                            
                            # 获取内容，确保是字符串
                            content = item.get('Label_content', '')
                            if isinstance(content, str):
                                # 按段落分割并格式化输出
                                paragraphs = content.split('\n')
                                for para in paragraphs:
                                    if para.strip():  # 只输出非空段落
                                        f.write(f"  {para}\n")
                            else:
                                f.write(f"  {str(content)}\n")
                            
                            f.write("-"*50 + "\n\n")
                    elif len(stage1_results) < searcher.MIN_CASES_FOR_STAGE1 and len(keyword_groups) >= 2:
                        f.write("\n第二阶段未找到相关案例\n\n")
                    
                    f.write("="*60 + "\n")
                
                print(f"第{n}组关键词查询完成，结果已写入txt文件")
                
            except Exception as e:
                print(f"第{n}组关键词查询出现异常: {e}")
                traceback.print_exc()
                # 继续执行下一组
                continue
                
        print("\n所有案例查询完成")
        
    except Exception as e:
        print(f"程序执行出现异常: {e}")
        traceback.print_exc()
    
    # 通过序号检索法条内容（保持不变）
    print("\n开始法条查询...")
    try:
        provision_query = Queryprovision()  # 实例化类
        
        # 指定保存路径
        provision_text_save_path = r"C:\Users\29944\Desktop\法条正文查询结果.txt"
        provision_allcontent_save_path = r"C:\Users\29944\Desktop\法条全部相关查询结果.txt"
        
        all_input = [
            [
                ("中华人民共和国民法典", "第二百七十二条"),
                ("中华人民共和国民法典", "第二百三十六条"),
                ("中华人民共和国民法典", "第二百八十八条"),
                ("中华人民共和国民法典", "第二百八十六条")
            ],
            [
                ("中华人民共和国刑事诉讼法", "第一百九十二条")
            ],
            [
                ("中华人民共和国刑法", "第一百八十二条"),
                ("中华人民共和国证券法", "第五十五条"),
                ("中华人民共和国证券法", "第一百九十二条")
            ]
        ]
        
        # 清空法条查询结果文件
        for path in [provision_text_save_path, provision_allcontent_save_path]:
            with open(path, 'w', encoding='utf-8') as f:
                f.write("法条查询结果\n")
                f.write(f"查询时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}\n")
                f.write("="*60 + "\n\n")
        
        # 调用onlyprovision
        for n in range(len(all_input)):
            a_input = all_input[n]
            provision_output = provision_query.onlyprovision(a_input)
            
            # 直接输出原始内容到txt文件
            with open(provision_text_save_path, 'a', encoding='utf-8') as f:
                f.write(f"\n第{n+1}组法条查询结果:\n")
                f.write("="*20 + "\n")
                for item in provision_output:
                    f.write(str(item) + "\n")
        
        # 调用provision_allcontent
        for n in range(len(all_input)):
            a_input = all_input[n]
            provision_output = provision_query.provision_allcontent(a_input)
            
            # 直接输出原始内容到txt文件
            with open(provision_allcontent_save_path, 'a', encoding='utf-8') as f:
                f.write(f"\n第{n+1}组法条查询结果（全部内容）:\n")
                f.write("="*100 + "\n")
                for item in provision_output:
                    f.write(str(item) + "\n")
        
        print("法条内容已输出到txt文件")
        
    except Exception as e:
        print(f"法条查询出现异常: {e}")
        traceback.print_exc()
    
    print("\n程序执行完毕")