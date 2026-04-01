import pymysql
from typing import List, Tuple, Dict, Any
import datetime

class Querykeyword:
    # 可调节的变量
    MAX_OUTPUT = 40       # 最多输出论文数量
    MIN_PAPERS = 20        # 第一部分最少输出数量
    MAX_ABSTRACT = 20      # 第二部分最多输出数量
    
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
        self.mysql_conn = None
    
    def connect_db(self):
        """连接数据库"""
        try:
            self.mysql_conn = pymysql.connect(**self.mysql_config)
            return True
        except Exception as e:
            print(f"数据库连接失败: {e}")
            return False
    
    def disconnect_db(self):
        """断开数据库连接"""
        if self.mysql_conn:
            self.mysql_conn.close()
    
    def escape_like_string(self, s: str) -> str:
        """转义LIKE语句中的特殊字符"""
        if not self.mysql_conn:
            return s
        try:
            # 使用数据库连接的转义函数
            escaped = pymysql.converters.escape_string(s)
            # 对于LIKE语句，还需要转义%和_
            escaped = escaped.replace('%', '\\%').replace('_', '\\_')
            return escaped.decode('utf-8') if isinstance(escaped, bytes) else escaped
        except:
            # 如果转义失败，直接返回原字符串（有SQL注入风险，但处理简单查询）
            return s.replace('%', '\\%').replace('_', '\\_')
    
    def build_or_conditions(self, synonyms: List[str], columns: List[str]) -> str:
        """构建OR条件语句，用于同义替换词"""
        if not synonyms:
            return "1=0"  # 如果没有同义词，返回假条件
        
        conditions = []
        for col in columns:
            for word in synonyms:
                if word and word.strip():  # 确保词不为空
                    # 转义关键词
                    escaped_word = self.escape_like_string(word.strip())
                    # 直接构建LIKE语句
                    conditions.append(f"{col} LIKE '%{escaped_word}%'")
        
        return "(" + " OR ".join(conditions) + ")" if conditions else "1=0"
    
    def build_general_word_sql(self, general_words: List[List[str]], table_alias: str = "") -> Tuple[str, List[str]]:
        """
        构建一般词的匹配条件，返回排序用的SQL和条件SQL
        
        Args:
            general_words: 一般词列表 [[B1,B2], [C1,C2], ...]
            table_alias: 表别名
            
        Returns:
            (order_by_sql, where_conditions): 排序SQL和WHERE条件列表
        """
        if not general_words:
            return "", []
        
        order_by_parts = []
        where_conditions = []
        
        for i, synonyms in enumerate(general_words, 1):
            if not synonyms:
                continue
                
            # 构建匹配条件
            cols = ["Paper_title", "Keywords", "Abstract"]
            if table_alias:
                cols = [f"{table_alias}.{col}" for col in cols]
            
            condition = self.build_or_conditions(synonyms, cols)
            
            # 用于排序的CASE WHEN语句
            order_by_parts.append(f"CASE WHEN {condition} THEN 1 ELSE 0 END")
            
            # 用于WHERE的条件
            where_conditions.append(condition)
        
        # 构建ORDER BY语句，优先级高的词在前面
        # 注意：order_by_parts列表是按优先级顺序存储的，但SQL ORDER BY需要从高优先级到低优先级
        # 所以我们需要反转顺序
        order_by_sql = "ORDER BY " + ", ".join(reversed(order_by_parts)) if order_by_parts else ""
        
        return order_by_sql, where_conditions
    
    def execute_query(self, sql: str, limit: int = None) -> List[Tuple]:
        """执行SQL查询（直接执行，不使用参数化查询）"""
        try:
            cursor = self.mysql_conn.cursor()
            
            if limit:
                sql += f" LIMIT {limit}"
                
            print(f"执行SQL查询...")
            cursor.execute(sql)
            results = cursor.fetchall()
            cursor.close()
            print(f"查询到 {len(results)} 条结果")
            return results
        except pymysql.Error as e:
            print(f"查询执行失败: {e}")
            print(f"错误代码: {e.args[0]}")
            print(f"错误信息: {e.args[1]}")
            return []
        except Exception as e:
            print(f"查询执行失败: {e}")
            return []
    
    def get_column_names(self, table_name: str) -> List[str]:
        """获取表的列名"""
        try:
            cursor = self.mysql_conn.cursor()
            cursor.execute(f"DESCRIBE `{table_name}`")
            columns = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return columns
        except Exception as e:
            print(f"获取列名失败: {e}")
            # 返回默认列名或空列表
            return []
    
    def query_papers(self, keyword_groups: List[List[str]], output_file: str = "results.txt") -> Dict[str, Any]:
        """
        主查询函数
        
        Args:
            keyword_groups: 关键词组 [[A1,A2,A3], [B1,B2], [C1,C2], ...]
            output_file: 输出文件路径
            
        Returns:
            查询结果统计信息
        """
        if not self.connect_db():
            return {"error": "数据库连接失败"}
        
        try:
            # 分离主题词和一般词
            if not keyword_groups:
                return {"error": "关键词组不能为空"}
            
            topic_synonyms = keyword_groups[0]  # 主题词的同义替换词
            general_words = keyword_groups[1:]  # 一般词列表
            
            if not topic_synonyms:
                return {"error": "主题词不能为空"}
            
            # 清理关键词，去除空值和空格
            topic_synonyms = [word.strip() for word in topic_synonyms if word and word.strip()]
            general_words = [[word.strip() for word in group if word and word.strip()] 
                           for group in general_words]
            
            print(f"主题词: {topic_synonyms}")
            print(f"一般词: {general_words}")
            
            # 获取列名
            columns = self.get_column_names("3_1Paper")
            if not columns:
                # 如果无法获取列名，使用默认值
                columns = ["PaperID", "Paper_title", "Authors", "Keywords", "Abstract", 
                          "Publication", "Year", "DOI", "URL"]
                print(f"使用默认列名: {columns}")
            
            # 阶段一：在Paper_title和Keywords中匹配主题词
            print("开始阶段一查询：在Paper_title和Keywords中匹配主题词...")
            phase1_results = self._query_phase1(topic_synonyms, general_words, columns)
            
            # 确保phase1_results是列表
            if not isinstance(phase1_results, list):
                phase1_results = list(phase1_results) if phase1_results else []
            
            # 阶段二：如果阶段一结果不足，在Abstract中匹配主题词
            phase2_results = []
            if len(phase1_results) < self.MIN_PAPERS:
                print(f"阶段一结果不足{self.MIN_PAPERS}篇，开始阶段二查询...")
                phase2_results = self._query_phase2(topic_synonyms, general_words, columns)
                # 确保phase2_results是列表
                if not isinstance(phase2_results, list):
                    phase2_results = list(phase2_results) if phase2_results else []
            
            # 合并结果并限制总数
            all_results = phase1_results + phase2_results
            final_results = all_results[:self.MAX_OUTPUT]
            
            # 输出结果到文件（追加模式）
            self._write_results_to_file(final_results, columns, phase1_results, 
                                      phase2_results, output_file, keyword_groups)
            
            # 返回统计信息
            return {
                "phase1_count": len(phase1_results),
                "phase2_count": len(phase2_results),
                "total_found": len(all_results),
                "total_output": len(final_results),
                "output_file": output_file
            }
            
        except Exception as e:
            print(f"查询过程中发生错误: {e}")
            import traceback
            traceback.print_exc()
            return {"error": str(e)}
        finally:
            self.disconnect_db()
    
    def _query_phase1(self, topic_synonyms: List[str], general_words: List[List[str]], 
                     columns: List[str]) -> List[Tuple]:
        """阶段一查询：在Paper_title和Keywords中匹配主题词"""
        # 构建主题词匹配条件
        topic_condition = self.build_or_conditions(topic_synonyms, ["Paper_title", "Keywords"])
        
        # 构建一般词排序条件
        order_by_sql, _ = self.build_general_word_sql(general_words)
        
        # 构建完整SQL
        sql = f"""
        SELECT * FROM `3_1Paper`
        WHERE {topic_condition}
        """
        
        # 添加排序
        if order_by_sql:
            sql += f" {order_by_sql}"
        
        # 查询数据
        results = self.execute_query(sql, limit=self.MAX_OUTPUT)
        # 确保返回列表
        return list(results) if results else []
    
    def _query_phase2(self, topic_synonyms: List[str], general_words: List[List[str]],
                     columns: List[str]) -> List[Tuple]:
        """阶段二查询：在Abstract中匹配主题词"""
        # 构建主题词匹配条件（仅在Abstract中）
        topic_condition = self.build_or_conditions(topic_synonyms, ["Abstract"])
        
        # 构建阶段一已匹配的主题词条件（用于排除）
        phase1_topic_condition = self.build_or_conditions(topic_synonyms, ["Paper_title", "Keywords"])
        
        # 构建一般词排序条件
        order_by_sql, _ = self.build_general_word_sql(general_words)
        
        # 构建完整SQL
        sql = f"""
        SELECT * FROM `3_1Paper`
        WHERE {topic_condition}
        AND PaperID NOT IN (
            SELECT PaperID FROM `3_1Paper` 
            WHERE {phase1_topic_condition}
        )
        """
        
        # 添加排序
        if order_by_sql:
            sql += f" {order_by_sql}"
        
        # 查询数据
        results = self.execute_query(sql, limit=self.MAX_ABSTRACT)
        # 确保返回列表
        return list(results) if results else []
    
    def _write_results_to_file(self, results: List[Tuple], columns: List[str],
                             phase1_results: List[Tuple], phase2_results: List[Tuple],
                             output_file: str, keyword_groups: List[List[str]] = None):
        """将结果写入文件（追加模式）"""
        with open(output_file, 'a', encoding='utf-8') as f:
            # 首先写入关键词组信息
            if keyword_groups:
                f.write("检索关键词组：\n")
                f.write(f"主题词: {', '.join(keyword_groups[0])}\n")
                for i, general_words in enumerate(keyword_groups[1:], 1):
                    f.write(f"优先级{i}一般词: {', '.join(general_words)}\n")
                f.write("\n")

            # 写入统计信息
            f.write(f"\n统计信息：\n")
            f.write(f"阶段一找到论文数：{len(phase1_results)}\n")
            f.write(f"阶段二找到论文数：{len(phase2_results)}\n")
            f.write(f"总计找到论文数：{len(phase1_results) + len(phase2_results)}\n")
            f.write(f"最终输出论文数：{len(results)}\n")
            
            # 写入阶段一结果
            if phase1_results:
                f.write("=== 在标题和关键词中匹配主题词的结果 ===\n\n")
                # 筛选出在最终结果中的阶段一论文
                phase1_in_output = [r for r in results if r in phase1_results]
                for i, row in enumerate(phase1_in_output, 1):
                    f.write(f"【论文 {i}】\n")
                    for col, value in zip(columns, row):
                        # 处理可能为None的值
                        display_value = value if value is not None else ""
                        f.write(f"{col}: {display_value}\n")
                    f.write("\n" + "="*50 + "\n\n")
            
            # 写入阶段二结果
            if phase2_results:
                f.write("=== 以下为在摘要中检索主题词的结果 ===\n\n")
                # 筛选出在最终结果中的阶段二论文
                phase2_in_output = [r for r in results if r in phase2_results]
                start_idx = len(phase1_in_output) + 1 if phase1_results else 1
                for i, row in enumerate(phase2_in_output, start_idx):
                    f.write(f"【论文 {i}】\n")
                    for col, value in zip(columns, row):
                        display_value = value if value is not None else ""
                        f.write(f"{col}: {display_value}\n")
                    f.write("\n" + "="*50 + "\n\n")
            

    
    def update_config(self, max_output: int = None, min_papers: int = None, 
                     max_abstract: int = None):
        """更新配置参数"""
        if max_output is not None:
            self.MAX_OUTPUT = max_output
        if min_papers is not None:
            self.MIN_PAPERS = min_papers
        if max_abstract is not None:
            self.MAX_ABSTRACT = max_abstract


# 使用示例
if __name__ == "__main__":
    # 批量查询示例 - 三级数组结构
    batch_keyword_groups = [
              
        [
            ['数字游民', '数字游牧民', '远程工作者', '电子居民'],
            ['签证', '签证政策', '居留许可', '工作签证'],
            ['税收', '税收政策', '税务规则', '税收管辖权'],
            ['协同', '协同治理', '协调机制', '政策衔接'],
            ['功能导向', '政策目标', '功能原则', '目的导向']
        ],
        
        [
            ['预备行为实行化', '预备犯', '预备行为犯罪化', '预备实行化'],
            ['立法限度', '立法谦抑性', '立法边界', '立法原则'],
            ['司法认定', '司法判断', '司法标准', '犯罪认定'],
            ['着手实行', '实行着手', '着手', '实行行为'],
            ['可罚性', '处罚必要性', '法益侵害紧迫性', '危险判断']
        ],
        
        [
            ['打击错误', '对象错误', '目标错误'],
            ['法定符合说', '法定一致说', '抽象符合说'],
            ['具体符合说', '具体一致说', '个别化说'],
            ['修正的具体符合说', '修正的具体一致说', '法定具体符合说'],
            ['刑法功能主义', '功能主义刑法', '机能主义刑法']
        ],
        
        [
            ['严打', '专项斗争', '严打斗争', '严打政策'],
            ['刑事政策', '犯罪治理', '刑事司法政策', '治乱世用重典'],
            ['效益', '威慑效应', '威慑力', '威慑效果'],
            ['成本', '负面效应', '副作用', '社会代价'],
            ['法经济学', '法律经济学', '经济分析法学', '法律的经济分析']
        ],
        
        [
            ['借款', '借贷', '民间借贷', '贷款'],
            ['催收', '催告', '清偿', '偿还'],
            ['违约', '债务不履行', '违约责任'],
            ['诉讼时效', '诉讼期间', '除斥期间'],
            ['证据', '借据', '欠条', '转账记录']
        ],
        
        [
            ['老龄化社会', '老龄化', '老龄社会', '老年型社会'],
            ['人口老龄化', '老年抚养比', '少子老龄化'],
            ['法理学', '法哲学', '法律理论'],
            ['社会法', '社会保障法', '社会照顾法'],
            ['权利理论', '权利保障', '权利基础', '人权']
        ]
    ]
    
    # 创建查询对象
    query = Querykeyword()
    
    # 修改配置参数
    query.update_config(max_output=50, min_papers=10, max_abstract=20)
    
    # 批量查询
    all_results = []
    output_file = "paper_results.txt"  # 固定文件名
    
    # 清空或创建输出文件
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("批量查询结果\n")
        f.write(f"生成时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("="*60 + "\n\n")
    
    for i, keyword_groups in enumerate(batch_keyword_groups, 1):
        print(f"\n{'='*60}")
        print(f"开始执行第{i}组关键词查询")
        print(f"关键词组: {keyword_groups}")
        print('='*60)
        
        # 在结果文件中添加组信息标题
        with open(output_file, 'a', encoding='utf-8') as f:
            f.write(f"\n{'='*60}\n")
            f.write(f"第{i}组关键词查询\n")
            f.write(f"查询时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write('='*60 + '\n\n')
        
        # 执行查询（追加到同一个文件）
        result = query.query_papers(keyword_groups, output_file)
        
        # 在结果文件中添加分隔线
        with open(output_file, 'a', encoding='utf-8') as f:
            f.write(f"\n{'='*60}\n")
            f.write(f"第{i}组关键词查询完成\n")
            f.write('='*60 + '\n\n')
        
        if "error" in result:
            error_msg = f"查询失败: {result['error']}\n"
            print(error_msg)
            all_results.append({"group": i, "error": result['error']})
        else:
            success_msg = f"查询成功！\n"
            success_msg += f"阶段一找到：{result.get('phase1_count', 0)}篇\n"
            success_msg += f"阶段二找到：{result.get('phase2_count', 0)}篇\n"
            success_msg += f"总计输出：{result.get('total_output', 0)}篇\n"
            
            print(success_msg)
            all_results.append({
                "group": i,
                "phase1_count": result.get('phase1_count', 0),
                "phase2_count": result.get('phase2_count', 0),
                "total_output": result.get('total_output', 0)
            })
    
    # 输出批量查询总结
    print(f"\n{'='*60}")
    print("批量查询完成！")
    print(f"{'='*60}")
    
    successful_groups = [r for r in all_results if "error" not in r]
    failed_groups = [r for r in all_results if "error" in r]
    
    print(f"成功查询组数: {len(successful_groups)}")
    print(f"失败查询组数: {len(failed_groups)}")
    
    if successful_groups:
        total_phase1 = sum(r.get('phase1_count', 0) for r in successful_groups)
        total_phase2 = sum(r.get('phase2_count', 0) for r in successful_groups)
        total_output = sum(r.get('total_output', 0) for r in successful_groups)
        
        print(f"\n总计统计：")
        print(f"所有组阶段一总计找到：{total_phase1}篇")
        print(f"所有组阶段二总计找到：{total_phase2}篇")
        print(f"所有组总计输出：{total_output}篇")
    
    if failed_groups:
        print(f"\n失败组详情：")
        for failed in failed_groups:
            print(f"第{failed['group']}组: {failed['error']}")
    
    print(f"\n所有查询结果已保存到：{output_file}")