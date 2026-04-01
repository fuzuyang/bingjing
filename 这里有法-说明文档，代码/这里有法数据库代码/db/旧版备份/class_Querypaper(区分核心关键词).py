# -*- coding: utf-8 -*-
"""
Created on Wed Nov 12 00:38:31 2025

@author: 29944
"""
#%% 库的导入
import pymysql
from pymilvus import connections, utility, FieldSchema, CollectionSchema, DataType, Collection
import time
import hashlib
import pandas as pd
import random
import torch
from datetime import datetime
import numpy as np
import logging
import os
import requests
import json
import re
#%% milvus建库(已注释掉)
'''
from pymilvus import db
#链接milvus
connections.connect(alias="default", host="211.149.136.21", port="19531")
db.create_database("LAWdb")
print(db.list_database())
'''
#%% 配置日志；数据库配置
# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Querypaper:
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

        self.milvus_config = {
            'host': '211.149.136.21',
            'port': '19531',
            'db_name':'LAWdb'
        }

        self.mysql_conn = None    # 用于存储 MySQL 连接对象
        self.vector_model = None  # 用于存储向量模型
        self.connect_databases()  # 调用方法加载向量模型
        self.load_vector_model()  # 调用方法加载向量模型
#%% 连接MySQL、milvus
    def connect_databases(self):
        """连接数据库"""
        print("连接数据库中...")
        try:
            self.mysql_conn = pymysql.connect(**self.mysql_config)
            connections.connect("default", **self.milvus_config)
            print("数据库连接成功")
            return True
        except Exception as e:
            print(f"数据库连接失败: {e}")
            return False

#%% 向量模型相关  get_text_vector (生成文本向量)
    def load_vector_model(self):
        """加载向量化模型 - 修复版本"""
        print("加载向量化模型中...")
        try:
            # 尝试加载本地模型
            model_path = r"D:\my_downloads\models\sentence-transformers_paraphrase-multilingual-MiniLM-L12-v2"
            if os.path.exists(model_path):
                print(f"使用本地模型: {model_path}")
                try:
                    # 修复导入问题 - 使用更兼容的方式
                    try:
                        from sentence_transformers import SentenceTransformer
                        self.vector_model = SentenceTransformer(model_path)
                        print("本地模型加载成功")
                        print(f"向量维度: {self.vector_model.get_sentence_embedding_dimension()}")
                        return
                    except ImportError as e:
                        print(f"SentenceTransformer导入失败: {e}")
                        # 尝试使用transformers
                        from transformers import AutoTokenizer, AutoModel
                        import torch
                        self.vector_model = {
                            'tokenizer': AutoTokenizer.from_pretrained(model_path),
                            'model': AutoModel.from_pretrained(model_path),
                            'type': 'transformers'
                        }
                        print("使用transformers加载模型成功")
                except Exception as e:
                    print(f"本地模型加载失败: {e}")

            # 如果本地模型不可用，使用增强语义哈希
            print("使用增强语义哈希方法生成向量")
            self.vector_model = None
            print("向量维度: 384")

        except Exception as e:
            print(f"向量化模型加载失败，使用备用方法: {e}")
            self.vector_model = None

    def get_text_vector(self, text):
        """获取文本向量 - 修复版本"""
        if not text or not isinstance(text, str):
            return [random.random() for _ in range(384)]

        try:
            # 如果有可用的向量模型
            if self.vector_model is not None:
                if hasattr(self.vector_model, 'encode'):
                    # 使用SentenceTransformer模型
                    vector = self.vector_model.encode(text).tolist()
                    return vector
                elif isinstance(self.vector_model, dict) and self.vector_model.get('type') == 'transformers':
                    # 使用transformers模型
                    return self._get_transformers_vector(text)

            # 使用增强语义哈希方法
            return self._get_enhanced_semantic_hash_vector(text)

        except Exception as e:
            print(f"向量生成失败，使用备用方法: {e}")
            return self._get_enhanced_semantic_hash_vector(text)

    def _get_transformers_vector(self, text):
        """使用transformers模型获取向量"""
        try:
            inputs = self.vector_model['tokenizer'](text, return_tensors="pt", truncation=True, padding=True,
                                                    max_length=512)

            with torch.no_grad():
                outputs = self.vector_model['model'](**inputs)
            # 使用平均池化获取句子向量
            embeddings = outputs.last_hidden_state.mean(dim=1).squeeze().numpy().tolist()
            # 如果维度不是384，进行调整
            if len(embeddings) != 384:
                if len(embeddings) > 384:
                    embeddings = embeddings[:384]
                else:
                    embeddings.extend([0.0] * (384 - len(embeddings)))
            return embeddings
        except Exception as e:
            print(f"Transformers向量生成失败: {e}")
            return self._get_enhanced_semantic_hash_vector(text)

    def _get_enhanced_semantic_hash_vector(self, text):
        """增强的语义哈希向量生成方法"""
        if not isinstance(text, str):
            text = str(text)

        text = text.strip().lower()

        # 使用多种哈希算法组合
        md5_hash = hashlib.md5(text.encode('utf-8')).hexdigest()
        sha1_hash = hashlib.sha1(text.encode('utf-8')).hexdigest()
        sha256_hash = hashlib.sha256(text.encode('utf-8')).hexdigest()[:32]

        combined_hash = md5_hash + sha1_hash + sha256_hash

        # 生成更均匀的向量
        vector = []
        for i in range(0, min(len(combined_hash), 384 * 3), 3):
            hex_part = combined_hash[i:i + 3]
            try:
                num = int(hex_part, 16) / 4095.0
                vector.append(num)
            except:
                vector.append(random.random())

        # 如果向量长度不够，用随机数填充
        while len(vector) < 384:
            vector.append(random.random())

        return vector[:384]

#%% 查询
    def mysql_query_paperinfo(self,query_keyword_list,limit=30):
        #论文信息表查询
        print("\n" + "="*80)
        print("论文信息表查询")
        print("="*80)
   
        if not query_keyword_list:
            return []
        
        try:
            # 构建查询条件和命中计数
            conditions = []
            hit_count_conditions = []
            params = []
            
            for keyword in query_keyword_list:
                # 查询条件
                conditions.append("Keywords LIKE %s")
                # 命中计数条件（每个关键词命中计1分）
                hit_count_conditions.append(f"CASE WHEN Keywords LIKE %s THEN 1 ELSE 0 END")
                params.extend([f"%{keyword}%"])
            
            where_clause = " OR ".join(conditions)
            hit_count_clause = " + ".join(hit_count_conditions)
            all_params=params + params
        
            query = f"""
                SELECT Paper_title, Author, Keywords, Abstract, Publication_date, 
                ({hit_count_clause}) as hit_count 
                FROM `3_1Paper_basic_information`
                WHERE {where_clause}
                ORDER BY hit_count DESC, STR_TO_DATE(Publication_date, '%%Y-%%m-%%d') DESC
                LIMIT {limit}
                """
                   
            cursor = self.mysql_conn.cursor()
            cursor.execute(query, all_params)
            results = cursor.fetchall()
            cursor.close()
                
            return results    
        except Exception as e:
            print(f"❌ 查询执行错误: {e}")
            import traceback
            traceback.print_exc()
            return[]
            
#%%       
# 使用示例（核心关键词＋全关键词）

queries = [
    {
        "core_keywords": ["滥用市场支配地位", "垄断协议", "排他性交易"],
        "all_keywords": ["滥用市场支配地位", "垄断协议", "排他性交易", "限定交易", "相对优势地位", "相关市场界定", "市场支配地位认定", "反垄断法", "公平竞争", "经营者集中", "行政处罚", "民事赔偿"]
    }
]

output_filename = r"C:\Users\29944\Desktop\查询结果(核心_全).xlsx"

if __name__ == "__main__":
    search=Querypaper()
    results_list=[]  # 修改变量名避免冲突
    
    for i, query in enumerate(queries,1):
        print(f"正在处理第{i}个关键词列表：{query}")
        print(f"核心关键词:{query['core_keywords']}")
        print(f"全关键词:{query['all_keywords']}")
        
        try:
            #检索核心关键词
            core_results = search.mysql_query_paperinfo(query['core_keywords'], limit=30)
            core_count = len(core_results) if core_results else 0
            
            #检索全关键词 - 获取更多结果用于去重
            all_results = search.mysql_query_paperinfo(query['all_keywords'], limit=50)
            all_count = len(all_results) if all_results else 0

            # 去重处理：从全关键词结果中剔除核心关键词结果
            filtered_all_results = []
            if all_results and core_results:
                # 提取核心关键词结果的标题用于去重
                core_titles = set()
                for result in core_results:
                    if len(result) > 0:
                        core_titles.add(result[0].strip().lower())  # 标题转小写去重
                
                # 过滤全关键词结果
                for result in all_results:
                    if len(result) > 0 and result[0].strip().lower() not in core_titles:
                        filtered_all_results.append(result)
                    # 如果已经达到15篇，就停止
                    if len(filtered_all_results) >= 15:
                        break
            elif all_results:
                # 如果没有核心关键词结果，直接取前15篇
                filtered_all_results = all_results[:15]
            
            filtered_count = len(filtered_all_results)

            output_content = f"根据核心关键词检索到{core_count}篇文献，分别如下：\n"

            if core_results:
                for j, result in enumerate(core_results, 1):
                    # 修正索引位置
                    paper_title = result[0] if len(result) > 0 else "未知标题"
                    paper_author = result[1] if len(result) > 1 else "未知作者"
                    paper_keywords = result[2] if len(result) > 2 else "未知关键词"
                    paper_abstract = result[3] if len(result) > 3 else "未知摘要"
                    paper_year = result[4] if len(result) > 4 else "未知年份"
                    
                    paper_info = f"{j}. 题目：{paper_title}\n"
                    paper_info += f"   作者：{paper_author}\n"
                    paper_info += f"   关键词：{paper_keywords}\n"
                    paper_info += f"   摘要：{paper_abstract}\n"
                    paper_info += f"   出版年份：{paper_year}\n"
                    output_content += paper_info + "\n"
            else:
                output_content += "无相关文献\n"
                
            output_content += f"\n根据全关键词（剔除核心关键词结果后）检索到{filtered_count}篇文献，分别如下：\n"
            
            if filtered_all_results:
                for k, result in enumerate(filtered_all_results, 1):
                    # 修正索引位置
                    paper_title = result[0] if len(result) > 0 else "未知标题"
                    paper_author = result[1] if len(result) > 1 else "未知作者"
                    paper_keywords = result[2] if len(result) > 2 else "未知关键词"
                    paper_abstract = result[3] if len(result) > 3 else "未知摘要"
                    paper_year = result[4] if len(result) > 4 else "未知年份"
                    
                    paper_info = f"{k}. 题目：{paper_title}\n"
                    paper_info += f"   作者：{paper_author}\n"
                    paper_info += f"   关键词：{paper_keywords}\n"
                    paper_info += f"   摘要：{paper_abstract}\n"
                    paper_info += f"   出版年份：{paper_year}\n"
                    output_content += paper_info + "\n"
            else:
                output_content += "无相关文献\n"                
            
            # 将结果添加到总结果中
            results_list.append([i, output_content])
            print(f"第{i}个查询完成：核心关键词找到{core_count}篇，全关键词去重后找到{filtered_count}篇")
            
        except Exception as e:
            print(f"第{i}个查询出错：{e}")
            error_content = f"查询出错: {str(e)}"
            results_list.append([i, error_content])
            
            
    # 输出格式，防止截断
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)

    if results_list:
        result_df = pd.DataFrame(results_list, columns=['查询序号', '查询结果'])
        print(result_df)
        result_df.to_excel(output_filename, index=False)
        print(f"查询完成！")
        print(f"总共处理{len(results_list)}个查询项")
        print(f"结果已保存到: {output_filename}")
    else:
        print("没有查询到任何结果")