#%% 库的导入
import pymysql
from pymilvus import connections, utility, FieldSchema, CollectionSchema, DataType, Collection
from pymysql.cursors import DictCursor
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
from typing import Dict, List, Any, Optional

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#%% class queryprovision
class Queryprovision:
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
        self.connect_databases()  
        
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
    
    #%% 法条查询 onlyprovision , provision_allcontent
    def mysql_query_provision(self, Law_name: str, Provision_number: str) -> Dict[str, Any]:
        # 输入输出仅一个法条，查询法条相关所有内容
        """
        根据法律名称和法条编号查询法条内容及标签
        Args:
            Law_name: 法律名称
            Provision_number: 法条编号（字符串类型，对应数据库中的Provision_number字段）
        """
        try:
            # 第一步：查询1_2Provision_content表获取法条基本信息
            content_query = """
            SELECT 
                Law_ID,
                Provision_ID,        # 这个字段用于查询标签表
                Chapter,
                Provision_number,    # 法条编号字符串
                Provision_text,
                Law_name
            FROM 1_2Provision_content
            WHERE Law_name = %s 
                AND Provision_number = %s  # 使用Provision_number字段匹配输入的法条编号
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
            provision_id = content_result['Provision_ID']  # 这是数字ID，用于查询标签表
            
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
            
            # 构建返回结果 - 恢复原来的完整结构
            result = {
                "data": {
                    "law_info": {
                        "Law_name": content_result['Law_name'],
                        "provision_number": Provision_number   # 输入的法条编号（字符串）
                    },
                    "content": {
                        "chapter": content_result['Chapter'],
                        "provision_number": content_result['Provision_number'],  # 数据库中的法条编号
                        "provision_text": content_result['Provision_text']
                    },
                    "labels": label_results if label_results else []  # 标签结果（可能有多个）
                }
            }
            
            return result
            
        except Exception as e:
            return {
                "success": False,
                "message": f"查询失败: {str(e)}",
                "data": None
            }


    def query_provision_text(self, Law_name: str, Provision_number: str) -> Dict[str, Any]:
        # 输入输出仅一个法条，仅检索法条正文
        """
        根据法律名称和法条编号查询法条内容
        Args:
            Law_name: 法律名称
            Provision_number: 法条编号（字符串类型）
        """
        try:
            # 查询1_2Provision_content表获取法条基本信息
            # 现在使用Provision_number字段进行匹配
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
            return {
                "success": False,
                "message": f"查询失败: {str(e)}",
                "data": None
            }
        
    def provision_allcontent(self,Provision_Input):
        #查询法条相关全部内容,输入为一个例子涉及的所有法条索引
        result = []
        for i in range(len(Provision_Input)):
            law_name, provision_num = Provision_Input[i]  
            query_result = self.mysql_query_provision(law_name, provision_num)
            result.append(query_result) 
        return result 
    
    def onlyprovision(self,Provision_Input):
        #仅查询法条,输入为一个例子涉及的所有法条索引
        result = []
        for i in range(len(Provision_Input)):
            law_name, provision_num = Provision_Input[i]  
            query_result = self.query_provision_text(law_name, provision_num)
            result.append(query_result) 
        return result  

#%% 实际调用
if __name__ == "__main__":
                          
# 通过序号检索法条内容
    provision_query = Queryprovision()  # 实例化类
    
    # 指定保存路径
    provision_text_save_path = r"C:\Users\29944\Desktop\法条正文查询结果.txt"
    provision_allcontent_save_path = r"C:\Users\29944\Desktop\法条全部相关查询结果.txt"
    all_input=[
        [
            ("中华人民共和国民法典", "第一千零六十二条"),
            ("中华人民共和国民法典", "第一百五十三条"),
            ("中华人民共和国民法典", "第一百五十七条"),
            ("最高人民法院关于适用《中华人民共和国民法典》婚姻家庭编的解释（一）", "第二十八条")
            ],
        
        [
            ("中华人民共和国刑法","第四十八条"),
            ("中华人民共和国刑法","第二百三十二条"),
            ("中华人民共和国刑法","第二百四十条"),
            ("中华人民共和国刑法","第四百二十一条")
            ],
        
        [
            ("中华人民共和国刑法","第三百八十三条"),
            ("最高人民法院、最高人民检察院关于办理贪污贿赂刑事案件适用法律若干问题的解释","第一条"),
            ("最高人民法院、最高人民检察院关于办理贪污贿赂刑事案件适用法律若干问题的解释","第十一条"),
            ("最高人民法院、最高人民检察院关于办理贪污贿赂刑事案件适用法律若干问题的解释","第十九条")
            ]
        ]
    
    # 调用onlyprovision
    for n in range(len(all_input)): # 批量查询，一个列为一个例子：a_input
        a_input=all_input[n]
        provision_output=provision_query.onlyprovision(a_input)
        #print(provision_output)
         
        # 直接输出原始内容到txt文件
        with open(provision_text_save_path, 'a', encoding='utf-8') as f:
            # ‘a’ 继续写入而不是清除原有内容写入
            f.write("="*20 + "\n")
            for item in provision_output:
                f.write(str(item) + "\n")  # 直接写入字符串形式
                
    #调用 provision_allcontent
    for n in range(len(all_input)): # 批量查询，一个列为一个例子：a_input
        a_input=all_input[n]
        provision_output=provision_query.provision_allcontent(a_input)
        #print(provision_output)
         
        # 直接输出原始内容到txt文件
        with open(provision_allcontent_save_path, 'a', encoding='utf-8') as f:
            # ‘a’ 继续写入而不是清除原有内容写入
            f.write("="*100 + "\n")
            for item in provision_output:
                f.write(str(item) + "\n")  # 直接写入字符串形式
    
    print("内容已直接输出到txt文件")