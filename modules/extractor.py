import concurrent.futures
import json
import os
import sys
from typing import Dict, List, Optional

from dotenv import load_dotenv
from openai import OpenAI
from sqlalchemy import func
from sqlalchemy.orm import Session

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from core import LegalKnowledge, SessionLocal, SourceDocument, generate_content_hash

load_dotenv()


class KnowledgeExtractor:
    """Extract structured legal knowledge from source documents into biz_legal_knowledge."""

    def __init__(self):
        self.client = OpenAI(
            api_key=os.getenv("SILICONFLOW_API_KEY"),
            base_url="https://api.siliconflow.cn/v1",
        )
        self.model = "deepseek-ai/DeepSeek-V3"
        self.max_workers = 5

    @staticmethod
    def _role_text(category: str) -> str:
        mapping = {
            "speech": "政策研究专家",
            "case": "司法裁判专家",
            "policy": "营商环境政策专家",
        }
        return mapping.get(category, "法律政策分析专家")

    def _build_prompt(self, category: str, title: str, content: str) -> str:
        role = self._role_text(category)
        return f"""
你是{role}。请阅读文档《{title}》，提炼与营商环境相关的规范性知识点。

要求：
1. 只提炼可用于风险判断的规则或原则，不要口号。
2. 每篇文档提炼1-3条，避免重复。
3. 必须给出原文证据句（golden_quote）。
4. 只返回JSON，不要其它文本。

输出格式：
{{
  "knowledges": [
    {{
      "name": "知识点名称",
      "description": "对规则的解释（30-120字）",
      "golden_quote": "原文关键句",
      "domain": "市场准入/知识产权/行政执法/企业退出/财税金融/其他"
    }}
  ]
}}

文档正文（已截断）：
{content[:12000]}
"""

    @staticmethod
    def _normalize_item(item: Dict) -> Optional[Dict]:
        name = str(item.get("name", "")).strip()
        description = str(item.get("description", "")).strip()
        if not name or not description:
            return None
        return {
            "name": name,
            "description": description,
            "golden_quote": str(item.get("golden_quote", "")).strip(),
            "domain": str(item.get("domain", "其他")).strip() or "其他",
        }

    def _extract_single_doc(self, doc_id: int):
        db: Session = SessionLocal()
        try:
            doc = db.query(SourceDocument).filter(SourceDocument.id == doc_id).first()
            if not doc:
                return

            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "你是严格的结构化信息抽取器，只输出合法JSON对象。",
                    },
                    {
                        "role": "user",
                        "content": self._build_prompt(doc.category, doc.title, doc.content_text),
                    },
                ],
                response_format={"type": "json_object"},
                temperature=0.2,
                max_tokens=2000,
            )

            payload = json.loads(response.choices[0].message.content)
            raw_items = payload.get("knowledges", [])

            new_count = 0
            for raw in raw_items:
                item = self._normalize_item(raw)
                if not item:
                    continue

                content_hash = generate_content_hash(f"{item['name']}{item['description']}")
                exists = db.query(LegalKnowledge).filter(LegalKnowledge.content_hash == content_hash).first()
                if exists:
                    continue

                db.add(
                    LegalKnowledge(
                        source_doc_id=doc.id,
                        knowledge_type="spirit" if doc.category == "speech" else "principle",
                        name=item["name"],
                        description=item["description"],
                        golden_quote=item["golden_quote"],
                        domain=item["domain"],
                        content_hash=content_hash,
                    )
                )
                new_count += 1

            db.commit()
            print(f" + 提取成功: {doc.title[:18]}... -> 新增 {new_count} 条")
        except Exception as exc:
            db.rollback()
            print(f" ! 提取失败: doc_id={doc_id}, error={str(exc)[:100]}")
        finally:
            db.close()

    def _query_unprocessed_doc_ids(self, db: Session, limit: Optional[int] = None) -> List[int]:
        query = (
            db.query(SourceDocument.id)
            .outerjoin(LegalKnowledge, LegalKnowledge.source_doc_id == SourceDocument.id)
            .group_by(SourceDocument.id)
            .having(func.count(LegalKnowledge.id) == 0)
            .order_by(SourceDocument.id.asc())
        )
        if limit is not None:
            query = query.limit(limit)
        return [row[0] for row in query.all()]

    def run_parallel_extraction(self, limit: Optional[int] = None):
        db: Session = SessionLocal()
        try:
            target_ids = self._query_unprocessed_doc_ids(db, limit=limit)
        finally:
            db.close()

        if not target_ids:
            print("知识提取跳过: 没有待处理的原始文档")
            return

        print(f"启动并发知识提取，文档数: {len(target_ids)}，并发数: {self.max_workers}")
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            executor.map(self._extract_single_doc, target_ids)
        print("知识提取完成")


if __name__ == "__main__":
    extractor = KnowledgeExtractor()
    extractor.run_parallel_extraction()
