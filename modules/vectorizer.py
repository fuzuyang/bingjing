import os
import pickle
import sys
import time
from typing import List

import faiss
import numpy as np
from dotenv import load_dotenv
from openai import OpenAI
from sqlalchemy.orm import Session

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from core import LegalKnowledge, SessionLocal

load_dotenv()


def load_faiss_index(index_path: str):
    """Load FAISS index with a Unicode-path-safe fallback."""
    try:
        return faiss.read_index(index_path)
    except Exception:
        with open(index_path, "rb") as f:
            raw = f.read()
        return faiss.deserialize_index(np.frombuffer(raw, dtype="uint8"))


def save_faiss_index(index, index_path: str):
    """Save FAISS index with a Unicode-path-safe fallback."""
    try:
        faiss.write_index(index, index_path)
    except Exception:
        raw = faiss.serialize_index(index)
        with open(index_path, "wb") as f:
            f.write(raw.tobytes())


class KnowledgeVectorizer:
    """Vectorize rows in biz_legal_knowledge and persist to FAISS."""

    def __init__(self):
        self.client = OpenAI(
            api_key=os.getenv("SILICONFLOW_API_KEY"),
            base_url="https://api.siliconflow.cn/v1",
        )
        self.embedding_model = "baai/bge-m3"

        self.data_dir = os.path.join(PROJECT_ROOT, "data")
        self.index_path = os.path.join(self.data_dir, "vector_db.index")
        self.meta_path = os.path.join(self.data_dir, "vector_meta.pkl")
        os.makedirs(self.data_dir, exist_ok=True)

    def _get_embeddings(self, texts: List[str]) -> np.ndarray:
        max_batch_size = 64
        all_embeddings = []

        print(f"开始生成向量，总文本数: {len(texts)}")
        for i in range(0, len(texts), max_batch_size):
            batch = texts[i : i + max_batch_size]
            retry_count = 3
            while retry_count > 0:
                try:
                    response = self.client.embeddings.create(
                        model=self.embedding_model,
                        input=batch,
                    )
                    all_embeddings.extend([item.embedding for item in response.data])

                    done = min(i + max_batch_size, len(texts))
                    print(f"  向量进度: {done}/{len(texts)}")
                    break
                except Exception as exc:
                    retry_count -= 1
                    print(f"  ! 批次向量化失败，重试中 ({3 - retry_count}/3): {exc}")
                    time.sleep(2)
                    if retry_count == 0:
                        raise RuntimeError(f"向量化连续失败: {exc}") from exc

        return np.array(all_embeddings).astype("float32")

    def run_incremental_update(self):
        db: Session = SessionLocal()
        try:
            items = db.query(LegalKnowledge).filter(LegalKnowledge.is_indexed == False).all()
            if not items:
                print("向量库已是最新状态，无需更新")
                return

            print(f"发现待向量化知识点: {len(items)} 条")
            texts = [f"{item.name}: {item.description}" for item in items]
            vectors = self._get_embeddings(texts)
            faiss.normalize_L2(vectors)

            metadata = []
            if os.path.exists(self.index_path) and os.path.exists(self.meta_path):
                try:
                    index = load_faiss_index(self.index_path)
                    with open(self.meta_path, "rb") as f:
                        metadata = pickle.load(f)
                    print(f"已加载现有向量索引，当前向量数: {index.ntotal}")
                except Exception as exc:
                    print(f"! 读取旧索引失败，将重建索引: {exc}")
                    index = None
            else:
                index = None

            if index is None:
                index = faiss.IndexFlatIP(vectors.shape[1])
                print(f"已初始化新索引，维度: {vectors.shape[1]}")

            base_id = index.ntotal
            index.add(vectors)

            for i, item in enumerate(items):
                metadata.append(
                    {
                        "faiss_id": base_id + i,
                        "knowledge_id": item.id,
                        "name": item.name,
                        "domain": item.domain,
                        "description": item.description,
                        "golden_quote": item.golden_quote,
                        "source_doc_id": item.source_doc_id,
                    }
                )
                item.is_indexed = True

            save_faiss_index(index, self.index_path)
            with open(self.meta_path, "wb") as f:
                pickle.dump(metadata, f)

            db.commit()
            print("向量化完成")
            print(f"向量总数: {index.ntotal}")
            print(f"索引文件: {self.index_path}")
        except Exception as exc:
            db.rollback()
            print(f"! 向量化任务失败: {exc}")
        finally:
            db.close()


if __name__ == "__main__":
    vectorizer = KnowledgeVectorizer()
    vectorizer.run_incremental_update()
