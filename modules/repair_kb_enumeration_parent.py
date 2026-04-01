import argparse
import os
import re
import sys
from dataclasses import dataclass
from typing import List, Tuple

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from core import KBChunk, KBDocument, SessionLocal  # noqa: E402
from modules.evaluator.vector_kb_retriever import VectorKBRetriever  # noqa: E402


ARTICLE_RE = re.compile(r"^第[一二三四五六七八九十百千万零〇\d]+条$")
LIST_RE = re.compile(r"^(?:[（(][一二三四五六七八九十百千万零〇\d]{1,4}[）)]|[一二三四五六七八九十百千万零〇\d]{1,4}[\.、])")


@dataclass
class RepairStats:
    doc_id: int
    doc_title: str
    scanned_rows: int = 0
    update_rows: int = 0


def _is_article(section_no: str) -> bool:
    return bool(ARTICLE_RE.match(str(section_no or "").strip()))


def _is_list_item(section_no: str) -> bool:
    sec = str(section_no or "").strip()
    if not sec:
        return False
    base = sec.split(".p", 1)[0]
    return bool(LIST_RE.match(base))


def _pick_updates(rows: List[KBChunk]) -> List[Tuple[KBChunk, str]]:
    updates: List[Tuple[KBChunk, str]] = []
    last_article: Tuple[str, int, int] | None = None  # (section_no, chunk_no, level_no)

    for row in rows:
        sec = str(row.section_no or "").strip()
        if _is_article(sec):
            last_article = (sec, int(row.chunk_no or 0), int(row.level_no or 2))
            continue

        if not sec or int(row.is_title or 0) == 1:
            continue
        if not _is_list_item(sec):
            continue
        if str(row.parent_section or "").strip():
            continue
        if not last_article:
            continue

        anchor_sec, anchor_no, anchor_level = last_article
        row_no = int(row.chunk_no or 0)
        if row_no <= anchor_no:
            continue
        if row_no - anchor_no > 24:
            continue

        row_level = int(row.level_no or 0)
        if row_level > 0 and row_level <= anchor_level:
            continue

        updates.append((row, anchor_sec))

    return updates


def repair_document(doc: KBDocument, apply: bool) -> RepairStats:
    db = SessionLocal()
    stats = RepairStats(doc_id=int(doc.id), doc_title=str(doc.title or ""))
    try:
        rows = (
            db.query(KBChunk)
            .filter(KBChunk.document_id == doc.id)
            .order_by(KBChunk.chunk_no.asc(), KBChunk.id.asc())
            .all()
        )
        stats.scanned_rows = len(rows)
        updates = _pick_updates(rows)
        stats.update_rows = len(updates)

        if apply and updates:
            for row, parent_sec in updates:
                row.parent_section = parent_sec
            db.commit()
        else:
            db.rollback()
        return stats
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
        SessionLocal.remove()


def run(doc_id: int = 0, apply: bool = False, rebuild_vector: bool = False):
    db = SessionLocal()
    try:
        query = db.query(KBDocument).order_by(KBDocument.id.asc())
        if doc_id > 0:
            query = query.filter(KBDocument.id == doc_id)
        docs = query.all()
    finally:
        db.close()
        SessionLocal.remove()

    if not docs:
        print("No kb_document found.")
        return

    total_updates = 0
    for doc in docs:
        stats = repair_document(doc, apply=apply)
        total_updates += stats.update_rows
        print(
            f"[doc_id={stats.doc_id}] {stats.doc_title} -> "
            f"scanned={stats.scanned_rows}, candidate_updates={stats.update_rows}, applied={1 if apply else 0}"
        )

    if apply and rebuild_vector:
        retriever = VectorKBRetriever()
        status = retriever.rebuild_index_now()
        print(f"vector_rebuild={status}")

    print(f"done: docs={len(docs)}, total_candidate_updates={total_updates}, applied={1 if apply else 0}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill kb_chunk.parent_section for list items under article clauses.")
    parser.add_argument("--doc-id", type=int, default=0, help="Only process one kb_document.id.")
    parser.add_argument("--apply", action="store_true", help="Apply changes. Default is dry-run.")
    parser.add_argument("--rebuild-vector", action="store_true", help="Rebuild vector index after apply.")
    args = parser.parse_args()

    run(doc_id=int(args.doc_id or 0), apply=bool(args.apply), rebuild_vector=bool(args.rebuild_vector))
