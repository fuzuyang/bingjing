import argparse
import os
import re
import sys
from dataclasses import dataclass
from typing import List

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from core import KBChunk, KBDocument, SessionLocal, init_db  # noqa: E402

SENT_END_RE = re.compile(r"[。！？；!?;：:]$")
ARTICLE_RE = re.compile(r"^第[一二三四五六七八九十百千万零〇\d]+条$")


@dataclass
class RepairStats:
    doc_id: int
    doc_title: str
    merged_groups: int = 0
    deleted_rows: int = 0
    updated_rows: int = 0
    kept_rows: int = 0


def _clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _merge_parts(parts: List[str]) -> str:
    merged = ""
    for raw in parts:
        piece = _clean_text(raw)
        if not piece:
            continue
        if not merged:
            merged = piece
            continue
        merged = f"{merged}{piece}"
    return _clean_text(merged)


def _should_merge(base: KBChunk, conts: List[KBChunk]) -> bool:
    if not conts or int(base.is_title or 0) != 0:
        return False

    section_no = str(base.section_no or "").strip()
    base_text = _clean_text(base.content)
    if not section_no or not base_text:
        return False

    # 法条正文优先合并，避免“第X条 + 第X条.p1”碎片化。
    if ARTICLE_RE.match(section_no):
        return True

    # 非法条场景仅在疑似断行时合并。
    if len(base_text) <= 32:
        return True
    if not SENT_END_RE.search(base_text):
        return True
    return False


def repair_document(doc: KBDocument, dry_run: bool = True) -> RepairStats:
    db = SessionLocal()
    stats = RepairStats(doc_id=int(doc.id), doc_title=str(doc.title or ""))

    try:
        rows = (
            db.query(KBChunk)
            .filter(KBChunk.document_id == doc.id)
            .order_by(KBChunk.chunk_no.asc(), KBChunk.id.asc())
            .all()
        )
        if not rows:
            return stats

        i = 0
        while i < len(rows):
            base = rows[i]
            section_no = str(base.section_no or "").strip()
            if int(base.is_title or 0) != 0 or (not section_no) or ".p" in section_no:
                i += 1
                continue

            conts: List[KBChunk] = []
            j = i + 1
            while j < len(rows):
                nxt = rows[j]
                nxt_section = str(nxt.section_no or "").strip()
                nxt_parent = str(nxt.parent_section or "").strip()
                if int(nxt.is_title or 0) != 0:
                    break
                if nxt_parent == section_no and nxt_section.startswith(section_no + ".p"):
                    conts.append(nxt)
                    j += 1
                    continue
                break

            if conts and _should_merge(base, conts):
                merged_text = _merge_parts([base.content] + [x.content for x in conts])
                if merged_text and merged_text != _clean_text(base.content):
                    stats.updated_rows += 1
                    if not dry_run:
                        base.content = merged_text

                stats.merged_groups += 1
                stats.deleted_rows += len(conts)
                if not dry_run:
                    for row in conts:
                        db.delete(row)

                i = j
                continue

            i += 1

        if not dry_run and (stats.merged_groups > 0 or stats.deleted_rows > 0):
            db.flush()
            kept_rows = (
                db.query(KBChunk)
                .filter(KBChunk.document_id == doc.id)
                .order_by(KBChunk.chunk_no.asc(), KBChunk.id.asc())
                .all()
            )
            for idx, row in enumerate(kept_rows, start=1):
                row.chunk_no = idx
            stats.kept_rows = len(kept_rows)
            db.commit()
        else:
            stats.kept_rows = len(rows) - stats.deleted_rows
            db.rollback()

        return stats
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
        SessionLocal.remove()


def run(doc_id: int = 0, dry_run: bool = True):
    init_db()
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
        print("未找到可修复文档。")
        return

    print(f"启动 KB 断行修复: dry_run={1 if dry_run else 0}, 文档数={len(docs)}")
    total_groups = 0
    total_deleted = 0
    for doc in docs:
        stats = repair_document(doc, dry_run=dry_run)
        total_groups += stats.merged_groups
        total_deleted += stats.deleted_rows
        print(
            f"[doc_id={stats.doc_id}] {stats.doc_title} -> "
            f"merge_groups={stats.merged_groups}, deleted_rows={stats.deleted_rows}, "
            f"updated_rows={stats.updated_rows}, kept_rows={stats.kept_rows}"
        )

    print(f"修复结束: merge_groups={total_groups}, deleted_rows={total_deleted}, dry_run={1 if dry_run else 0}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Repair kb_chunk line-wrap splits (section + section.p1)")
    parser.add_argument("--doc-id", type=int, default=0, help="Only repair one kb_document.id")
    parser.add_argument("--apply", action="store_true", help="Apply changes. Default is dry-run.")
    args = parser.parse_args()

    run(doc_id=int(args.doc_id or 0), dry_run=not bool(args.apply))
