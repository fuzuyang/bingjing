import os
import sys
from typing import Optional

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from modules.pdf_kb_ingester import PDFKBIngestor


def run(raw_folder: Optional[str] = None):
    target_folder = raw_folder or os.path.join(PROJECT_ROOT, "raw_file")
    print(f"启动 PDF 入库管道，目录: {target_folder}")

    ingestor = PDFKBIngestor(raw_folder=target_folder)
    results = ingestor.ingest_folder(target_folder)
    if not results:
        print("未发现可入库PDF文件。")
        return

    ok = 0
    skipped = 0
    failed = 0
    for row in results:
        print(
            f"[{row.status}] {os.path.basename(row.file_path)} -> {row.message}; "
            f"doc_id={row.document_id}, pages={row.page_count}, chunks={row.chunk_count}, llm_refined={row.llm_refined_count}"
        )
        if row.status == "ok":
            ok += 1
        elif row.status == "skipped":
            skipped += 1
        else:
            failed += 1

    print(f"入库结束: 成功 {ok}, 跳过 {skipped}, 失败 {failed}")


if __name__ == "__main__":
    run()
