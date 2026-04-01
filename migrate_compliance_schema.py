from sqlalchemy import text

from core.database import engine, init_db


def _has_column(table_name: str, col_name: str) -> bool:
    sql = text(
        """
        SELECT COUNT(*) AS cnt
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = :table_name
          AND COLUMN_NAME = :col_name
        """
    )
    with engine.connect() as conn:
        cnt = conn.execute(sql, {"table_name": table_name, "col_name": col_name}).scalar() or 0
    return cnt > 0


def _exec(sql: str):
    with engine.begin() as conn:
        conn.execute(text(sql))


def migrate():
    print("== 初始化缺失表 ==")
    init_db()

    print("== 检查并升级 biz_compliance_task ==")
    if not _has_column("biz_compliance_task", "compliance_status"):
        _exec("ALTER TABLE biz_compliance_task ADD COLUMN compliance_status VARCHAR(32) NULL")
        print("  + 已新增字段: compliance_status")
    else:
        print("  - 字段已存在: compliance_status")

    # 兼容历史 tinyint 输入模式，升级为可读字符串
    if _has_column("biz_compliance_task", "input_mode"):
        _exec(
            """
            ALTER TABLE biz_compliance_task
            MODIFY COLUMN input_mode VARCHAR(32) NOT NULL DEFAULT 'text'
            """
        )
        print("  + 已调整字段类型: input_mode -> VARCHAR(32)")

    if _has_column("biz_compliance_task", "risk_level"):
        _exec(
            """
            ALTER TABLE biz_compliance_task
            MODIFY COLUMN risk_level VARCHAR(20) NULL
            """
        )
        print("  + 已调整字段类型: risk_level -> VARCHAR(20)")

    if _has_column("biz_compliance_task", "input_text"):
        _exec(
            """
            ALTER TABLE biz_compliance_task
            MODIFY COLUMN input_text LONGTEXT NULL
            """
        )
        print("  + 已调整字段类型: input_text -> LONGTEXT")

    print("== 迁移完成 ==")


if __name__ == "__main__":
    migrate()
