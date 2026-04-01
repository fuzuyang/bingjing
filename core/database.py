import os
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import declarative_base, scoped_session, sessionmaker

load_dotenv(override=True)

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

safe_password = quote_plus(DB_PASS) if DB_PASS else ""
SQLALCHEMY_DATABASE_URL = f"mysql+pymysql://{DB_USER}:{safe_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_recycle=3600,
    pool_pre_ping=True,
    echo=False,
)

session_factory = sessionmaker(autocommit=False, autoflush=False, bind=engine)
SessionLocal = scoped_session(session_factory)
Base = declarative_base()


def _normalize_kb_chunk_schema():
    """
    Ensure kb_chunk v4 schema:
    id, document_id, chunk_no, section_no, section_title,
    subsection_no, subsection_title, content, page_no, created_at
    """
    inspector = inspect(engine)
    if "kb_chunk" not in set(inspector.get_table_names()):
        return

    columns = {c["name"] for c in inspector.get_columns("kb_chunk")}
    indexes = {idx["name"] for idx in inspector.get_indexes("kb_chunk")}

    with engine.begin() as conn:
        if "subsection_no" not in columns:
            conn.execute(
                text(
                    "ALTER TABLE kb_chunk "
                    "ADD COLUMN subsection_no VARCHAR(50) NULL AFTER section_title"
                )
            )
        if "subsection_title" not in columns:
            conn.execute(
                text(
                    "ALTER TABLE kb_chunk "
                    "ADD COLUMN subsection_title VARCHAR(255) NULL AFTER subsection_no"
                )
            )

        if "idx_parent_section" in indexes:
            conn.execute(text("ALTER TABLE kb_chunk DROP INDEX idx_parent_section"))

        for col in ["parent_section", "is_title", "level_no", "updated_at"]:
            if col in columns:
                conn.execute(text(f"ALTER TABLE kb_chunk DROP COLUMN {col}"))

        refreshed = inspect(engine)
        refreshed_indexes = {idx["name"] for idx in refreshed.get_indexes("kb_chunk")}
        if "idx_subsection_no" not in refreshed_indexes:
            conn.execute(text("ALTER TABLE kb_chunk ADD INDEX idx_subsection_no (subsection_no)"))
        if "idx_doc_subsection" not in refreshed_indexes:
            conn.execute(text("ALTER TABLE kb_chunk ADD INDEX idx_doc_subsection (document_id, subsection_no)"))


def init_db():
    from core import models

    try:
        Base.metadata.create_all(
            bind=engine,
            tables=[
                models.KBDocument.__table__,
                models.KBChunk.__table__,
            ],
        )
        _normalize_kb_chunk_schema()
        print("Database schema synced (kb_document / kb_chunk).")
    except SQLAlchemyError as e:
        print(f"Database init failed: {e}")


def ensure_compliance_tables() -> dict:
    from core import models

    required_tables = [
        models.RiskAssessment.__table__,
        models.SourceDocument.__table__,
        models.BusinessType.__table__,
        models.PolicyDocument.__table__,
        models.PolicyClause.__table__,
        models.BusinessClauseMap.__table__,
        models.ProcedureStep.__table__,
        models.RequiredMaterial.__table__,
        models.ComplianceTask.__table__,
        models.TaskTypeHit.__table__,
        models.TaskGap.__table__,
    ]
    required_names = [table.name for table in required_tables]

    try:
        with engine.connect() as conn:
            existing = set(engine.dialect.get_table_names(conn))
        missing = [table for table in required_tables if table.name not in existing]
        if missing:
            Base.metadata.create_all(bind=engine, tables=missing)
        created = [table.name for table in missing]
        return {
            "success": True,
            "created_tables": created,
            "required_tables": required_names,
            "error": None,
        }
    except Exception as e:
        return {
            "success": False,
            "created_tables": [],
            "required_tables": required_names,
            "error": str(e),
        }


def get_db():
    db = SessionLocal()
    return db
