#此文件是初始化功能二所需表格（存储源文件内容以及大模型解析后的要素）

import os
from datetime import datetime

from dotenv import load_dotenv
from sqlalchemy import create_engine, Integer, Text, DateTime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

DATABASE_URL = (
    f"mysql+pymysql://{DB_USER}:{DB_PASS}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
)


class Base(DeclarativeBase):
    pass


class LegalOpinion(Base):
    __tablename__ = "legal_opinions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    document_title: Mapped[str | None] = mapped_column(Text)
    document_type: Mapped[str | None] = mapped_column(Text)
    issuing_law_firm: Mapped[str | None] = mapped_column(Text)
    client_name: Mapped[str | None] = mapped_column(Text)
    issue_date: Mapped[str | None] = mapped_column(Text)
    case_names: Mapped[str | None] = mapped_column(Text)
    case_causes: Mapped[str | None] = mapped_column(Text)
    adjudicating_bodies: Mapped[str | None] = mapped_column(Text)
    case_numbers: Mapped[str | None] = mapped_column(Text)
    procedure_stage: Mapped[str | None] = mapped_column(Text)
    parties: Mapped[str | None] = mapped_column(Text)
    key_facts: Mapped[str | None] = mapped_column(Text)
    dispute_issues: Mapped[str | None] = mapped_column(Text)
    applicable_laws: Mapped[str | None] = mapped_column(Text)
    lawyer_analysis: Mapped[str | None] = mapped_column(Text)
    lawyer_conclusion: Mapped[str | None] = mapped_column(Text)
    risk_level: Mapped[str | None] = mapped_column(Text)
    material_adverse_impact: Mapped[str | None] = mapped_column(Text)
    reviewed_materials: Mapped[str | None] = mapped_column(Text)
    attachments: Mapped[str | None] = mapped_column(Text)


class Complaint(Base):
    __tablename__ = "complaints"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    document_name: Mapped[str | None] = mapped_column(Text)
    document_type: Mapped[str | None] = mapped_column(Text)
    case_cause: Mapped[str | None] = mapped_column(Text)
    court_name: Mapped[str | None] = mapped_column(Text)
    filing_date: Mapped[str | None] = mapped_column(Text)
    plaintiff_names: Mapped[str | None] = mapped_column(Text)
    plaintiff_addresses: Mapped[str | None] = mapped_column(Text)
    plaintiff_legal_representatives: Mapped[str | None] = mapped_column(Text)
    plaintiff_attorneys: Mapped[str | None] = mapped_column(Text)
    defendant_names: Mapped[str | None] = mapped_column(Text)
    defendant_addresses: Mapped[str | None] = mapped_column(Text)
    defendant_legal_representatives: Mapped[str | None] = mapped_column(Text)
    defendant_attorneys: Mapped[str | None] = mapped_column(Text)
    third_party_entities: Mapped[str | None] = mapped_column(Text)
    claims_full_text: Mapped[str | None] = mapped_column(Text)
    claims_items: Mapped[str | None] = mapped_column(Text)
    principal_amount: Mapped[str | None] = mapped_column(Text)
    interest_amount: Mapped[str | None] = mapped_column(Text)
    penalty_interest_amount: Mapped[str | None] = mapped_column(Text)
    compound_interest_amount: Mapped[str | None] = mapped_column(Text)
    attorney_fees: Mapped[str | None] = mapped_column(Text)
    other_costs: Mapped[str | None] = mapped_column(Text)
    non_monetary_claims: Mapped[str | None] = mapped_column(Text)
    facts_and_reasons: Mapped[str | None] = mapped_column(Text)
    related_contracts_or_documents: Mapped[str | None] = mapped_column(Text)
    plaintiff_performance_claims: Mapped[str | None] = mapped_column(Text)
    plaintiff_breach_or_tort_claims: Mapped[str | None] = mapped_column(Text)
    plaintiff_pre_actions: Mapped[str | None] = mapped_column(Text)
    joint_liability_basis: Mapped[str | None] = mapped_column(Text)
    legal_basis: Mapped[str | None] = mapped_column(Text)
    attachments: Mapped[str | None] = mapped_column(Text)



def main():
    engine = create_engine(DATABASE_URL, echo=True)
    Base.metadata.create_all(engine)
    print("表创建成功")


if __name__ == "__main__":
    main()
