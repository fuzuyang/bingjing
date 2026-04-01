import hashlib
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.orm import relationship

from core.database import Base


def generate_content_hash(text: str) -> str:
    if not text:
        return ""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class SourceDocument(Base):
    __tablename__ = "biz_source_documents"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    file_name = Column(String(255), nullable=False)
    title = Column(String(512), nullable=False)
    content_text = Column(LONGTEXT, nullable=False)
    content_hash = Column(String(64), unique=True, nullable=False)
    category = Column(Enum("policy", "case", "speech"), default="policy", index=True)
    created_at = Column(DateTime, default=datetime.now)

    knowledge_items = relationship("LegalKnowledge", back_populates="source", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<SourceDocument(title='{self.title[:20]}...', category='{self.category}')>"


class KBDocument(Base):
    __tablename__ = "kb_document"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    title = Column(String(255), nullable=False, index=True)
    doc_type = Column(String(50))
    status = Column(String(20), default="有效", index=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    chunks = relationship("KBChunk", back_populates="document", cascade="all, delete-orphan")


class KBChunk(Base):
    __tablename__ = "kb_chunk"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    document_id = Column(BigInteger, ForeignKey("kb_document.id", ondelete="CASCADE"), nullable=False, index=True)
    chunk_no = Column(Integer, nullable=False)
    section_no = Column(String(50), index=True)
    section_title = Column(String(255))
    subsection_no = Column(String(50), index=True)
    subsection_title = Column(String(255))
    content = Column(LONGTEXT, nullable=False)
    page_no = Column(Integer)
    created_at = Column(DateTime, default=datetime.now)

    document = relationship("KBDocument", back_populates="chunks")

    # Compatibility fields for legacy retrieval code paths.
    @property
    def parent_section(self) -> str:
        section_no = str(self.section_no or "").strip()
        subsection_no = str(self.subsection_no or "").strip()
        if not subsection_no:
            return ""
        if section_no and subsection_no != section_no:
            return section_no
        if "." in subsection_no:
            return subsection_no.rsplit(".", 1)[0]
        return ""

    @property
    def is_title(self) -> int:
        content = str(self.content or "").strip()
        if (not content or content == ".") and (self.section_title or self.subsection_title):
            return 1
        return 0

    @property
    def level_no(self) -> int:
        if str(self.subsection_no or "").strip():
            return 2
        if str(self.section_no or "").strip():
            return 1
        return 0

    @property
    def updated_at(self):
        return self.created_at


class LegalKnowledge(Base):
    __tablename__ = "biz_legal_knowledge"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    source_doc_id = Column(BigInteger, ForeignKey("biz_source_documents.id", ondelete="CASCADE"), nullable=False)
    knowledge_type = Column(Enum("spirit", "principle"), nullable=False, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=False)
    golden_quote = Column(Text)
    domain = Column(String(100), index=True)
    is_indexed = Column(Boolean, default=False, index=True)
    content_hash = Column(String(64), unique=True, nullable=False)

    source = relationship("SourceDocument", back_populates="knowledge_items")

    def __repr__(self):
        return f"<LegalKnowledge(name='{self.name}', type='{self.knowledge_type}')>"


class RiskAssessment(Base):
    """
    保留历史表结构，兼容旧页面和历史查询。
    在新流程下：
    - risk_level: 可存 "通过/需补充/不通过"
    - total_score: 存完整度评分（0~100）
    """

    __tablename__ = "biz_risk_assessments"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    event_summary = Column(Text, nullable=False)
    risk_level = Column(String(20), nullable=False)
    total_score = Column(Integer, nullable=False)
    full_report_md = Column(Text)
    created_at = Column(DateTime, default=datetime.now)


class BusinessType(Base):
    __tablename__ = "biz_business_type"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    type_code = Column(String(64), unique=True, nullable=False, index=True)
    type_name = Column(String(128), nullable=False, index=True)
    parent_id = Column(BigInteger, ForeignKey("biz_business_type.id"), nullable=True)
    description = Column(String(500))
    status = Column(Integer, default=1, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    parent = relationship("BusinessType", remote_side=[id])


class PolicyDocument(Base):
    __tablename__ = "biz_policy_document"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    doc_code = Column(String(64), unique=True, nullable=False, index=True)
    doc_name = Column(String(255), nullable=False, index=True)
    doc_category = Column(String(64), nullable=False, default="other", index=True)
    version_no = Column(String(32), nullable=False, default="v1.0")
    issuing_dept = Column(String(128))
    effective_date = Column(Date)
    expiry_date = Column(Date)
    status = Column(Integer, default=1, nullable=False, index=True)
    source_doc_id = Column(BigInteger, ForeignKey("biz_source_documents.id"), nullable=True)
    source_uri = Column(String(500))
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    clauses = relationship("PolicyClause", back_populates="policy_doc", cascade="all, delete-orphan")


class PolicyClause(Base):
    __tablename__ = "biz_policy_clause"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    policy_doc_id = Column(BigInteger, ForeignKey("biz_policy_document.id", ondelete="CASCADE"), nullable=False, index=True)
    clause_no = Column(String(64), nullable=False, index=True)
    clause_title = Column(String(255))
    clause_text = Column(LONGTEXT, nullable=False)
    page_no = Column(Integer)
    anchor_code = Column(String(128))
    content_hash = Column(String(64), unique=True, nullable=False)
    status = Column(Integer, default=1, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    policy_doc = relationship("PolicyDocument", back_populates="clauses")


class BusinessClauseMap(Base):
    __tablename__ = "biz_business_clause_map"
    __table_args__ = (
        UniqueConstraint("business_type_id", "clause_id", name="uk_biz_type_clause"),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    business_type_id = Column(BigInteger, ForeignKey("biz_business_type.id", ondelete="CASCADE"), nullable=False, index=True)
    clause_id = Column(BigInteger, ForeignKey("biz_policy_clause.id", ondelete="CASCADE"), nullable=False, index=True)
    mandatory_level = Column(Integer, default=1, nullable=False)  # 1必须 2建议 3禁止
    relevance_weight = Column(Float, default=1.0, nullable=False)
    trigger_keywords = Column(Text)  # JSON字符串或逗号分隔词
    remark = Column(String(500))
    status = Column(Integer, default=1, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.now)

    business_type = relationship("BusinessType")
    clause = relationship("PolicyClause")


class ProcedureStep(Base):
    __tablename__ = "biz_procedure_step"
    __table_args__ = (
        UniqueConstraint("business_type_id", "step_no", name="uk_biz_type_step"),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    business_type_id = Column(BigInteger, ForeignKey("biz_business_type.id", ondelete="CASCADE"), nullable=False, index=True)
    step_no = Column(Integer, nullable=False)
    step_name = Column(String(255), nullable=False)
    step_desc = Column(Text, nullable=False)
    responsible_role = Column(String(128))
    due_rule = Column(String(255))
    output_deliverable = Column(String(255))
    clause_id = Column(BigInteger, ForeignKey("biz_policy_clause.id"), nullable=True, index=True)
    status = Column(Integer, default=1, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.now)

    business_type = relationship("BusinessType")
    clause = relationship("PolicyClause")


class RequiredMaterial(Base):
    __tablename__ = "biz_required_material"
    __table_args__ = (
        UniqueConstraint("business_type_id", "material_code", name="uk_biz_type_material"),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    business_type_id = Column(BigInteger, ForeignKey("biz_business_type.id", ondelete="CASCADE"), nullable=False, index=True)
    material_code = Column(String(64), nullable=False)
    material_name = Column(String(255), nullable=False)
    required_level = Column(Integer, default=1, nullable=False)  # 1必须 2条件必需 3可选
    format_rule = Column(String(255))
    validator_rule = Column(Text)  # JSON字符串规则
    clause_id = Column(BigInteger, ForeignKey("biz_policy_clause.id"), nullable=True, index=True)
    status = Column(Integer, default=1, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.now)

    business_type = relationship("BusinessType")
    clause = relationship("PolicyClause")


class ComplianceTask(Base):
    __tablename__ = "biz_compliance_task"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    task_no = Column(String(64), unique=True, nullable=False, index=True)
    input_mode = Column(String(32), nullable=False, default="text")  # text/file/text+file
    input_text = Column(LONGTEXT)
    uploaded_doc_id = Column(BigInteger, ForeignKey("biz_source_documents.id"), nullable=True)
    applicant_id = Column(String(64))
    applicant_dept = Column(String(128))
    model_version = Column(String(64))
    overall_score = Column(Float)
    compliance_status = Column(String(32))  # 通过/需补充/不通过
    risk_level = Column(String(20))
    status = Column(Integer, default=1, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.now, index=True)
    completed_at = Column(DateTime)


class TaskTypeHit(Base):
    __tablename__ = "biz_task_type_hit"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    task_id = Column(BigInteger, ForeignKey("biz_compliance_task.id", ondelete="CASCADE"), nullable=False, index=True)
    business_type_id = Column(BigInteger, ForeignKey("biz_business_type.id", ondelete="CASCADE"), nullable=False, index=True)
    confidence = Column(Float, nullable=False, default=0.0)
    evidence_text = Column(String(1000))
    created_at = Column(DateTime, default=datetime.now)


class TaskGap(Base):
    __tablename__ = "biz_task_gap"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    task_id = Column(BigInteger, ForeignKey("biz_compliance_task.id", ondelete="CASCADE"), nullable=False, index=True)
    gap_type = Column(String(32), nullable=False)  # 缺程序/缺材料/缺授权/规范性不足
    gap_item = Column(String(255), nullable=False)
    expected_req = Column(Text, nullable=False)
    detected_content = Column(Text)
    severity = Column(Integer, default=2, nullable=False)  # 1提示 2一般 3严重
    fix_suggestion = Column(Text)
    clause_id = Column(BigInteger, ForeignKey("biz_policy_clause.id"), nullable=True, index=True)
    trace_link = Column(String(500))
    created_at = Column(DateTime, default=datetime.now)


Index("idx_knowledge_query", LegalKnowledge.knowledge_type, LegalKnowledge.is_indexed)
