import os
import re
import sys
import json
from dataclasses import dataclass
from typing import Dict, List, Sequence, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from core import (  # noqa: E402
    BusinessClauseMap,
    BusinessType,
    PolicyClause,
    PolicyDocument,
    ProcedureStep,
    RequiredMaterial,
    SessionLocal,
    SourceDocument,
    generate_content_hash,
    init_db,
)


@dataclass(frozen=True)
class BusinessRule:
    type_code: str
    type_name: str
    category: str
    keywords: Tuple[str, ...]
    description: str = ""


BUSINESS_RULES: Tuple[BusinessRule, ...] = (
    BusinessRule(
        type_code="COMPANY_DEREG",
        type_name="公司注销",
        category="legal_auth",
        keywords=("注销", "清算", "解散", "终止经营", "deregistration", "liquidation"),
        description="企业注销与清算事项",
    ),
    BusinessRule(
        type_code="EXTERNAL_GUARANTEE",
        type_name="对外担保",
        category="finance",
        keywords=("担保", "保证", "反担保", "抵押", "质押", "guarantee"),
        description="对外担保与增信事项",
    ),
    BusinessRule(
        type_code="PROCUREMENT",
        type_name="采购管理",
        category="procurement",
        keywords=("采购", "招标", "供应商", "比选", "询价", "purchase", "procurement"),
        description="采购、招采及供应商管理事项",
    ),
    BusinessRule(
        type_code="INVESTMENT",
        type_name="投资并购",
        category="asset",
        keywords=("投资", "并购", "收购", "股权", "增资", "merger", "acquisition"),
        description="投资、并购与股权变动事项",
    ),
    BusinessRule(
        type_code="LEGAL_AUTH",
        type_name="法律授权",
        category="legal_auth",
        keywords=("授权", "审批权限", "签字权限", "法定代表人", "授权委托"),
        description="法律授权与审批权限事项",
    ),
    BusinessRule(
        type_code="ASSET_DISPOSAL",
        type_name="资产处置",
        category="asset",
        keywords=("资产处置", "固定资产", "无形资产", "资产转让", "报废", "评估"),
        description="资产评估、处置与核销事项",
    ),
    BusinessRule(
        type_code="TAX_FINANCE",
        type_name="财税管理",
        category="tax",
        keywords=("税务", "纳税", "清税", "发票", "审计", "财务", "税"),
        description="税务与财务合规事项",
    ),
    BusinessRule(
        type_code="CONTRACT_REVIEW",
        type_name="合同审查",
        category="legal_auth",
        keywords=("合同", "协议", "补充协议", "违约", "签署", "contract"),
        description="合同起草、审查与签署事项",
    ),
    BusinessRule(
        type_code="HR_EMPLOYMENT",
        type_name="劳动用工",
        category="hr",
        keywords=("劳动", "员工", "社保", "薪酬", "离职", "用工"),
        description="劳动用工与人力合规事项",
    ),
)


DOC_CATEGORY_RULES: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    ("finance", ("财务", "资金", "担保", "融资")),
    ("asset", ("资产", "处置", "评估", "固定资产", "无形资产")),
    ("legal_auth", ("授权", "审批", "法务", "合同")),
    ("tax", ("税", "发票", "纳税", "清税")),
    ("hr", ("劳动", "员工", "薪酬", "社保")),
    ("procurement", ("采购", "招标", "供应商", "询价")),
)


MATERIAL_HINTS: Tuple[Tuple[str, str], ...] = (
    ("申请", "申请书"),
    ("报告", "报告"),
    ("证明", "证明材料"),
    ("决议", "决议文件"),
    ("授权", "授权委托书"),
    ("清算", "清算报告"),
    ("税", "税务凭证"),
    ("发票", "发票"),
    ("合同", "合同文本"),
    ("协议", "协议文本"),
    ("审计", "审计报告"),
    ("评估", "评估报告"),
    ("清单", "材料清单"),
    ("审批表", "审批表"),
)


class RelationshipBuilder:
    """
    Build relation-layer entities from raw source documents:
    source -> policy_document -> policy_clause -> business mapping -> procedures/materials
    """

    ARTICLE_PATTERN = re.compile(
        r"(?:^|\n)\s*(第[一二三四五六七八九十百千万〇零0-9]+条)\s*([^\n]{0,80})",
        re.MULTILINE,
    )
    PROCEDURE_HINTS = ("流程", "程序", "步骤", "办理", "审批", "报批", "备案", "受理", "审核", "登记", "公告")
    REQUIRED_TOKENS = ("应当", "必须", "需", "须", "不得", "严禁")
    OPTIONAL_TOKENS = ("可", "可以", "建议", "酌情", "视情况")
    DUE_RULE_PATTERN = re.compile(r"(\d+\s*(?:个)?(?:工作)?日内|\d+\s*天内|当日|即时|月底前|季度内|年度内)")
    MATERIAL_REGEX = re.compile(r"(申请书|证明|报告|清单|决议|合同|协议|审批表|授权委托书)")

    def __init__(self, max_clause_chars: int = 1200, max_clauses: int = 80):
        self.max_clause_chars = max_clause_chars
        self.max_clauses = max_clauses

    @staticmethod
    def _normalize_text(text: str) -> str:
        if not text:
            return ""
        cleaned = text.replace("\r\n", "\n").replace("\r", "\n")
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
        return cleaned.strip()

    def _pick_doc_category(self, doc: SourceDocument) -> str:
        sample = f"{doc.file_name}\n{doc.title}\n{(doc.content_text or '')[:2000]}".lower()
        for category, keys in DOC_CATEGORY_RULES:
            if any(k.lower() in sample for k in keys):
                return category
        return "other"

    @staticmethod
    def _doc_code(source_doc_id: int) -> str:
        return f"RAW-POLICY-{source_doc_id}"

    def _upsert_policy_document(self, db: Session, source_doc: SourceDocument) -> PolicyDocument:
        doc = (
            db.query(PolicyDocument)
            .filter(PolicyDocument.source_doc_id == source_doc.id)
            .order_by(PolicyDocument.id.asc())
            .first()
        )
        if not doc:
            doc = db.query(PolicyDocument).filter(PolicyDocument.doc_code == self._doc_code(source_doc.id)).first()

        category = self._pick_doc_category(source_doc)
        name = (source_doc.title or source_doc.file_name or f"source-{source_doc.id}")[:255]
        uri = f"source://biz_source_documents/{source_doc.id}"

        if doc:
            doc.doc_name = name
            doc.doc_category = category
            doc.version_no = doc.version_no or "v1.0"
            doc.source_doc_id = source_doc.id
            doc.source_uri = doc.source_uri or uri
            doc.status = 1
            return doc

        doc = PolicyDocument(
            doc_code=self._doc_code(source_doc.id),
            doc_name=name,
            doc_category=category,
            version_no="v1.0",
            status=1,
            source_doc_id=source_doc.id,
            source_uri=uri,
        )
        db.add(doc)
        db.flush()
        return doc

    def _split_articles(self, text: str) -> List[Dict]:
        matches = list(self.ARTICLE_PATTERN.finditer(text))
        if not matches:
            return []

        clauses: List[Dict] = []
        for idx, match in enumerate(matches):
            start = match.start(1)
            end = matches[idx + 1].start(1) if idx + 1 < len(matches) else len(text)
            chunk = text[start:end].strip()
            if len(chunk) < 20:
                continue

            clause_no = match.group(1).strip()
            title = (match.group(2) or "").strip(" ：:;；\t")
            if not title:
                title = clause_no
            clauses.append({"clause_no": clause_no, "clause_title": title[:255], "clause_text": chunk})
        return clauses[: self.max_clauses]

    def _split_fallback(self, text: str) -> List[Dict]:
        paragraphs = [p.strip() for p in re.split(r"\n{2,}", text) if p and len(p.strip()) >= 20]
        if not paragraphs:
            paragraphs = [text.strip()] if text.strip() else []

        clauses: List[Dict] = []
        cursor: List[str] = []
        for para in paragraphs:
            if sum(len(x) for x in cursor) + len(para) <= self.max_clause_chars:
                cursor.append(para)
                continue
            if cursor:
                chunk = "\n\n".join(cursor).strip()
                clauses.append(self._make_auto_clause(chunk, len(clauses) + 1))
            cursor = [para]

        if cursor:
            chunk = "\n\n".join(cursor).strip()
            clauses.append(self._make_auto_clause(chunk, len(clauses) + 1))

        return clauses[: self.max_clauses]

    @staticmethod
    def _make_auto_clause(chunk: str, idx: int) -> Dict:
        sentence = re.split(r"[。；;\n]", chunk)[0].strip()
        sentence = sentence[:36] if sentence else f"自动条款{idx}"
        return {"clause_no": f"AUTO-{idx:03d}", "clause_title": sentence, "clause_text": chunk}

    def _extract_clauses(self, source_doc: SourceDocument) -> List[Dict]:
        text = self._normalize_text(source_doc.content_text or "")
        if not text:
            return []
        clauses = self._split_articles(text)
        if clauses:
            return clauses
        return self._split_fallback(text)

    def _upsert_clauses(self, db: Session, policy_doc: PolicyDocument, clauses: Sequence[Dict]) -> List[PolicyClause]:
        rows: List[PolicyClause] = []
        for idx, item in enumerate(clauses, start=1):
            clause_no = str(item.get("clause_no") or f"AUTO-{idx:03d}")[:64]
            clause_title = str(item.get("clause_title") or clause_no)[:255]
            clause_text = str(item.get("clause_text") or "").strip()
            if len(clause_text) < 20:
                continue

            content_hash = generate_content_hash(f"{policy_doc.id}|{clause_no}|{clause_text}")
            row = db.query(PolicyClause).filter(PolicyClause.content_hash == content_hash).first()
            if row:
                row.status = 1
                rows.append(row)
                continue

            row = (
                db.query(PolicyClause)
                .filter(PolicyClause.policy_doc_id == policy_doc.id, PolicyClause.clause_no == clause_no)
                .order_by(PolicyClause.id.asc())
                .first()
            )
            if row:
                row.clause_title = clause_title
                row.clause_text = clause_text
                row.content_hash = content_hash
                row.status = 1
                rows.append(row)
                continue

            row = PolicyClause(
                policy_doc_id=policy_doc.id,
                clause_no=clause_no,
                clause_title=clause_title,
                clause_text=clause_text,
                content_hash=content_hash,
                status=1,
            )
            db.add(row)
            db.flush()
            rows.append(row)
        return rows

    def _match_business_rules(self, source_doc: SourceDocument, clauses: Sequence[PolicyClause]) -> List[BusinessRule]:
        sample = [source_doc.file_name or "", source_doc.title or "", (source_doc.content_text or "")[:6000]]
        sample.extend((c.clause_text or "")[:500] for c in clauses[:20])
        text = "\n".join(sample).lower()

        scored: List[Tuple[BusinessRule, int]] = []
        for rule in BUSINESS_RULES:
            hit_count = sum(1 for k in rule.keywords if k and k.lower() in text)
            if hit_count > 0:
                scored.append((rule, hit_count))

        matched: List[BusinessRule] = []
        if scored:
            strong = [item for item in scored if item[1] >= 2]
            if strong:
                strong.sort(key=lambda x: x[1], reverse=True)
                matched = [item[0] for item in strong[:3]]
            else:
                scored.sort(key=lambda x: x[1], reverse=True)
                matched = [scored[0][0]]

        if not matched:
            matched.append(
                BusinessRule(
                    type_code="GENERAL_COMPLIANCE",
                    type_name="综合合规事项",
                    category="other",
                    keywords=("合规", "制度"),
                    description="自动归类到综合合规事项",
                )
            )
        dynamic = self._build_doc_business_rule(source_doc)
        if dynamic:
            matched.insert(0, dynamic)

        dedup: List[BusinessRule] = []
        seen = set()
        for item in matched:
            if item.type_code in seen:
                continue
            seen.add(item.type_code)
            dedup.append(item)
        return dedup

    def _build_doc_business_rule(self, source_doc: SourceDocument) -> BusinessRule:
        title = re.sub(r"\s+", " ", str(source_doc.title or source_doc.file_name or "")).strip()
        if not title:
            title = f"文档{source_doc.id}"
        tokens = re.findall(r"[\u4e00-\u9fff]{2,8}|[A-Za-z]{3,20}", title)
        uniq_tokens: List[str] = []
        for t in tokens:
            if t not in uniq_tokens:
                uniq_tokens.append(t)
        if not uniq_tokens:
            uniq_tokens = ["合规", "制度"]

        return BusinessRule(
            type_code=f"RAW_DOC_{source_doc.id}",
            type_name=title[:120],
            category=self._pick_doc_category(source_doc),
            keywords=tuple(uniq_tokens[:6]),
            description=f"源文档自动映射: {source_doc.file_name}",
        )

    @staticmethod
    def _upsert_business_type(db: Session, rule: BusinessRule) -> BusinessType:
        row = db.query(BusinessType).filter(BusinessType.type_code == rule.type_code).first()
        if row:
            row.type_name = rule.type_name
            row.description = rule.description or row.description
            row.status = 1
            return row

        row = BusinessType(
            type_code=rule.type_code,
            type_name=rule.type_name,
            description=rule.description,
            status=1,
        )
        db.add(row)
        db.flush()
        return row

    def _mandatory_level(self, text: str) -> int:
        raw = text or ""
        if "不得" in raw or "禁止" in raw:
            return 3
        if any(token in raw for token in self.REQUIRED_TOKENS):
            return 1
        if any(token in raw for token in self.OPTIONAL_TOKENS):
            return 2
        return 1

    @staticmethod
    def _relevance_weight(text: str, keywords: Sequence[str]) -> float:
        if not keywords:
            return 0.8
        hit = sum(1 for k in keywords if k and k in text)
        ratio = hit / max(len(keywords), 1)
        return round(min(1.0, 0.6 + ratio * 0.4), 2)

    def _upsert_clause_maps(
        self,
        db: Session,
        business_type: BusinessType,
        clauses: Sequence[PolicyClause],
        trigger_keywords: Sequence[str],
    ) -> int:
        added = 0
        trigger = json.dumps([k for k in trigger_keywords[:8] if k], ensure_ascii=False)
        for clause in clauses:
            exists = (
                db.query(BusinessClauseMap.id)
                .filter(
                    BusinessClauseMap.business_type_id == business_type.id,
                    BusinessClauseMap.clause_id == clause.id,
                )
                .first()
            )
            if exists:
                continue

            db.add(
                BusinessClauseMap(
                    business_type_id=business_type.id,
                    clause_id=clause.id,
                    mandatory_level=self._mandatory_level(clause.clause_text or ""),
                    relevance_weight=self._relevance_weight(clause.clause_text or "", trigger_keywords),
                    trigger_keywords=trigger,
                    status=1,
                )
            )
            added += 1
        return added

    @staticmethod
    def _normalize_name(name: str) -> str:
        return re.sub(r"\s+", "", str(name or "")).lower()

    def _extract_procedure_candidates(self, clauses: Sequence[PolicyClause]) -> List[Dict]:
        candidates: List[Dict] = []
        for clause in clauses:
            raw = clause.clause_text or ""
            score = sum(1 for h in self.PROCEDURE_HINTS if h in raw)
            if score == 0 and len(candidates) >= 6:
                continue

            step_name = (clause.clause_title or clause.clause_no or "").strip()
            if not step_name or step_name.startswith("AUTO-"):
                step_name = re.split(r"[。；;\n]", raw.strip())[0][:24]
            if not step_name:
                continue

            step_desc = raw[:240]
            due_match = self.DUE_RULE_PATTERN.search(raw)
            due_rule = due_match.group(1) if due_match else ""
            role = self._infer_role(raw)
            deliverable = self._infer_deliverable(raw, step_name)

            candidates.append(
                {
                    "clause_id": clause.id,
                    "step_name": step_name[:255],
                    "step_desc": step_desc,
                    "due_rule": due_rule[:255],
                    "responsible_role": role[:128],
                    "output_deliverable": deliverable[:255],
                    "score": score,
                }
            )

        if not candidates:
            return []

        selected = sorted(candidates, key=lambda x: (-x["score"], x["step_name"]))[:10]
        dedup: List[Dict] = []
        seen = set()
        for item in selected:
            key = self._normalize_name(item["step_name"])
            if key in seen:
                continue
            seen.add(key)
            dedup.append(item)
        return dedup

    @staticmethod
    def _infer_role(text: str) -> str:
        if any(k in text for k in ("财务", "税", "发票", "审计")):
            return "财务"
        if any(k in text for k in ("法务", "授权", "合同")):
            return "法务"
        if any(k in text for k in ("资产", "评估", "处置")):
            return "资产管理"
        if any(k in text for k in ("采购", "招标", "供应商")):
            return "采购"
        if any(k in text for k in ("人力", "员工", "劳动")):
            return "人力"
        return "业务部门"

    def _infer_deliverable(self, text: str, step_name: str) -> str:
        m = self.MATERIAL_REGEX.search(text or "")
        if m:
            return f"{m.group(1)}"
        return f"{step_name}相关输出材料"

    def _upsert_procedures(self, db: Session, business_type: BusinessType, clauses: Sequence[PolicyClause]) -> int:
        candidates = self._extract_procedure_candidates(clauses)
        if not candidates:
            return 0

        existing_names = {
            self._normalize_name(row[0])
            for row in db.query(ProcedureStep.step_name)
            .filter(ProcedureStep.business_type_id == business_type.id)
            .all()
        }
        max_step_no = (
            db.query(func.max(ProcedureStep.step_no))
            .filter(ProcedureStep.business_type_id == business_type.id)
            .scalar()
            or 0
        )

        added = 0
        for item in candidates:
            norm = self._normalize_name(item["step_name"])
            if norm in existing_names:
                continue
            max_step_no += 1
            db.add(
                ProcedureStep(
                    business_type_id=business_type.id,
                    step_no=max_step_no,
                    step_name=item["step_name"],
                    step_desc=item["step_desc"],
                    responsible_role=item["responsible_role"],
                    due_rule=item["due_rule"],
                    output_deliverable=item["output_deliverable"],
                    clause_id=item["clause_id"],
                    status=1,
                )
            )
            existing_names.add(norm)
            added += 1
        return added

    def _extract_material_candidates(self, clauses: Sequence[PolicyClause]) -> List[Dict]:
        candidates: List[Dict] = []
        for clause in clauses:
            text = clause.clause_text or ""
            for key, name in MATERIAL_HINTS:
                if key not in text:
                    continue
                candidates.append(
                    {
                        "material_name": name,
                        "required_level": self._material_level(text),
                        "format_rule": self._material_format_rule(text),
                        "validator_rule": {"source_clause": clause.clause_no},
                        "clause_id": clause.id,
                    }
                )

            for m in self.MATERIAL_REGEX.finditer(text):
                candidates.append(
                    {
                        "material_name": m.group(1),
                        "required_level": self._material_level(text),
                        "format_rule": self._material_format_rule(text),
                        "validator_rule": {"source_clause": clause.clause_no},
                        "clause_id": clause.id,
                    }
                )

        dedup: List[Dict] = []
        seen = set()
        for item in candidates:
            name = str(item["material_name"]).strip()
            key = self._normalize_name(name)
            if not name or key in seen:
                continue
            seen.add(key)
            item["material_name"] = name[:255]
            dedup.append(item)
        return dedup[:15]

    def _material_level(self, text: str) -> int:
        if any(token in text for token in ("应当", "必须", "须提交", "不得缺失")):
            return 1
        if any(token in text for token in ("视情况", "必要时", "如涉及")):
            return 2
        if any(token in text for token in ("可", "可以")):
            return 3
        return 2

    @staticmethod
    def _material_format_rule(text: str) -> str:
        snippet = re.split(r"[。；;\n]", text or "")[0]
        return snippet[:255]

    @staticmethod
    def _material_code(biz_type_id: int, material_name: str, clause_id: int) -> str:
        digest = generate_content_hash(f"{biz_type_id}|{material_name}|{clause_id}")[:12].upper()
        return f"AUTO_{digest}"

    def _upsert_materials(self, db: Session, business_type: BusinessType, clauses: Sequence[PolicyClause]) -> int:
        candidates = self._extract_material_candidates(clauses)
        if not candidates and clauses:
            fallback_clause = clauses[0]
            candidates = [
                {
                    "material_name": f"{business_type.type_name}相关申请与证明材料",
                    "required_level": 2,
                    "format_rule": "请依据制度条款补充完整材料",
                    "validator_rule": {"source_clause": fallback_clause.clause_no},
                    "clause_id": fallback_clause.id,
                }
            ]

        added = 0
        for item in candidates:
            material_code = self._material_code(business_type.id, item["material_name"], int(item["clause_id"]))
            exists = (
                db.query(RequiredMaterial.id)
                .filter(
                    RequiredMaterial.business_type_id == business_type.id,
                    RequiredMaterial.material_code == material_code,
                )
                .first()
            )
            if exists:
                continue

            db.add(
                RequiredMaterial(
                    business_type_id=business_type.id,
                    material_code=material_code,
                    material_name=item["material_name"],
                    required_level=item["required_level"],
                    format_rule=item["format_rule"],
                    validator_rule=json.dumps(item["validator_rule"], ensure_ascii=False),
                    clause_id=item["clause_id"],
                    status=1,
                )
            )
            added += 1
        return added

    def run(self, limit: int = None) -> Dict:
        init_db()
        db: Session = SessionLocal()
        stats = {
            "docs_total": 0,
            "docs_success": 0,
            "docs_failed": 0,
            "policy_docs_upserted": 0,
            "clauses_upserted": 0,
            "maps_upserted": 0,
            "steps_upserted": 0,
            "materials_upserted": 0,
        }
        try:
            query = (
                db.query(SourceDocument)
                .filter(SourceDocument.category == "policy")
                .order_by(SourceDocument.id.asc())
            )
            if isinstance(limit, int) and limit > 0:
                query = query.limit(limit)
            source_docs = query.all()
            stats["docs_total"] = len(source_docs)

            if not source_docs:
                print("关系层构建跳过: 没有可处理的 policy 源文档")
                return stats

            print(f"启动关系层自动构建，文档数: {len(source_docs)}")
            for source_doc in source_docs:
                try:
                    policy_doc = self._upsert_policy_document(db, source_doc)
                    clauses_payload = self._extract_clauses(source_doc)
                    clause_rows = self._upsert_clauses(db, policy_doc, clauses_payload)

                    if not clause_rows:
                        db.commit()
                        stats["docs_success"] += 1
                        print(f" - source_id={source_doc.id} 条款抽取为空，已跳过映射")
                        continue

                    rule_matches = self._match_business_rules(source_doc, clause_rows)
                    map_added = 0
                    step_added = 0
                    material_added = 0
                    for rule in rule_matches:
                        biz_type = self._upsert_business_type(db, rule)
                        map_added += self._upsert_clause_maps(db, biz_type, clause_rows, rule.keywords)
                        step_added += self._upsert_procedures(db, biz_type, clause_rows)
                        material_added += self._upsert_materials(db, biz_type, clause_rows)

                    db.commit()
                    stats["docs_success"] += 1
                    stats["policy_docs_upserted"] += 1
                    stats["clauses_upserted"] += len(clause_rows)
                    stats["maps_upserted"] += map_added
                    stats["steps_upserted"] += step_added
                    stats["materials_upserted"] += material_added
                    print(
                        f" + source_id={source_doc.id} -> clauses={len(clause_rows)}, "
                        f"maps+={map_added}, steps+={step_added}, materials+={material_added}"
                    )
                except Exception as exc:
                    db.rollback()
                    stats["docs_failed"] += 1
                    print(f" ! source_id={source_doc.id} 关系构建失败: {str(exc)[:200]}")
            print("关系层自动构建完成")
            return stats
        finally:
            db.close()


if __name__ == "__main__":
    builder = RelationshipBuilder()
    summary = builder.run()
    print(summary)
