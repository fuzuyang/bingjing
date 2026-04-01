from core import (
    BusinessClauseMap,
    BusinessType,
    ensure_compliance_tables,
    PolicyClause,
    PolicyDocument,
    ProcedureStep,
    RequiredMaterial,
    SessionLocal,
    generate_content_hash,
    init_db,
)


def _upsert_business_type(db, type_code: str, type_name: str, desc: str):
    row = db.query(BusinessType).filter(BusinessType.type_code == type_code).first()
    if row:
        row.type_name = type_name
        row.description = desc
        row.status = 1
        return row
    row = BusinessType(type_code=type_code, type_name=type_name, description=desc, status=1)
    db.add(row)
    db.flush()
    return row


def _upsert_policy_doc(db, doc_code: str, doc_name: str, category: str, version_no: str):
    row = db.query(PolicyDocument).filter(PolicyDocument.doc_code == doc_code).first()
    if row:
        row.doc_name = doc_name
        row.doc_category = category
        row.version_no = version_no
        row.status = 1
        return row
    row = PolicyDocument(
        doc_code=doc_code,
        doc_name=doc_name,
        doc_category=category,
        version_no=version_no,
        status=1,
        source_uri=f"policy://{doc_code}",
    )
    db.add(row)
    db.flush()
    return row


def _upsert_clause(db, doc_id: int, clause_no: str, clause_title: str, clause_text: str):
    h = generate_content_hash(f"{doc_id}|{clause_no}|{clause_text}")
    row = db.query(PolicyClause).filter(PolicyClause.content_hash == h).first()
    if row:
        row.clause_title = clause_title
        row.clause_text = clause_text
        row.status = 1
        return row
    row = PolicyClause(
        policy_doc_id=doc_id,
        clause_no=clause_no,
        clause_title=clause_title,
        clause_text=clause_text,
        content_hash=h,
        status=1,
    )
    db.add(row)
    db.flush()
    return row


def seed():
    init_db()
    schema_state = ensure_compliance_tables()
    if not schema_state.get("success"):
        raise RuntimeError(f"结构化表初始化失败: {schema_state.get('error', 'unknown error')}")
    db = SessionLocal()
    try:
        bt = _upsert_business_type(
            db,
            type_code="COMPANY_DEREG",
            type_name="公司注销",
            desc="适用于企业终止经营、进入清算并办理注销登记的场景",
        )

        doc = _upsert_policy_doc(
            db,
            doc_code="LEGAL-AUTH-2026-001",
            doc_name="公司注销合规管理办法",
            category="legal_auth",
            version_no="v1.0",
        )

        c1 = _upsert_clause(
            db,
            doc.id,
            "第3条",
            "注销决议与授权",
            "公司申请注销前，应形成有效决议文件，并明确法定代表人或授权代理人的办理权限。",
        )
        c2 = _upsert_clause(
            db,
            doc.id,
            "第5条",
            "税务与财务清算",
            "注销前应完成税务清税、债权债务梳理和财务结账，并形成清算报告。",
        )
        c3 = _upsert_clause(
            db,
            doc.id,
            "第7条",
            "资产处置",
            "涉及固定资产或无形资产处置的，应履行资产评估、审批及台账注销手续。",
        )
        c4 = _upsert_clause(
            db,
            doc.id,
            "第9条",
            "档案留存",
            "注销全过程资料应完整留存，不得缺失关键审批与证明文件。",
        )

        # 条款映射
        for clause, level, weight in [(c1, 1, 1.0), (c2, 1, 1.0), (c3, 1, 0.9), (c4, 2, 0.8)]:
            exists = (
                db.query(BusinessClauseMap)
                .filter(BusinessClauseMap.business_type_id == bt.id, BusinessClauseMap.clause_id == clause.id)
                .first()
            )
            if not exists:
                db.add(
                    BusinessClauseMap(
                        business_type_id=bt.id,
                        clause_id=clause.id,
                        mandatory_level=level,
                        relevance_weight=weight,
                        status=1,
                    )
                )

        # 程序步骤
        procedure_data = [
            (1, "形成注销决议与授权", "提交董事会/股东会决议并出具授权委托文件。", "董办/法务", c1.id),
            (2, "办理税务清税", "完成税务申报、欠税处理并取得清税证明。", "财务", c2.id),
            (3, "出具清算报告", "完成债权债务清理并形成清算报告。", "财务/法务", c2.id),
            (4, "资产处置与台账注销", "对固定资产、无形资产完成处置审批和台账核销。", "资产管理", c3.id),
        ]
        for step_no, step_name, step_desc, role, clause_id in procedure_data:
            exists = (
                db.query(ProcedureStep)
                .filter(ProcedureStep.business_type_id == bt.id, ProcedureStep.step_no == step_no)
                .first()
            )
            if not exists:
                db.add(
                    ProcedureStep(
                        business_type_id=bt.id,
                        step_no=step_no,
                        step_name=step_name,
                        step_desc=step_desc,
                        responsible_role=role,
                        clause_id=clause_id,
                        status=1,
                    )
                )

        # 材料清单
        material_data = [
            ("M001", "注销决议文件", 1, "需盖章扫描件(PDF)", c1.id),
            ("M002", "授权委托书", 1, "需明确授权范围与期限", c1.id),
            ("M003", "税务清税证明", 1, "税务机关出具", c2.id),
            ("M004", "清算报告", 1, "含债权债务清理说明", c2.id),
            ("M005", "资产处置审批材料", 2, "涉及资产处置时必需", c3.id),
        ]
        for code, name, level, rule, clause_id in material_data:
            exists = (
                db.query(RequiredMaterial)
                .filter(RequiredMaterial.business_type_id == bt.id, RequiredMaterial.material_code == code)
                .first()
            )
            if not exists:
                db.add(
                    RequiredMaterial(
                        business_type_id=bt.id,
                        material_code=code,
                        material_name=name,
                        required_level=level,
                        format_rule=rule,
                        clause_id=clause_id,
                        status=1,
                    )
                )

        db.commit()
        print("seed completed: COMPANY_DEREG")
    except Exception as e:
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    seed()
