import os
from datetime import datetime
from typing import Dict, List

from sqlalchemy import inspect

from core import (
    BusinessClauseMap,
    BusinessType,
    PolicyClause,
    PolicyDocument,
    ProcedureStep,
    RequiredMaterial,
    SessionLocal,
    engine,
    ensure_compliance_tables,
)
from seed_compliance_demo import seed as seed_compliance_demo


class GlobalSelfChecker:
    """
    全局自检：
    1) 结构化表可用
    2) 场景化最小数据可用（公司注销）
    3) 端到端结果符合“制度匹配 + 缺漏诊断 + 可溯源”
    """

    REQUIRED_TABLES = {
        "biz_business_type",
        "biz_business_clause_map",
        "biz_source_documents",
        "biz_policy_document",
        "biz_policy_clause",
        "biz_procedure_step",
        "biz_required_material",
        "biz_compliance_task",
        "biz_task_type_hit",
        "biz_task_gap",
        "biz_risk_assessments",
    }

    def __init__(self, auto_fix: bool = True):
        self.auto_fix = bool(auto_fix)

    @staticmethod
    def _now_str() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _new_check(name: str, critical: bool = True) -> Dict:
        return {
            "name": name,
            "critical": critical,
            "passed": False,
            "fixed": False,
            "detail": "",
            "metrics": {},
        }

    @staticmethod
    def _set_check(
        check: Dict,
        passed: bool,
        detail: str,
        metrics: Dict | None = None,
        fixed: bool = False,
    ):
        check["passed"] = bool(passed)
        check["detail"] = detail
        check["metrics"] = metrics or {}
        check["fixed"] = bool(fixed)

    @staticmethod
    def _with_llm_disabled():
        keys = ("ENABLE_LLM_INTENT", "ENABLE_LLM_INTENT_RESCUE", "ENABLE_LLM_ANSWER")
        backup = {k: os.getenv(k) for k in keys}
        os.environ["ENABLE_LLM_INTENT"] = "0"
        os.environ["ENABLE_LLM_INTENT_RESCUE"] = "0"
        os.environ["ENABLE_LLM_ANSWER"] = "0"
        return backup

    @staticmethod
    def _restore_env(backup: Dict[str, str | None]):
        for k, v in backup.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    @staticmethod
    def _run_case(user_text: str) -> Dict:
        from modules.evaluator.coordinator import AssessmentCoordinator

        backup = GlobalSelfChecker._with_llm_disabled()
        try:
            coordinator = AssessmentCoordinator()
            coordinator.run_full_assessment(user_text, persist=False)
            return coordinator.latest_result or {}
        finally:
            GlobalSelfChecker._restore_env(backup)

    def _check_db_connection(self, checks: List[Dict]) -> bool:
        check = self._new_check("db_connectivity")
        checks.append(check)
        try:
            table_names = set(inspect(engine).get_table_names())
            self._set_check(
                check,
                passed=True,
                detail="数据库连接正常。",
                metrics={"table_count": len(table_names)},
            )
            return True
        except Exception as e:
            self._set_check(check, passed=False, detail=f"数据库连接失败: {e}")
            return False

    def _check_schema(self, checks: List[Dict]) -> bool:
        check = self._new_check("schema_integrity")
        checks.append(check)

        table_names = set(inspect(engine).get_table_names())
        missing = sorted(self.REQUIRED_TABLES - table_names)
        if not missing:
            self._set_check(
                check,
                passed=True,
                detail="结构化合规表齐全。",
                metrics={"missing_tables": []},
            )
            return True

        if not self.auto_fix:
            self._set_check(
                check,
                passed=False,
                detail=f"缺失结构化表: {', '.join(missing)}",
                metrics={"missing_tables": missing},
            )
            return False

        schema_state = ensure_compliance_tables()
        table_names = set(inspect(engine).get_table_names())
        missing_after_fix = sorted(self.REQUIRED_TABLES - table_names)
        fixed_ok = schema_state.get("success", False) and not missing_after_fix
        self._set_check(
            check,
            passed=fixed_ok,
            detail="缺失结构化表已自动修复。" if fixed_ok else f"结构化表仍缺失: {', '.join(missing_after_fix)}",
            metrics={
                "missing_before": missing,
                "missing_after": missing_after_fix,
                "created_tables": schema_state.get("created_tables", []),
            },
            fixed=True,
        )
        return fixed_ok

    def _check_seed_data(self, checks: List[Dict]) -> bool:
        check = self._new_check("seed_data_company_dereg")
        checks.append(check)

        def collect() -> Dict:
            db = SessionLocal()
            try:
                bt = db.query(BusinessType).filter(BusinessType.type_code == "COMPANY_DEREG").first()
                if not bt:
                    return {
                        "exists": False,
                        "policy_docs": 0,
                        "clauses": 0,
                        "maps": 0,
                        "procedures": 0,
                        "materials": 0,
                    }
                metrics = {
                    "exists": True,
                    "policy_docs": db.query(PolicyDocument).count(),
                    "clauses": db.query(PolicyClause).count(),
                    "maps": db.query(BusinessClauseMap).filter(BusinessClauseMap.business_type_id == bt.id).count(),
                    "procedures": db.query(ProcedureStep).filter(ProcedureStep.business_type_id == bt.id).count(),
                    "materials": db.query(RequiredMaterial).filter(RequiredMaterial.business_type_id == bt.id).count(),
                }
                return metrics
            finally:
                db.close()

        before = collect()
        ready_before = (
            before["exists"]
            and before["maps"] > 0
            and before["procedures"] > 0
            and before["materials"] > 0
        )
        if ready_before:
            self._set_check(check, passed=True, detail="公司注销场景最小数据齐备。", metrics=before)
            return True

        if not self.auto_fix:
            self._set_check(check, passed=False, detail="公司注销场景最小数据缺失。", metrics=before)
            return False

        try:
            seed_compliance_demo()
        except Exception as e:
            self._set_check(check, passed=False, detail=f"自动补种子数据失败: {e}", metrics=before, fixed=True)
            return False

        after = collect()
        ready_after = (
            after["exists"]
            and after["maps"] > 0
            and after["procedures"] > 0
            and after["materials"] > 0
        )
        self._set_check(
            check,
            passed=ready_after,
            detail="公司注销场景种子数据已自动修复。" if ready_after else "种子数据仍不完整。",
            metrics={"before": before, "after": after},
            fixed=True,
        )
        return ready_after

    def _check_pipeline_conformance(self, checks: List[Dict]) -> bool:
        check = self._new_check("pipeline_conformance")
        checks.append(check)

        consult_case = "公司注销事项咨询：请按制度给出办理流程和材料清单，并标注条款依据。"
        review_case = "公司注销请示：请审查是否合规。"

        try:
            consult_result = self._run_case(consult_case)
            review_result = self._run_case(review_case)
        except Exception as e:
            self._set_check(check, passed=False, detail=f"端到端自检执行异常: {e}")
            return False

        consult_intent = consult_result.get("intent", {}) or {}
        consult_analysis = consult_result.get("analysis", {}) or {}
        consult_eval = consult_result.get("evaluation", {}) or {}
        consult_trace = consult_analysis.get("retrieval_trace", {}) or {}

        review_analysis = review_result.get("analysis", {}) or {}
        review_eval = review_result.get("evaluation", {}) or {}

        recognized_codes = {
            str(item.get("type_code", "")).strip()
            for item in (consult_intent.get("business_types") or [])
            if isinstance(item, dict)
        }
        matched_business_types = consult_analysis.get("matched_business_types", []) or []
        procedure_checks = consult_analysis.get("procedure_checks", []) or []
        material_checks = consult_analysis.get("material_checks", []) or []
        clause_refs = consult_analysis.get("clause_refs", []) or []
        review_gaps = review_analysis.get("gaps", []) or []
        review_status = str((review_eval.get("compliance_rating") or {}).get("status", "")).strip()

        consult_pass = (
            "COMPANY_DEREG" in recognized_codes
            and len(matched_business_types) > 0
            and len(procedure_checks) > 0
            and len(material_checks) > 0
            and any(str(c.get("trace_link", "")).strip() for c in clause_refs)
            and not bool(consult_trace.get("kb_only_mode"))
        )
        review_pass = (
            len(review_gaps) > 0
            and review_status in {"需补充", "不通过", "需完善制度映射"}
        )
        passed = consult_pass and review_pass

        self._set_check(
            check,
            passed=passed,
            detail="端到端结果符合场景化合规要求。" if passed else "端到端结果未达到场景化合规要求。",
            metrics={
                "consult": {
                    "recognized_codes": sorted(list(recognized_codes)),
                    "matched_business_types": len(matched_business_types),
                    "procedure_count": len(procedure_checks),
                    "material_count": len(material_checks),
                    "traceable_clause_count": len([c for c in clause_refs if str(c.get("trace_link", "")).strip()]),
                    "kb_only_mode": bool(consult_trace.get("kb_only_mode")),
                    "status": str((consult_eval.get("compliance_rating") or {}).get("status", "")).strip(),
                },
                "review": {
                    "gap_count": len(review_gaps),
                    "status": review_status,
                    "summary": str(review_analysis.get("summary", "")),
                },
            },
        )
        return passed

    def run(self) -> Dict:
        started_at = self._now_str()
        checks: List[Dict] = []

        db_ok = self._check_db_connection(checks)
        schema_ok = self._check_schema(checks) if db_ok else False
        seed_ok = self._check_seed_data(checks) if schema_ok else False
        pipeline_ok = self._check_pipeline_conformance(checks) if seed_ok else False

        critical_checks = [c for c in checks if c.get("critical", False)]
        passed_critical = [c for c in critical_checks if c.get("passed", False)]
        overall_passed = len(critical_checks) == len(passed_critical) and pipeline_ok

        return {
            "started_at": started_at,
            "finished_at": self._now_str(),
            "auto_fix": self.auto_fix,
            "overall_status": "passed" if overall_passed else "failed",
            "conclusion": (
                "当前系统可实现“场景化合规自查与制度推荐”，且结果满足自检标准。"
                if overall_passed
                else "当前系统尚未完全满足“场景化合规自查与制度推荐”要求，请按失败项修复。"
            ),
            "checks": checks,
            "summary": {
                "check_count": len(checks),
                "passed_count": len([c for c in checks if c.get("passed")]),
                "fixed_count": len([c for c in checks if c.get("fixed")]),
            },
        }
