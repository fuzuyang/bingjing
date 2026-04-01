import os
import sys
import logging
from datetime import datetime
from typing import Dict

from sqlalchemy import inspect

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from core import (  # noqa: E402
    BusinessType,
    ComplianceTask,
    ensure_compliance_tables,
    engine,
    RiskAssessment,
    SessionLocal,
    TaskGap,
    TaskTypeHit,
    init_db,
)
from modules.evaluator.analyzer import PolicySpiritAnalyzer  # noqa: E402
from modules.evaluator.answer_generator import GroundedAnswerGenerator  # noqa: E402
from modules.evaluator.intent_recognizer import IntentRecognizer  # noqa: E402
from modules.evaluator.reporter import ReportGenerator  # noqa: E402
from modules.evaluator.retriever import PolicyRetriever  # noqa: E402
from modules.evaluator.risk_evaluator import RiskEvaluator  # noqa: E402

logger = logging.getLogger(__name__)


class AssessmentCoordinator:
    """
    新流程编排：
    输入 -> 意图识别 -> 关系检索 -> 合规比对 -> 结果评价 -> 报告生成 -> 落库
    """
    _schema_checked = False

    def __init__(self):
        # 确保新增合规模型表在启动时可用（按进程仅检查一次，避免重复开销）
        if not AssessmentCoordinator._schema_checked:
            init_db()
            schema_state = ensure_compliance_tables()
            if not schema_state.get("success"):
                logger.warning("合规结构化表自检失败: %s", schema_state.get("error", "unknown error"))
            elif schema_state.get("created_tables"):
                logger.info("合规结构化表已自动创建: %s", ", ".join(schema_state["created_tables"]))
            AssessmentCoordinator._schema_checked = True

        self.recognizer = IntentRecognizer()
        self.retriever = PolicyRetriever()
        self.analyzer = PolicySpiritAnalyzer()
        self.answer_generator = GroundedAnswerGenerator()
        self.evaluator = RiskEvaluator()
        self.reporter = ReportGenerator()

        # 给 API 提供结构化结果
        self.latest_result: Dict = {}

    def run_full_assessment(self, user_event: str, persist: bool = True, input_meta: Dict | None = None):
        logger.info("启动场景化合规自查与制度推荐流程")

        intent_data = self.recognizer.recognize(user_event)
        if not intent_data:
            return "流程中断：未识别到有效意图。"

        retrieved_bundle = self.retriever.retrieve(intent_data)
        compliance_analysis = self.analyzer.analyze_compliance(user_event, intent_data, retrieved_bundle)
        llm_answer = self.answer_generator.generate(user_event, intent_data, compliance_analysis)
        compliance_analysis["llm_answer"] = llm_answer
        evaluation_result = self.evaluator.evaluate(user_event, intent_data, compliance_analysis, retrieved_bundle)
        report_md = self.reporter.generate(intent_data, compliance_analysis, evaluation_result)

        self.latest_result = {
            "intent": intent_data,
            "retrieved": retrieved_bundle,
            "analysis": compliance_analysis,
            "generation": llm_answer,
            "evaluation": evaluation_result,
            "markdown": report_md,
            "input_meta": input_meta or {"input_mode": "text", "file_name": None, "extracted_chars": 0},
        }

        if persist:
            self._save_to_db(user_event, self.latest_result)

        logger.info("合规流程执行完成")
        return report_md

    @staticmethod
    def _build_task_no() -> str:
        return f"CT{datetime.now().strftime('%Y%m%d%H%M%S%f')}"

    def _save_to_db(self, user_event: str, result_bundle: Dict):
        table_names = set(inspect(engine).get_table_names())
        if "biz_risk_assessments" not in table_names:
            logger.info("结果落库已跳过：当前为 kb-only 模式（仅保留 kb_document / kb_chunk）。")
            return

        db = SessionLocal()
        try:
            intent = result_bundle.get("intent", {})
            analysis = result_bundle.get("analysis", {})
            evaluation = result_bundle.get("evaluation", {})
            report_md = result_bundle.get("markdown", "")
            input_meta = result_bundle.get("input_meta", {}) or {}

            compliance_rating = evaluation.get("compliance_rating", {})
            risk_rating = evaluation.get("risk_rating", {})

            # 1) 兼容历史表
            history_row = RiskAssessment(
                event_summary=(user_event or "")[:200],
                risk_level=str(compliance_rating.get("status", risk_rating.get("level", "未判定"))),
                total_score=int(compliance_rating.get("completeness_score", risk_rating.get("total_score", 0)) or 0),
                full_report_md=report_md,
            )
            db.add(history_row)
            db.commit()
            db.refresh(history_row)

            # 2) 新合规任务表
            task_no = ""
            try:
                input_mode = str(input_meta.get("input_mode") or "text")
                risk_level = str(
                    risk_rating.get("level")
                    or compliance_rating.get("status")
                    or "未判定"
                )
                task = ComplianceTask(
                    task_no=self._build_task_no(),
                    input_mode=input_mode,
                    input_text=user_event,
                    model_version="compliance-v1",
                    overall_score=float(compliance_rating.get("completeness_score", 0) or 0),
                    compliance_status=str(compliance_rating.get("status", "")),
                    risk_level=risk_level,
                    status=2,
                    completed_at=datetime.now(),
                )
                db.add(task)
                db.flush()  # 取 task.id
                task_no = task.task_no

                # 3) 业务类型命中
                for bt_item in intent.get("business_types", []):
                    bt_id = None
                    type_code = str(bt_item.get("type_code", "")).strip()
                    type_name = str(bt_item.get("type_name", "")).strip()

                    bt = None
                    if type_code:
                        bt = db.query(BusinessType).filter(BusinessType.type_code == type_code).first()
                    if not bt and type_name:
                        bt = db.query(BusinessType).filter(BusinessType.type_name == type_name).first()
                    if bt:
                        bt_id = bt.id

                    if bt_id:
                        db.add(
                            TaskTypeHit(
                                task_id=task.id,
                                business_type_id=bt_id,
                                confidence=float(bt_item.get("confidence", 0.0) or 0.0),
                                evidence_text=intent.get("core_issue", "")[:1000],
                            )
                        )

                # 4) 缺漏项
                for gap in analysis.get("gaps", []):
                    gap_type_raw = str(gap.get("gap_type", "缺漏"))
                    db.add(
                        TaskGap(
                            task_id=task.id,
                            gap_type=gap_type_raw,
                            gap_item=str(gap.get("gap_item", ""))[:255],
                            expected_req=str(gap.get("expected_req", "")),
                            detected_content=str(gap.get("detected_content", "")),
                            severity=int(gap.get("severity", 2) or 2),
                            fix_suggestion=str(gap.get("fix_suggestion", "")),
                            clause_id=None,
                            trace_link=str(gap.get("trace_link", ""))[:500],
                        )
                    )
                db.commit()
            except Exception as task_error:
                db.rollback()
                logger.warning("合规任务扩展落库跳过(结构未同步或字段不兼容): %s", task_error)

            logger.info(
                "结果已落库：history_id=%s, task_no=%s, 状态=%s, 分数=%s",
                history_row.id,
                task_no or "N/A",
                compliance_rating.get("status", "未判定"),
                compliance_rating.get("completeness_score", 0),
            )
        except Exception as e:
            db.rollback()
            logger.error("结果落库失败: %s", e)
        finally:
            db.close()


if __name__ == "__main__":
    sample = "公司注销请示：请审核是否符合财务清算、资产处置与授权审批要求。"
    c = AssessmentCoordinator()
    md = c.run_full_assessment(sample)
    print(md[:800])
