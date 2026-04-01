import os
import sys
import logging
from typing import Dict, List

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

logger = logging.getLogger(__name__)


class RiskEvaluator:
    """
    合规结果评估器。
    保留类名与输出结构，兼容现有调用与历史展示。
    """

    @staticmethod
    def _score_items(procedure_checks: List[Dict], material_checks: List[Dict]) -> Dict:
        required_total = 0
        completed = 0
        missing_required = 0

        for item in procedure_checks:
            required_total += 1
            if item.get("status") == "已覆盖":
                completed += 1
            else:
                missing_required += 1

        for item in material_checks:
            level = int(item.get("required_level", 1) or 1)
            # 仅“必需/条件必需”计入完整度硬要求。
            if level in (1, 2):
                required_total += 1
                if item.get("status") == "已提供":
                    completed += 1
                else:
                    missing_required += 1

        completeness = int(round((completed / required_total) * 100)) if required_total else 0
        return {
            "required_total": required_total,
            "completed": completed,
            "missing_required": missing_required,
            "completeness_score": completeness,
        }

    @staticmethod
    def _status_from_score(score_info: Dict, gaps: List[Dict], intent_type: str) -> str:
        if score_info["required_total"] == 0:
            return "需完善制度映射"

        severe_gaps = [g for g in gaps if int(g.get("severity", 2)) >= 3]
        if intent_type in {"请示审查", "混合"}:
            if severe_gaps:
                return "不通过"
            if score_info["missing_required"] > 0:
                return "需补充"
            return "通过"

        # 事项咨询默认给办理建议，不直接判“不通过”。
        if score_info["missing_required"] > 0:
            return "建议补充"
        return "可执行"

    @staticmethod
    def _to_legacy_level(status: str) -> str:
        if status in {"不通过", "需补充"}:
            return "高"
        if status in {"建议补充", "需完善制度映射", "可参考知识点"}:
            return "中"
        return "低"

    def evaluate(self, user_event: str, intent_data: Dict, compliance_analysis: Dict, retrieved: Dict) -> Dict:
        logger.info("[Evaluator] 正在计算合规状态与完整度评分...")

        procedure_checks = compliance_analysis.get("procedure_checks", [])
        material_checks = compliance_analysis.get("material_checks", [])
        gaps = compliance_analysis.get("gaps", [])
        fallback_knowledge = compliance_analysis.get("fallback_knowledge", [])
        intent_type = str(compliance_analysis.get("intent_type", intent_data.get("intent_type", "事项咨询")))

        score_info = self._score_items(procedure_checks, material_checks)
        if intent_type not in {"请示审查", "混合"}:
            required_total = len(procedure_checks) + len(
                [m for m in material_checks if int(m.get("required_level", 1) or 1) in (1, 2)]
            )
            score_info = {
                "required_total": required_total,
                "completed": required_total if required_total > 0 else 0,
                "missing_required": 0,
                "completeness_score": 100 if required_total > 0 else 0,
            }

        compliance_status = self._status_from_score(score_info, gaps, intent_type)
        # 知识讲解场景：无结构化命中但命中知识点时，避免误导为“完全未命中”。
        if compliance_status == "需完善制度映射" and fallback_knowledge and intent_type not in {"请示审查", "混合"}:
            compliance_status = "可参考知识点"
        legacy_level = self._to_legacy_level(compliance_status)

        leading_factors = []
        if score_info["missing_required"] > 0:
            leading_factors.append(f"必需项未满足 {score_info['missing_required']} 项")
        if gaps:
            leading_factors.append(f"缺漏项 {len(gaps)} 项")
        if fallback_knowledge:
            leading_factors.append(f"知识库补充命中 {len(fallback_knowledge)} 项")
        if not leading_factors:
            leading_factors.append("制度程序与材料要求基本满足")

        recommendations_for_applicant = []
        for g in gaps[:10]:
            recommendations_for_applicant.append(
                f"补齐[{g.get('gap_type')}] {g.get('gap_item')}（依据: {g.get('policy_doc_name', '')} {g.get('clause_no', '')}）"
            )
        if not recommendations_for_applicant and compliance_status in {"可执行", "通过"}:
            recommendations_for_applicant.append("按匹配到的流程步骤逐项办理，并保留证明材料。")
        if not recommendations_for_applicant and compliance_status == "可参考知识点":
            recommendations_for_applicant.append("可基于已命中的知识点形成讲解答复；如需办理结论，请补充具体业务场景。")

        recommendations_for_reviewer = [
            "优先核验必需流程步骤和必需材料是否齐全。",
            "缺漏项应逐条给出制度条款依据并形成补正清单。",
            "对高频事项沉淀标准化模板与校验规则。",
        ]

        return {
            "overall_conclusion": {
                "evaluation": compliance_status,
                "core_reason": f"{compliance_analysis.get('summary', '')} 完整度 {score_info['completeness_score']} 分。",
            },
            "compliance_rating": {
                "status": compliance_status,
                "completeness_score": score_info["completeness_score"],
                "required_total": score_info["required_total"],
                "completed": score_info["completed"],
                "missing_required": score_info["missing_required"],
                "blocking_gaps": [g for g in gaps if int(g.get("severity", 2)) >= 3],
            },
            # 兼容旧字段供历史页和旧模板使用。
            "risk_rating": {
                "level": legacy_level,
                "total_score": score_info["completeness_score"],
                "leading_factors": "；".join(leading_factors),
            },
            "judicial_behavior_analysis": [],
            "risk_list": [
                {
                    "point": g.get("gap_item", ""),
                    "description": g.get("fix_suggestion", "") or g.get("expected_req", ""),
                }
                for g in gaps
            ],
            "recommendations": {
                "for_applicant": recommendations_for_applicant,
                "for_reviewer": recommendations_for_reviewer,
            },
            "gaps": gaps,
            "procedure_checks": procedure_checks,
            "material_checks": material_checks,
            "matched_business_types": compliance_analysis.get("matched_business_types", []),
            "clause_refs": compliance_analysis.get("clause_refs", []),
            "fallback_knowledge": fallback_knowledge,
        }
