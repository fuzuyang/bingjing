# D:\sjtu\test_run.py
import sys
import os

# 确保路径正确
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR)

from modules.evaluator.coordinator import AssessmentCoordinator


def test_system():
    # 构造一个复杂的“营商环境”典型测试案例
    # 涉及领域：人工智能/行政执法/市场准入
    complex_case = """
    某初创人工智能企业研发了一款“无人巡检机器人”，在某工业园区进行试点运行。
    最近，当地城管部门以“未经城市道路占用审批”和“可能危害公共安全”为由，
    扣押了企业的全部测试设备，并开出了50万元的高额罚款（该企业年营收仅100万）。

    企业反映：
    1. 现行审批目录中并没有“巡检机器人”这一项，导致企业“想办证无处办”。
    2. 执法部门未经过任何风险评估，直接采取了最严厉的强制措施。
    3. 附近其他大型国企的类似设备却未受到限制，涉嫌选择性执法。
    """

    print(" 正在启动营商环境 AI 评估测试...")
    print("-" * 50)

    try:
        coordinator = AssessmentCoordinator()
        # 执行全流程评估
        report = coordinator.run_full_assessment(complex_case)

        # 将结果输出到文件，方便你查看 Markdown 渲染效果
        output_file = "Test_Evaluation_Report.md"
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(report)

        print("-" * 50)
        print(f" 测试完成！请打开目录下的 [{output_file}] 查看深度评估报告。")

    except Exception as e:
        print(f" 测试过程中发生错误: {e}")


if __name__ == "__main__":
    test_system()