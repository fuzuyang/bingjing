import argparse
import json

from modules.evaluator.self_check import GlobalSelfChecker


def main():
    parser = argparse.ArgumentParser(description="全局自检：场景化合规自查与制度推荐")
    parser.add_argument(
        "--no-auto-fix",
        action="store_true",
        help="仅检查，不自动修复缺失结构/数据",
    )
    args = parser.parse_args()

    checker = GlobalSelfChecker(auto_fix=not args.no_auto_fix)
    result = checker.run()
    print(json.dumps(result, ensure_ascii=False, indent=2))

    if result.get("overall_status") != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()

