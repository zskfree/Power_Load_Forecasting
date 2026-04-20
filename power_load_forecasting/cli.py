from __future__ import annotations

import argparse
from datetime import date
import json
import logging

from .config import load_config
from .service import WeatherCollectorService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="面向负荷预测和电价预测的数据采集命令行工具。"
    )
    parser.add_argument(
        "--config",
        default="config/collector.toml",
        help="采集器 TOML 配置文件路径。",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="日志级别。",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser(
        "sync",
        help="抓取一版预报快照，并同步缺失的历史实况天气。",
    )

    backfill_actual = subparsers.add_parser(
        "backfill-actual",
        help="回填历史实况天气数据。",
    )
    backfill_actual.add_argument("--start-date", required=True, help="开始日期，格式 YYYY-MM-DD")
    backfill_actual.add_argument("--end-date", default=None, help="结束日期，格式 YYYY-MM-DD")

    backfill_forecast = subparsers.add_parser(
        "backfill-forecast-snapshots",
        help="按历史运行时刻回填天气预报快照。",
    )
    backfill_forecast.add_argument("--start-date", required=True, help="开始日期，格式 YYYY-MM-DD")
    backfill_forecast.add_argument("--end-date", default=None, help="结束日期，格式 YYYY-MM-DD")
    backfill_forecast.add_argument(
        "--interval-hours",
        type=int,
        default=None,
        help="历史快照回填间隔小时数，默认读取配置文件。",
    )
    backfill_forecast.add_argument(
        "--force",
        action="store_true",
        help="即使目标快照已存在，也重新抓取并覆盖。",
    )

    subparsers.add_parser(
        "status",
        help="查看采集状态和当前水位。",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    config = load_config(args.config)
    service = WeatherCollectorService(config)
    try:
        if args.command == "sync":
            result = service.sync()
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return _exit_code_from_sync(result)

        if args.command == "backfill-actual":
            start_date = date.fromisoformat(args.start_date)
            end_date = date.fromisoformat(args.end_date) if args.end_date else None
            if end_date is not None and start_date > end_date:
                raise ValueError("start-date 不能晚于 end-date")
            result = service.backfill_actual(start_date=start_date, end_date=end_date)
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return _exit_code_from_backfill(result)

        if args.command == "backfill-forecast-snapshots":
            start_date = date.fromisoformat(args.start_date)
            end_date = date.fromisoformat(args.end_date) if args.end_date else None
            if end_date is not None and start_date > end_date:
                raise ValueError("start-date 不能晚于 end-date")
            result = service.backfill_forecast_snapshots(
                start_date=start_date,
                end_date=end_date,
                interval_hours=args.interval_hours,
                force=args.force,
            )
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return _exit_code_from_backfill(result)

        if args.command == "status":
            result = service.status()
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0

        raise ValueError(f"不支持的命令：{args.command}")
    finally:
        service.close()


def _exit_code_from_sync(result: dict) -> int:
    forecast_errors = sum(1 for item in result["forecast"] if item["status"] == "error")
    actual_errors = sum(1 for item in result["actual"] if item["status"] == "error")
    return 1 if forecast_errors or actual_errors else 0


def _exit_code_from_backfill(result: list[dict]) -> int:
    return 1 if any(item["status"] == "error" for item in result) else 0
