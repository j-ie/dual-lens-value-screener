from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from value_screener.application.batch_screening_service import BatchScreeningApplicationService
from value_screener.application.screening_service import ScreeningApplicationService
from value_screener.infrastructure.app_db import get_engine
from value_screener.infrastructure.factory import build_composite_provider
from value_screener.infrastructure.settings import AShareIngestionSettings


def main(argv: list[str] | None = None) -> int:
    _root = Path(__file__).resolve().parents[2]
    load_dotenv(_root / ".env", override=False)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(prog="value-screener")
    sub = parser.add_subparsers(dest="command", required=True)

    batch = sub.add_parser("batch-screen", help="全 A 或子集拉数并双维度算分（耗时任务，建议限制 max-symbols）")
    batch.add_argument("--max-symbols", type=int, default=None, help="最多处理标的数，用于试跑")
    batch.add_argument(
        "--symbols-file",
        type=Path,
        default=None,
        help="每行一个代码（6 位或 ts_code），缺省则从数据源拉全市场列表",
    )
    batch.add_argument(
        "--output",
        "-o",
        type=Path,
        default=None,
        help="结果 JSON 路径，缺省输出到 stdout",
    )
    batch.add_argument(
        "--primary",
        choices=("tushare", "akshare"),
        default=None,
        help="主数据源（覆盖环境变量 VALUE_SCREENER_PRIMARY）",
    )
    batch.add_argument(
        "--persist",
        action="store_true",
        help="将本次批跑结果写入 MySQL（需 DATABASE_URL，建议先 alembic upgrade）",
    )

    sync_ref = sub.add_parser(
        "sync-reference",
        help="将 TuShare stock_basic 同步到 MySQL 表 security_reference（需 TUSHARE_TOKEN 与 DATABASE_URL）",
    )

    sync_fs = sub.add_parser(
        "sync-financial-statements",
        help="从 TuShare 同步利润表/资产负债表/现金流量表至 MySQL（需 TUSHARE_TOKEN、DATABASE_URL，建议先 alembic upgrade）",
    )
    sync_fs.add_argument("--max-symbols", type=int, default=None, help="最多处理标的数，用于试跑")
    sync_fs.add_argument(
        "--since-years",
        type=int,
        default=3,
        help="报告期窗口：从当前年起向前 since_years 个日历年（默认 3）",
    )
    sync_fs.add_argument(
        "--scheduled-date",
        type=str,
        default=None,
        help="调度日 YYYY-MM-DD（默认按 VALUE_SCREENER_FS_SYNC_SCHEDULE_TZ 时区的当天）",
    )
    sync_fs.add_argument(
        "--reset-job",
        action="store_true",
        help="清除当日同参数 ingestion_job 游标后从头拉取",
    )
    sync_fs.add_argument(
        "--no-resume",
        action="store_true",
        help="忽略已存游标，从 universe 第一只开始（仍写入同一 job 键）",
    )

    attach_tl = sub.add_parser(
        "attach-third-lens",
        help="对已有 screening_run 写入第三套分与三元综合分（需 DATABASE_URL、fs_income、security_reference，建议先 alembic upgrade 006）",
    )
    attach_tl.add_argument("--run-id", type=int, required=True, dest="run_id", help="screening_run.id")

    args = parser.parse_args(argv)
    if args.command == "batch-screen":
        return _run_batch_screen(args)
    if args.command == "sync-reference":
        return _run_sync_reference()
    if args.command == "sync-financial-statements":
        return _run_sync_financial_statements(args)
    if args.command == "attach-third-lens":
        return _run_attach_third_lens(args)
    return 1


def _run_batch_screen(args: argparse.Namespace) -> int:
    base = AShareIngestionSettings.from_env()
    primary = args.primary if args.primary is not None else base.primary_backend
    settings = AShareIngestionSettings(
        tushare_token=base.tushare_token,
        primary_backend=primary,
        max_symbols=base.max_symbols,
        request_sleep_seconds=base.request_sleep_seconds,
        tushare_max_workers=base.tushare_max_workers,
        tushare_max_retries=base.tushare_max_retries,
        tushare_retry_backoff_seconds=base.tushare_retry_backoff_seconds,
        tushare_max_calls_per_minute=base.tushare_max_calls_per_minute,
        tushare_rpm_headroom=base.tushare_rpm_headroom,
        fs_sync_schedule_tz=base.fs_sync_schedule_tz,
        batch_screen_persist_chunk_size=base.batch_screen_persist_chunk_size,
    )
    symbols = None
    if args.symbols_file is not None:
        raw = args.symbols_file.read_text(encoding="utf-8")
        symbols = [ln.strip() for ln in raw.splitlines() if ln.strip() and not ln.strip().startswith("#")]

    provider = build_composite_provider(settings)
    screening_engine = None
    if os.environ.get("DATABASE_URL", "").strip():
        screening_engine = get_engine()
    batch_svc = BatchScreeningApplicationService(
        provider,
        ScreeningApplicationService(),
        screening_engine=screening_engine,
    )
    result = batch_svc.run(symbols=symbols, max_symbols=args.max_symbols)

    payload = {
        "results": result.results,
        "failures": result.failures,
        "meta": result.meta,
    }
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    if args.output:
        args.output.write_text(text, encoding="utf-8")
    else:
        sys.stdout.write(text)
        sys.stdout.write("\n")

    if args.persist:
        from value_screener.application.persist_screening_run import persist_batch_screening
        from value_screener.infrastructure.app_db import get_engine

        try:
            prov = result.meta.get("provider")
            run_id = persist_batch_screening(
                get_engine(),
                result,
                provider_label=str(prov) if prov else None,
            )
        except Exception as exc:  # noqa: BLE001
            logging.exception("持久化失败: %s", exc)
            return 1
        logging.info("已写入 screening_run id=%s", run_id)

    return 0


def _run_sync_reference() -> int:
    from value_screener.application.sync_stock_basic import sync_stock_basic_to_mysql
    from value_screener.infrastructure.app_db import get_engine

    base = AShareIngestionSettings.from_env()
    token = (base.tushare_token or "").strip()
    if not token:
        logging.error("未配置 TUSHARE_TOKEN")
        return 1
    try:
        engine = get_engine()
    except Exception as exc:  # noqa: BLE001
        logging.error("数据库不可用: %s", exc)
        return 1
    try:
        n = sync_stock_basic_to_mysql(engine, token)
    except Exception as exc:  # noqa: BLE001
        logging.exception("同步失败: %s", exc)
        return 1
    logging.info("security_reference 已同步，约 %s 行", n)
    return 0


def _run_sync_financial_statements(args: argparse.Namespace) -> int:
    from value_screener.application.sync_financial_statements import sync_financial_statements_to_mysql
    from value_screener.infrastructure.app_db import get_engine

    base = AShareIngestionSettings.from_env()
    token = (base.tushare_token or "").strip()
    if not token:
        logging.error("未配置 TUSHARE_TOKEN")
        return 1
    if args.since_years < 1:
        logging.error("--since-years 至少为 1")
        return 1
    sched = None
    if args.scheduled_date:
        try:
            sched = datetime.strptime(args.scheduled_date.strip(), "%Y-%m-%d").date()
        except ValueError:
            logging.error("--scheduled-date 须为 YYYY-MM-DD")
            return 1
    try:
        engine = get_engine()
    except Exception as exc:  # noqa: BLE001
        logging.error("数据库不可用: %s", exc)
        return 1
    try:
        meta = sync_financial_statements_to_mysql(
            engine,
            base,
            token,
            max_symbols=args.max_symbols,
            since_years=args.since_years,
            scheduled_date=sched,
            resume=not args.no_resume,
            reset_job=args.reset_job,
        )
    except Exception as exc:  # noqa: BLE001
        logging.exception("财报同步失败: %s", exc)
        return 1
    logging.info(
        "财报同步完成 universe=%s ok=%s failures=%s workers=%s window=[%s,%s] "
        "scheduled_date=%s skipped_completed=%s resumed_from_index=%s",
        meta.get("universe"),
        meta.get("ok"),
        len(meta.get("failures") or []),
        meta.get("workers"),
        meta.get("api_start"),
        meta.get("api_end"),
        meta.get("scheduled_date"),
        meta.get("skipped_completed"),
        meta.get("resumed_from_index"),
    )
    return 0


def _run_attach_third_lens(args: argparse.Namespace) -> int:
    from value_screener.application.attach_third_lens_scores import attach_third_lens_for_run
    from value_screener.infrastructure.app_db import get_engine

    try:
        engine = get_engine()
    except Exception as exc:  # noqa: BLE001
        logging.error("数据库不可用: %s", exc)
        return 1
    try:
        meta = attach_third_lens_for_run(engine, args.run_id)
    except ValueError as exc:
        logging.error("%s", exc)
        return 1
    except Exception as exc:  # noqa: BLE001
        logging.exception("attach-third-lens 失败: %s", exc)
        return 1
    logging.info("第三套分已写入 run_id=%s updated=%s", meta.get("run_id"), meta.get("updated"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
