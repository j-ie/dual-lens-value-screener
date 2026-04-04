from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

from value_screener.application.batch_screening_service import BatchScreeningApplicationService
from value_screener.application.screening_service import ScreeningApplicationService
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

    args = parser.parse_args(argv)
    if args.command == "batch-screen":
        return _run_batch_screen(args)
    if args.command == "sync-reference":
        return _run_sync_reference()
    return 1


def _run_batch_screen(args: argparse.Namespace) -> int:
    base = AShareIngestionSettings.from_env()
    primary = args.primary if args.primary is not None else base.primary_backend
    settings = AShareIngestionSettings(
        tushare_token=base.tushare_token,
        primary_backend=primary,
        max_symbols=base.max_symbols,
        request_sleep_seconds=base.request_sleep_seconds,
    )
    symbols = None
    if args.symbols_file is not None:
        raw = args.symbols_file.read_text(encoding="utf-8")
        symbols = [ln.strip() for ln in raw.splitlines() if ln.strip() and not ln.strip().startswith("#")]

    provider = build_composite_provider(settings)
    batch_svc = BatchScreeningApplicationService(provider, ScreeningApplicationService())
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


if __name__ == "__main__":
    raise SystemExit(main())
