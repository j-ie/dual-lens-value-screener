from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any

from sqlalchemy.engine import Engine

from value_screener.application.financial_statement_window import (
    end_date_in_window,
    statement_api_date_bounds,
    utc_now,
)
from value_screener.infrastructure.financial_statement_repository import FinancialStatementRepository
from value_screener.infrastructure.reference_repository import ReferenceMasterRepository
from value_screener.infrastructure.settings import AShareIngestionSettings
from value_screener.infrastructure.tushare_financial_statement_fetcher import TushareFinancialStatementFetcher
from value_screener.infrastructure.tushare_provider import TushareAShareProvider

logger = logging.getLogger(__name__)

_DATA_SOURCE = "tushare"


def _resolve_universe(engine: Engine, token: str, settings: AShareIngestionSettings) -> list[str]:
    ref = ReferenceMasterRepository(engine)
    with engine.connect() as conn:
        codes = ref.list_active_ts_codes(conn)
    if codes:
        return codes
    prov = TushareAShareProvider(
        token,
        request_sleep_seconds=settings.request_sleep_seconds,
        max_workers=1,
        max_retries=settings.tushare_max_retries,
        retry_backoff_seconds=settings.tushare_retry_backoff_seconds,
    )
    return prov.list_universe()


def _filter_window(rows: list[dict[str, Any]], *, start: str, end: str) -> list[dict[str, Any]]:
    """
    仅按报告期 end_date 是否落在 [start, end] 过滤。
    不按 report_type 或 MMDD 后缀排除：年报(1231)、半年报(0630 等)、季报(0331/0630/0930/1231) 只要在窗内均保留。
    """

    out: list[dict[str, Any]] = []
    for r in rows:
        ed = str(r.get("end_date") or "").strip()
        if end_date_in_window(ed, start=start, end=end):
            out.append(r)
    return out


def _merged_end_date_suffix_counts(
    inc: list[dict[str, Any]],
    bal: list[dict[str, Any]],
    cf: list[dict[str, Any]],
) -> dict[str, int]:
    """三表合并后的唯一 end_date，按 MMDD 后缀计数（用于核对季末/半年末是否落库）。"""

    seen: set[str] = set()
    for bucket in (inc, bal, cf):
        for r in bucket:
            ed = str(r.get("end_date") or "").strip()
            if len(ed) == 8 and ed.isdigit():
                seen.add(ed)
    counts: dict[str, int] = {}
    for ed in seen:
        suf = ed[4:]
        counts[suf] = counts.get(suf, 0) + 1
    return dict(sorted(counts.items()))


def _sync_one_symbol(
    fetcher: TushareFinancialStatementFetcher,
    repo: FinancialStatementRepository,
    engine: Engine,
    ts_code: str,
    start: str,
    end: str,
    fetched_at: datetime,
) -> tuple[str, int, int, int, dict[str, int]]:
    inc = _filter_window(fetcher.fetch_income(ts_code, start, end), start=start, end=end)
    bal = _filter_window(fetcher.fetch_balancesheet(ts_code, start, end), start=start, end=end)
    cf = _filter_window(fetcher.fetch_cashflow(ts_code, start, end), start=start, end=end)
    with engine.begin() as conn:
        repo.upsert_income_rows(conn, inc, data_source=_DATA_SOURCE, fetched_at=fetched_at)
        repo.upsert_balance_rows(conn, bal, data_source=_DATA_SOURCE, fetched_at=fetched_at)
        repo.upsert_cashflow_rows(conn, cf, data_source=_DATA_SOURCE, fetched_at=fetched_at)
    suffix_hist = _merged_end_date_suffix_counts(inc, bal, cf)
    return ts_code, len(inc), len(bal), len(cf), suffix_hist


def sync_financial_statements_to_mysql(
    engine: Engine,
    settings: AShareIngestionSettings,
    token: str,
    *,
    max_symbols: int | None = None,
    since_years: int = 3,
) -> dict[str, Any]:
    """
    按 TuShare 三表接口拉取近 since_years 年财报，写入 fs_income / fs_balance / fs_cashflow。
    单标的失败记入 failures，不中断全市场。
    """

    if not token or not token.strip():
        raise ValueError("TUSHARE_TOKEN 不能为空")
    start, end = statement_api_date_bounds(since_years=since_years)
    symbols = _resolve_universe(engine, token.strip(), settings)
    if max_symbols is not None and max_symbols > 0:
        symbols = symbols[:max_symbols]
    total = len(symbols)
    if total == 0:
        logger.warning("标的列表为空，跳过同步")
        return {
            "universe": 0,
            "since_years": since_years,
            "api_start": start,
            "api_end": end,
            "ok": 0,
            "failures": [],
            "workers": 1,
        }

    fetcher = TushareFinancialStatementFetcher(
        token.strip(),
        request_sleep_seconds=settings.request_sleep_seconds,
        max_retries=settings.tushare_max_retries,
        retry_backoff_seconds=settings.tushare_retry_backoff_seconds,
    )
    repo = FinancialStatementRepository(engine)
    workers = max(1, min(settings.tushare_max_workers, 64))
    fetched_at = utc_now()
    failures: list[dict[str, Any]] = []
    ok_count = 0

    def _job(code: str) -> tuple[str, int, int, int, dict[str, int]]:
        return _sync_one_symbol(fetcher, repo, engine, code, start, end, fetched_at)

    if workers <= 1:
        for i, code in enumerate(symbols, start=1):
            try:
                _, ni, nb, nc, suf = _job(code)
                ok_count += 1
                logger.info(
                    "财报同步 %s/%s %s income=%s balance=%s cashflow=%s end_date_mmdd=%s",
                    i,
                    total,
                    code,
                    ni,
                    nb,
                    nc,
                    suf,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("标的 %s 财报同步失败（%s/%s）: %s", code, i, total, exc)
                failures.append({"ts_code": code, "error": str(exc)})
    else:
        done = 0
        with ThreadPoolExecutor(max_workers=workers) as pool:
            future_map = {pool.submit(_job, code): code for code in symbols}
            for fut in as_completed(future_map):
                code = future_map[fut]
                done += 1
                try:
                    _, ni, nb, nc, suf = fut.result()
                    ok_count += 1
                    logger.info(
                        "财报同步 %s/%s %s income=%s balance=%s cashflow=%s end_date_mmdd=%s",
                        done,
                        total,
                        code,
                        ni,
                        nb,
                        nc,
                        suf,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning("标的 %s 财报同步失败（%s/%s）: %s", code, done, total, exc)
                    failures.append({"ts_code": code, "error": str(exc)})

    return {
        "universe": total,
        "since_years": since_years,
        "api_start": start,
        "api_end": end,
        "ok": ok_count,
        "failures": failures,
        "workers": workers,
    }
