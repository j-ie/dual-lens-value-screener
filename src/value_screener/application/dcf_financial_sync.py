"""
DCF 专用：在本地财报字段不足时拉取 TuShare 现金流量表与资产负债表并落库。
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from sqlalchemy.engine import Engine

from value_screener.application.financial_statement_payload import investing_cashflow_net_from_row, to_float_or_none
from value_screener.application.financial_statement_window import (
    end_date_in_window,
    statement_api_date_bounds,
    utc_now,
)
from value_screener.infrastructure.financial_statement_repository import FinancialStatementRepository
from value_screener.infrastructure.settings import AShareIngestionSettings
from value_screener.infrastructure.tushare_financial_statement_fetcher import (
    TushareFinancialStatementFetcher,
)

logger = logging.getLogger(__name__)

_DATA_SOURCE = "tushare"


def _sort_by_end_date_desc(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda r: str(r.get("end_date") or ""), reverse=True)


def _filter_window(rows: list[dict[str, Any]], *, start: str, end: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in rows:
        ed = str(r.get("end_date") or "").strip()
        if end_date_in_window(ed, start=start, end=end):
            out.append(r)
    return out


def dcf_financials_need_tushare_refresh(
    cashflow_rows: list[dict[str, Any]],
    balance_rows: list[dict[str, Any]],
    *,
    scan_cashflow_head: int = 12,
) -> bool:
    """
    判断是否有必要为 DCF 触发 TuShare 同步。

    条件：无表、关键标量缺失、或最近若干期投资现金流整列缺失（常见于旧库字段未写入）。
    """
    if not cashflow_rows or not balance_rows:
        return True

    head = max(1, int(scan_cashflow_head))
    cf_desc = _sort_by_end_date_desc([dict(x) for x in cashflow_rows])
    sample = cf_desc[:head]

    if any(to_float_or_none(r.get("n_cashflow_act")) is None for r in sample):
        return True

    if all(investing_cashflow_net_from_row(r) is None for r in sample):
        return True

    bal0 = _sort_by_end_date_desc([dict(x) for x in balance_rows])[0]
    if to_float_or_none(bal0.get("total_liab")) is None:
        return True

    return False


def sync_cashflow_and_balance_for_dcf(
    engine: Engine,
    ts_code: str,
    *,
    token: str,
    settings: AShareIngestionSettings,
    since_years: int,
) -> dict[str, Any]:
    """
    拉取并写入现金流量表、资产负债表（不强制同步利润表，减少调用）。

    返回 {"cashflow_upserted": int, "balance_upserted": int, "api_start": str, "api_end": str}。
    """
    code = str(ts_code).strip()
    if not code:
        raise ValueError("ts_code 不能为空")
    tok = str(token).strip()
    if not tok:
        raise ValueError("TUSHARE_TOKEN 不能为空")

    years = max(1, min(int(since_years), 20))
    start, end = statement_api_date_bounds(since_years=years)
    fetcher = TushareFinancialStatementFetcher(
        tok,
        request_sleep_seconds=settings.request_sleep_seconds,
        max_retries=settings.tushare_max_retries,
        retry_backoff_seconds=settings.tushare_retry_backoff_seconds,
        rate_limiter=None,
    )
    repo = FinancialStatementRepository(engine)
    fetched_at: datetime = utc_now()

    raw_cf = fetcher.fetch_cashflow(code, start, end)
    raw_bal = fetcher.fetch_balancesheet(code, start, end)
    cf = _filter_window(raw_cf, start=start, end=end)
    bal = _filter_window(raw_bal, start=start, end=end)

    with engine.begin() as conn:
        n_cf = repo.upsert_cashflow_rows(conn, cf, data_source=_DATA_SOURCE, fetched_at=fetched_at)
        n_bal = repo.upsert_balance_rows(conn, bal, data_source=_DATA_SOURCE, fetched_at=fetched_at)

    logger.info(
        "dcf 财报同步完成 ts_code=%s cashflow=%s balance=%s window=[%s,%s]",
        code,
        n_cf,
        n_bal,
        start,
        end,
    )
    return {
        "cashflow_upserted": n_cf,
        "balance_upserted": n_bal,
        "api_start": start,
        "api_end": end,
    }
