from __future__ import annotations

import logging
import os
import threading
import time
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy.engine import Engine

from value_screener.application.result_enrichment import enrich_screening_result_row
from value_screener.domain.combined_ranking_params import CombinedRankingParams
from value_screener.domain.ts_code_format import is_valid_ts_code
from value_screener.infrastructure.financial_statement_repository import FinancialStatementRepository
from value_screener.infrastructure.reference_repository import ReferenceMasterRepository
from value_screener.infrastructure.screening_repository import ScreeningRepository
from value_screener.application.dcf_company_valuation import build_company_dcf_payload
from value_screener.application.dcf_financial_sync import (
    dcf_financials_need_tushare_refresh,
    sync_cashflow_and_balance_for_dcf,
)
from value_screener.infrastructure.settings import AShareIngestionSettings, DcfValuationSettings
from value_screener.infrastructure.tushare_daily_basic_fetcher import TushareDailyBasicFetcher
from value_screener.infrastructure.tushare_live_quote_fetcher import TushareLiveQuoteFetcher

logger = logging.getLogger(__name__)

_quote_cache_lock = threading.Lock()
_quote_cache: dict[str, tuple[float, dict[str, Any]]] = {}


def _detail_quote_ttl_seconds() -> float:
    raw = os.environ.get("VALUE_SCREENER_DETAIL_QUOTE_TTL_SECONDS", "60").strip()
    try:
        v = float(raw)
    except ValueError:
        return 60.0
    return max(0.0, v)


def _detail_quote_timeout_seconds() -> float:
    raw = os.environ.get("VALUE_SCREENER_DETAIL_QUOTE_TIMEOUT_SECONDS", "12").strip()
    try:
        v = float(raw)
    except ValueError:
        return 12.0
    return max(1.0, v)


def _json_safe(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
    if isinstance(v, dict):
        return {str(k): _json_safe(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_json_safe(x) for x in v]
    return v


def _reference_for_api(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not row:
        return None
    drop = {"synced_at"}  # 单独序列化
    out: dict[str, Any] = {}
    for k, val in row.items():
        if k in drop:
            continue
        out[str(k)] = _json_safe(val)
    if row.get("synced_at") is not None:
        out["synced_at"] = _json_safe(row["synced_at"])
    return out


def _financial_row_for_api(row: dict[str, Any], *, include_payload: bool) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, val in row.items():
        if k == "payload" and not include_payload:
            continue
        if k == "payload" and isinstance(val, dict):
            out["payload"] = _json_safe(val)
            continue
        out[str(k)] = _json_safe(val)
    return out


def _fetch_live_quote_block(
    ts_code: str,
    *,
    token: str | None,
    sleep_s: float,
) -> dict[str, Any]:
    ttl = _detail_quote_ttl_seconds()
    now = time.time()
    if ttl > 0:
        with _quote_cache_lock:
            hit = _quote_cache.get(ts_code)
            if hit is not None and now - hit[0] < ttl:
                return dict(hit[1])

    if not token:
        return {
            "ok": False,
            "fetched_at": datetime.now().isoformat(),
            "error": "未配置 TUSHARE_TOKEN",
            "data": None,
        }

    try:
        fetcher = TushareLiveQuoteFetcher(
            token,
            request_sleep_seconds=sleep_s,
            fetch_timeout_seconds=_detail_quote_timeout_seconds(),
        )
        data = fetcher.fetch_daily_last_bar(ts_code)
        block = {
            "ok": True,
            "fetched_at": datetime.now().isoformat(),
            "error": None,
            "data": data,
        }
    except Exception as exc:  # noqa: BLE001
        logger.warning("live_quote %s: %s", ts_code, exc)
        block = {
            "ok": False,
            "fetched_at": datetime.now().isoformat(),
            "error": str(exc),
            "data": None,
        }

    if ttl > 0 and block.get("ok"):
        with _quote_cache_lock:
            _quote_cache[ts_code] = (time.time(), dict(block))
    return block


class CompanyDetailQueryService:
    """公司详情读模型：Run 冻结快照 + 主数据 + 财报摘要 + 独立行情。"""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def load(
        self,
        run_id: int,
        ts_code: str,
        *,
        include_financial_payload: bool = False,
        financial_limit: int = 12,
        include_dcf: bool = False,
        dcf_wacc: float | None = None,
        dcf_stage1_growth: float | None = None,
        dcf_terminal_growth: float | None = None,
    ) -> dict[str, Any]:
        if not is_valid_ts_code(ts_code):
            raise ValueError("非法 ts_code")
        code = str(ts_code).strip()
        lim = max(1, min(int(financial_limit), 48))
        ranking = CombinedRankingParams.from_env()
        settings = AShareIngestionSettings.from_env()
        dcf_settings: DcfValuationSettings | None = (
            DcfValuationSettings.from_env() if include_dcf else None
        )
        fs_limit = max(lim, 24) if include_dcf else lim

        repo = ScreeningRepository(self._engine)
        ref_repo = ReferenceMasterRepository(self._engine)
        fs_repo = FinancialStatementRepository(self._engine)

        with self._engine.connect() as conn:
            run_row = repo.get_run(conn, run_id)
            if run_row is None:
                return {"_error": "run_not_found"}

            raw_row = repo.get_result_row_for_run_symbol(
                conn,
                run_id,
                code,
                ranking=ranking,
            )
            if raw_row is None:
                return {"_error": "symbol_not_in_run"}

            ref_row = ref_repo.fetch_one_by_ts_code(conn, code)
            income = fs_repo.list_recent_income(conn, code, limit=fs_limit)
            balance = fs_repo.list_recent_balance(conn, code, limit=fs_limit)
            cashflow = fs_repo.list_recent_cashflow(conn, code, limit=fs_limit)

        if (
            include_dcf
            and dcf_settings is not None
            and dcf_settings.enabled
            and settings.tushare_token
            and dcf_financials_need_tushare_refresh(
                cashflow,
                balance,
                scan_cashflow_head=max(12, dcf_settings.ttm_periods_max * 3),
            )
        ):
            try:
                sync_cashflow_and_balance_for_dcf(
                    self._engine,
                    code,
                    token=settings.tushare_token,
                    settings=settings,
                    since_years=dcf_settings.sync_since_years,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("dcf 财报按需同步失败 ts_code=%s: %s", code, exc)
            else:
                with self._engine.connect() as conn:
                    income = fs_repo.list_recent_income(conn, code, limit=fs_limit)
                    balance = fs_repo.list_recent_balance(conn, code, limit=fs_limit)
                    cashflow = fs_repo.list_recent_cashflow(conn, code, limit=fs_limit)

        enriched = enrich_screening_result_row(raw_row)
        run_snapshot = {k: _json_safe(v) for k, v in enriched.items()}

        financials = {
            "income": [_financial_row_for_api(r, include_payload=include_financial_payload) for r in income],
            "balance": [_financial_row_for_api(r, include_payload=include_financial_payload) for r in balance],
            "cashflow": [_financial_row_for_api(r, include_payload=include_financial_payload) for r in cashflow],
        }

        live_quote = _fetch_live_quote_block(
            code,
            token=settings.tushare_token,
            sleep_s=settings.request_sleep_seconds,
        )

        dcf_block: dict[str, Any] | None = None
        if include_dcf and dcf_settings is not None:
            cf_for_dcf = [_financial_row_for_api(r, include_payload=False) for r in cashflow]
            bal_for_dcf = [_financial_row_for_api(r, include_payload=True) for r in balance]
            inc_for_dcf = [_financial_row_for_api(r, include_payload=False) for r in income]
            if not dcf_settings.enabled:
                dcf_block = {
                    "ok": False,
                    "skip_reason": "disabled",
                    "message": "DCF 未启用：将 VALUE_SCREENER_DCF_ENABLED 设为 1 / true / yes / on",
                    "warnings": [],
                    "notes": [],
                    "assumptions": None,
                    "values": None,
                }
            elif not settings.tushare_token:
                dcf_block = {
                    "ok": False,
                    "skip_reason": "no_token",
                    "message": "未配置 TUSHARE_TOKEN，无法拉取 daily_basic 总股本",
                    "warnings": [],
                    "notes": [],
                    "assumptions": None,
                    "values": None,
                }
            else:

                def _fetch_shares() -> float:
                    fetcher = TushareDailyBasicFetcher(
                        settings.tushare_token,
                        request_sleep_seconds=settings.request_sleep_seconds,
                        fetch_timeout_seconds=dcf_settings.daily_basic_timeout_seconds,
                    )
                    return fetcher.fetch_latest_total_shares(code)

                dcf_block = build_company_dcf_payload(
                    cashflow_rows=cf_for_dcf,
                    balance_rows=bal_for_dcf,
                    settings=dcf_settings,
                    wacc_override=dcf_wacc,
                    stage1_override=dcf_stage1_growth,
                    terminal_override=dcf_terminal_growth,
                    fetch_total_shares=_fetch_shares,
                    industry=(enriched.get("industry") or None),
                    income_rows=inc_for_dcf,
                )

        out: dict[str, Any] = {
            "run_id": run_id,
            "ts_code": code,
            "run": {
                "id": run_row.id,
                "status": run_row.status,
                "created_at": _json_safe(run_row.created_at),
                "finished_at": _json_safe(run_row.finished_at),
            },
            "run_snapshot": run_snapshot,
            "reference": _reference_for_api(ref_row),
            "financials": financials,
            "live_quote": live_quote,
        }
        if include_dcf:
            out["dcf"] = _json_safe(dcf_block)
        return out
