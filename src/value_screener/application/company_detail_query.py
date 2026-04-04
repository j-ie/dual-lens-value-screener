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
from value_screener.infrastructure.settings import AShareIngestionSettings
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
    ) -> dict[str, Any]:
        if not is_valid_ts_code(ts_code):
            raise ValueError("非法 ts_code")
        code = str(ts_code).strip()
        lim = max(1, min(int(financial_limit), 48))
        ranking = CombinedRankingParams.from_env()
        settings = AShareIngestionSettings.from_env()

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
            income = fs_repo.list_recent_income(conn, code, limit=lim)
            balance = fs_repo.list_recent_balance(conn, code, limit=lim)
            cashflow = fs_repo.list_recent_cashflow(conn, code, limit=lim)

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

        return {
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
