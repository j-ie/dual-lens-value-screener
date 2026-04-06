from __future__ import annotations

import os
import threading
import time
from typing import Any, Literal

from value_screener.infrastructure.settings import AShareIngestionSettings
from value_screener.infrastructure.tushare_market_dividend_yield_fetcher import (
    TushareMarketDividendYieldFetcher,
    page_slice,
    sort_dividend_rows,
)

SortField = Literal["dv_ratio", "dv_ttm"]
OrderKey = Literal["asc", "desc"]

_CACHE_LOCK = threading.Lock()
_RAW_ROWS_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}


def _raw_cache_ttl_seconds() -> float:
    raw = os.environ.get("VALUE_SCREENER_MARKET_DIVIDEND_CACHE_TTL_SECONDS", "120").strip()
    try:
        v = float(raw)
    except ValueError:
        v = 120.0
    return max(0.0, min(v, 3600.0))


class MarketDividendYieldQueryService:
    """
    全 A 股息率读模型：TuShare daily_basic 单日全市场，支持排序与分页。

    领域上视为「市场参考指标」查询，与单次 screening run 的冻结快照无关。
    """

    def __init__(self, settings: AShareIngestionSettings | None = None) -> None:
        self._settings = settings or AShareIngestionSettings.from_env()

    def load(
        self,
        *,
        trade_date: str | None,
        sort: SortField,
        order: OrderKey,
        page: int,
        page_size: int,
    ) -> dict[str, Any]:
        token = (self._settings.tushare_token or "").strip()
        if not token:
            return {
                "ok": False,
                "error": "未配置 TUSHARE_TOKEN",
                "trade_date": None,
                "fetched_at": None,
                "total": 0,
                "page": max(1, int(page)),
                "page_size": max(1, min(int(page_size), 10_000)),
                "sort": sort,
                "order": order,
                "items": [],
            }

        fetcher = TushareMarketDividendYieldFetcher(
            token,
            request_sleep_seconds=self._settings.request_sleep_seconds,
        )
        resolved = (trade_date or "").strip() or fetcher.resolve_latest_sse_trade_date()
        rows = self._get_raw_rows(fetcher, resolved)
        sorted_rows = sort_dividend_rows(rows, sort=sort, order=order)
        p = max(1, int(page))
        ps = max(1, min(int(page_size), 10_000))
        page_items, total = page_slice(sorted_rows, p, ps)
        return {
            "ok": True,
            "error": None,
            "trade_date": resolved,
            "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "total": total,
            "page": p,
            "page_size": ps,
            "sort": sort,
            "order": order,
            "items": page_items,
        }

    def _get_raw_rows(
        self,
        fetcher: TushareMarketDividendYieldFetcher,
        trade_date: str,
    ) -> list[dict[str, Any]]:
        ttl = _raw_cache_ttl_seconds()
        now = time.time()
        if ttl > 0:
            with _CACHE_LOCK:
                hit = _RAW_ROWS_CACHE.get(trade_date)
                if hit is not None and now - hit[0] < ttl:
                    return [dict(r) for r in hit[1]]

        rows = fetcher.fetch_all_rows(trade_date)
        if ttl > 0:
            with _CACHE_LOCK:
                _RAW_ROWS_CACHE[trade_date] = (now, rows)
        return rows
