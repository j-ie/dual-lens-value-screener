from __future__ import annotations

import logging
import threading
import time
from typing import Any

from value_screener.infrastructure.tushare_rate_limiter import (
    is_tushare_minute_rate_limit_error,
    sleep_until_next_minute_wall_clock,
)

logger = logging.getLogger(__name__)


class TushareFinancialStatementFetcher:
    """TuShare 利润表 / 资产负债表 / 现金流量表（按 ts_code + 日期窗）。"""

    def __init__(
        self,
        token: str,
        *,
        request_sleep_seconds: float = 0.12,
        max_retries: int = 2,
        retry_backoff_seconds: float = 0.5,
        rate_limiter: Any | None = None,
    ) -> None:
        if not token or not token.strip():
            raise ValueError("TuShare token 不能为空")
        import tushare as ts

        self._token = token.strip()
        ts.set_token(self._token)
        self._pro = ts.pro_api()
        self._sleep = request_sleep_seconds
        self._max_retries = max(0, min(int(max_retries), 10))
        self._retry_backoff = max(0.0, float(retry_backoff_seconds))
        self._local = threading.local()
        self._rate_limiter = rate_limiter

    def _api(self) -> Any:
        pro = getattr(self._local, "pro", None)
        if pro is None:
            import tushare as ts

            ts.set_token(self._token)
            self._local.pro = ts.pro_api()
            pro = self._local.pro
        return pro

    def fetch_income(self, ts_code: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
        return self._call_df("income", ts_code, start_date, end_date)

    def fetch_balancesheet(self, ts_code: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
        return self._call_df("balancesheet", ts_code, start_date, end_date)

    def fetch_cashflow(self, ts_code: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
        return self._call_df("cashflow", ts_code, start_date, end_date)

    def _throttle_before_request(self) -> None:
        if self._rate_limiter is not None:
            self._rate_limiter.acquire()
        else:
            time.sleep(self._sleep)

    def _call_df(self, method: str, ts_code: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
        # 仅传 ts_code + 日期窗，不传 report_type 等过滤，避免缩小年报/半年报/季报覆盖。
        minute_retries = 0
        max_minute_retries = 12
        max_attempts = 1 + self._max_retries
        attempt = 0
        while attempt < max_attempts:
            try:
                self._throttle_before_request()
                fn = getattr(self._api(), method)
                df = fn(ts_code=ts_code, start_date=start_date, end_date=end_date)
                if df is None or df.empty:
                    return []
                return df.to_dict(orient="records")
            except Exception as exc:  # noqa: BLE001
                if is_tushare_minute_rate_limit_error(exc) and minute_retries < max_minute_retries:
                    minute_retries += 1
                    logger.warning(
                        "tushare %s %s 命中分钟限流，睡到下一分钟重试 (%s/%s): %s",
                        method,
                        ts_code,
                        minute_retries,
                        max_minute_retries,
                        exc,
                    )
                    sleep_until_next_minute_wall_clock()
                    continue
                if attempt + 1 >= max_attempts:
                    logger.warning("tushare %s %s failed: %s", method, ts_code, exc)
                    raise
                delay = self._retry_backoff * (2**attempt)
                time.sleep(delay)
                attempt += 1
        raise RuntimeError("tushare _call_df 逻辑应不可达")  # pragma: no cover
