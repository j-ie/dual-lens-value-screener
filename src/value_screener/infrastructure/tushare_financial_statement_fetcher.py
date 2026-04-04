from __future__ import annotations

import logging
import threading
import time
from typing import Any

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

    def _call_df(self, method: str, ts_code: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
        # 仅传 ts_code + 日期窗，不传 report_type 等过滤，避免缩小年报/半年报/季报覆盖。
        attempts = 1 + self._max_retries
        last: BaseException | None = None
        for attempt in range(attempts):
            try:
                time.sleep(self._sleep)
                fn = getattr(self._api(), method)
                df = fn(ts_code=ts_code, start_date=start_date, end_date=end_date)
                if df is None or df.empty:
                    return []
                return df.to_dict(orient="records")
            except Exception as exc:  # noqa: BLE001
                last = exc
                if attempt + 1 >= attempts:
                    logger.warning("tushare %s %s failed: %s", method, ts_code, exc)
                    raise
                delay = self._retry_backoff * (2**attempt)
                time.sleep(delay)
        raise last  # pragma: no cover
