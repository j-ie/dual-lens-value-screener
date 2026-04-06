from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from datetime import datetime, timedelta, timezone
from typing import Any


class TushareDailyBasicFetcher:
    """TuShare `daily_basic` 最近一条，用于总股本（万股 → 股）。"""

    def __init__(
        self,
        token: str,
        *,
        request_sleep_seconds: float = 0.12,
        fetch_timeout_seconds: float = 12.0,
    ) -> None:
        if not token or not str(token).strip():
            raise ValueError("TuShare token 不能为空")
        self._token = str(token).strip()
        self._sleep = max(0.0, float(request_sleep_seconds))
        self._timeout = max(1.0, float(fetch_timeout_seconds))
        self._local = threading.local()

    def _api(self) -> Any:
        pro = getattr(self._local, "pro", None)
        if pro is None:
            import tushare as ts

            ts.set_token(self._token)
            self._local.pro = ts.pro_api()
            pro = self._local.pro
        return pro

    def fetch_latest_total_shares(self, ts_code: str) -> float:
        """
        返回总股数（单位：股）。TuShare `total_share` 为万股，乘以 10000。

        :raises RuntimeError: 无数据或缺列
        """

        code = str(ts_code).strip()
        end = datetime.now(timezone.utc).strftime("%Y%m%d")
        start = (datetime.now(timezone.utc) - timedelta(days=400)).strftime("%Y%m%d")

        def _call() -> Any:
            time.sleep(self._sleep)
            return self._api().daily_basic(
                ts_code=code,
                start_date=start,
                end_date=end,
                fields="ts_code,trade_date,total_share",
            )

        with ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(_call)
            try:
                df = fut.result(timeout=self._timeout)
            except FuturesTimeoutError as exc:
                raise TimeoutError(f"daily_basic 超时（>{self._timeout}s）") from exc

        if df is None or df.empty:
            raise RuntimeError("daily_basic 无数据")

        work = df.copy()
        if "trade_date" in work.columns:
            work = work.sort_values("trade_date", ascending=False)
        row = work.iloc[0]
        if "total_share" not in work.columns:
            raise RuntimeError("daily_basic 缺 total_share 列")
        raw = row["total_share"]
        try:
            wan = float(raw)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("total_share 非数值") from exc
        if wan <= 0:
            raise RuntimeError("total_share 非正")
        return wan * 10_000.0
