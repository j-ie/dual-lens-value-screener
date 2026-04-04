from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any


class TushareLiveQuoteFetcher:
    """TuShare `daily` 最近一根 K 线，供详情页行情区块（与批跑快照时刻无关）。"""

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

    def fetch_daily_last_bar(self, ts_code: str) -> dict[str, Any]:
        """
        返回最近一条日 K 关键字段；失败抛异常（由调用方捕获）。
        字段：trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount
        """

        code = str(ts_code).strip()
        end = datetime.now(timezone.utc).strftime("%Y%m%d")
        start = (datetime.now(timezone.utc) - timedelta(days=120)).strftime("%Y%m%d")

        def _call() -> Any:
            time.sleep(self._sleep)
            return self._api().daily(ts_code=code, start_date=start, end_date=end)

        with ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(_call)
            try:
                df = fut.result(timeout=self._timeout)
            except FuturesTimeoutError as exc:
                raise TimeoutError(f"daily 超时（>{self._timeout}s）") from exc

        if df is None or df.empty:
            raise RuntimeError("daily 无数据")

        work = df.copy()
        if "trade_date" in work.columns:
            work = work.sort_values("trade_date", ascending=False)
        row = work.iloc[0]
        out: dict[str, Any] = {"ts_code": code}
        for col in (
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "pre_close",
            "change",
            "pct_chg",
            "vol",
            "amount",
        ):
            if col not in work.columns:
                continue
            raw = row.get(col)
            out[col] = _to_json_scalar(raw)
        return out


def _to_json_scalar(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, Decimal):
        return float(v)
    if hasattr(v, "item"):
        try:
            return v.item()
        except (ValueError, TypeError):
            pass
    if isinstance(v, float):
        return v
    try:
        if isinstance(v, (int, str)):
            return v
    except TypeError:
        pass
    return str(v)
