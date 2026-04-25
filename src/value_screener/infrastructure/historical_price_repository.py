from __future__ import annotations

from datetime import datetime
from typing import Any


class HistoricalPriceRepository:
    """历史行情读取：基于 TuShare trade_cal + daily_basic（trade_date 口径）。"""

    def __init__(self, token: str) -> None:
        if not token or not str(token).strip():
            raise ValueError("TuShare token 不能为空")
        self._token = str(token).strip()
        self._pro: Any | None = None

    def _api(self) -> Any:
        if self._pro is None:
            import tushare as ts

            ts.set_token(self._token)
            self._pro = ts.pro_api()
        return self._pro

    @staticmethod
    def _normalize_date(s: str) -> str:
        return str(s).replace("-", "").strip()

    def list_trade_dates(self, start_date: str, end_date: str) -> list[str]:
        st = self._normalize_date(start_date)
        ed = self._normalize_date(end_date)
        cal = self._api().trade_cal(exchange="SSE", start_date=st, end_date=ed, is_open="1")
        if cal is None or cal.empty:
            return []
        out = sorted({str(x) for x in cal["cal_date"].astype(str).tolist() if str(x).strip()})
        return out

    def fetch_market_caps(self, trade_date: str, symbols: list[str] | None = None) -> dict[str, float]:
        td = self._normalize_date(trade_date)
        df = self._api().daily_basic(trade_date=td, fields="ts_code,trade_date,total_mv")
        if df is None or df.empty:
            return {}
        filt = set(symbols or [])
        out: dict[str, float] = {}
        for _, row in df.iterrows():
            ts_code = str(row.get("ts_code") or "").strip()
            if not ts_code:
                continue
            if filt and ts_code not in filt:
                continue
            try:
                total_mv_wanyuan = float(row.get("total_mv"))
            except (TypeError, ValueError):
                continue
            if total_mv_wanyuan <= 0:
                continue
            out[ts_code] = total_mv_wanyuan * 10_000.0
        return out

    def coverage_bounds(self) -> tuple[str | None, str | None]:
        today = datetime.now().strftime("%Y%m%d")
        trade_dates = self.list_trade_dates("20000101", today)
        if not trade_dates:
            return None, None
        return trade_dates[0], trade_dates[-1]

