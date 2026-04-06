from __future__ import annotations

import math
import threading
import time
from datetime import datetime
from typing import Any, Literal, TypeVar

SortField = Literal["dv_ratio", "dv_ttm"]


class TushareMarketDividendYieldFetcher:
    """
    TuShare `daily_basic` 按交易日批量拉取全 A 股息率相关字段。

    文档字段：dv_ratio 股息率（%），dv_ttm 股息率（TTM）（%）。
    需有效 TUSHARE_TOKEN；接口积分要求与 daily_basic 一致。
    """

    _FIELDS = "ts_code,trade_date,close,dv_ratio,dv_ttm"

    def __init__(
        self,
        token: str,
        *,
        request_sleep_seconds: float = 0.12,
    ) -> None:
        if not token or not str(token).strip():
            raise ValueError("TuShare token 不能为空")
        self._token = str(token).strip()
        self._sleep = max(0.0, float(request_sleep_seconds))
        self._local = threading.local()

    def _api(self) -> Any:
        pro = getattr(self._local, "pro", None)
        if pro is None:
            import tushare as ts

            ts.set_token(self._token)
            self._local.pro = ts.pro_api()
            pro = self._local.pro
        return pro

    def resolve_latest_sse_trade_date(self) -> str:
        """取上交所日历最近一个开市日 YYYYMMDD（与现有批跑口径一致）。"""

        end = datetime.now().strftime("%Y%m%d")
        cal = self._api().trade_cal(exchange="SSE", start_date="20200101", end_date=end, is_open="1")
        if cal is None or cal.empty:
            raise RuntimeError("trade_cal 为空，无法确定最近交易日")
        is_open = cal["is_open"]
        open_mask = (is_open == 1) | (is_open == "1")
        open_days = cal.loc[open_mask, "cal_date"].astype(str).tolist()
        if not open_days:
            raise RuntimeError("无开市日")
        return max(open_days)

    def fetch_all_rows(self, trade_date: str) -> list[dict[str, Any]]:
        """
        拉取指定交易日全市场 daily_basic 行（仅含股息率相关列）。

        :param trade_date: YYYYMMDD
        :return: 每条含 ts_code, trade_date, close, dv_ratio, dv_ttm（数值或 None）
        """

        td = str(trade_date).strip()
        if len(td) != 8 or not td.isdigit():
            raise ValueError("trade_date 须为 YYYYMMDD")

        time.sleep(self._sleep)
        df = self._api().daily_basic(trade_date=td, fields=self._FIELDS)
        if df is None or df.empty:
            raise RuntimeError(f"daily_basic 在 {td} 无数据")

        out: list[dict[str, Any]] = []
        for _, row in df.iterrows():
            code = str(row.get("ts_code", "") or "").strip()
            if not code:
                continue
            out.append(
                {
                    "ts_code": code,
                    "trade_date": str(row.get("trade_date", "") or td),
                    "close": _optional_float(row.get("close")),
                    "dv_ratio": _optional_float(row.get("dv_ratio")),
                    "dv_ttm": _optional_float(row.get("dv_ttm")),
                }
            )
        return out


def _optional_float(raw: Any) -> float | None:
    if raw is None:
        return None
    try:
        x = float(raw)
    except (TypeError, ValueError):
        return None
    if math.isnan(x) or math.isinf(x):
        return None
    return x


def sort_dividend_rows(
    rows: list[dict[str, Any]],
    *,
    sort: SortField,
    order: Literal["asc", "desc"],
) -> list[dict[str, Any]]:
    """按 dv_ratio 或 dv_ttm 排序；缺失值排在末尾。"""

    key_name = "dv_ratio" if sort == "dv_ratio" else "dv_ttm"
    with_val = [r for r in rows if r.get(key_name) is not None]
    without = [r for r in rows if r.get(key_name) is None]
    reverse = order == "desc"
    with_val.sort(key=lambda r: float(r[key_name]), reverse=reverse)
    return with_val + without


_T = TypeVar("_T")


def page_slice(items: list[_T], page: int, page_size: int) -> tuple[list[_T], int]:
    """返回 (当前页切片, total)。"""

    p = max(1, int(page))
    ps = max(1, min(int(page_size), 10_000))
    total = len(items)
    start = (p - 1) * ps
    return items[start : start + ps], total
