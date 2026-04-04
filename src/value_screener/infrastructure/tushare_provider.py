from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

import pandas as pd

from value_screener.domain.snapshot import StockFinancialSnapshot
from value_screener.infrastructure.fetch_types import SymbolFetchFailure
from value_screener.infrastructure.provider_protocol import FetchSnapshotProgressCallback
from value_screener.infrastructure.symbol_normalize import to_ts_code

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class TushareAShareProvider:
    """TuShare 实现：全 A 列表 + 最新交易日 bulk 市值 + 逐标的财报 TTM 近似。"""

    backend_name = "tushare"

    def __init__(self, token: str, request_sleep_seconds: float = 0.12) -> None:
        if not token or not token.strip():
            raise ValueError("TuShare token 不能为空")
        import tushare as ts

        ts.set_token(token.strip())
        self._pro = ts.pro_api()
        self._sleep = request_sleep_seconds

    def list_universe(self) -> list[str]:
        df = self._pro.stock_basic(
            exchange="",
            list_status="L",
            fields="ts_code,name,market,list_date",
        )
        if df is None or df.empty:
            return []
        return df["ts_code"].astype(str).tolist()

    def fetch_snapshots(
        self,
        symbols: list[str],
        *,
        on_progress: FetchSnapshotProgressCallback | None = None,
    ) -> list[StockFinancialSnapshot | SymbolFetchFailure]:
        ts_codes = [to_ts_code(s) for s in symbols]
        trade_date = self._latest_open_trade_date()
        mv_wan = self._load_total_mv_map(trade_date)
        out: list[StockFinancialSnapshot | SymbolFetchFailure] = []
        total = len(ts_codes)
        for idx, ts_code in enumerate(ts_codes, start=1):
            if on_progress is not None:
                on_progress(idx, total, ts_code)
            try:
                time.sleep(self._sleep)
                snap = self._fetch_one(ts_code, trade_date, mv_wan)
                if snap is None:
                    out.append(
                        SymbolFetchFailure(
                            symbol=ts_code,
                            reason="balancesheet_or_mv_missing",
                            source=self.backend_name,
                        )
                    )
                else:
                    out.append(snap)
            except Exception as exc:  # noqa: BLE001 — 数据源异常按单票降级
                logger.warning("tushare fetch %s: %s", ts_code, exc)
                out.append(
                    SymbolFetchFailure(
                        symbol=ts_code,
                        reason=str(exc)[:500],
                        source=self.backend_name,
                    )
                )
        return out

    def _latest_open_trade_date(self) -> str:
        from datetime import datetime

        end = datetime.now().strftime("%Y%m%d")
        cal = self._pro.trade_cal(exchange="SSE", start_date="20200101", end_date=end, is_open="1")
        if cal is None or cal.empty:
            raise RuntimeError("trade_cal 为空，无法确定最近交易日")
        # TuShare trade_cal 常见为按 cal_date 降序返回，[-1] 会变成区间内最早开市日（如 20200102）。
        is_open = cal["is_open"]
        open_mask = (is_open == 1) | (is_open == "1")
        open_days = cal.loc[open_mask, "cal_date"].astype(str).tolist()
        if not open_days:
            raise RuntimeError("无开市日")
        return max(open_days)

    def _load_total_mv_map(self, trade_date: str) -> dict[str, float]:
        time.sleep(self._sleep)
        db = self._pro.daily_basic(trade_date=trade_date, fields="ts_code,total_mv")
        if db is None or db.empty:
            raise RuntimeError(f"daily_basic 在 {trade_date} 无数据")
        db = db.dropna(subset=["total_mv"])
        return dict(zip(db["ts_code"].astype(str), db["total_mv"].astype(float)))

    def _fetch_one(
        self,
        ts_code: str,
        trade_date: str,
        mv_wan: dict[str, float],
    ) -> StockFinancialSnapshot | None:
        mv_yuan = mv_wan.get(ts_code)
        if mv_yuan is None or mv_yuan <= 0:
            return None
        market_cap = float(mv_yuan) * 10_000.0

        bs = self._pro.balancesheet(
            ts_code=ts_code,
            fields=(
                "ts_code,end_date,ann_date,total_cur_assets,total_cur_liab,"
                "total_liab,total_hldr_eqy_exc_min_int,st_borrow,lt_borrow"
            ),
            limit=1,
        )
        if bs is None or bs.empty:
            return None
        b0 = bs.iloc[0]
        fin_end = str(b0.get("end_date", "") or "")

        inc = self._pro.income(ts_code=ts_code, fields="end_date,n_income_attr_p,revenue", limit=4)
        cf = self._pro.cashflow(ts_code=ts_code, fields="end_date,n_cash_flow_act", limit=4)
        ni_ttm = _sum_last_n(inc, "n_income_attr_p", 4)
        rev_ttm = _sum_last_n(inc, "revenue", 4)
        ocf_col = _pick_cashflow_col(cf)
        ocf_ttm = _sum_last_n(cf, ocf_col, 4) if ocf_col else None

        equity = _finite_positive(b0.get("total_hldr_eqy_exc_min_int"))
        tca = _finite_non_neg(b0.get("total_cur_assets"))
        tcl = _finite_non_neg(b0.get("total_cur_liab"))
        tl = _finite_non_neg(b0.get("total_liab"))
        stb = _finite_non_neg(b0.get("st_borrow"))
        ltb = _finite_non_neg(b0.get("lt_borrow"))
        ibd = None
        if stb is not None and ltb is not None:
            ibd = stb + ltb
        elif stb is not None:
            ibd = stb
        elif ltb is not None:
            ibd = ltb

        return StockFinancialSnapshot(
            symbol=ts_code,
            market_cap=market_cap,
            total_current_assets=tca,
            total_current_liabilities=tcl,
            total_liabilities=tl,
            total_equity=equity,
            net_income_ttm=ni_ttm,
            operating_cash_flow_ttm=ocf_ttm,
            revenue_ttm=rev_ttm,
            interest_bearing_debt=ibd,
            data_source=self.backend_name,
            trade_cal_date=trade_date,
            financials_end_date=fin_end or None,
        )


def _pick_cashflow_col(df: pd.DataFrame | None) -> str | None:
    if df is None or df.empty:
        return None
    for name in ("n_cash_flow_act", "n_cashflow_act"):
        if name in df.columns:
            return name
    return None


def _sum_last_n(df: pd.DataFrame | None, col: str, n: int) -> float | None:
    if df is None or df.empty or col not in df.columns:
        return None
    work = df.copy()
    if "end_date" in work.columns:
        work = work.sort_values("end_date", ascending=False)
    series = pd.to_numeric(work[col], errors="coerce").dropna().head(n)
    if series.empty:
        return None
    return float(series.sum())


def _finite_non_neg(v: object) -> float | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    if x < 0:
        return None
    return x


def _finite_positive(v: object) -> float | None:
    x = _finite_non_neg(v)
    if x is None or x <= 0:
        return None
    return x
