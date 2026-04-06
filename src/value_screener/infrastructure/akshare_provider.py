from __future__ import annotations

import logging
import os
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pandas as pd

from value_screener.domain.snapshot import StockFinancialSnapshot
from value_screener.infrastructure.fetch_types import SymbolFetchFailure
from value_screener.infrastructure.provider_protocol import FetchSnapshotProgressCallback
from value_screener.infrastructure.symbol_normalize import to_ak_symbol, to_ts_code

logger = logging.getLogger(__name__)

_PROXY_ENV_KEYS = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
)


@contextmanager
def _without_env_proxy_for_akshare() -> Iterator[None]:
    """
    AkShare 走 requests，会继承系统/终端里的代理环境变量。
    常见情况是本机配置了失效代理，直连东财反而可用；此时临时去掉代理键。
    若必须走系统代理，设置 VALUE_SCREENER_AKSHARE_USE_SYSTEM_PROXY=1。
    """

    flag = os.environ.get("VALUE_SCREENER_AKSHARE_USE_SYSTEM_PROXY", "").strip().lower()
    if flag in ("1", "true", "yes", "on"):
        yield
        return
    saved: dict[str, str] = {}
    for key in _PROXY_ENV_KEYS:
        if key in os.environ:
            saved[key] = os.environ.pop(key)
    try:
        yield
    finally:
        for key, value in saved.items():
            os.environ[key] = value


class AkShareAShareProvider:
    """AkShare 实现：A 股列表 + 东财行情市值 + 三大报表（报告期）滚动 TTM 近似。"""

    backend_name = "akshare"

    def __init__(self, request_sleep_seconds: float = 0.12) -> None:
        import akshare as ak

        self._ak = ak
        self._sleep = request_sleep_seconds

    def list_universe(self) -> list[str]:
        with _without_env_proxy_for_akshare():
            df = self._ak.stock_info_a_code_name()
        if df is None or df.empty:
            return []
        code_col = _first_col(df, ("code", "代码"))
        if code_col is None:
            return []
        codes = df[code_col].astype(str).str.replace(".0", "", regex=False).str.zfill(6)
        return [to_ts_code(c) for c in codes]

    def fetch_snapshots(
        self,
        symbols: list[str],
        *,
        on_progress: FetchSnapshotProgressCallback | None = None,
    ) -> list[StockFinancialSnapshot | SymbolFetchFailure]:
        ts_codes = [to_ts_code(s) for s in symbols]
        with _without_env_proxy_for_akshare():
            time.sleep(self._sleep)
            spot = self._ak.stock_zh_a_spot_em()
            mcap_map, dv_map, trade_hint = _spot_market_maps(spot)
            out: list[StockFinancialSnapshot | SymbolFetchFailure] = []
            total = len(ts_codes)
            for idx, ts_code in enumerate(ts_codes, start=1):
                if on_progress is not None:
                    on_progress(idx, total, ts_code)
                try:
                    time.sleep(self._sleep)
                    snap = self._fetch_one(ts_code, mcap_map, dv_map, trade_hint)
                    if snap is None:
                        out.append(
                            SymbolFetchFailure(
                                symbol=ts_code,
                                reason="mcap_or_balance_sheet_missing",
                                source=self.backend_name,
                            )
                        )
                    else:
                        out.append(snap)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("akshare fetch %s: %s", ts_code, exc)
                    out.append(
                        SymbolFetchFailure(
                            symbol=ts_code,
                            reason=str(exc)[:500],
                            source=self.backend_name,
                        )
                    )
        return out

    def _fetch_one(
        self,
        ts_code: str,
        mcap_map: dict[str, float],
        dv_map: dict[str, float | None],
        trade_hint: str | None,
    ) -> StockFinancialSnapshot | None:
        market_cap = mcap_map.get(ts_code)
        if market_cap is None or market_cap <= 0:
            return None
        dv_row = dv_map.get(ts_code)
        sym = to_ak_symbol(ts_code)
        bs = self._ak.stock_balance_sheet_by_report_em(symbol=sym)
        row_bs, fin_end = _latest_report_row(bs)
        if row_bs is None:
            return None

        tca = _cell_numeric(row_bs, ("流动资产合计",))
        tcl = _cell_numeric(row_bs, ("流动负债合计",))
        tl = _cell_numeric(row_bs, ("负债合计",))
        equity = None
        for keys in (
            ("归属于母公司所有者权益", "合计"),
            ("归属于母公司股东权益", "合计"),
            ("股东权益", "合计"),
        ):
            equity = _row_float_positive(row_bs, keys)
            if equity is not None:
                break

        inc = self._ak.stock_profit_sheet_by_report_em(symbol=sym)
        cf = self._ak.stock_cash_flow_sheet_by_report_em(symbol=sym)
        ni_ttm = _ttm_sum(inc, ("净利润", "归属于母公司所有者的净利润"))
        rev_ttm = _ttm_sum(inc, ("营业总收入", "营业收入"))
        ocf_ttm = _ttm_sum(cf, ("经营活动产生的现金流量净额",))

        ibd = _cell_numeric(row_bs, ("短期借款",))
        lt = _cell_numeric(row_bs, ("长期借款",))
        if ibd is not None or lt is not None:
            ibd = (ibd or 0.0) + (lt or 0.0)
            if ibd <= 0:
                ibd = None
        else:
            ibd = None

        return StockFinancialSnapshot(
            symbol=ts_code,
            market_cap=float(market_cap),
            total_current_assets=tca,
            total_current_liabilities=tcl,
            total_liabilities=tl,
            total_equity=equity,
            net_income_ttm=ni_ttm,
            operating_cash_flow_ttm=ocf_ttm,
            revenue_ttm=rev_ttm,
            interest_bearing_debt=ibd,
            data_source=self.backend_name,
            trade_cal_date=trade_hint,
            financials_end_date=fin_end,
            dv_ttm=dv_row,
            dv_ratio=None,
        )


def _first_col(df: pd.DataFrame, candidates: tuple[str, ...]) -> str | None:
    for c in df.columns:
        sc = str(c).strip()
        if sc in candidates:
            return sc
    return None


def em_spot_dividend_yield_percent_map() -> dict[str, float | None]:
    """
    东财 A 股现货全表：股息率（%%）→ ts_code。
    TuShare daily_basic 的 dv_ratio/dv_ttm 对大量标的长期为空，用作补全数据源。
    未安装 akshare 或拉取失败时返回空 dict。
    """

    try:
        import akshare as ak
    except ImportError:
        logger.info("未安装 akshare，跳过东财股息率补全（TuShare 路径仍可拉数）")
        return {}
    try:
        with _without_env_proxy_for_akshare():
            spot = ak.stock_zh_a_spot_em()
    except Exception as exc:  # noqa: BLE001
        logger.warning("东财现货股息率补全失败（忽略，仅用 TuShare 股息字段）: %s", exc)
        return {}
    if spot is None or spot.empty:
        return {}
    _, dv_map, _ = _spot_market_maps(spot)
    return dv_map


def _spot_market_maps(
    spot: pd.DataFrame,
) -> tuple[dict[str, float], dict[str, float | None], str | None]:
    code_col = _first_col(spot, ("代码",)) or next(
        (c for c in spot.columns if "代码" in str(c)),
        None,
    )
    mcol = next((c for c in spot.columns if "总市值" in str(c)), None)
    dv_col = next(
        (
            c
            for c in spot.columns
            if "股息率" in str(c) or str(c).strip() in ("股息率", "股息率%", "股息率％")
        ),
        None,
    )
    if code_col is None or mcol is None:
        return {}, {}, None
    out: dict[str, float] = {}
    dv_out: dict[str, float | None] = {}
    for _, row in spot.iterrows():
        raw = str(row[code_col]).replace(".0", "").strip()
        digits = "".join(ch for ch in raw if ch.isdigit())
        if len(digits) < 6:
            continue
        ts = to_ts_code(digits[:6])
        v = pd.to_numeric(row[mcol], errors="coerce")
        if pd.isna(v) or float(v) <= 0:
            continue
        out[ts] = float(v)
        if dv_col is not None:
            d = pd.to_numeric(row[dv_col], errors="coerce")
            if pd.isna(d) or float(d) < 0:
                dv_out[ts] = None
            else:
                dv_out[ts] = float(d)
    return out, dv_out, None


def _latest_report_row(df: pd.DataFrame | None) -> tuple[dict[str, Any] | None, str | None]:
    if df is None or df.empty:
        return None, None
    pcol = next((c for c in df.columns if "报告期" in str(c)), None)
    work = df.copy()
    if pcol:
        work = work.sort_values(pcol, ascending=False)
    row = work.iloc[0]
    end = str(row[pcol]) if pcol else None
    return row.to_dict(), end


def _cell_numeric(row: dict[str, Any], candidates: tuple[str, ...]) -> float | None:
    for cand in candidates:
        cn = cand.replace(" ", "").strip()
        for k, v in row.items():
            if str(k).replace(" ", "").strip() != cn:
                continue
            try:
                x = float(pd.to_numeric(v, errors="coerce"))
            except (TypeError, ValueError):
                continue
            if pd.isna(x):
                continue
            if x < 0:
                return None
            return x
    return None


def _row_float(row: dict[str, Any], keywords: tuple[str, ...]) -> float | None:
    for k, v in row.items():
        ks = str(k).replace(" ", "")
        if all(part in ks for part in keywords):
            try:
                x = float(pd.to_numeric(v, errors="coerce"))
            except (TypeError, ValueError):
                continue
            if pd.isna(x):
                continue
            if x < 0:
                return None
            return x
    return None


def _row_float_positive(row: dict[str, Any], keywords: tuple[str, ...]) -> float | None:
    x = _row_float(row, keywords)
    if x is None or x <= 0:
        return None
    return x


def _ttm_sum(df: pd.DataFrame | None, exact_names: tuple[str, ...]) -> float | None:
    if df is None or df.empty:
        return None
    pcol = next((c for c in df.columns if "报告期" in str(c)), None)
    vcol = None
    for name in exact_names:
        target = name.replace(" ", "")
        for c in df.columns:
            if str(c).replace(" ", "") == target:
                vcol = c
                break
        if vcol is not None:
            break
    if vcol is None:
        return None
    work = df[[pcol, vcol]].copy() if pcol else df[[vcol]].copy()
    if pcol:
        work = work.sort_values(pcol, ascending=False)
    series = pd.to_numeric(work[vcol], errors="coerce").dropna().head(4)
    if series.empty:
        return None
    return float(series.sum())
