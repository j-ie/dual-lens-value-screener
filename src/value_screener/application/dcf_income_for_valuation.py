"""
DCF 用利润表代理：最近一期年报归母净利润（与现金流量表年报择期逻辑对齐的简化版）。
"""

from __future__ import annotations

from datetime import date
from typing import Any

from value_screener.application.dcf_cashflow_aggregate import merge_statement_rows_by_end_date
from value_screener.application.financial_statement_payload import to_float_or_none


def _days_since_end_date(as_of: date, end_date_str: str) -> int | None:
    if len(end_date_str) != 8 or not end_date_str.isdigit():
        return None
    try:
        ed = date(int(end_date_str[:4]), int(end_date_str[4:6]), int(end_date_str[6:8]))
    except ValueError:
        return None
    return (as_of - ed).days


def pick_latest_annual_income_row(
    income_rows: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, str | None]:
    """
    选取最近一期年报（1231）合并行。

    返回 (row, end_date)；无年报则 (None, None)。
    """

    merged = merge_statement_rows_by_end_date([dict(x) for x in income_rows])
    candidates = [ed for ed in merged if len(ed) == 8 and ed.endswith("1231")]
    if not candidates:
        return None, None
    best_ed = max(candidates)
    row = dict(merged[best_ed])
    row["end_date"] = best_ed
    return row, best_ed


def latest_annual_n_income_attr_p(
    income_rows: list[dict[str, Any]],
    *,
    as_of: date | None = None,
    annual_stale_days: int = 550,
) -> tuple[float | None, str | None, list[str]]:
    """
    最近一期年报「归属于母公司股东的净利润」作金融业等场景的现金流代理基数。

    返回 (金额, 年报 end_date, warnings)。
    """

    warnings: list[str] = []
    ref = as_of or date.today()
    stale_days = max(30, int(annual_stale_days))

    if not income_rows:
        return None, None, ["缺少利润表数据"]

    row, ed = pick_latest_annual_income_row(income_rows)
    if row is None or ed is None:
        return None, None, ["利润表无年报（1231）报告期，无法取归母净利润"]

    ni = to_float_or_none(row.get("n_income_attr_p"))
    if ni is None:
        return None, ed, [f"年报 {ed} 缺少 n_income_attr_p（归母净利润）"]

    age = _days_since_end_date(ref, ed)
    if age is not None and age > stale_days:
        warnings.append(
            f"归母净利润取自年报 {ed}，距今 {age} 天，已超过 {stale_days} 天陈旧阈值，"
            "建议补全近期财报"
        )

    warnings.insert(0, f"基期现金流采用最近年报（{ed}）归母净利润 n_income_attr_p（金融业代理口径）")
    return float(ni), ed, warnings
