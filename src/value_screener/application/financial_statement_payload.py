from __future__ import annotations

import math
from typing import Any


def _is_nan_like(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, float) and math.isnan(v):
        return True
    try:
        import pandas as pd

        return bool(pd.isna(v))
    except Exception:
        return False


def to_float_or_none(v: Any) -> float | None:
    if _is_nan_like(v):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def sanitize_financial_row(row: dict[str, Any]) -> dict[str, Any]:
    """TuShare/pandas 行 → 可 JSON 序列化 dict（NaN→None）。"""

    out: dict[str, Any] = {}
    for k, v in row.items():
        if hasattr(v, "item"):
            try:
                v = v.item()
            except Exception:
                v = str(v)
        if _is_nan_like(v):
            out[str(k)] = None
        elif isinstance(v, (str, int, float, bool)) or v is None:
            out[str(k)] = v
        else:
            out[str(k)] = str(v)
    return out


def income_scalars(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "total_revenue": to_float_or_none(row.get("total_revenue")),
        "operate_profit": to_float_or_none(row.get("operate_profit")),
        "total_profit": to_float_or_none(row.get("total_profit")),
        "n_income": to_float_or_none(row.get("n_income")),
        "n_income_attr_p": to_float_or_none(row.get("n_income_attr_p")),
        "income_tax": to_float_or_none(row.get("income_tax")),
        "basic_eps": to_float_or_none(row.get("basic_eps")),
        "diluted_eps": to_float_or_none(row.get("diluted_eps")),
    }


def balance_scalars(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "total_assets": to_float_or_none(row.get("total_assets")),
        "total_liab": to_float_or_none(row.get("total_liab")),
        "total_cur_assets": to_float_or_none(row.get("total_cur_assets")),
        "total_cur_liab": to_float_or_none(row.get("total_cur_liab")),
        "money_cap": to_float_or_none(row.get("money_cap")),
        "inventories": to_float_or_none(row.get("inventories")),
        "total_hldr_eqy_exc_min_int": to_float_or_none(row.get("total_hldr_eqy_exc_min_int")),
    }


def cashflow_scalars(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "n_cashflow_act": to_float_or_none(row.get("n_cashflow_act")),
        "n_cash_flows_inv_act": to_float_or_none(row.get("n_cash_flows_inv_act")),
        "n_cash_flows_fnc_act": to_float_or_none(row.get("n_cash_flows_fnc_act")),
        "c_cash_equ_end_period": to_float_or_none(row.get("c_cash_equ_end_period")),
    }
