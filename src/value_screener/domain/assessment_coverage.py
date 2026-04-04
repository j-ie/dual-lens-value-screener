from __future__ import annotations

from typing import Any


def dual_lens_coverage_ok(graham: dict[str, Any], buffett: dict[str, Any]) -> bool:
    """
    与评估器可用输入一致：两维均至少有一项核心输入，否则视为信息不足以参与综合视图。
    """

    b_ok = any(buffett.get(k) is not None for k in ("roe", "debt_to_equity", "ocf_to_net_income"))
    g_ok = any(
        graham.get(k) is not None for k in ("market_cap_to_ncav", "current_ratio", "price_to_book")
    )
    return bool(b_ok and g_ok)


def combined_linear_score(
    buffett_score: float,
    graham_score: float,
    *,
    weight_buffett: float,
    weight_graham: float,
) -> float:
    return round(weight_buffett * float(buffett_score) + weight_graham * float(graham_score), 4)
