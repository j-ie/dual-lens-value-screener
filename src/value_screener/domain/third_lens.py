from __future__ import annotations

from typing import Any


def industry_bucket(raw: str | None) -> str:
    """与结果行业筛选一致：空参考行业归入空字符串桶。"""

    return (raw or "").strip()


def percentile_rank_0_100(sample: list[float], x: float) -> float:
    """
    样本内 x 的百分位（0–100），中位秩；样本为空时返回 50。
    """

    if not sample:
        return 50.0
    n = len(sample)
    below = sum(1 for t in sample if t < x)
    equal = sum(1 for t in sample if t == x)
    return 100.0 * (below + equal * 0.5) / n


def revenue_yoy_from_two_annual(rev_new: float, rev_old: float) -> float | None:
    """最近两年年报营收同比增速；旧期营收须为正。"""

    if rev_old <= 0:
        return None
    return (rev_new - rev_old) / rev_old


def earnings_yield_ratio(net_income: float, market_cap: float) -> float | None:
    """E/P = 净利润/市值；须均为正。"""

    if market_cap <= 0 or net_income <= 0:
        return None
    return net_income / market_cap


def combine_third_lens_subscores(
    growth_score: float | None,
    valuation_score: float | None,
    wg: float,
    wv: float,
) -> tuple[float | None, dict[str, Any]]:
    """
    合成第三套总分与子权重说明。
    仅一侧有值时该侧权重视为 1；皆无则 (None, meta)。
    """

    meta: dict[str, Any] = {}
    if growth_score is None and valuation_score is None:
        meta["third_lens_omit_reason"] = "no_growth_and_no_valuation"
        return None, meta
    if growth_score is None:
        meta["third_lens_valuation_only"] = True
        return round(float(valuation_score), 4), meta
    if valuation_score is None:
        meta["third_lens_growth_only"] = True
        return round(float(growth_score), 4), meta
    t = wg * growth_score + wv * valuation_score
    meta["third_lens_growth_weight"] = wg
    meta["third_lens_valuation_weight"] = wv
    return round(t, 4), meta


def final_triple_linear(
    buffett: float,
    graham: float,
    third: float | None,
    wb: float,
    wg: float,
    wt: float,
) -> tuple[float, dict[str, Any]]:
    """
    三元线性综合；third 为空时对 wb/wg 重归一。
    """

    detail: dict[str, Any] = {}
    if third is None:
        s2 = wb + wg
        if s2 <= 0:
            raise ValueError("buffett+graham 权重和须为正")
        wb2, wg2 = wb / s2, wg / s2
        detail["triple_renormalized"] = True
        detail["effective_weight_buffett"] = wb2
        detail["effective_weight_graham"] = wg2
        detail["effective_weight_third"] = 0.0
        return round(wb2 * buffett + wg2 * graham, 4), detail
    detail["triple_renormalized"] = False
    detail["effective_weight_buffett"] = wb
    detail["effective_weight_graham"] = wg
    detail["effective_weight_third"] = wt
    return round(wb * buffett + wg * graham + wt * third, 4), detail
