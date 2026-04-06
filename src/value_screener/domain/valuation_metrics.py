from __future__ import annotations


def compute_pe_ttm(market_cap: float, net_income_ttm: float | None) -> float | None:
    """
    市盈率（TTM）= 总市值 / 归属净利润 TTM。
    净利润缺失或非正时不定义 PE，返回 None。
    """

    if net_income_ttm is None or net_income_ttm <= 0:
        return None
    if market_cap <= 0:
        return None
    return market_cap / net_income_ttm
