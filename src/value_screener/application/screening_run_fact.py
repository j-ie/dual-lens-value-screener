from __future__ import annotations

from typing import Any

from value_screener.domain.snapshot import StockFinancialSnapshot
from value_screener.domain.valuation_metrics import compute_pe_ttm


def build_hybrid_persist_fields(snap: StockFinancialSnapshot) -> dict[str, Any]:
    """
    从批跑用快照生成 screening_result 混合存储字段（JSON + 列），
    与 provenance_json.market_cap 同源。
    """

    pe = compute_pe_ttm(float(snap.market_cap), snap.net_income_ttm)
    run_fact = snap.model_dump(mode="json")
    return {
        "run_fact_json": run_fact,
        "market_cap": float(snap.market_cap),
        "pe_ttm": float(pe) if pe is not None else None,
    }
