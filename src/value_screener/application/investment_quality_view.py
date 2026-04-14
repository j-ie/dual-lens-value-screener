from __future__ import annotations

from typing import Any

from value_screener.domain.dcf_sector_policy import resolve_dcf_sector_kind
from value_screener.domain.investment_quality import (
    CompanyFinancials,
    InvestmentQualityAnalyzer,
    resolve_worth_buy_decision,
)
from value_screener.domain.snapshot import StockFinancialSnapshot


def _resolve_market_cap_for_snapshot(
    row: dict[str, Any],
    run_fact: dict[str, Any],
    prov: Any,
) -> float | None:
    """run_fact → provenance → screening 行 market_cap，必须为正数。"""

    for src in (run_fact.get("market_cap"), (prov or {}).get("market_cap") if isinstance(prov, dict) else None, row.get("market_cap")):
        if src is None:
            continue
        try:
            x = float(src)
        except (TypeError, ValueError):
            continue
        if x > 0:
            return x
    return None


def build_investment_quality_from_snapshot(
    analyzer: InvestmentQualityAnalyzer,
    snap: StockFinancialSnapshot,
    *,
    industry: str | None = None,
    ts_code: str | None = None,
) -> dict[str, Any]:
    pe: float | None = None
    pb: float | None = None
    roe_pct: float | None = None
    net_margin_pct: float | None = None
    asset_liability_ratio_pct: float | None = None

    if snap.net_income_ttm is not None and snap.net_income_ttm > 0:
        pe = float(snap.market_cap) / float(snap.net_income_ttm)
    if snap.total_equity is not None and snap.total_equity > 0:
        pb = float(snap.market_cap) / float(snap.total_equity)
    if (
        snap.net_income_ttm is not None
        and snap.total_equity is not None
        and snap.total_equity > 0
    ):
        roe_pct = (float(snap.net_income_ttm) / float(snap.total_equity)) * 100.0
    if snap.net_income_ttm is not None and snap.revenue_ttm is not None and snap.revenue_ttm > 0:
        net_margin_pct = (float(snap.net_income_ttm) / float(snap.revenue_ttm)) * 100.0
    if (
        snap.total_liabilities is not None
        and snap.total_equity is not None
        and (float(snap.total_liabilities) + float(snap.total_equity)) > 0
    ):
        asset_liability_ratio_pct = (
            float(snap.total_liabilities)
            / (float(snap.total_liabilities) + float(snap.total_equity))
            * 100.0
        )

    result = analyzer.analyze(
        CompanyFinancials(
            name=snap.symbol,
            sector_kind=resolve_dcf_sector_kind(industry, ts_code=ts_code),
            revenue=(float(snap.revenue_ttm),) if snap.revenue_ttm is not None else (),
            net_profit=(float(snap.net_income_ttm),) if snap.net_income_ttm is not None else (),
            non_recurring_net_profit=(float(snap.net_income_ttm),) if snap.net_income_ttm is not None else (),
            net_margin=(float(net_margin_pct),) if net_margin_pct is not None else (),
            operating_cashflow=(
                (float(snap.operating_cash_flow_ttm),) if snap.operating_cash_flow_ttm is not None else ()
            ),
            cash=(float(snap.total_current_assets),) if snap.total_current_assets is not None else (),
            short_debt=(
                (float(snap.total_current_liabilities),)
                if snap.total_current_liabilities is not None
                else ()
            ),
            interest_bearing_debt=(
                (float(snap.interest_bearing_debt),) if snap.interest_bearing_debt is not None else ()
            ),
            net_assets=(float(snap.total_equity),) if snap.total_equity is not None else (),
            asset_liability_ratio=(
                (float(asset_liability_ratio_pct),) if asset_liability_ratio_pct is not None else ()
            ),
            roe=(float(roe_pct),) if roe_pct is not None else (),
            pe=pe,
            pb=pb,
        )
    )
    worth_buy = resolve_worth_buy_decision(result)
    return {
        "total_score": result.total_score,
        "module_scores": result.module_scores,
        "decision": result.decision.value,
        "decision_label_zh": result.decision_label_zh,
        "is_undervalued": result.is_undervalued,
        "is_worth_buy": worth_buy.is_worth_buy,
        "worth_buy_label_zh": worth_buy.label_zh,
        "worth_buy_reason_codes": list(worth_buy.reason_codes),
        "reasons": list(result.reasons),
        "risk_flags": [
            {"code": f.code, "severity": f.severity, "message": f.message} for f in result.risk_flags
        ],
        "metadata": result.metadata,
    }


def attach_investment_quality_for_result_row(
    analyzer: InvestmentQualityAnalyzer,
    row: dict[str, Any],
) -> dict[str, Any]:
    run_fact = row.get("run_fact_json")
    prov = row.get("provenance")
    industry = row.get("industry")
    if not isinstance(run_fact, dict):
        return row
    mcap = _resolve_market_cap_for_snapshot(row, run_fact, prov)
    if mcap is None:
        return row
    snap = StockFinancialSnapshot.model_validate(
        {
            "symbol": row.get("symbol"),
            "market_cap": mcap,
            "total_current_assets": run_fact.get("total_current_assets"),
            "total_current_liabilities": run_fact.get("total_current_liabilities"),
            "total_liabilities": run_fact.get("total_liabilities"),
            "total_equity": run_fact.get("total_equity"),
            "net_income_ttm": run_fact.get("net_income_ttm"),
            "operating_cash_flow_ttm": run_fact.get("operating_cash_flow_ttm"),
            "revenue_ttm": run_fact.get("revenue_ttm"),
            "interest_bearing_debt": run_fact.get("interest_bearing_debt"),
            "data_source": (prov or {}).get("data_source") if isinstance(prov, dict) else None,
            "trade_cal_date": (prov or {}).get("trade_cal_date") if isinstance(prov, dict) else None,
            "financials_end_date": (prov or {}).get("financials_end_date") if isinstance(prov, dict) else None,
            "dv_ratio": run_fact.get("dv_ratio"),
            "dv_ttm": run_fact.get("dv_ttm"),
        }
    )
    out = dict(row)
    sym = row.get("symbol")
    ts_c = str(sym).strip() if sym else None
    out["investment_quality"] = build_investment_quality_from_snapshot(
        analyzer, snap, industry=industry, ts_code=ts_c
    )
    return out

