from __future__ import annotations

from typing import Any

from value_screener.domain.score_explanation import build_score_explanation_zh


def enrich_screening_result_row(row: dict[str, Any]) -> dict[str, Any]:
    """为分页行补充展示字段（JOIN 列 ref_* 由仓储注入）。"""

    prov = row.get("provenance")
    if not isinstance(prov, dict):
        prov = None

    display_name = (row.get("ref_name") or "").strip()
    company_full = (row.get("ref_fullname") or "").strip() or None
    industry = (row.get("ref_industry") or "").strip()
    region = (row.get("ref_area") or "").strip()

    trade_cal_date = prov.get("trade_cal_date") if prov else None
    financials_end_date = prov.get("financials_end_date") if prov else None
    data_source = prov.get("data_source") if prov else None
    if trade_cal_date is not None:
        trade_cal_date = str(trade_cal_date)
    if financials_end_date is not None:
        financials_end_date = str(financials_end_date)
    if data_source is not None:
        data_source = str(data_source)

    graham = row["graham"]
    buffett = row["buffett"]
    explanation = build_score_explanation_zh(graham, buffett, prov)

    mcap_row = row.get("market_cap")
    if mcap_row is not None:
        try:
            market_cap_val = float(mcap_row)
        except (TypeError, ValueError):
            market_cap_val = None
    elif prov and prov.get("market_cap") is not None:
        try:
            market_cap_val = float(prov["market_cap"])
        except (TypeError, ValueError):
            market_cap_val = None
    else:
        market_cap_val = None

    pe_raw = row.get("pe_ttm")
    if pe_raw is not None:
        try:
            pe_ttm_val = float(pe_raw)
        except (TypeError, ValueError):
            pe_ttm_val = None
    else:
        pe_ttm_val = None

    net_income_ttm_val: float | None = None
    dv_ratio_val: float | None = None
    dv_ttm_val: float | None = None
    rf = row.get("run_fact_json")
    if isinstance(rf, dict):
        ni = rf.get("net_income_ttm")
        if ni is not None:
            try:
                net_income_ttm_val = float(ni)
            except (TypeError, ValueError):
                net_income_ttm_val = None
        dr = rf.get("dv_ratio")
        if dr is not None:
            try:
                dv_ratio_val = float(dr)
            except (TypeError, ValueError):
                dv_ratio_val = None
        dt = rf.get("dv_ttm")
        if dt is not None:
            try:
                dv_ttm_val = float(dt)
            except (TypeError, ValueError):
                dv_ttm_val = None

    if prov:
        if dv_ratio_val is None and prov.get("dv_ratio") is not None:
            try:
                dv_ratio_val = float(prov["dv_ratio"])
            except (TypeError, ValueError):
                pass
        if dv_ttm_val is None and prov.get("dv_ttm") is not None:
            try:
                dv_ttm_val = float(prov["dv_ttm"])
            except (TypeError, ValueError):
                pass

    iq = row.get("investment_quality")
    iq_dict = iq if isinstance(iq, dict) else None
    iq_label = iq_dict.get("decision_label_zh") if iq_dict else None
    iq_dec = row.get("iq_decision")
    if isinstance(iq_dec, str) and iq_dec.strip():
        iq_decision_code = iq_dec.strip()
    elif iq_dict and isinstance(iq_dict.get("decision"), str):
        iq_decision_code = str(iq_dict["decision"]).strip()
    else:
        iq_decision_code = None

    return {
        "symbol": row["symbol"],
        "graham_score": row["graham_score"],
        "buffett_score": row["buffett_score"],
        "combined_score": row.get("combined_score"),
        "third_lens_score": row.get("third_lens_score"),
        "third_lens": row.get("third_lens"),
        "final_triple_score": row.get("final_triple_score"),
        "coverage_ok": bool(row.get("coverage_ok", True)),
        "graham": graham,
        "buffett": buffett,
        "provenance": prov,
        "display_name": display_name,
        "company_full_name": company_full,
        "industry": industry,
        "region": region,
        "score_explanation_zh": explanation,
        "trade_cal_date": trade_cal_date,
        "financials_end_date": financials_end_date,
        "data_source": data_source,
        "ai_persist_id": row.get("ai_persist_id"),
        "ai_score": row.get("ai_score"),
        "opportunity_score": row.get("opportunity_score"),
        "ai_analysis_date": row.get("ai_analysis_date"),
        "ai_run_id": row.get("ai_run_id"),
        "ai_summary_preview": row.get("ai_summary_preview"),
        "market_cap": market_cap_val,
        "pe_ttm": pe_ttm_val,
        "net_income_ttm": net_income_ttm_val,
        "dv_ratio": dv_ratio_val,
        "dv_ttm": dv_ttm_val,
        "investment_quality": iq_dict,
        "iq_decision": iq_decision_code,
        "iq_decision_label_zh": iq_label if isinstance(iq_label, str) else None,
    }
