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
    }
