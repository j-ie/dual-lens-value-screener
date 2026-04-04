from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from sqlalchemy import desc, func, select, update
from sqlalchemy.engine import Engine

from value_screener.domain.third_lens import (
    combine_third_lens_subscores,
    earnings_yield_ratio,
    final_triple_linear,
    industry_bucket,
    percentile_rank_0_100,
    revenue_yoy_from_two_annual,
)
from value_screener.domain.triple_composite_params import ThirdLensSubWeights, TripleCompositeParams
from value_screener.infrastructure.financial_statement_schema import fs_income
from value_screener.infrastructure.screening_repository import ScreeningRepository
from value_screener.infrastructure.screening_schema import screening_result, security_reference

logger = logging.getLogger(__name__)


def _num(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, Decimal):
        x = float(v)
    else:
        try:
            x = float(v)
        except (TypeError, ValueError):
            return None
    return x


def _load_annual_fs_income_map(engine: Engine, symbols: list[str]) -> dict[str, list[dict[str, Any]]]:
    """每个 ts_code 最多保留按 end_date 倒序的两条年报（MMDD=1231）。"""

    if not symbols:
        return {}
    rows_by_code: dict[str, list[dict[str, Any]]] = {s: [] for s in symbols}
    stmt = (
        select(
            fs_income.c.ts_code,
            fs_income.c.end_date,
            fs_income.c.total_revenue,
            fs_income.c.n_income,
        )
        .where(fs_income.c.ts_code.in_(symbols))
        .where(fs_income.c.end_date.like("%1231"))
        .order_by(fs_income.c.ts_code, desc(fs_income.c.end_date))
    )
    with engine.connect() as conn:
        for m in conn.execute(stmt).mappings():
            code = str(m["ts_code"])
            if code not in rows_by_code:
                continue
            if len(rows_by_code[code]) >= 2:
                continue
            rows_by_code[code].append(dict(m))
    return rows_by_code


def attach_third_lens_for_run(engine: Engine, run_id: int) -> dict[str, Any]:
    """
    对指定 run 的全部 screening_result 行计算第三套分与 final_triple_score 并 UPDATE。
    依赖 fs_income 年报行与 provenance_json.market_cap。
    """

    triple = TripleCompositeParams.from_env()
    sub_w = ThirdLensSubWeights.from_env()
    repo = ScreeningRepository(engine)

    with engine.connect() as conn:
        if repo.get_run(conn, run_id) is None:
            raise ValueError(f"run_id={run_id} 不存在")

        ref_ind = func.coalesce(security_reference.c.industry, "").label("ref_industry")
        base_stmt = (
            select(
                screening_result.c.symbol,
                screening_result.c.provenance_json,
                screening_result.c.buffett_score,
                screening_result.c.graham_score,
                ref_ind,
            )
            .select_from(
                screening_result.outerjoin(
                    security_reference,
                    screening_result.c.symbol == security_reference.c.ts_code,
                )
            )
            .where(screening_result.c.run_id == run_id)
        )
        result_rows = list(conn.execute(base_stmt).mappings())

    if not result_rows:
        return {"run_id": run_id, "updated": 0, "message": "无结果行"}

    symbols = [str(r["symbol"]) for r in result_rows]
    rows_by_code = _load_annual_fs_income_map(engine, symbols)

    yoy_by_ind: dict[str, list[float]] = {}
    ep_by_ind: dict[str, list[float]] = {}
    symbol_pre: dict[str, dict[str, Any]] = {}

    for r in result_rows:
        sym = str(r["symbol"])
        ind_key = industry_bucket(r.get("ref_industry"))
        prov = r["provenance_json"]
        prov_d = dict(prov) if isinstance(prov, dict) else {}
        mcap = _num(prov_d.get("market_cap"))

        two = rows_by_code.get(sym) or []
        yoy: float | None = None
        ep: float | None = None
        n_inc_latest: float | None = None
        if len(two) >= 2:
            tr0 = _num(two[0].get("total_revenue"))
            tr1 = _num(two[1].get("total_revenue"))
            if tr0 is not None and tr1 is not None:
                yoy = revenue_yoy_from_two_annual(tr0, tr1)
            n_inc_latest = _num(two[0].get("n_income"))
            if mcap is not None and n_inc_latest is not None:
                ep = earnings_yield_ratio(n_inc_latest, mcap)

        symbol_pre[sym] = {
            "ind_key": ind_key,
            "yoy": yoy,
            "ep": ep,
            "buffett": _num(r["buffett_score"]) or 0.0,
            "graham": _num(r["graham_score"]) or 0.0,
            "mcap": mcap,
            "n_income_latest": n_inc_latest,
        }
        if yoy is not None:
            yoy_by_ind.setdefault(ind_key, []).append(yoy)
        if ep is not None:
            ep_by_ind.setdefault(ind_key, []).append(ep)

    updated = 0
    with engine.begin() as conn:
        for r in result_rows:
            sym = str(r["symbol"])
            pre = symbol_pre[sym]
            ind_key = pre["ind_key"]
            yoy = pre["yoy"]
            ep = pre["ep"]

            g_sample = yoy_by_ind.get(ind_key, [])
            v_sample = ep_by_ind.get(ind_key, [])

            growth_score: float | None = None
            val_score: float | None = None
            if yoy is not None and len(g_sample) >= 1:
                growth_score = round(percentile_rank_0_100(g_sample, yoy), 4)
            if ep is not None and len(v_sample) >= 1:
                val_score = round(percentile_rank_0_100(v_sample, ep), 4)

            third, lens_meta = combine_third_lens_subscores(
                growth_score,
                val_score,
                sub_w.weight_growth,
                sub_w.weight_valuation,
            )
            lens_meta["industry_bucket"] = ind_key
            lens_meta["industry_sample_growth"] = len(g_sample)
            lens_meta["industry_sample_valuation"] = len(v_sample)
            if yoy is not None:
                lens_meta["revenue_yoy"] = round(yoy, 6)
            if ep is not None:
                lens_meta["earnings_yield"] = round(ep, 8)
            if pre["mcap"] is None:
                lens_meta["omit_market_cap"] = True

            final, fin_meta = final_triple_linear(
                pre["buffett"],
                pre["graham"],
                third,
                triple.weight_buffett,
                triple.weight_graham,
                triple.weight_third,
            )
            lens_meta.update(fin_meta)

            conn.execute(
                update(screening_result)
                .where(
                    screening_result.c.run_id == run_id,
                    screening_result.c.symbol == sym,
                )
                .values(
                    third_lens_score=third,
                    third_lens_json=lens_meta,
                    final_triple_score=final,
                )
            )
            updated += 1

    logger.info("attach_third_lens run_id=%s updated_rows=%s", run_id, updated)
    return {"run_id": run_id, "updated": updated}

