from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Literal, Mapping, Sequence

from sqlalchemy import Select, and_, asc, delete, desc, exists, func, insert, or_, select, text, update
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.engine import Connection, Engine

from value_screener.domain.batch_run_progress import strip_progress_keys
from value_screener.domain.combined_ranking_params import CombinedRankingParams
from value_screener.infrastructure.company_name_search import (
    company_display_and_code_match_clause,
    normalized_company_search_term,
)
from value_screener.infrastructure.mysql_collation import ts_code_equals
from value_screener.infrastructure.screening_schema import (
    company_ai_analysis,
    screening_result,
    screening_run,
    security_reference,
)

SortKey = Literal[
    "buffett",
    "graham",
    "combined",
    "industry",
    "third_lens",
    "triple",
    "ai_score",
    "market_cap",
    "dividend_yield",
]
OrderKey = Literal["asc", "desc"]

# 与 facets 及筛选参数一致：空/未匹配 reference 的行业用该字面量
INDUSTRY_EMPTY_QUERY_VALUE = "__EMPTY__"
# 批跑早于价值质量落库、或计算失败未写入 iq_decision 时用该字面量筛选
IQ_DECISION_EMPTY_QUERY_VALUE = "__IQ_EMPTY__"


def _coalesced_dividend_yield_percent_sql() -> Any:
    """
    与前端展示一致：优先 dv_ttm，否则 dv_ratio（单位：百分点）。
    来源顺序：run_fact_json → provenance_json（TuShare 常空时东财补全会落在快照/provenance）。
    """

    j = screening_result.c.run_fact_json
    p = screening_result.c.provenance_json
    return func.coalesce(
        j["dv_ttm"].as_float(),
        j["dv_ratio"].as_float(),
        p["dv_ttm"].as_float(),
        p["dv_ratio"].as_float(),
    )


def _effective_market_cap_sql() -> Any:
    """
    与 result_enrichment 展示口径一致：优先列 market_cap，否则 provenance_json.market_cap。
    """

    p = screening_result.c.provenance_json
    return func.coalesce(screening_result.c.market_cap, p["market_cap"].as_float())


def _latest_company_ai_per_ts_code_subquery() -> Any:
    """
    每个 ts_code 取 analysis_date 最新、同日期下 id 最大的一条记录。
    语义等价于 row_number() OVER (PARTITION BY ts_code ORDER BY analysis_date DESC, id DESC) = 1，
    且不依赖 MySQL 8 窗口函数，便于在 5.7 等环境运行。
    """

    outer = company_ai_analysis.alias("cai_latest_outer")
    newer = company_ai_analysis.alias("cai_latest_newer")
    has_newer = exists(
        select(newer.c.id).where(
            and_(
                ts_code_equals(newer.c.ts_code, outer.c.ts_code),
                or_(
                    newer.c.analysis_date > outer.c.analysis_date,
                    and_(
                        newer.c.analysis_date == outer.c.analysis_date,
                        newer.c.id > outer.c.id,
                    ),
                ),
            )
        )
    )
    return (
        select(
            outer.c.id.label("latest_cai_id"),
            outer.c.ts_code.label("latest_ts_code"),
            outer.c.analysis_date.label("latest_ai_date"),
            outer.c.run_id.label("latest_ai_run_id"),
            outer.c.ai_score.label("latest_ai_score"),
            outer.c.opportunity_score.label("latest_opportunity_score"),
            outer.c.summary.label("latest_ai_summary"),
        )
        .where(~has_newer)
        .subquery()
    )


@dataclass(frozen=True, slots=True)
class RunRow:
    id: int
    external_uuid: str
    status: str
    created_at: datetime
    finished_at: datetime | None
    universe_size: int | None
    snapshot_ok: int | None
    snapshot_failed: int | None
    provider_label: str | None
    meta_json: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class ResultPage:
    items: list[dict[str, Any]]
    total: int


class ScreeningRepository:
    """MySQL 读写：偏 Core，排序列白名单。"""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def create_run(
        self,
        conn: Connection,
        *,
        provider_label: str | None,
        meta: dict[str, Any] | None,
    ) -> int:
        uid = str(uuid.uuid4())
        conn.execute(
            insert(screening_run).values(
                external_uuid=uid,
                status="running",
                created_at=datetime.now(timezone.utc),
                finished_at=None,
                universe_size=None,
                snapshot_ok=None,
                snapshot_failed=None,
                provider_label=provider_label,
                meta_json=meta,
            )
        )
        rid = conn.execute(text("SELECT LAST_INSERT_ID() AS id")).scalar_one()
        return int(rid)

    def get_run(self, conn: Connection, run_id: int) -> RunRow | None:
        stmt: Select[Any] = (
            select(
                screening_run.c.id,
                screening_run.c.external_uuid,
                screening_run.c.status,
                screening_run.c.created_at,
                screening_run.c.finished_at,
                screening_run.c.universe_size,
                screening_run.c.snapshot_ok,
                screening_run.c.snapshot_failed,
                screening_run.c.provider_label,
                screening_run.c.meta_json,
            )
            .where(screening_run.c.id == run_id)
            .limit(1)
        )
        r = conn.execute(stmt).mappings().first()
        if r is None:
            return None
        mj = r["meta_json"]
        meta = dict(mj) if isinstance(mj, dict) else None
        return RunRow(
            id=int(r["id"]),
            external_uuid=str(r["external_uuid"]),
            status=str(r["status"]),
            created_at=r["created_at"],
            finished_at=r["finished_at"],
            universe_size=int(r["universe_size"]) if r["universe_size"] is not None else None,
            snapshot_ok=int(r["snapshot_ok"]) if r["snapshot_ok"] is not None else None,
            snapshot_failed=int(r["snapshot_failed"]) if r["snapshot_failed"] is not None else None,
            provider_label=str(r["provider_label"]) if r["provider_label"] else None,
            meta_json=meta,
        )

    def delete_run(self, conn: Connection, run_id: int) -> bool:
        """
        删除 screening_run；screening_result 随外键级联删除。
        company_ai_analysis 须在应用层先按 run_id 删除（迁移为 ON DELETE SET NULL）。
        """

        res = conn.execute(delete(screening_run).where(screening_run.c.id == run_id))
        rc = getattr(res, "rowcount", None)
        try:
            n = int(rc) if rc is not None else 0
        except (TypeError, ValueError):
            n = 0
        return n > 0

    def fail_run(self, conn: Connection, run_id: int, *, error_detail: str) -> None:
        meta_row = conn.execute(
            select(screening_run.c.meta_json).where(screening_run.c.id == run_id)
        ).first()
        raw = meta_row[0] if meta_row and meta_row[0] is not None else None
        meta: dict[str, Any] = dict(
            strip_progress_keys(raw if isinstance(raw, dict) else None),
        )
        meta["batch_error"] = error_detail[:2000]
        conn.execute(
            update(screening_run)
            .where(screening_run.c.id == run_id)
            .values(
                status="failed",
                finished_at=datetime.now(timezone.utc),
                meta_json=meta,
            )
        )

    def merge_meta_json_patch(self, run_id: int, patch: dict[str, Any]) -> None:
        """合并写入 meta_json（短事务，供异步批跑节流更新进度）。"""

        with self._engine.begin() as conn:
            row = conn.execute(
                select(screening_run.c.meta_json).where(screening_run.c.id == run_id)
            ).first()
            base: dict[str, Any] = {}
            if row is not None and row[0] is not None and isinstance(row[0], dict):
                base = dict(row[0])
            base.update(patch)
            conn.execute(
                update(screening_run)
                .where(screening_run.c.id == run_id)
                .values(meta_json=base)
            )

    def merge_run_meta_after_success(
        self,
        conn: Connection,
        run_id: int,
        batch_meta: dict[str, Any],
    ) -> None:
        """成功落库后：去掉进度字段并合并批跑统计类 meta。"""

        row = conn.execute(
            select(screening_run.c.meta_json).where(screening_run.c.id == run_id)
        ).first()
        raw = row[0] if row and row[0] is not None else None
        merged: dict[str, Any] = dict(
            strip_progress_keys(raw if isinstance(raw, dict) else None),
        )
        merged.update(batch_meta)
        conn.execute(
            update(screening_run)
            .where(screening_run.c.id == run_id)
            .values(meta_json=merged)
        )

    def finalize_run(
        self,
        conn: Connection,
        run_id: int,
        *,
        status: str,
        universe_size: int | None,
        snapshot_ok: int | None,
        snapshot_failed: int | None,
        provider_label: str | None = None,
    ) -> None:
        values: dict[str, Any] = {
            "status": status,
            "finished_at": datetime.now(timezone.utc),
            "universe_size": universe_size,
            "snapshot_ok": snapshot_ok,
            "snapshot_failed": snapshot_failed,
        }
        if provider_label is not None:
            values["provider_label"] = provider_label
        conn.execute(update(screening_run).where(screening_run.c.id == run_id).values(**values))

    def bulk_insert_results(
        self,
        conn: Connection,
        run_id: int,
        rows: Sequence[dict[str, Any]],
    ) -> None:
        if not rows:
            return
        payload = [{**r, "run_id": run_id} for r in rows]
        conn.execute(insert(screening_result), payload)

    def bulk_upsert_results(
        self,
        conn: Connection,
        run_id: int,
        rows: Sequence[dict[str, Any]],
    ) -> None:
        """MySQL：按 (run_id, symbol) 插入或覆盖核心分数字段（异步批跑分块落库）。"""

        if not rows:
            return
        payload = [{**r, "run_id": run_id} for r in rows]
        stmt = mysql_insert(screening_result).values(payload)
        stmt = stmt.on_duplicate_key_update(
            graham_score=stmt.inserted.graham_score,
            buffett_score=stmt.inserted.buffett_score,
            graham_json=stmt.inserted.graham_json,
            buffett_json=stmt.inserted.buffett_json,
            provenance_json=stmt.inserted.provenance_json,
            combined_score=stmt.inserted.combined_score,
            coverage_ok=stmt.inserted.coverage_ok,
            run_fact_json=stmt.inserted.run_fact_json,
            market_cap=stmt.inserted.market_cap,
            pe_ttm=stmt.inserted.pe_ttm,
            investment_quality_json=stmt.inserted.investment_quality_json,
            iq_decision=stmt.inserted.iq_decision,
        )
        conn.execute(stmt)

    def list_runs(self, conn: Connection, *, limit: int = 50) -> list[RunRow]:
        stmt: Select[Any] = (
            select(
                screening_run.c.id,
                screening_run.c.external_uuid,
                screening_run.c.status,
                screening_run.c.created_at,
                screening_run.c.finished_at,
                screening_run.c.universe_size,
                screening_run.c.snapshot_ok,
                screening_run.c.snapshot_failed,
                screening_run.c.provider_label,
                screening_run.c.meta_json,
            )
            .order_by(desc(screening_run.c.id))
            .limit(limit)
        )
        out: list[RunRow] = []
        for r in conn.execute(stmt).mappings():
            mj = r["meta_json"]
            meta = dict(mj) if isinstance(mj, dict) else None
            out.append(
                RunRow(
                    id=int(r["id"]),
                    external_uuid=str(r["external_uuid"]),
                    status=str(r["status"]),
                    created_at=r["created_at"],
                    finished_at=r["finished_at"],
                    universe_size=int(r["universe_size"]) if r["universe_size"] is not None else None,
                    snapshot_ok=int(r["snapshot_ok"]) if r["snapshot_ok"] is not None else None,
                    snapshot_failed=int(r["snapshot_failed"]) if r["snapshot_failed"] is not None else None,
                    provider_label=str(r["provider_label"]) if r["provider_label"] else None,
                    meta_json=meta,
                )
            )
        return out

    def list_investment_quality_inputs_for_runs(
        self,
        conn: Connection,
        run_ids: Sequence[int],
    ) -> dict[int, list[dict[str, Any]]]:
        ids = [int(x) for x in run_ids if int(x) > 0]
        if not ids:
            return {}
        stmt = (
            select(
                screening_result.c.run_id,
                screening_result.c.symbol,
                screening_result.c.run_fact_json,
                screening_result.c.provenance_json,
                screening_result.c.market_cap,
                security_reference.c.industry.label("ref_industry"),
            )
            .select_from(
                screening_result.outerjoin(
                    security_reference,
                    screening_result.c.symbol == security_reference.c.ts_code,
                )
            )
            .where(screening_result.c.run_id.in_(ids))
        )
        out: dict[int, list[dict[str, Any]]] = {}
        for row in conn.execute(stmt).mappings():
            rid = int(row["run_id"])
            run_fact = row.get("run_fact_json")
            if not isinstance(run_fact, dict):
                continue
            out.setdefault(rid, []).append(
                {
                    "symbol": row.get("symbol"),
                    "run_fact_json": dict(run_fact),
                    "provenance": dict(row.get("provenance_json"))
                    if isinstance(row.get("provenance_json"), dict)
                    else None,
                    "industry": row.get("ref_industry") or "",
                    "market_cap": _decimal_to_float(row["market_cap"])
                    if row.get("market_cap") is not None
                    else None,
                }
            )
        return out

    def list_distinct_industries_for_run(
        self,
        conn: Connection,
        run_id: int,
        *,
        limit: int = 500,
    ) -> list[str]:
        """某 run 结果 JOIN 参考表后的去重行业（空行业 → INDUSTRY_EMPTY_QUERY_VALUE）。"""

        stmt = (
            select(security_reference.c.industry)
            .select_from(
                screening_result.outerjoin(
                    security_reference,
                    screening_result.c.symbol == security_reference.c.ts_code,
                )
            )
            .where(screening_result.c.run_id == run_id)
            .distinct()
            .limit(limit)
        )
        raw = {row[0] for row in conn.execute(stmt)}
        out: set[str] = set()
        for v in raw:
            s = (v or "").strip()
            out.add(INDUSTRY_EMPTY_QUERY_VALUE if not s else s)
        return sorted(out)

    def list_distinct_iq_decisions_for_run(
        self,
        conn: Connection,
        run_id: int,
        *,
        limit: int = 64,
    ) -> list[str]:
        """某 run 已落库的价值质量结论（iq_decision）去重；含未计算占位 `__IQ_EMPTY__`。"""

        stmt = (
            select(screening_result.c.iq_decision)
            .where(screening_result.c.run_id == run_id)
            .distinct()
            .limit(max(1, min(int(limit), 256)))
        )
        raw = [row[0] for row in conn.execute(stmt)]
        out: set[str] = set()
        for v in raw:
            if v is None or (isinstance(v, str) and not str(v).strip()):
                out.add(IQ_DECISION_EMPTY_QUERY_VALUE)
            else:
                out.add(str(v).strip())
        return sorted(out)

    def list_top_symbols_by_combined(
        self,
        conn: Connection,
        run_id: int,
        *,
        ranking: CombinedRankingParams,
        limit: int,
    ) -> list[str]:
        """
        与分页接口 sort=combined、order=desc 相同的门槛与加权口径，返回前 limit 个 ts_code。
        """

        lim = max(0, min(int(limit), 10_000))
        if lim == 0:
            return []
        wb = ranking.weight_buffett
        wg = ranking.weight_graham
        combined_linear = (
            screening_result.c.buffett_score * wb + screening_result.c.graham_score * wg
        )
        coalesced_combined = func.coalesce(screening_result.c.combined_score, combined_linear)
        parts: list[Any] = [
            screening_result.c.run_id == run_id,
            screening_result.c.coverage_ok.is_(True),
        ]
        if ranking.gate_min_buffett is not None:
            parts.append(screening_result.c.buffett_score >= ranking.gate_min_buffett)
        if ranking.gate_min_graham is not None:
            parts.append(screening_result.c.graham_score >= ranking.gate_min_graham)
        if ranking.gate_min_combined is not None:
            parts.append(coalesced_combined >= ranking.gate_min_combined)
        where_clause = and_(*parts)
        if ranking.tiebreak == "sum_bg":
            tie = screening_result.c.buffett_score + screening_result.c.graham_score
        else:
            tie = func.least(screening_result.c.buffett_score, screening_result.c.graham_score)
        stmt = (
            select(screening_result.c.symbol)
            .where(where_clause)
            .order_by(desc(coalesced_combined), desc(tie), screening_result.c.symbol.asc())
            .limit(lim)
        )
        return [str(row[0]).strip() for row in conn.execute(stmt) if row[0]]

    def list_top_symbols_weighted_desc_coverage_only(
        self,
        conn: Connection,
        run_id: int,
        *,
        ranking: CombinedRankingParams,
        limit: int,
    ) -> list[str]:
        """
        仅要求 coverage_ok，按与综合分相同的加权与排序取 Top N（不应用 gate_min_*）。
        用于后置流水线在「综合榜门槛过严导致无人上榜」时的回退。
        """

        lim = max(0, min(int(limit), 10_000))
        if lim == 0:
            return []
        wb = ranking.weight_buffett
        wg = ranking.weight_graham
        combined_linear = (
            screening_result.c.buffett_score * wb + screening_result.c.graham_score * wg
        )
        coalesced_combined = func.coalesce(screening_result.c.combined_score, combined_linear)
        where_clause = and_(
            screening_result.c.run_id == run_id,
            screening_result.c.coverage_ok.is_(True),
        )
        if ranking.tiebreak == "sum_bg":
            tie = screening_result.c.buffett_score + screening_result.c.graham_score
        else:
            tie = func.least(screening_result.c.buffett_score, screening_result.c.graham_score)
        stmt = (
            select(screening_result.c.symbol)
            .where(where_clause)
            .order_by(desc(coalesced_combined), desc(tie), screening_result.c.symbol.asc())
            .limit(lim)
        )
        return [str(row[0]).strip() for row in conn.execute(stmt) if row[0]]

    def page_results(
        self,
        conn: Connection,
        run_id: int,
        *,
        sort_key: SortKey,
        order: OrderKey,
        page: int,
        page_size: int,
        ranking: CombinedRankingParams | None = None,
        industries: list[str] | None = None,
        has_ai_analysis: bool | None = None,
        ai_score_min: float | None = None,
        company_name: str | None = None,
        market_cap_min: float | None = None,
        market_cap_max: float | None = None,
        dividend_yield_min: float | None = None,
        dividend_yield_max: float | None = None,
        iq_decisions: list[str] | None = None,
    ) -> ResultPage:
        if page < 1:
            page = 1
        if page_size < 1 or page_size > 500:
            page_size = 20

        inds = [i.strip() for i in (industries or []) if i and i.strip()]

        wb = ranking.weight_buffett if ranking is not None else 0.5
        wg = ranking.weight_graham if ranking is not None else 0.5
        combined_linear = (
            screening_result.c.buffett_score * wb + screening_result.c.graham_score * wg
        )
        coalesced_combined = func.coalesce(screening_result.c.combined_score, combined_linear)

        base_where = screening_result.c.run_id == run_id
        where_clause: Any = base_where
        if sort_key == "combined":
            if ranking is None:
                raise ValueError("sort=combined 时必须提供 CombinedRankingParams")
            parts: list[Any] = [
                base_where,
                screening_result.c.coverage_ok.is_(True),
            ]
            if ranking.gate_min_buffett is not None:
                parts.append(screening_result.c.buffett_score >= ranking.gate_min_buffett)
            if ranking.gate_min_graham is not None:
                parts.append(screening_result.c.graham_score >= ranking.gate_min_graham)
            if ranking.gate_min_combined is not None:
                parts.append(coalesced_combined >= ranking.gate_min_combined)
            where_clause = and_(*parts)

        if inds:
            ind_or = _industry_filter_or(inds)
            where_clause = and_(where_clause, ind_or)

        latest_ai_sq = _latest_company_ai_per_ts_code_subquery()

        if has_ai_analysis is True:
            where_clause = and_(where_clause, latest_ai_sq.c.latest_cai_id.isnot(None))
        if ai_score_min is not None:
            where_clause = and_(
                where_clause,
                latest_ai_sq.c.latest_ai_score.isnot(None),
                latest_ai_sq.c.latest_ai_score >= ai_score_min,
            )

        cn_term = normalized_company_search_term(company_name)
        if cn_term:
            where_clause = and_(
                where_clause,
                company_display_and_code_match_clause(
                    ref_name_col=security_reference.c.name,
                    ref_fullname_col=security_reference.c.fullname,
                    ts_code_col=screening_result.c.symbol,
                    term=cn_term,
                ),
            )

        mcap_eff = _effective_market_cap_sql()
        if market_cap_min is not None:
            where_clause = and_(where_clause, mcap_eff.isnot(None), mcap_eff >= market_cap_min)
        if market_cap_max is not None:
            where_clause = and_(where_clause, mcap_eff.isnot(None), mcap_eff <= market_cap_max)

        dv_eff = _coalesced_dividend_yield_percent_sql()
        if dividend_yield_min is not None:
            where_clause = and_(where_clause, dv_eff.isnot(None), dv_eff >= dividend_yield_min)
        if dividend_yield_max is not None:
            where_clause = and_(where_clause, dv_eff.isnot(None), dv_eff <= dividend_yield_max)

        iq_list = [i.strip() for i in (iq_decisions or []) if i and i.strip()]
        if iq_list:
            where_clause = and_(where_clause, _iq_decision_filter_or(iq_list))

        join_from = (
            screening_result.outerjoin(
                security_reference,
                screening_result.c.symbol == security_reference.c.ts_code,
            ).outerjoin(
                latest_ai_sq,
                ts_code_equals(screening_result.c.symbol, latest_ai_sq.c.latest_ts_code),
            )
        )

        if sort_key == "buffett":
            sort_col = screening_result.c.buffett_score
            order_clause = desc(sort_col) if order == "desc" else asc(sort_col)
            second_clause = screening_result.c.symbol.asc()
            order_by_list = [order_clause, second_clause]
        elif sort_key == "graham":
            sort_col = screening_result.c.graham_score
            order_clause = desc(sort_col) if order == "desc" else asc(sort_col)
            second_clause = screening_result.c.symbol.asc()
            order_by_list = [order_clause, second_clause]
        elif sort_key == "industry":
            ind_sort = func.coalesce(security_reference.c.industry, "")
            primary = desc(ind_sort) if order == "desc" else asc(ind_sort)
            order_by_list = [primary, screening_result.c.symbol.asc()]
        elif sort_key == "third_lens":
            sentinel = -10**9 if order == "desc" else 10**9
            sort_col = func.coalesce(screening_result.c.third_lens_score, sentinel)
            order_clause = desc(sort_col) if order == "desc" else asc(sort_col)
            order_by_list = [order_clause, screening_result.c.symbol.asc()]
        elif sort_key == "triple":
            sentinel = -10**9 if order == "desc" else 10**9
            sort_col = func.coalesce(screening_result.c.final_triple_score, sentinel)
            order_clause = desc(sort_col) if order == "desc" else asc(sort_col)
            order_by_list = [order_clause, screening_result.c.symbol.asc()]
        elif sort_key == "ai_score":
            sentinel = -10**9 if order == "desc" else 10**9
            sort_col = func.coalesce(latest_ai_sq.c.latest_ai_score, sentinel)
            order_clause = desc(sort_col) if order == "desc" else asc(sort_col)
            order_by_list = [order_clause, screening_result.c.symbol.asc()]
        elif sort_key == "market_cap":
            mcol = _effective_market_cap_sql()
            null_last = asc(mcol.is_(None))
            primary = desc(mcol) if order == "desc" else asc(mcol)
            order_by_list = [null_last, primary, screening_result.c.symbol.asc()]
        elif sort_key == "dividend_yield":
            dv = _coalesced_dividend_yield_percent_sql()
            null_last = asc(dv.is_(None))
            primary = desc(dv) if order == "desc" else asc(dv)
            order_by_list = [null_last, primary, screening_result.c.symbol.asc()]
        else:
            primary = desc(coalesced_combined) if order == "desc" else asc(coalesced_combined)
            if ranking is not None and ranking.tiebreak == "sum_bg":
                tie = screening_result.c.buffett_score + screening_result.c.graham_score
            else:
                tie = func.least(screening_result.c.buffett_score, screening_result.c.graham_score)
            tie_ord = desc(tie) if order == "desc" else asc(tie)
            order_by_list = [primary, tie_ord, screening_result.c.symbol.asc()]

        count_stmt = select(func.count()).select_from(join_from).where(where_clause)
        total = int(conn.execute(count_stmt).scalar_one())

        offset = (page - 1) * page_size
        data_stmt = (
            select(
                screening_result.c.symbol,
                screening_result.c.graham_score,
                screening_result.c.buffett_score,
                screening_result.c.graham_json,
                screening_result.c.buffett_json,
                screening_result.c.provenance_json,
                screening_result.c.combined_score,
                screening_result.c.coverage_ok,
                screening_result.c.third_lens_score,
                screening_result.c.third_lens_json,
                screening_result.c.final_triple_score,
                screening_result.c.run_fact_json,
                screening_result.c.market_cap,
                screening_result.c.pe_ttm,
                screening_result.c.investment_quality_json,
                screening_result.c.iq_decision,
                security_reference.c.name.label("ref_name"),
                security_reference.c.fullname.label("ref_fullname"),
                security_reference.c.industry.label("ref_industry"),
                security_reference.c.area.label("ref_area"),
                latest_ai_sq.c.latest_cai_id,
                latest_ai_sq.c.latest_ai_date,
                latest_ai_sq.c.latest_ai_run_id,
                latest_ai_sq.c.latest_ai_score,
                latest_ai_sq.c.latest_opportunity_score,
                latest_ai_sq.c.latest_ai_summary,
            )
            .select_from(join_from)
            .where(where_clause)
            .order_by(*order_by_list)
            .offset(offset)
            .limit(page_size)
        )
        items: list[dict[str, Any]] = []
        for r in conn.execute(data_stmt).mappings():
            items.append(_mapping_to_screening_item(r, wb=wb, wg=wg))
        return ResultPage(items=items, total=total)

    def get_result_row_for_run_symbol(
        self,
        conn: Connection,
        run_id: int,
        ts_code: str,
        *,
        ranking: CombinedRankingParams | None = None,
    ) -> dict[str, Any] | None:
        """单标的筛选结果行（含 ref_* JOIN）；不在该 run 中则 None。"""

        code = str(ts_code).strip()
        if not code:
            return None
        wb = ranking.weight_buffett if ranking is not None else 0.5
        wg = ranking.weight_graham if ranking is not None else 0.5
        latest2 = _latest_company_ai_per_ts_code_subquery()
        join_from = screening_result.outerjoin(
            security_reference,
            screening_result.c.symbol == security_reference.c.ts_code,
        ).outerjoin(latest2, ts_code_equals(screening_result.c.symbol, latest2.c.latest_ts_code))
        stmt = (
            select(
                screening_result.c.symbol,
                screening_result.c.graham_score,
                screening_result.c.buffett_score,
                screening_result.c.graham_json,
                screening_result.c.buffett_json,
                screening_result.c.provenance_json,
                screening_result.c.combined_score,
                screening_result.c.coverage_ok,
                screening_result.c.third_lens_score,
                screening_result.c.third_lens_json,
                screening_result.c.final_triple_score,
                screening_result.c.run_fact_json,
                screening_result.c.market_cap,
                screening_result.c.pe_ttm,
                screening_result.c.investment_quality_json,
                screening_result.c.iq_decision,
                security_reference.c.name.label("ref_name"),
                security_reference.c.fullname.label("ref_fullname"),
                security_reference.c.industry.label("ref_industry"),
                security_reference.c.area.label("ref_area"),
                latest2.c.latest_cai_id,
                latest2.c.latest_ai_date,
                latest2.c.latest_ai_run_id,
                latest2.c.latest_ai_score,
                latest2.c.latest_opportunity_score,
                latest2.c.latest_ai_summary,
            )
            .select_from(join_from)
            .where(
                and_(
                    screening_result.c.run_id == run_id,
                    screening_result.c.symbol == code,
                )
            )
            .limit(1)
        )
        r = conn.execute(stmt).mappings().first()
        if r is None:
            return None
        return _mapping_to_screening_item(r, wb=wb, wg=wg)


def _mapping_to_screening_item(r: Mapping[str, Any], *, wb: float, wg: float) -> dict[str, Any]:
    bsc = _decimal_to_float(r["buffett_score"])
    gsc = _decimal_to_float(r["graham_score"])
    cc_raw = r["combined_score"]
    cc = _decimal_to_float(cc_raw) if cc_raw is not None else round(wb * bsc + wg * gsc, 4)
    tls_raw = r.get("third_lens_score")
    tls = _decimal_to_float(tls_raw) if tls_raw is not None else None
    fts_raw = r.get("final_triple_score")
    fts = _decimal_to_float(fts_raw) if fts_raw is not None else None
    tlj = r.get("third_lens_json")
    third_lens_obj = dict(tlj) if isinstance(tlj, dict) else None
    rfj = r.get("run_fact_json")
    run_fact = dict(rfj) if isinstance(rfj, dict) else None
    mcap_col = r.get("market_cap")
    pe_col = r.get("pe_ttm")
    iqj = r.get("investment_quality_json")
    iq_obj = dict(iqj) if isinstance(iqj, dict) else None
    iq_dec = r.get("iq_decision")
    out: dict[str, Any] = {
        "symbol": r["symbol"],
        "graham_score": gsc,
        "buffett_score": bsc,
        "graham": r["graham_json"],
        "buffett": r["buffett_json"],
        "provenance": r["provenance_json"],
        "combined_score": cc,
        "coverage_ok": bool(r["coverage_ok"]),
        "third_lens_score": tls,
        "third_lens": third_lens_obj,
        "final_triple_score": fts,
        "run_fact_json": run_fact,
        "market_cap": _decimal_to_float(mcap_col) if mcap_col is not None else None,
        "pe_ttm": _decimal_to_float(pe_col) if pe_col is not None else None,
        "investment_quality": iq_obj,
        "iq_decision": str(iq_dec).strip() if iq_dec is not None and str(iq_dec).strip() else None,
        "ref_name": r["ref_name"],
        "ref_fullname": r["ref_fullname"],
        "ref_industry": r["ref_industry"],
        "ref_area": r["ref_area"],
    }
    lc = r.get("latest_cai_id")
    if lc is not None:
        las = r.get("latest_ai_score")
        out["ai_persist_id"] = int(lc)
        out["ai_score"] = float(las) if las is not None else None
        los = r.get("latest_opportunity_score")
        out["opportunity_score"] = _decimal_to_float(los) if los is not None else None
        lad = r.get("latest_ai_date")
        out["ai_analysis_date"] = lad.isoformat() if lad is not None and hasattr(lad, "isoformat") else None
        lr = r.get("latest_ai_run_id")
        out["ai_run_id"] = int(lr) if lr is not None else None
        summ = str(r.get("latest_ai_summary") or "")
        out["ai_summary_preview"] = summ if len(summ) <= 200 else summ[:197] + "..."
    else:
        out["ai_persist_id"] = None
        out["ai_score"] = None
        out["opportunity_score"] = None
        out["ai_analysis_date"] = None
        out["ai_run_id"] = None
        out["ai_summary_preview"] = None
    return out


def _iq_decision_filter_or(iq_decisions: list[str]) -> Any:
    parts: list[Any] = []
    for raw in iq_decisions:
        s = raw.strip()
        if s == IQ_DECISION_EMPTY_QUERY_VALUE or s == "":
            parts.append(screening_result.c.iq_decision.is_(None))
        else:
            parts.append(screening_result.c.iq_decision == s)
    if len(parts) == 1:
        return parts[0]
    return or_(*parts)


def _industry_filter_or(industries: list[str]) -> Any:
    parts: list[Any] = []
    for raw in industries:
        s = raw.strip()
        if s == INDUSTRY_EMPTY_QUERY_VALUE or s == "":
            parts.append(
                or_(
                    security_reference.c.industry.is_(None),
                    security_reference.c.industry == "",
                )
            )
        else:
            parts.append(security_reference.c.industry == s)
    if len(parts) == 1:
        return parts[0]
    return or_(*parts)


def _decimal_to_float(v: Any) -> float:
    if isinstance(v, Decimal):
        return float(v)
    return float(v)
