from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Literal, Mapping, Sequence

from sqlalchemy import Select, and_, asc, desc, func, insert, or_, select, text, update
from sqlalchemy.engine import Connection, Engine

from value_screener.domain.batch_run_progress import strip_progress_keys
from value_screener.domain.combined_ranking_params import CombinedRankingParams
from value_screener.infrastructure.screening_schema import screening_result, screening_run, security_reference

SortKey = Literal["buffett", "graham", "combined", "industry", "third_lens", "triple"]
OrderKey = Literal["asc", "desc"]

# 与 facets 及筛选参数一致：空/未匹配 reference 的行业用该字面量
INDUSTRY_EMPTY_QUERY_VALUE = "__EMPTY__"


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

        join_from = screening_result.outerjoin(
            security_reference,
            screening_result.c.symbol == security_reference.c.ts_code,
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
                security_reference.c.name.label("ref_name"),
                security_reference.c.fullname.label("ref_fullname"),
                security_reference.c.industry.label("ref_industry"),
                security_reference.c.area.label("ref_area"),
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
        join_from = screening_result.outerjoin(
            security_reference,
            screening_result.c.symbol == security_reference.c.ts_code,
        )
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
                security_reference.c.name.label("ref_name"),
                security_reference.c.fullname.label("ref_fullname"),
                security_reference.c.industry.label("ref_industry"),
                security_reference.c.area.label("ref_area"),
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
    return {
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
        "ref_name": r["ref_name"],
        "ref_fullname": r["ref_fullname"],
        "ref_industry": r["ref_industry"],
        "ref_area": r["ref_area"],
    }


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
