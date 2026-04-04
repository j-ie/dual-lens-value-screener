from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Literal, Sequence

from sqlalchemy import Select, desc, func, insert, select, text, update
from sqlalchemy.engine import Connection, Engine

from value_screener.domain.batch_run_progress import strip_progress_keys
from value_screener.infrastructure.screening_schema import screening_result, screening_run, security_reference

SortKey = Literal["buffett", "graham"]
OrderKey = Literal["asc", "desc"]


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

    def page_results(
        self,
        conn: Connection,
        run_id: int,
        *,
        sort_key: SortKey,
        order: OrderKey,
        page: int,
        page_size: int,
    ) -> ResultPage:
        if page < 1:
            page = 1
        if page_size < 1 or page_size > 500:
            page_size = 20

        sort_col = screening_result.c.buffett_score if sort_key == "buffett" else screening_result.c.graham_score
        order_clause = desc(sort_col) if order == "desc" else sort_col.asc()
        second_clause = screening_result.c.symbol.asc()

        count_stmt = select(func.count()).select_from(screening_result).where(
            screening_result.c.run_id == run_id
        )
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
                security_reference.c.name.label("ref_name"),
                security_reference.c.fullname.label("ref_fullname"),
                security_reference.c.industry.label("ref_industry"),
                security_reference.c.area.label("ref_area"),
            )
            .select_from(
                screening_result.outerjoin(
                    security_reference,
                    screening_result.c.symbol == security_reference.c.ts_code,
                )
            )
            .where(screening_result.c.run_id == run_id)
            .order_by(order_clause, second_clause)
            .offset(offset)
            .limit(page_size)
        )
        items: list[dict[str, Any]] = []
        for r in conn.execute(data_stmt).mappings():
            items.append(
                {
                    "symbol": r["symbol"],
                    "graham_score": _decimal_to_float(r["graham_score"]),
                    "buffett_score": _decimal_to_float(r["buffett_score"]),
                    "graham": r["graham_json"],
                    "buffett": r["buffett_json"],
                    "provenance": r["provenance_json"],
                    "ref_name": r["ref_name"],
                    "ref_fullname": r["ref_fullname"],
                    "ref_industry": r["ref_industry"],
                    "ref_area": r["ref_area"],
                }
            )
        return ResultPage(items=items, total=total)


def _decimal_to_float(v: Any) -> float:
    if isinstance(v, Decimal):
        return float(v)
    return float(v)
