from __future__ import annotations

from typing import Any

from sqlalchemy.engine import Engine

from value_screener.application.batch_screening_service import BatchScreeningResult
from value_screener.infrastructure.screening_repository import ScreeningRepository


def _rows_from_screening_results(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in results:
        g = item["graham"]
        b = item["buffett"]
        rows.append(
            {
                "symbol": item["symbol"],
                "graham_score": float(g["score"]),
                "buffett_score": float(b["score"]),
                "graham_json": g,
                "buffett_json": b,
                "provenance_json": item.get("provenance"),
            }
        )
    return rows


def persist_batch_screening(
    engine: Engine,
    batch: BatchScreeningResult,
    *,
    provider_label: str | None,
) -> int:
    """将一次批跑结果写入新 Run；单事务，失败则整批回滚。"""

    repo = ScreeningRepository(engine)
    meta: dict[str, Any] = dict(batch.meta)
    with engine.begin() as conn:
        run_id = repo.create_run(
            conn,
            provider_label=provider_label,
            meta=meta,
        )
        repo.bulk_insert_results(conn, run_id, _rows_from_screening_results(batch.results))
        repo.finalize_run(
            conn,
            run_id,
            status="success",
            universe_size=int(meta.get("universe_requested") or 0) or None,
            snapshot_ok=int(meta.get("snapshot_ok") or 0) or None,
            snapshot_failed=int(meta.get("snapshot_failed") or 0) or None,
            provider_label=provider_label,
        )
    return run_id


def create_running_screening_run(
    engine: Engine,
    *,
    provider_label: str | None,
    meta: dict[str, Any] | None,
) -> int:
    """插入 status=running 的 Run，供异步批跑后续写入结果。"""

    repo = ScreeningRepository(engine)
    with engine.begin() as conn:
        return repo.create_run(conn, provider_label=provider_label, meta=meta)


def persist_batch_screening_for_run(
    engine: Engine,
    run_id: int,
    batch: BatchScreeningResult,
    *,
    provider_label: str | None,
) -> None:
    """向已存在的 Run 写入结果行并置为 success（单事务）。"""

    repo = ScreeningRepository(engine)
    meta: dict[str, Any] = dict(batch.meta)
    with engine.begin() as conn:
        repo.bulk_insert_results(conn, run_id, _rows_from_screening_results(batch.results))
        repo.finalize_run(
            conn,
            run_id,
            status="success",
            universe_size=int(meta.get("universe_requested") or 0) or None,
            snapshot_ok=int(meta.get("snapshot_ok") or 0) or None,
            snapshot_failed=int(meta.get("snapshot_failed") or 0) or None,
            provider_label=provider_label,
        )
        repo.merge_run_meta_after_success(conn, run_id, meta)


def mark_screening_run_failed(engine: Engine, run_id: int, error_detail: str) -> None:
    repo = ScreeningRepository(engine)
    with engine.begin() as conn:
        repo.fail_run(conn, run_id, error_detail=error_detail)
