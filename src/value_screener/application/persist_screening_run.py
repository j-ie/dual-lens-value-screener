from __future__ import annotations

from typing import Any

from sqlalchemy.engine import Engine

from value_screener.application.batch_screening_service import BatchScreeningResult
from value_screener.application.screening_run_fact import build_hybrid_persist_fields
from value_screener.domain.assessment_coverage import combined_linear_score, dual_lens_coverage_ok
from value_screener.domain.combined_ranking_params import CombinedRankingParams
from value_screener.domain.snapshot import StockFinancialSnapshot
from value_screener.infrastructure.screening_repository import ScreeningRepository


def _rows_from_screening_results(
    results: list[dict[str, Any]],
    ranking: CombinedRankingParams,
    snaps: tuple[StockFinancialSnapshot, ...] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    use_hybrid = snaps is not None and len(snaps) == len(results)
    for idx, item in enumerate(results):
        g = item["graham"]
        b = item["buffett"]
        bs = float(b["score"])
        gs = float(g["score"])
        cov = dual_lens_coverage_ok(g, b)
        comb = combined_linear_score(
            bs,
            gs,
            weight_buffett=ranking.weight_buffett,
            weight_graham=ranking.weight_graham,
        )
        row: dict[str, Any] = {
            "symbol": item["symbol"],
            "graham_score": gs,
            "buffett_score": bs,
            "graham_json": g,
            "buffett_json": b,
            "provenance_json": item.get("provenance"),
            "combined_score": comb,
            "coverage_ok": cov,
        }
        if use_hybrid:
            hy = build_hybrid_persist_fields(snaps[idx])
            row["run_fact_json"] = hy["run_fact_json"]
            row["market_cap"] = hy["market_cap"]
            row["pe_ttm"] = hy["pe_ttm"]
        rows.append(row)
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
    ranking = CombinedRankingParams.from_env()
    with engine.begin() as conn:
        run_id = repo.create_run(
            conn,
            provider_label=provider_label,
            meta=meta,
        )
        repo.bulk_insert_results(
            conn,
            run_id,
            _rows_from_screening_results(
                batch.results,
                ranking,
                batch.snapshots_for_persist,
            ),
        )
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


def append_screening_results_chunk(
    engine: Engine,
    run_id: int,
    screened: list[dict[str, Any]],
    snaps: list[StockFinancialSnapshot] | None = None,
) -> None:
    """将本批已算分结果增量写入 screening_result（MySQL upsert），Run 仍为 running。"""

    if not screened:
        return
    ranking = CombinedRankingParams.from_env()
    repo = ScreeningRepository(engine)
    snap_tuple: tuple[StockFinancialSnapshot, ...] | None = None
    if snaps is not None and len(snaps) == len(screened):
        snap_tuple = tuple(snaps)
    rows = _rows_from_screening_results(screened, ranking, snap_tuple)
    with engine.begin() as conn:
        repo.bulk_upsert_results(conn, run_id, rows)


def persist_batch_screening_for_run(
    engine: Engine,
    run_id: int,
    batch: BatchScreeningResult,
    *,
    provider_label: str | None,
    results_already_persisted: bool = False,
) -> None:
    """向已存在的 Run 写入结果行（可选跳过，若已分块落库）并置为 success。"""

    repo = ScreeningRepository(engine)
    meta: dict[str, Any] = dict(batch.meta)
    ranking = CombinedRankingParams.from_env()
    with engine.begin() as conn:
        if not results_already_persisted:
            repo.bulk_insert_results(
                conn,
                run_id,
                _rows_from_screening_results(batch.results, ranking),
            )
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
