from __future__ import annotations

import logging
from typing import Any, Literal

from fastapi import APIRouter, BackgroundTasks, Body, HTTPException, Query
from pydantic import BaseModel, Field, field_validator

from value_screener.application.batch_screening_workflow import run_batch_screen_background
from value_screener.application.persist_screening_run import create_running_screening_run
from value_screener.application.result_enrichment import enrich_screening_result_row
from value_screener.infrastructure.app_db import get_engine
from value_screener.infrastructure.settings import AShareIngestionSettings
from value_screener.infrastructure.result_cache import (
    cache_get_json,
    cache_key,
    cache_set_json,
    cache_ttl_seconds,
)
from value_screener.infrastructure.screening_repository import RunRow, ScreeningRepository

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["screening-runs"])


class RunListItem(BaseModel):
    id: int
    external_uuid: str
    status: str
    created_at: str
    finished_at: str | None
    universe_size: int | None
    snapshot_ok: int | None
    snapshot_failed: int | None
    provider_label: str | None
    progress_percent: int | None = None
    progress_current: int | None = None
    progress_total: int | None = None
    progress_phase: str | None = None
    progress_symbol: str | None = None


class ResultItem(BaseModel):
    symbol: str
    graham_score: float
    buffett_score: float
    graham: dict[str, Any]
    buffett: dict[str, Any]
    provenance: dict[str, Any] | None
    display_name: str = ""
    company_full_name: str | None = None
    industry: str = ""
    region: str = ""
    score_explanation_zh: str = ""
    trade_cal_date: str | None = None
    financials_end_date: str | None = None
    data_source: str | None = None


class PagedResults(BaseModel):
    items: list[ResultItem]
    total: int
    page: int
    page_size: int
    sort: str
    order: str


class BatchScreenTriggerRequest(BaseModel):
    """
    一键批跑：主 TuShare、备 AkShare。
    省略 max_symbols 或传 null 表示处理全市场枚举；正整数表示最多处理 N 只（1～10000）。
    """

    max_symbols: int | None = Field(
        default=None,
        description="全市场时不传或 null；否则 1～10000",
    )

    @field_validator("max_symbols")
    @classmethod
    def validate_max_symbols(cls, value: int | None) -> int | None:
        if value is not None and not (1 <= value <= 10_000):
            raise ValueError("max_symbols 须在 1～10000 之间，或使用 null 表示全市场")
        return value


class BatchScreenTriggerResponse(BaseModel):
    """异步接受：立即返回 run_id；完成后见 GET /api/v1/runs 或 /runs/{id}。"""

    run_id: int
    status: str = "running"
    universe_requested: int | None = None
    snapshot_ok: int | None = None
    snapshot_failed: int | None = None
    provider_label: str | None = None
    message: str | None = None


def _meta_int(meta: dict[str, Any] | None, key: str) -> int | None:
    if not meta or key not in meta:
        return None
    v = meta.get(key)
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _run_to_item(r: RunRow) -> RunListItem:
    m = r.meta_json
    sym_raw = m.get("progress_symbol") if m else None
    sym = str(sym_raw).strip() if sym_raw is not None else ""
    phase_s: str | None = None
    if m and m.get("progress_phase") is not None:
        phase_s = str(m.get("progress_phase")).strip() or None
    return RunListItem(
        id=r.id,
        external_uuid=r.external_uuid,
        status=r.status,
        created_at=r.created_at.isoformat() if r.created_at else "",
        finished_at=r.finished_at.isoformat() if r.finished_at else None,
        universe_size=r.universe_size,
        snapshot_ok=r.snapshot_ok,
        snapshot_failed=r.snapshot_failed,
        provider_label=r.provider_label,
        progress_percent=_meta_int(m, "progress_percent"),
        progress_current=_meta_int(m, "progress_current"),
        progress_total=_meta_int(m, "progress_total"),
        progress_phase=phase_s,
        progress_symbol=sym or None,
    )


@router.post(
    "/runs/batch-screen",
    response_model=BatchScreenTriggerResponse,
    status_code=202,
)
def trigger_batch_screen(
    background_tasks: BackgroundTasks,
    body: BatchScreenTriggerRequest | None = Body(default=None),
) -> BatchScreenTriggerResponse:
    """
    异步批跑：立即返回 202 与 run_id，后台写入结果；请轮询 runs 列表直至 status 非 running。
    """

    req = body if body is not None else BatchScreenTriggerRequest()

    base = AShareIngestionSettings.from_env()
    token = (base.tushare_token or "").strip()
    if not token:
        raise HTTPException(
            status_code=400,
            detail="未配置 TUSHARE_TOKEN：请在环境变量或项目根 .env 中设置后再试。",
        )

    try:
        engine = get_engine()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    run_id = create_running_screening_run(
        engine,
        provider_label=None,
        meta={"async_batch": True, "max_symbols": req.max_symbols},
    )
    background_tasks.add_task(run_batch_screen_background, run_id, req.max_symbols)

    return BatchScreenTriggerResponse(
        run_id=run_id,
        status="running",
        message="批跑已在后台执行，请轮询 GET /api/v1/runs 直至该 run 状态为 success 或 failed",
    )


@router.get("/runs", response_model=list[RunListItem])
def list_runs(limit: int = Query(50, ge=1, le=200)) -> list[RunListItem]:
    try:
        engine = get_engine()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    repo = ScreeningRepository(engine)
    with engine.connect() as conn:
        rows = repo.list_runs(conn, limit=limit)
    return [_run_to_item(r) for r in rows]


@router.get("/runs/{run_id}", response_model=RunListItem)
def get_run(run_id: int) -> RunListItem:
    try:
        engine = get_engine()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    repo = ScreeningRepository(engine)
    with engine.connect() as conn:
        row = repo.get_run(conn, run_id)
    if row is None:
        raise HTTPException(status_code=404, detail="run 不存在")
    return _run_to_item(row)


@router.get("/runs/{run_id}/results", response_model=PagedResults)
def paged_results(
    run_id: int,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    sort: Literal["buffett", "graham"] = "buffett",
    order: Literal["asc", "desc"] = "desc",
) -> PagedResults:
    try:
        engine = get_engine()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    ck = cache_key(run_id, page, page_size, sort, order)
    ttl = cache_ttl_seconds()
    cached = cache_get_json(ck)
    if cached is not None:
        return PagedResults.model_validate(cached)

    repo = ScreeningRepository(engine)
    with engine.connect() as conn:
        page_data = repo.page_results(
            conn,
            run_id,
            sort_key=sort,
            order=order,
            page=page,
            page_size=page_size,
        )

    enriched = [enrich_screening_result_row(x) for x in page_data.items]
    payload = PagedResults(
        items=[ResultItem.model_validate(x) for x in enriched],
        total=page_data.total,
        page=page,
        page_size=page_size,
        sort=sort,
        order=order,
    )
    cache_set_json(ck, payload.model_dump(mode="json"), ttl)
    return payload
