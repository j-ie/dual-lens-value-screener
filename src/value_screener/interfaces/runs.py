from __future__ import annotations

import logging
from typing import Annotated, Any, Literal

from fastapi import APIRouter, BackgroundTasks, Body, HTTPException, Query
from pydantic import BaseModel, Field, field_validator

from value_screener.application.batch_screening_workflow import run_batch_screen_background
from value_screener.application.company_detail_query import CompanyDetailQueryService
from value_screener.application.persist_screening_run import create_running_screening_run
from value_screener.application.result_enrichment import enrich_screening_result_row
from value_screener.domain.combined_ranking_params import CombinedRankingParams
from value_screener.domain.triple_composite_params import TripleCompositeParams
from value_screener.infrastructure.app_db import get_engine
from value_screener.infrastructure.settings import AShareIngestionSettings
from value_screener.infrastructure.result_cache import (
    cache_get_json,
    cache_key,
    cache_set_json,
    cache_ttl_seconds,
    industries_cache_fingerprint,
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
    combined_score: float | None = None
    third_lens_score: float | None = None
    third_lens: dict[str, Any] | None = None
    final_triple_score: float | None = None
    coverage_ok: bool = True
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


class RunIndustriesResponse(BaseModel):
    """`industry` 查询参数多选 OR；空行业为字面量 `__EMPTY__`（与 INDUSTRY_EMPTY_QUERY_VALUE 一致）。"""

    industries: list[str]


class CompanyDetailRunMeta(BaseModel):
    id: int
    status: str
    created_at: str | None
    finished_at: str | None


class LiveQuoteBlock(BaseModel):
    ok: bool
    fetched_at: str
    error: str | None = None
    data: dict[str, Any] | None = None


class CompanyFinancialsSection(BaseModel):
    income: list[dict[str, Any]]
    balance: list[dict[str, Any]]
    cashflow: list[dict[str, Any]]


class CompanyDetailResponse(BaseModel):
    run_id: int
    ts_code: str
    run: CompanyDetailRunMeta
    run_snapshot: dict[str, Any]
    reference: dict[str, Any] | None
    financials: CompanyFinancialsSection
    live_quote: LiveQuoteBlock


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


@router.get(
    "/runs/{run_id}/companies/{ts_code}/detail",
    response_model=CompanyDetailResponse,
)
def company_detail(
    run_id: int,
    ts_code: str,
    include_financial_payload: bool = Query(False, description="为 true 时财报行附带 payload JSON"),
    financial_limit: int = Query(12, ge=1, le=48, description="每表最多返回的报告期条数"),
) -> CompanyDetailResponse:
    """
    单公司详情：Run 内冻结筛分快照 + 主数据 + 三大表摘要 + 独立拉取的日 K 行情（与算分时刻无关）。
    ts_code 须为 TuShare 格式，如 600519.SH。
    """

    try:
        engine = get_engine()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    svc = CompanyDetailQueryService(engine)
    try:
        payload = svc.load(
            run_id,
            ts_code,
            include_financial_payload=include_financial_payload,
            financial_limit=financial_limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    err = payload.get("_error")
    if err == "run_not_found":
        raise HTTPException(status_code=404, detail="run 不存在")
    if err == "symbol_not_in_run":
        raise HTTPException(status_code=404, detail="该 run 中无此标的")

    lq = payload["live_quote"]
    return CompanyDetailResponse(
        run_id=int(payload["run_id"]),
        ts_code=str(payload["ts_code"]),
        run=CompanyDetailRunMeta.model_validate(payload["run"]),
        run_snapshot=payload["run_snapshot"],
        reference=payload.get("reference"),
        financials=CompanyFinancialsSection.model_validate(payload["financials"]),
        live_quote=LiveQuoteBlock.model_validate(lq),
    )


@router.get("/runs/{run_id}/result-industries", response_model=RunIndustriesResponse)
def list_run_industries(run_id: int) -> RunIndustriesResponse:
    """当前 run 结果集中出现的去重行业，供筛选下拉；空参考行业为 `__EMPTY__`。"""

    try:
        engine = get_engine()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    repo = ScreeningRepository(engine)
    with engine.connect() as conn:
        if repo.get_run(conn, run_id) is None:
            raise HTTPException(status_code=404, detail="run 不存在")
        industries = repo.list_distinct_industries_for_run(conn, run_id)
    return RunIndustriesResponse(industries=industries)


@router.get("/runs/{run_id}/results", response_model=PagedResults)
def paged_results(
    run_id: int,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    sort: Literal["buffett", "graham", "combined", "industry", "third_lens", "triple"] = "buffett",
    order: Literal["asc", "desc"] = "desc",
    industry: Annotated[list[str] | None, Query()] = None,
) -> PagedResults:
    try:
        engine = get_engine()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    ind_list = list(industry) if industry else []
    ranking = CombinedRankingParams.from_env()
    fp_parts: list[str] = []
    if sort == "combined":
        fp_parts.append(ranking.cache_fingerprint())
    if sort == "triple":
        fp_parts.append(TripleCompositeParams.from_env().cache_fingerprint())
    ind_fp = industries_cache_fingerprint(ind_list)
    if ind_fp:
        fp_parts.append(ind_fp)
    fp_merged = "|".join(fp_parts) if fp_parts else ""
    ck = cache_key(run_id, page, page_size, sort, order, filter_fingerprint=fp_merged)
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
            ranking=ranking if sort == "combined" else None,
            industries=ind_list if ind_list else None,
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
