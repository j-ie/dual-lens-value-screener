from __future__ import annotations

import logging
from typing import Annotated, Any, Literal

from fastapi import APIRouter, BackgroundTasks, Body, HTTPException, Query
from pydantic import BaseModel, Field, field_validator

from value_screener.application.batch_screening_workflow import (
    run_batch_screen_background,
    run_post_full_batch_pipeline_background,
)
from value_screener.application.post_full_batch_pipeline import is_post_pipeline_busy
from value_screener.application.company_ai_analysis import (
    CompanyAiAnalysisApplicationService,
    CompanyAiDetailError,
    CompanyAiTimeoutError,
    CompanyAiUnavailableError,
    CompanyAiUpstreamError,
)
from value_screener.application.company_detail_query import CompanyDetailQueryService
from value_screener.application.persist_screening_run import create_running_screening_run
from value_screener.application.result_enrichment import enrich_screening_result_row
from value_screener.domain.combined_ranking_params import CombinedRankingParams
from value_screener.domain.triple_composite_params import TripleCompositeParams
from value_screener.infrastructure.app_db import get_engine
from value_screener.infrastructure.company_ai_analysis_repository import CompanyAiAnalysisRepository
from value_screener.infrastructure.settings import AShareIngestionSettings
from value_screener.infrastructure.result_cache import (
    ai_results_cache_fingerprint,
    company_name_cache_fingerprint,
    cache_get_json,
    cache_key,
    cache_set_json,
    cache_ttl_seconds,
    industries_cache_fingerprint,
    valuation_filters_cache_fingerprint,
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
    post_pipeline_phase: str | None = None
    post_pipeline_started_at: str | None = None
    post_pipeline_activity_at: str | None = None
    post_pipeline_ai_index: int | None = None
    post_pipeline_ai_total: int | None = None
    post_pipeline_ai_symbol: str | None = None
    post_pipeline_ai_ok: int | None = None
    post_pipeline_ai_failed: int | None = None
    post_pipeline_ai_skip_reason: str | None = None
    post_pipeline_ai_symbol_pick: str | None = None
    post_pipeline_finished_at: str | None = None


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
    ai_persist_id: int | None = None
    ai_score: float | None = None
    opportunity_score: float | None = None
    ai_analysis_date: str | None = None
    ai_run_id: int | None = None
    ai_summary_preview: str | None = None
    market_cap: float | None = None
    pe_ttm: float | None = None
    net_income_ttm: float | None = None
    dv_ratio: float | None = Field(None, description="股息率（%），年报口径")
    dv_ttm: float | None = Field(None, description="股息率 TTM（%）")


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


class PersistedAiAnalysisBlock(BaseModel):
    """当日历日最新一条落库 AI 分析（含分析时点的 DCF 快照）。"""

    ts_code: str
    analysis_date: str
    run_id: int | None = None
    ai_score: float
    ai_score_rationale: str | None = None
    opportunity_score: float | None = None
    opportunity_score_rationale: str | None = None
    summary: str
    key_metrics_commentary: str
    risks: str
    alignment_with_scores: str
    narrative_markdown: str
    dcf_snapshot: dict[str, Any] | None = None
    dcf_ok: bool | None = None
    dcf_headline: str | None = None
    context_hash: str = ""
    prompt_version: str = ""
    model: str = ""
    generated_at: str | None = None


class CompanyDetailResponse(BaseModel):
    run_id: int
    ts_code: str
    run: CompanyDetailRunMeta
    run_snapshot: dict[str, Any]
    reference: dict[str, Any] | None
    financials: CompanyFinancialsSection
    live_quote: LiveQuoteBlock
    dcf: dict[str, Any] | None = Field(
        default=None,
        description="简化 DCF 块；仅当 include_dcf=1 时出现。ok=true 时含 assumptions/values",
    )
    persisted_ai_analysis: PersistedAiAnalysisBlock | None = Field(
        default=None,
        description="当 include_persisted_ai=1 时附带：该代码最新落库 AI 分析（含 dcf 快照）",
    )


class CompanyAiAnalysisMeta(BaseModel):
    context_hash: str
    prompt_version: str
    model: str
    generated_at: str
    cached: bool = False
    analysis_date: str | None = None


class CompanyAiAnalysisResponse(BaseModel):
    summary: str
    key_metrics_commentary: str
    risks: str
    alignment_with_scores: str
    narrative_markdown: str
    ai_score: float
    ai_score_rationale: str = ""
    opportunity_score: float
    opportunity_score_rationale: str = ""
    dcf_snapshot: dict[str, Any] | None = Field(
        default=None,
        description="本次分析上下文中的 DCF 块（与落库 dcf_json 一致）",
    )
    meta: CompanyAiAnalysisMeta


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


class PostPipelineAcceptedResponse(BaseModel):
    """手动后置流水线已入队（后台线程）；请轮询 GET /runs 查看 meta 中的 post_pipeline_* 字段。"""

    run_id: int
    status: str = "accepted"
    message: str


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


def _meta_str(meta: dict[str, Any] | None, key: str) -> str | None:
    if not meta or key not in meta:
        return None
    v = meta.get(key)
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _run_to_item(r: RunRow) -> RunListItem:
    m = r.meta_json
    sym_raw = m.get("progress_symbol") if m else None
    sym = str(sym_raw).strip() if sym_raw is not None else ""
    phase_s: str | None = None
    if m and m.get("progress_phase") is not None:
        phase_s = str(m.get("progress_phase")).strip() or None
    pp_done = m.get("post_pipeline_finished_at") if m else None
    pp_finished: str | None = None
    if pp_done is not None:
        pp_finished = pp_done.isoformat() if hasattr(pp_done, "isoformat") else str(pp_done).strip() or None
    pp_start = m.get("post_pipeline_started_at") if m else None
    pp_started: str | None = None
    if pp_start is not None:
        pp_started = pp_start.isoformat() if hasattr(pp_start, "isoformat") else str(pp_start).strip() or None
    pp_act = m.get("post_pipeline_activity_at") if m else None
    pp_activity: str | None = None
    if pp_act is not None:
        pp_activity = pp_act.isoformat() if hasattr(pp_act, "isoformat") else str(pp_act).strip() or None
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
        post_pipeline_phase=_meta_str(m, "post_pipeline_phase"),
        post_pipeline_started_at=pp_started,
        post_pipeline_activity_at=pp_activity,
        post_pipeline_ai_index=_meta_int(m, "post_pipeline_ai_index"),
        post_pipeline_ai_total=_meta_int(m, "post_pipeline_ai_total"),
        post_pipeline_ai_symbol=_meta_str(m, "post_pipeline_ai_symbol"),
        post_pipeline_ai_ok=_meta_int(m, "post_pipeline_ai_ok"),
        post_pipeline_ai_failed=_meta_int(m, "post_pipeline_ai_failed"),
        post_pipeline_ai_skip_reason=_meta_str(m, "post_pipeline_ai_skip_reason"),
        post_pipeline_ai_symbol_pick=_meta_str(m, "post_pipeline_ai_symbol_pick"),
        post_pipeline_finished_at=pp_finished,
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


def _max_symbols_from_run_meta(meta: dict[str, Any] | None) -> int | None:
    """从既有 Run 的 meta 解析 max_symbols；非法则抛 HTTP 400。"""

    if not meta:
        return None
    raw = meta.get("max_symbols")
    if raw is None:
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=400,
            detail="源 Run 的 meta.max_symbols 不是合法整数",
        ) from exc
    if not (1 <= value <= 10_000):
        raise HTTPException(
            status_code=400,
            detail="源 Run 的 meta.max_symbols 须在 1～10000 之间，或为 null 表示全市场",
        )
    return value


@router.post(
    "/runs/{run_id}/requeue-batch-screen",
    response_model=BatchScreenTriggerResponse,
    status_code=202,
)
def requeue_batch_screen(
    run_id: int,
    background_tasks: BackgroundTasks,
) -> BatchScreenTriggerResponse:
    """
    按指定 Run 记录过的上限（meta.max_symbols，缺省为全市场）再排队一次异步批跑，生成新的 run_id。
    允许源 Run 仍为 running：用于进程崩溃后留下的僵尸「进行中」记录；若确有后台在写同一 run_id，新任务与其并行，请自行避免混淆。
    """

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

    repo = ScreeningRepository(engine)
    with engine.connect() as conn:
        row = repo.get_run(conn, run_id)
    if row is None:
        raise HTTPException(status_code=404, detail="run 不存在")

    max_symbols = _max_symbols_from_run_meta(row.meta_json)
    new_meta: dict[str, Any] = {
        "async_batch": True,
        "max_symbols": max_symbols,
        "requeued_from_run_id": run_id,
    }
    new_run_id = create_running_screening_run(
        engine,
        provider_label=None,
        meta=new_meta,
    )
    background_tasks.add_task(run_batch_screen_background, new_run_id, max_symbols)

    tail = "请轮询 GET /api/v1/runs 直至状态为 success 或 failed。"
    if row.status == "running":
        tail += " 源 Run 仍为「进行中」；若为僵尸任务可自行删除该条记录。"
    msg = f"已按 Run #{run_id} 的参数排队新批跑（Run #{new_run_id}）。{tail}"
    return BatchScreenTriggerResponse(
        run_id=new_run_id,
        status="running",
        message=msg,
    )


@router.delete("/runs/{run_id}", status_code=204)
def delete_screening_run(run_id: int) -> None:
    """
    删除一次筛选批次：本 run 下 screening_result 由外键级联删除；
    同时删除 company_ai_analysis 中 run_id 指向本批次的记录（落库时带了 run 关联的分析）。
    允许删除 status=running：用于僵尸记录；若确有后台在写该 run_id，可能导致其写库报错（仅日志）。
    不删除：financial_snapshot、三大财报 fs_*、证券主数据等（非按单次 run 归属的共享数据）。
    """

    try:
        engine = get_engine()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    repo = ScreeningRepository(engine)
    ai_repo = CompanyAiAnalysisRepository(engine)
    deleted = False
    with engine.begin() as conn:
        row = repo.get_run(conn, run_id)
        if row is None:
            raise HTTPException(status_code=404, detail="run 不存在")
        ai_repo.delete_by_run_id(conn, run_id)
        deleted = repo.delete_run(conn, run_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="run 不存在")


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


@router.post(
    "/runs/{run_id}/post-pipeline",
    response_model=PostPipelineAcceptedResponse,
    status_code=202,
)
def trigger_post_pipeline(
    run_id: int,
    background_tasks: BackgroundTasks,
) -> PostPipelineAcceptedResponse:
    """
    手动触发后置流水线：刷新第三套/三元综合分，对综合分 Top N（环境变量）逐只算 DCF 并调用 AI 落库。
    仅允许 status=success；若已有流水线在执行中则 409。
    """

    try:
        engine = get_engine()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    repo = ScreeningRepository(engine)
    with engine.connect() as conn:
        row = repo.get_run(conn, run_id)
    if row is None:
        raise HTTPException(status_code=404, detail="run 不存在")
    if row.status != "success":
        raise HTTPException(
            status_code=400,
            detail="仅能在批跑成功（status=success）后触发后置流水线",
        )
    meta = row.meta_json or {}
    if is_post_pipeline_busy(meta):
        raise HTTPException(
            status_code=409,
            detail="该 Run 的后置流水线正在执行中，请稍后在列表中查看进度",
        )

    background_tasks.add_task(run_post_full_batch_pipeline_background, run_id)
    return PostPipelineAcceptedResponse(
        run_id=run_id,
        status="accepted",
        message="已排队后置任务（第三套/三元、Top N DCF+AI），请轮询本 Run 的 post_pipeline_* 字段",
    )


@router.get(
    "/runs/{run_id}/companies/{ts_code}/detail",
    response_model=CompanyDetailResponse,
)
def company_detail(
    run_id: int,
    ts_code: str,
    include_financial_payload: bool = Query(False, description="为 true 时财报行附带 payload JSON"),
    financial_limit: int = Query(12, ge=1, le=48, description="每表最多返回的报告期条数"),
    include_dcf: bool = Query(False, description="为 true 时计算并返回 dcf（需启用 VALUE_SCREENER_DCF_ENABLED）"),
    include_persisted_ai: bool = Query(
        False,
        description="为 true 时附带该代码最新落库 AI 分析（含 dcf 快照）",
    ),
    dcf_wacc: float | None = Query(None, ge=0.04, le=0.25, description="覆盖 WACC，缺省读环境变量"),
    dcf_stage1_growth: float | None = Query(
        None, ge=-0.05, le=0.20, description="覆盖预测期增长率 g，缺省读环境变量"
    ),
    dcf_terminal_growth: float | None = Query(
        None, ge=-0.02, le=0.06, description="覆盖永续增长率 g_terminal，缺省读环境变量"
    ),
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
            include_dcf=include_dcf,
            dcf_wacc=dcf_wacc,
            dcf_stage1_growth=dcf_stage1_growth,
            dcf_terminal_growth=dcf_terminal_growth,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    err = payload.get("_error")
    if err == "run_not_found":
        raise HTTPException(status_code=404, detail="run 不存在")
    if err == "symbol_not_in_run":
        raise HTTPException(status_code=404, detail="该 run 中无此标的")

    lq = payload["live_quote"]
    persisted: PersistedAiAnalysisBlock | None = None
    if include_persisted_ai:
        code = str(payload["ts_code"]).strip()
        ai_repo = CompanyAiAnalysisRepository(engine)
        with engine.connect() as conn:
            row = ai_repo.fetch_latest_by_ts_code(conn, code)
        if row is not None:
            persisted = PersistedAiAnalysisBlock.model_validate(row)

    return CompanyDetailResponse(
        run_id=int(payload["run_id"]),
        ts_code=str(payload["ts_code"]),
        run=CompanyDetailRunMeta.model_validate(payload["run"]),
        run_snapshot=payload["run_snapshot"],
        reference=payload.get("reference"),
        financials=CompanyFinancialsSection.model_validate(payload["financials"]),
        live_quote=LiveQuoteBlock.model_validate(lq),
        dcf=payload.get("dcf") if include_dcf else None,
        persisted_ai_analysis=persisted,
    )


@router.post(
    "/runs/{run_id}/companies/{ts_code}/ai-analysis",
    response_model=CompanyAiAnalysisResponse,
)
def company_ai_analysis(run_id: int, ts_code: str) -> CompanyAiAnalysisResponse:
    """
    详情页触发的 AI 分析：基于与 detail 同源的结构化上下文调用大模型；未启用或缺配置时返回 503。
    """

    try:
        engine = get_engine()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    svc = CompanyAiAnalysisApplicationService(engine)
    try:
        payload = svc.analyze(run_id, ts_code)
    except CompanyAiDetailError as exc:
        if exc.code == "run_not_found":
            raise HTTPException(status_code=404, detail="run 不存在") from exc
        if exc.code == "symbol_not_in_run":
            raise HTTPException(status_code=404, detail="该 run 中无此标的") from exc
        if exc.code == "bad_ts_code":
            raise HTTPException(status_code=400, detail="非法 ts_code") from exc
        raise HTTPException(status_code=500, detail="AI 详情聚合失败") from exc
    except CompanyAiUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except CompanyAiTimeoutError as exc:
        raise HTTPException(status_code=504, detail=str(exc)) from exc
    except CompanyAiUpstreamError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return CompanyAiAnalysisResponse.model_validate(payload)


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
    sort: Literal[
        "buffett",
        "graham",
        "combined",
        "industry",
        "third_lens",
        "triple",
        "ai_score",
        "market_cap",
        "dividend_yield",
    ] = "combined",
    order: Literal["asc", "desc"] = "desc",
    industry: Annotated[list[str] | None, Query()] = None,
    has_ai_analysis: bool | None = Query(
        None,
        description="为 true 时仅保留已有持久化 AI 分析的标的",
    ),
    ai_score_min: float | None = Query(None, ge=0.0, le=100.0, description="持久化 AI 分下限"),
    company_name: str | None = Query(
        None,
        max_length=128,
        description="公司名称/全称/ts_code 模糊匹配（子串）",
    ),
    market_cap_min: float | None = Query(
        None,
        ge=0.0,
        description="总市值下限（元），含端点；仅保留有市值列的记录",
    ),
    market_cap_max: float | None = Query(
        None,
        ge=0.0,
        description="总市值上限（元），含端点",
    ),
    dividend_yield_min: float | None = Query(
        None,
        ge=0.0,
        le=100.0,
        description="股息率下限（与列表一致，单位百分点；优先 dv_ttm 否则 dv_ratio）",
    ),
    dividend_yield_max: float | None = Query(
        None,
        ge=0.0,
        le=100.0,
        description="股息率上限（单位百分点）",
    ),
) -> PagedResults:
    try:
        engine = get_engine()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if market_cap_min is not None and market_cap_max is not None and market_cap_min > market_cap_max:
        raise HTTPException(status_code=400, detail="market_cap_min 不能大于 market_cap_max")
    if (
        dividend_yield_min is not None
        and dividend_yield_max is not None
        and dividend_yield_min > dividend_yield_max
    ):
        raise HTTPException(status_code=400, detail="dividend_yield_min 不能大于 dividend_yield_max")

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
    afp = ai_results_cache_fingerprint(has_ai_analysis, ai_score_min)
    if afp:
        fp_parts.append(afp)
    cn_fp = company_name_cache_fingerprint(company_name)
    if cn_fp:
        fp_parts.append(cn_fp)
    vfp = valuation_filters_cache_fingerprint(
        market_cap_min,
        market_cap_max,
        dividend_yield_min,
        dividend_yield_max,
    )
    if vfp:
        fp_parts.append(vfp)
    fp_merged = "|".join(fp_parts) if fp_parts else ""
    ck = cache_key(run_id, page, page_size, sort, order, filter_fingerprint=fp_merged)
    ttl = cache_ttl_seconds()

    repo = ScreeningRepository(engine)
    with engine.connect() as conn:
        run_row = repo.get_run(conn, run_id)
        if run_row is None:
            raise HTTPException(status_code=404, detail="run 不存在")
        is_running = run_row.status == "running"

    if not is_running:
        cached = cache_get_json(ck)
        if cached is not None:
            return PagedResults.model_validate(cached)

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
            has_ai_analysis=has_ai_analysis,
            ai_score_min=ai_score_min,
            company_name=company_name,
            market_cap_min=market_cap_min,
            market_cap_max=market_cap_max,
            dividend_yield_min=dividend_yield_min,
            dividend_yield_max=dividend_yield_max,
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
    if not is_running and ttl > 0:
        cache_set_json(ck, payload.model_dump(mode="json"), ttl)
    return payload
