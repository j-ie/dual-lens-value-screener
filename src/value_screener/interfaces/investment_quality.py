from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel, Field, model_validator

from value_screener.application.batch_screening_workflow import run_batch_screen_background
from value_screener.application.investment_master_summary import (
    InvestmentMasterSummaryError,
    InvestmentMasterUnavailableError,
    summarize_investment_master,
)
from value_screener.application.investment_quality_view import (
    attach_investment_quality_for_result_row,
    build_investment_quality_from_snapshot,
)
from value_screener.application.persist_screening_run import create_running_screening_run
from value_screener.application.result_enrichment import enrich_screening_result_row
from value_screener.domain.combined_ranking_params import CombinedRankingParams
from value_screener.domain.investment_quality import InvestmentQualityAnalyzer
from value_screener.domain.snapshot import StockFinancialSnapshot
from value_screener.infrastructure.app_db import get_engine
from value_screener.infrastructure.factory import build_composite_provider
from value_screener.infrastructure.fetch_types import SymbolFetchFailure
from value_screener.infrastructure.screening_repository import ScreeningRepository
from value_screener.infrastructure.settings import AShareIngestionSettings
from value_screener.infrastructure.symbol_normalize import to_ts_code

router = APIRouter(prefix="/api/v1/investment-quality", tags=["investment-quality"])


class SingleAnalyzeRequest(BaseModel):
    item: StockFinancialSnapshot | None = None
    symbol: str | None = Field(default=None, min_length=1, description="股票代码，仅传代码时后端自动拉取快照")
    industry: str | None = Field(default=None, description="可选行业标签，用于估值策略路由")

    @model_validator(mode="after")
    def validate_input(self) -> "SingleAnalyzeRequest":
        if self.item is None and (self.symbol is None or not self.symbol.strip()):
            raise ValueError("item 与 symbol 不能同时为空")
        return self


class SingleAnalyzeResponse(BaseModel):
    symbol: str
    investment_quality: dict[str, Any]


class BatchTriggerRequest(BaseModel):
    max_symbols: int | None = Field(default=None, ge=1, le=10000)


class BatchTriggerResponse(BaseModel):
    run_id: int
    status: str = "running"
    message: str | None = None


class InvestmentQualityRunItem(BaseModel):
    id: int
    status: str
    created_at: str
    finished_at: str | None
    universe_size: int | None
    snapshot_ok: int | None
    snapshot_failed: int | None
    progress_percent: int | None = None
    progress_current: int | None = None
    progress_total: int | None = None
    progress_phase: str | None = None
    progress_symbol: str | None = None


class InvestmentQualityResultItem(BaseModel):
    symbol: str
    display_name: str = ""
    industry: str = ""
    region: str = ""
    market_cap: float | None = None
    investment_quality: dict[str, Any]


class InvestmentQualityPagedResults(BaseModel):
    items: list[InvestmentQualityResultItem]
    total: int
    page: int
    page_size: int
    sort: str
    order: str


class InvestmentMasterSummaryRequest(BaseModel):
    symbol: str | None = Field(default=None, description="单公司模式：股票代码")
    run_id: int | None = Field(default=None, description="批量模式：run_id")
    industry: str | None = Field(default=None, description="可选行业")
    item: StockFinancialSnapshot | None = Field(default=None, description="可选：直接传快照")

    @model_validator(mode="after")
    def validate_mode(self) -> "InvestmentMasterSummaryRequest":
        has_single = self.item is not None or (self.symbol is not None and self.symbol.strip())
        has_run = self.run_id is not None and self.symbol is not None and self.symbol.strip()
        if not has_single and not has_run:
            raise ValueError("请提供 symbol（单公司）或 run_id+symbol（批量）")
        return self


class InvestmentMasterSummaryResponse(BaseModel):
    symbol: str
    mode: Literal["single", "run"]
    report_context: dict[str, Any]
    ai_summary: dict[str, Any]
    prompt_version: str
    model: str


@router.post("/single", response_model=SingleAnalyzeResponse)
def analyze_single(req: SingleAnalyzeRequest) -> SingleAnalyzeResponse:
    analyzer = InvestmentQualityAnalyzer()
    if req.item is not None:
        snap = req.item
    else:
        code = to_ts_code(str(req.symbol or "").strip())
        settings = AShareIngestionSettings.from_env()
        provider = build_composite_provider(settings)
        fetched = provider.fetch_snapshots([code])
        if not fetched:
            raise HTTPException(status_code=503, detail=f"未能获取 {code} 的财务快照")
        first = fetched[0]
        if isinstance(first, SymbolFetchFailure):
            raise HTTPException(
                status_code=503,
                detail=f"拉取 {code} 快照失败: {first.reason}",
            )
        snap = first
    iq = build_investment_quality_from_snapshot(
        analyzer, snap, industry=req.industry, ts_code=snap.symbol
    )
    return SingleAnalyzeResponse(symbol=snap.symbol, investment_quality=iq)


@router.post("/ai-summary", response_model=InvestmentMasterSummaryResponse)
def investment_master_ai_summary(req: InvestmentMasterSummaryRequest) -> InvestmentMasterSummaryResponse:
    analyzer = InvestmentQualityAnalyzer()
    mode: Literal["single", "run"] = "single"
    symbol = str(req.symbol or "").strip()
    context: dict[str, Any]

    if req.run_id is not None and symbol:
        mode = "run"
        try:
            engine = get_engine()
        except RuntimeError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        repo = ScreeningRepository(engine)
        with engine.connect() as conn:
            row = repo.get_result_row_for_run_symbol(
                conn,
                req.run_id,
                to_ts_code(symbol),
                ranking=CombinedRankingParams.from_env(),
            )
            run = repo.get_run(conn, req.run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="run 不存在")
        if row is None:
            raise HTTPException(status_code=404, detail="该 run 中无此 symbol")
        enriched = enrich_screening_result_row(row)
        decorated = attach_investment_quality_for_result_row(analyzer, enriched)
        iq = decorated.get("investment_quality")
        if not isinstance(iq, dict):
            raise HTTPException(status_code=500, detail="投资质量结果缺失")
        context = {
            "mode": "run",
            "run_id": req.run_id,
            "symbol": to_ts_code(symbol),
            "display_name": decorated.get("display_name"),
            "industry": decorated.get("industry"),
            "market_cap": decorated.get("market_cap"),
            "investment_quality": iq,
            "run_fact_json": decorated.get("run_fact_json"),
            "provenance": decorated.get("provenance"),
        }
        symbol = to_ts_code(symbol)
    else:
        single = analyze_single(
            SingleAnalyzeRequest(
                item=req.item,
                symbol=symbol if symbol else None,
                industry=req.industry,
            )
        )
        context = {
            "mode": "single",
            "symbol": single.symbol,
            "industry": req.industry,
            "investment_quality": single.investment_quality,
            "snapshot": (req.item.model_dump(mode="json") if req.item is not None else None),
        }
        symbol = single.symbol

    try:
        result = summarize_investment_master(context)
    except InvestmentMasterUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except InvestmentMasterSummaryError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return InvestmentMasterSummaryResponse(
        symbol=symbol,
        mode=mode,
        report_context=context,
        ai_summary=result.summary.model_dump(mode="json"),
        prompt_version=result.prompt_version,
        model=result.model,
    )


@router.post("/runs", response_model=BatchTriggerResponse, status_code=202)
def trigger_batch(
    background_tasks: BackgroundTasks,
    body: BatchTriggerRequest | None = None,
) -> BatchTriggerResponse:
    req = body if body is not None else BatchTriggerRequest()
    try:
        engine = get_engine()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    run_id = create_running_screening_run(
        engine,
        provider_label=None,
        meta={"job_kind": "investment_quality", "max_symbols": req.max_symbols},
    )
    background_tasks.add_task(run_batch_screen_background, run_id, req.max_symbols)
    return BatchTriggerResponse(
        run_id=run_id,
        message="投资质量批跑任务已入队，请轮询 runs 与 results 接口。",
    )


@router.get("/runs", response_model=list[InvestmentQualityRunItem])
def list_runs(limit: int = Query(50, ge=1, le=200)) -> list[InvestmentQualityRunItem]:
    try:
        engine = get_engine()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    repo = ScreeningRepository(engine)
    with engine.connect() as conn:
        rows = repo.list_runs(conn, limit=limit)
    out: list[InvestmentQualityRunItem] = []
    for r in rows:
        meta = r.meta_json or {}
        if meta.get("job_kind") != "investment_quality":
            continue
        out.append(
            InvestmentQualityRunItem(
                id=r.id,
                status=r.status,
                created_at=r.created_at.isoformat() if r.created_at else "",
                finished_at=r.finished_at.isoformat() if r.finished_at else None,
                universe_size=r.universe_size,
                snapshot_ok=r.snapshot_ok,
                snapshot_failed=r.snapshot_failed,
                progress_percent=_to_int(meta.get("progress_percent")),
                progress_current=_to_int(meta.get("progress_current")),
                progress_total=_to_int(meta.get("progress_total")),
                progress_phase=_to_str(meta.get("progress_phase")),
                progress_symbol=_to_str(meta.get("progress_symbol")),
            )
        )
    return out


@router.get("/runs/{run_id}", response_model=InvestmentQualityRunItem)
def get_run(run_id: int) -> InvestmentQualityRunItem:
    try:
        engine = get_engine()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    repo = ScreeningRepository(engine)
    with engine.connect() as conn:
        row = repo.get_run(conn, run_id)
    if row is None:
        raise HTTPException(status_code=404, detail="run 不存在")
    meta = row.meta_json or {}
    if meta.get("job_kind") != "investment_quality":
        raise HTTPException(status_code=404, detail="不是 investment-quality run")
    return InvestmentQualityRunItem(
        id=row.id,
        status=row.status,
        created_at=row.created_at.isoformat() if row.created_at else "",
        finished_at=row.finished_at.isoformat() if row.finished_at else None,
        universe_size=row.universe_size,
        snapshot_ok=row.snapshot_ok,
        snapshot_failed=row.snapshot_failed,
        progress_percent=_to_int(meta.get("progress_percent")),
        progress_current=_to_int(meta.get("progress_current")),
        progress_total=_to_int(meta.get("progress_total")),
        progress_phase=_to_str(meta.get("progress_phase")),
        progress_symbol=_to_str(meta.get("progress_symbol")),
    )


@router.get("/runs/{run_id}/results", response_model=InvestmentQualityPagedResults)
def paged_results(
    run_id: int,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    sort: Literal["combined", "market_cap"] = "combined",
    order: Literal["asc", "desc"] = "desc",
) -> InvestmentQualityPagedResults:
    try:
        engine = get_engine()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    repo = ScreeningRepository(engine)
    ranking = CombinedRankingParams.from_env() if sort == "combined" else None
    with engine.connect() as conn:
        run = repo.get_run(conn, run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="run 不存在")
        if (run.meta_json or {}).get("job_kind") != "investment_quality":
            raise HTTPException(status_code=404, detail="不是 investment-quality run")
        try:
            data = repo.page_results(
                conn,
                run_id,
                sort_key=sort,
                order=order,
                page=page,
                page_size=page_size,
                ranking=ranking,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    analyzer = InvestmentQualityAnalyzer()
    enriched_rows = [enrich_screening_result_row(x) for x in data.items]
    items: list[InvestmentQualityResultItem] = []
    for row in enriched_rows:
        decorated = attach_investment_quality_for_result_row(analyzer, row)
        iq = decorated.get("investment_quality")
        if not isinstance(iq, dict):
            continue
        items.append(
            InvestmentQualityResultItem(
                symbol=str(decorated.get("symbol") or ""),
                display_name=str(decorated.get("display_name") or ""),
                industry=str(decorated.get("industry") or ""),
                region=str(decorated.get("region") or ""),
                market_cap=_to_float(decorated.get("market_cap")),
                investment_quality=iq,
            )
        )
    return InvestmentQualityPagedResults(
        items=items,
        total=data.total,
        page=page,
        page_size=page_size,
        sort=sort,
        order=order,
    )


def _to_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _to_str(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s or None

