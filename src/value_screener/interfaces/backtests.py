from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel, Field, field_validator
from sqlalchemy.exc import OperationalError, ProgrammingError

from value_screener.application.backtest_engine import DefaultBacktestExecutor
from value_screener.application.backtest_service import BacktestApplicationService
from value_screener.domain.backtest import BacktestConfig
from value_screener.infrastructure.app_db import get_engine
from value_screener.infrastructure.backtest_repository import BacktestRepository
from value_screener.infrastructure.historical_price_repository import HistoricalPriceRepository
from value_screener.infrastructure.settings import AShareIngestionSettings

router = APIRouter(prefix="/api/v1/backtests", tags=["backtests"])


def _raise_if_backtest_table_missing(exc: Exception) -> None:
    text = str(exc).lower()
    if "backtest_job" in text and ("doesn't exist" in text or "no such table" in text):
        raise HTTPException(
            status_code=503,
            detail="回测表未初始化，请先执行数据库迁移（如 `alembic upgrade head`）后重试。",
        ) from exc
    if "backtest_result" in text and ("doesn't exist" in text or "no such table" in text):
        raise HTTPException(
            status_code=503,
            detail="回测结果表未初始化，请先执行数据库迁移（如 `alembic upgrade head`）后重试。",
        ) from exc


class BacktestCreateRequest(BaseModel):
    strategy_name: str = Field(default="investment_quality_score")
    start_date: str = Field(..., description="YYYY-MM-DD")
    end_date: str = Field(..., description="YYYY-MM-DD")
    rebalance_frequency: str = Field(default="monthly")
    holding_period_days: int = Field(default=20, ge=1, le=365)
    top_n: int | None = Field(default=None, ge=1, le=2000)
    top_quantile: float | None = Field(default=0.2, gt=0.0, le=1.0)
    benchmark: str = Field(default="000300.SH")
    transaction_cost_bps: int = Field(default=15, ge=0, le=500)
    filters: dict[str, Any] = Field(default_factory=dict)
    extras: dict[str, Any] = Field(default_factory=dict)
    run_sync: bool = Field(default=False, description="为 true 时创建后立即同步执行")

    @field_validator("start_date", "end_date")
    @classmethod
    def _validate_date(cls, value: str) -> str:
        import datetime as _dt

        _dt.datetime.strptime(value, "%Y-%m-%d")
        return value


class BacktestCreateResponse(BaseModel):
    job_id: int
    external_uuid: str
    status: str


@router.get("/coverage")
def get_backtest_coverage() -> dict[str, Any]:
    settings = AShareIngestionSettings.from_env()
    token = (settings.tushare_token or "").strip()
    if not token:
        raise HTTPException(status_code=503, detail="未配置 TUSHARE_TOKEN，无法获取回测可用区间")
    try:
        repo = HistoricalPriceRepository(token)
        start, end = repo.coverage_bounds()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"获取回测可用区间失败: {exc}") from exc
    return {"ok": True, "start_date": start, "end_date": end}


@router.post("", response_model=BacktestCreateResponse, status_code=202)
def create_backtest(body: BacktestCreateRequest) -> BacktestCreateResponse:
    try:
        engine = get_engine()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    svc = BacktestApplicationService(engine)
    cfg = BacktestConfig(
        strategy_name=body.strategy_name,
        start_date=body.start_date,
        end_date=body.end_date,
        rebalance_frequency=body.rebalance_frequency,
        holding_period_days=body.holding_period_days,
        top_n=body.top_n,
        top_quantile=body.top_quantile,
        benchmark=body.benchmark,
        transaction_cost_bps=body.transaction_cost_bps,
        filters=body.filters,
        extras=body.extras,
    )
    try:
        job = svc.create_job(cfg)
    except (ProgrammingError, OperationalError) as exc:
        _raise_if_backtest_table_missing(exc)
        raise
    if body.run_sync:
        executor = DefaultBacktestExecutor(engine)
        try:
            svc.execute_job(job.id, executor)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=f"回测执行失败: {exc}") from exc
        refreshed = svc.get_job(job.id)
        if refreshed is not None:
            job = refreshed
    return BacktestCreateResponse(job_id=job.id, external_uuid=job.external_uuid, status=job.status.value)


@router.post("/{job_id}/execute", response_model=BacktestCreateResponse)
def execute_backtest(job_id: int) -> BacktestCreateResponse:
    try:
        engine = get_engine()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    svc = BacktestApplicationService(engine)
    executor = DefaultBacktestExecutor(engine)
    try:
        svc.execute_job(job_id, executor)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (ProgrammingError, OperationalError) as exc:
        _raise_if_backtest_table_missing(exc)
        raise HTTPException(status_code=500, detail=f"回测执行失败: {exc}") from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"回测执行失败: {exc}") from exc
    try:
        job = svc.get_job(job_id)
    except (ProgrammingError, OperationalError) as exc:
        _raise_if_backtest_table_missing(exc)
        raise
    if job is None:
        raise HTTPException(status_code=404, detail="backtest job 不存在")
    return BacktestCreateResponse(job_id=job.id, external_uuid=job.external_uuid, status=job.status.value)


@router.delete("/{job_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_backtest(job_id: int) -> None:
    try:
        engine = get_engine()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    svc = BacktestApplicationService(engine)
    try:
        svc.delete_job(job_id)
    except ValueError as exc:
        detail = str(exc)
        if "执行中" in detail:
            raise HTTPException(status_code=409, detail=detail) from exc
        raise HTTPException(status_code=404, detail=detail) from exc
    except (ProgrammingError, OperationalError) as exc:
        _raise_if_backtest_table_missing(exc)
        raise


@router.get("/{job_id}")
def get_backtest(job_id: int) -> dict[str, Any]:
    try:
        engine = get_engine()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    svc = BacktestApplicationService(engine)
    try:
        payload = svc.get_result_summary(job_id)
    except (ProgrammingError, OperationalError) as exc:
        _raise_if_backtest_table_missing(exc)
        raise
    if payload is None:
        raise HTTPException(status_code=404, detail="backtest job 不存在")
    return payload


@router.get("")
def list_backtests(
    limit: int = Query(50, ge=1, le=200),
    status: str | None = Query(default=None, description="按状态过滤：pending/running/success/failed"),
) -> list[dict[str, Any]]:
    try:
        engine = get_engine()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    repo = BacktestRepository(engine)
    try:
        with engine.connect() as conn:
            jobs = repo.list_jobs(conn, limit=limit)
    except (ProgrammingError, OperationalError) as exc:
        _raise_if_backtest_table_missing(exc)
        raise
    out: list[dict[str, Any]] = []
    status_filter = (status or "").strip().lower()
    for job in jobs:
        if status_filter and job.status.value != status_filter:
            continue
        out.append(
            {
                "id": job.id,
                "external_uuid": job.external_uuid,
                "strategy_name": job.strategy_name,
                "status": job.status.value,
                "created_at": job.created_at.isoformat(),
                "started_at": job.started_at.isoformat() if job.started_at else None,
                "finished_at": job.finished_at.isoformat() if job.finished_at else None,
                "error": str(job.meta.get("error") or "") if isinstance(job.meta, dict) else "",
                "meta": dict(job.meta),
                "config": {
                    "strategy_name": job.config.strategy_name,
                    "start_date": job.config.start_date,
                    "end_date": job.config.end_date,
                    "rebalance_frequency": job.config.rebalance_frequency,
                    "holding_period_days": job.config.holding_period_days,
                    "top_n": job.config.top_n,
                    "top_quantile": job.config.top_quantile,
                    "benchmark": job.config.benchmark,
                    "transaction_cost_bps": job.config.transaction_cost_bps,
                    "filters": dict(job.config.filters),
                    "extras": dict(job.config.extras),
                },
            }
        )
    return out

