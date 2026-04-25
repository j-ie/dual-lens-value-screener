from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import delete, insert, select, update
from sqlalchemy.engine import Connection, Engine

from value_screener.domain.backtest import (
    BacktestConfig,
    BacktestJob,
    BacktestJobStatus,
    BacktestResult,
)
from value_screener.infrastructure.screening_schema import backtest_job, backtest_result


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class BacktestRepository:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    @staticmethod
    def _to_job(row: Any) -> BacktestJob:
        config_raw = row.config_json if isinstance(row.config_json, dict) else {}
        return BacktestJob(
            id=int(row.id),
            external_uuid=str(row.external_uuid),
            strategy_name=str(row.strategy_name),
            status=BacktestJobStatus(str(row.status)),
            config=BacktestConfig(
                strategy_name=str(config_raw.get("strategy_name") or row.strategy_name),
                start_date=str(config_raw.get("start_date") or ""),
                end_date=str(config_raw.get("end_date") or ""),
                rebalance_frequency=str(config_raw.get("rebalance_frequency") or "monthly"),
                holding_period_days=int(config_raw.get("holding_period_days") or 20),
                top_n=int(config_raw["top_n"]) if config_raw.get("top_n") is not None else None,
                top_quantile=(
                    float(config_raw["top_quantile"])
                    if config_raw.get("top_quantile") is not None
                    else None
                ),
                benchmark=str(config_raw.get("benchmark") or "000300.SH"),
                transaction_cost_bps=int(config_raw.get("transaction_cost_bps") or 15),
                filters=config_raw.get("filters") if isinstance(config_raw.get("filters"), dict) else {},
                extras=config_raw.get("extras") if isinstance(config_raw.get("extras"), dict) else {},
            ),
            meta=row.meta_json if isinstance(row.meta_json, dict) else {},
            created_at=row.created_at,
            started_at=row.started_at,
            finished_at=row.finished_at,
        )

    @staticmethod
    def _to_result(row: Any) -> BacktestResult:
        return BacktestResult(
            id=int(row.id),
            job_id=int(row.job_id),
            summary=row.summary_json if isinstance(row.summary_json, dict) else {},
            metrics=row.metrics_json if isinstance(row.metrics_json, dict) else {},
            curve=row.curve_json if isinstance(row.curve_json, dict) else None,
            diagnostics=row.diagnostics_json if isinstance(row.diagnostics_json, dict) else None,
            created_at=row.created_at,
            updated_at=row.updated_at,
        )

    def create_job(self, conn: Connection, config: BacktestConfig, meta: dict[str, Any] | None = None) -> BacktestJob:
        now = utc_now()
        payload = {
            "strategy_name": config.strategy_name,
            "start_date": config.start_date,
            "end_date": config.end_date,
            "rebalance_frequency": config.rebalance_frequency,
            "holding_period_days": config.holding_period_days,
            "top_n": config.top_n,
            "top_quantile": config.top_quantile,
            "benchmark": config.benchmark,
            "transaction_cost_bps": config.transaction_cost_bps,
            "filters": dict(config.filters),
            "extras": dict(config.extras),
        }
        values = dict(
            external_uuid=str(uuid.uuid4()),
            strategy_name=config.strategy_name,
            status=BacktestJobStatus.PENDING.value,
            config_json=payload,
            meta_json=dict(meta or {}),
            created_at=now,
            started_at=None,
            finished_at=None,
        )
        if conn.engine.dialect.name == "sqlite":
            next_id = conn.execute(select(backtest_job.c.id).order_by(backtest_job.c.id.desc()).limit(1)).scalar()
            values["id"] = int(next_id or 0) + 1
        conn.execute(insert(backtest_job).values(**values))
        row = conn.execute(select(backtest_job).order_by(backtest_job.c.id.desc()).limit(1)).first()
        if row is None:
            raise RuntimeError("backtest_job 插入后未读到行")
        return self._to_job(row)

    def mark_running(self, conn: Connection, job_id: int) -> None:
        conn.execute(
            update(backtest_job)
            .where(backtest_job.c.id == job_id)
            .values(status=BacktestJobStatus.RUNNING.value, started_at=utc_now())
        )

    def mark_finished(
        self,
        conn: Connection,
        job_id: int,
        *,
        success: bool,
        meta: dict[str, Any] | None = None,
    ) -> None:
        status = BacktestJobStatus.SUCCESS if success else BacktestJobStatus.FAILED
        values: dict[str, Any] = {
            "status": status.value,
            "finished_at": utc_now(),
        }
        if meta is not None:
            values["meta_json"] = dict(meta)
        conn.execute(update(backtest_job).where(backtest_job.c.id == job_id).values(**values))

    def get_job(self, conn: Connection, job_id: int) -> BacktestJob | None:
        row = conn.execute(select(backtest_job).where(backtest_job.c.id == job_id)).first()
        if row is None:
            return None
        return self._to_job(row)

    def list_jobs(self, conn: Connection, limit: int = 50) -> list[BacktestJob]:
        rows = conn.execute(select(backtest_job).order_by(backtest_job.c.id.desc()).limit(limit)).fetchall()
        return [self._to_job(r) for r in rows]

    def save_result(
        self,
        conn: Connection,
        *,
        job_id: int,
        summary: dict[str, Any],
        metrics: dict[str, Any],
        curve: dict[str, Any] | None = None,
        diagnostics: dict[str, Any] | None = None,
    ) -> BacktestResult:
        now = utc_now()
        existed = conn.execute(select(backtest_result).where(backtest_result.c.job_id == job_id)).first()
        if existed is None:
            values = dict(
                job_id=job_id,
                summary_json=dict(summary),
                metrics_json=dict(metrics),
                curve_json=dict(curve) if isinstance(curve, dict) else None,
                diagnostics_json=dict(diagnostics) if isinstance(diagnostics, dict) else None,
                created_at=now,
                updated_at=now,
            )
            if conn.engine.dialect.name == "sqlite":
                next_id = conn.execute(
                    select(backtest_result.c.id).order_by(backtest_result.c.id.desc()).limit(1)
                ).scalar()
                values["id"] = int(next_id or 0) + 1
            conn.execute(insert(backtest_result).values(**values))
        else:
            conn.execute(
                update(backtest_result)
                .where(backtest_result.c.job_id == job_id)
                .values(
                    summary_json=dict(summary),
                    metrics_json=dict(metrics),
                    curve_json=dict(curve) if isinstance(curve, dict) else None,
                    diagnostics_json=dict(diagnostics) if isinstance(diagnostics, dict) else None,
                    updated_at=now,
                )
            )
        row = conn.execute(select(backtest_result).where(backtest_result.c.job_id == job_id)).first()
        if row is None:
            raise RuntimeError("backtest_result 写入后未读到行")
        return self._to_result(row)

    def get_result_by_job_id(self, conn: Connection, job_id: int) -> BacktestResult | None:
        row = conn.execute(select(backtest_result).where(backtest_result.c.job_id == job_id)).first()
        if row is None:
            return None
        return self._to_result(row)

    def delete_job(self, conn: Connection, job_id: int) -> bool:
        existed = conn.execute(select(backtest_job.c.id).where(backtest_job.c.id == job_id)).first()
        if existed is None:
            return False
        conn.execute(delete(backtest_result).where(backtest_result.c.job_id == job_id))
        conn.execute(delete(backtest_job).where(backtest_job.c.id == job_id))
        return True

