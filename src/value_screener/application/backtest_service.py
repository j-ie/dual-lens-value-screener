from __future__ import annotations

from dataclasses import asdict
from typing import Any, Protocol

from sqlalchemy.engine import Engine

from value_screener.domain.backtest import (
    BacktestConfig,
    BacktestJob,
    BacktestJobStatus,
    BacktestResult,
)
from value_screener.infrastructure.backtest_repository import BacktestRepository


class BacktestExecutor(Protocol):
    def run(self, config: BacktestConfig) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
        """
        返回 (summary, metrics, curve, diagnostics)。
        """


class BacktestApplicationService:
    def __init__(self, engine: Engine, repo: BacktestRepository | None = None) -> None:
        self._engine = engine
        self._repo = repo or BacktestRepository(engine)

    def create_job(self, config: BacktestConfig, meta: dict[str, Any] | None = None) -> BacktestJob:
        with self._engine.begin() as conn:
            return self._repo.create_job(conn, config, meta=meta)

    def get_job(self, job_id: int) -> BacktestJob | None:
        with self._engine.connect() as conn:
            return self._repo.get_job(conn, job_id)

    def delete_job(self, job_id: int) -> None:
        with self._engine.begin() as conn:
            job = self._repo.get_job(conn, job_id)
            if job is None:
                raise ValueError("backtest job 不存在")
            if job.status == BacktestJobStatus.RUNNING:
                raise ValueError("任务执行中，无法删除")
            if not self._repo.delete_job(conn, job_id):
                raise ValueError("backtest job 不存在")

    def get_result_summary(self, job_id: int) -> dict[str, Any] | None:
        with self._engine.connect() as conn:
            job = self._repo.get_job(conn, job_id)
            if job is None:
                return None
            result = self._repo.get_result_by_job_id(conn, job_id)
            out: dict[str, Any] = {
                "job": {
                    "id": job.id,
                    "external_uuid": job.external_uuid,
                    "strategy_name": job.strategy_name,
                    "status": job.status.value,
                    "created_at": job.created_at.isoformat(),
                    "started_at": job.started_at.isoformat() if job.started_at else None,
                    "finished_at": job.finished_at.isoformat() if job.finished_at else None,
                    "config": asdict(job.config),
                    "meta": dict(job.meta),
                },
                "result": None,
            }
            if result is not None:
                out["result"] = {
                    "summary": dict(result.summary),
                    "metrics": dict(result.metrics),
                    "curve": dict(result.curve) if isinstance(result.curve, dict) else None,
                    "diagnostics": (
                        dict(result.diagnostics) if isinstance(result.diagnostics, dict) else None
                    ),
                    "updated_at": result.updated_at.isoformat(),
                }
            return out

    def execute_job(self, job_id: int, executor: BacktestExecutor) -> BacktestResult:
        with self._engine.begin() as conn:
            job = self._repo.get_job(conn, job_id)
            if job is None:
                raise ValueError("backtest job 不存在")
            if job.status not in (BacktestJobStatus.PENDING, BacktestJobStatus.FAILED):
                raise ValueError("backtest job 状态不允许执行")
            self._repo.mark_running(conn, job_id)
        try:
            summary, metrics, curve, diagnostics = executor.run(job.config)
        except Exception as exc:  # noqa: BLE001
            with self._engine.begin() as conn:
                latest = self._repo.get_job(conn, job_id)
                base_meta = latest.meta if latest is not None else {}
                err_meta = dict(base_meta)
                err_meta["error"] = str(exc)
                self._repo.mark_finished(conn, job_id, success=False, meta=err_meta)
            raise

        with self._engine.begin() as conn:
            result = self._repo.save_result(
                conn,
                job_id=job_id,
                summary=summary,
                metrics=metrics,
                curve=curve,
                diagnostics=diagnostics,
            )
            latest = self._repo.get_job(conn, job_id)
            base_meta = latest.meta if latest is not None else {}
            ok_meta = dict(base_meta)
            ok_meta["last_result_updated_at"] = result.updated_at.isoformat()
            self._repo.mark_finished(conn, job_id, success=True, meta=ok_meta)
            return result

