"""ingestion_job 表：按 job_type + scheduled_date + params_hash 加载/更新游标。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import delete, insert, select, update
from sqlalchemy.engine import Connection, Engine

from value_screener.infrastructure.screening_schema import ingestion_job

_JOB_TYPE_FS_STMT_TUSHARE = "fs_stmt_tushare"


@dataclass(frozen=True, slots=True)
class IngestionJobRow:
    """内存中的任务行视图。"""

    id: int
    job_type: str
    scheduled_date: date
    params_hash: str
    status: str
    cursor_ts_code: str | None
    universe_fingerprint: str | None


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class IngestionJobRepository:
    """ingestion_job 仓储。"""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    @staticmethod
    def financial_statement_job_type() -> str:
        return _JOB_TYPE_FS_STMT_TUSHARE

    def delete_job(
        self,
        conn: Connection,
        *,
        job_type: str,
        scheduled_date: date,
        params_hash: str,
    ) -> None:
        conn.execute(
            delete(ingestion_job).where(
                ingestion_job.c.job_type == job_type,
                ingestion_job.c.scheduled_date == scheduled_date,
                ingestion_job.c.params_hash == params_hash,
            )
        )

    def ensure_job(
        self,
        conn: Connection,
        *,
        job_type: str,
        scheduled_date: date,
        params_hash: str,
        universe_fingerprint_value: str | None,
    ) -> IngestionJobRow:
        """存在则读取；不存在则插入 running、cursor 为空（从第一只开始）。"""

        stmt = select(
            ingestion_job.c.id,
            ingestion_job.c.job_type,
            ingestion_job.c.scheduled_date,
            ingestion_job.c.params_hash,
            ingestion_job.c.status,
            ingestion_job.c.cursor_ts_code,
            ingestion_job.c.universe_fingerprint,
        ).where(
            ingestion_job.c.job_type == job_type,
            ingestion_job.c.scheduled_date == scheduled_date,
            ingestion_job.c.params_hash == params_hash,
        )
        row = conn.execute(stmt).first()
        if row is not None:
            return IngestionJobRow(
                id=int(row[0]),
                job_type=str(row[1]),
                scheduled_date=row[2],
                params_hash=str(row[3]),
                status=str(row[4]),
                cursor_ts_code=str(row[5]) if row[5] is not None else None,
                universe_fingerprint=str(row[6]) if row[6] is not None else None,
            )
        now = utc_now()
        conn.execute(
            insert(ingestion_job).values(
                job_type=job_type,
                scheduled_date=scheduled_date,
                params_hash=params_hash,
                status="running",
                cursor_ts_code=None,
                universe_fingerprint=universe_fingerprint_value,
                updated_at=now,
            )
        )
        row2 = conn.execute(stmt).first()
        if row2 is None:
            raise RuntimeError("ingestion_job 插入后未读到行")
        return IngestionJobRow(
            id=int(row2[0]),
            job_type=str(row2[1]),
            scheduled_date=row2[2],
            params_hash=str(row2[3]),
            status=str(row2[4]),
            cursor_ts_code=str(row2[5]) if row2[5] is not None else None,
            universe_fingerprint=str(row2[6]) if row2[6] is not None else None,
        )

    def update_progress(
        self,
        conn: Connection,
        *,
        job_id: int,
        cursor_ts_code: str | None,
        status: str,
    ) -> None:
        conn.execute(
            update(ingestion_job)
            .where(ingestion_job.c.id == job_id)
            .values(
                cursor_ts_code=cursor_ts_code,
                status=status,
                updated_at=utc_now(),
            )
        )

    def row_to_dict(self, row: IngestionJobRow) -> dict[str, Any]:
        return {
            "id": row.id,
            "job_type": row.job_type,
            "scheduled_date": row.scheduled_date.isoformat(),
            "params_hash": row.params_hash,
            "status": row.status,
            "cursor_ts_code": row.cursor_ts_code,
            "universe_fingerprint": row.universe_fingerprint,
        }
