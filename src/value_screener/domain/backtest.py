from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class BacktestJobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class BacktestConfig:
    strategy_name: str
    start_date: str
    end_date: str
    rebalance_frequency: str = "monthly"
    holding_period_days: int = 20
    top_n: int | None = None
    top_quantile: float | None = 0.2
    benchmark: str = "000300.SH"
    transaction_cost_bps: int = 15
    filters: dict[str, Any] = field(default_factory=dict)
    extras: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class BacktestJob:
    id: int
    external_uuid: str
    strategy_name: str
    status: BacktestJobStatus
    config: BacktestConfig
    meta: dict[str, Any]
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class BacktestResult:
    id: int
    job_id: int
    summary: dict[str, Any]
    metrics: dict[str, Any]
    curve: dict[str, Any] | None
    diagnostics: dict[str, Any] | None
    created_at: datetime
    updated_at: datetime

