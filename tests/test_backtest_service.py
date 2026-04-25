from __future__ import annotations

from sqlalchemy import create_engine

from value_screener.application.backtest_engine import SampleBacktestExecutor
from value_screener.application.backtest_service import BacktestApplicationService
from value_screener.domain.backtest import BacktestConfig
from value_screener.infrastructure.screening_schema import metadata


def test_backtest_job_lifecycle_with_sample_executor() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    metadata.create_all(engine)
    svc = BacktestApplicationService(engine)
    cfg = BacktestConfig(
        strategy_name="investment_quality_score",
        start_date="2024-01-01",
        end_date="2024-12-31",
        rebalance_frequency="monthly",
    )
    job = svc.create_job(cfg)
    assert job.id > 0
    assert job.status.value == "pending"

    result = svc.execute_job(job.id, SampleBacktestExecutor())
    assert result.job_id == job.id
    assert float(result.metrics.get("annualized_return") or 0.0) > 0

    payload = svc.get_result_summary(job.id)
    assert payload is not None
    assert payload["job"]["status"] == "success"
    assert isinstance(payload["result"], dict)
    assert "metrics" in payload["result"]

