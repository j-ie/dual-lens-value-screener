from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import create_engine, insert

from value_screener.application.backtest_engine import DefaultBacktestExecutor
from value_screener.domain.backtest import BacktestConfig
from value_screener.infrastructure.screening_schema import financial_snapshot, metadata


def _snap(symbol: str, market_cap: float, end_date: str) -> dict[str, object]:
    return {
        "symbol": symbol,
        "market_cap": market_cap,
        "total_current_assets": market_cap * 0.4,
        "total_current_liabilities": market_cap * 0.2,
        "total_liabilities": market_cap * 0.35,
        "total_equity": market_cap * 0.65,
        "net_income_ttm": market_cap * 0.05,
        "operating_cash_flow_ttm": market_cap * 0.06,
        "revenue_ttm": market_cap * 0.2,
        "interest_bearing_debt": market_cap * 0.1,
        "data_source": "test",
        "financials_end_date": end_date,
    }


def test_default_backtest_executor_runs_with_asof_snapshots() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(
            insert(financial_snapshot),
            [
                {
                    "id": 1,
                    "symbol": "000001.SZ",
                    "financials_end_date": "20240131",
                    "snapshot_json": _snap("000001.SZ", 100.0, "20240131"),
                    "data_source": "test",
                    "fetched_at": datetime(2024, 1, 31, tzinfo=timezone.utc),
                    "content_hash": "a1",
                },
                {
                    "id": 2,
                    "symbol": "000001.SZ",
                    "financials_end_date": "20240331",
                    "snapshot_json": _snap("000001.SZ", 120.0, "20240331"),
                    "data_source": "test",
                    "fetched_at": datetime(2024, 3, 31, tzinfo=timezone.utc),
                    "content_hash": "a2",
                },
                {
                    "id": 3,
                    "symbol": "000002.SZ",
                    "financials_end_date": "20240131",
                    "snapshot_json": _snap("000002.SZ", 80.0, "20240131"),
                    "data_source": "test",
                    "fetched_at": datetime(2024, 1, 31, tzinfo=timezone.utc),
                    "content_hash": "b1",
                },
                {
                    "id": 4,
                    "symbol": "000002.SZ",
                    "financials_end_date": "20240331",
                    "snapshot_json": _snap("000002.SZ", 88.0, "20240331"),
                    "data_source": "test",
                    "fetched_at": datetime(2024, 3, 31, tzinfo=timezone.utc),
                    "content_hash": "b2",
                },
            ],
        )

    cfg = BacktestConfig(
        strategy_name="investment_quality_score",
        start_date="2024-01-31",
        end_date="2024-03-31",
        rebalance_frequency="monthly",
        top_n=1,
        transaction_cost_bps=10,
        filters={"symbols": ["000001.SZ", "000002.SZ"]},
    )
    executor = DefaultBacktestExecutor(engine)
    summary, metrics, curve, diagnostics = executor.run(cfg)
    assert summary["strategy_name"] == "investment_quality_score"
    assert "annualized_return" in metrics
    assert isinstance(curve.get("points"), list)
    assert "ic_mean" in diagnostics

