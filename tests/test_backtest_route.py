from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, update
from sqlalchemy.pool import StaticPool

from value_screener.infrastructure.screening_schema import backtest_job, metadata
from value_screener.interfaces.main import app


class BacktestRouteTests(unittest.TestCase):
    @patch("value_screener.interfaces.backtests.HistoricalPriceRepository")
    @patch("value_screener.interfaces.backtests.AShareIngestionSettings")
    def test_coverage_endpoint_returns_range(self, mock_settings: object, mock_repo: object) -> None:
        settings_inst = mock_settings.from_env.return_value
        settings_inst.tushare_token = "token"
        mock_repo.return_value.coverage_bounds.return_value = ("20240101", "20241231")
        client = TestClient(app)
        r = client.get("/api/v1/backtests/coverage")
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertEqual(body["start_date"], "20240101")
        self.assertEqual(body["end_date"], "20241231")

    def test_list_backtests_returns_503_when_table_missing(self) -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with patch("value_screener.interfaces.backtests.get_engine", return_value=engine):
            client = TestClient(app)
            r = client.get("/api/v1/backtests")
            self.assertEqual(r.status_code, 503)
            self.assertIn("alembic upgrade head", r.text)

    def test_create_and_query_backtest_job(self) -> None:
        engine = create_engine(
            "sqlite+pysqlite://",
            future=True,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        metadata.create_all(engine)
        with patch("value_screener.interfaces.backtests.get_engine", return_value=engine):
            client = TestClient(app)
            r = client.post(
                "/api/v1/backtests",
                json={
                    "strategy_name": "investment_quality_score",
                    "start_date": "2024-01-01",
                    "end_date": "2024-12-31",
                    "rebalance_frequency": "monthly",
                    "top_quantile": 0.2,
                    "run_sync": False,
                },
            )
            self.assertEqual(r.status_code, 202)
            body = r.json()
            job_id = int(body["job_id"])

            g = client.get(f"/api/v1/backtests/{job_id}")
            self.assertEqual(g.status_code, 200)
            data = g.json()
            self.assertEqual(data["job"]["status"], "pending")

            lst = client.get("/api/v1/backtests?status=pending")
            self.assertEqual(lst.status_code, 200)
            items = lst.json()
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["status"], "pending")
            self.assertIn("strategy_name", items[0]["config"])

            d = client.delete(f"/api/v1/backtests/{job_id}")
            self.assertEqual(d.status_code, 204)
            g2 = client.get(f"/api/v1/backtests/{job_id}")
            self.assertEqual(g2.status_code, 404)

    def test_delete_running_job_returns_409(self) -> None:
        engine = create_engine(
            "sqlite+pysqlite://",
            future=True,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        metadata.create_all(engine)
        with patch("value_screener.interfaces.backtests.get_engine", return_value=engine):
            client = TestClient(app)
            r = client.post(
                "/api/v1/backtests",
                json={
                    "strategy_name": "investment_quality_score",
                    "start_date": "2024-01-01",
                    "end_date": "2024-12-31",
                    "rebalance_frequency": "monthly",
                    "top_quantile": 0.2,
                    "run_sync": False,
                },
            )
            self.assertEqual(r.status_code, 202)
            job_id = int(r.json()["job_id"])
            with engine.begin() as conn:
                conn.execute(update(backtest_job).where(backtest_job.c.id == job_id).values(status="running"))
            d = client.delete(f"/api/v1/backtests/{job_id}")
            self.assertEqual(d.status_code, 409)


if __name__ == "__main__":
    unittest.main()

