from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from value_screener.infrastructure.screening_repository import RunRow
from value_screener.interfaces.main import app


def _row(
    *,
    run_id: int = 18,
    status: str = "success",
    meta: dict | None = None,
) -> RunRow:
    return RunRow(
        id=run_id,
        external_uuid="u",
        status=status,
        created_at=datetime.now(timezone.utc),
        finished_at=datetime.now(timezone.utc),
        universe_size=100,
        snapshot_ok=100,
        snapshot_failed=0,
        provider_label="tushare",
        meta_json=meta,
    )


class PostPipelineRouteTests(unittest.TestCase):
    def test_404_when_run_missing(self) -> None:
        engine = MagicMock()
        conn = MagicMock()
        engine.connect.return_value.__enter__.return_value = conn
        engine.connect.return_value.__exit__.return_value = None
        with patch("value_screener.interfaces.runs.get_engine", return_value=engine):
            with patch("value_screener.interfaces.runs.ScreeningRepository") as Repo:
                Repo.return_value.get_run.return_value = None
                client = TestClient(app)
                r = client.post("/api/v1/runs/999/post-pipeline")
        self.assertEqual(r.status_code, 404)

    def test_400_when_run_not_success(self) -> None:
        engine = MagicMock()
        conn = MagicMock()
        engine.connect.return_value.__enter__.return_value = conn
        engine.connect.return_value.__exit__.return_value = None
        with patch("value_screener.interfaces.runs.get_engine", return_value=engine):
            with patch("value_screener.interfaces.runs.ScreeningRepository") as Repo:
                Repo.return_value.get_run.return_value = _row(status="running")
                client = TestClient(app)
                r = client.post("/api/v1/runs/18/post-pipeline")
        self.assertEqual(r.status_code, 400)

    def test_409_when_pipeline_busy(self) -> None:
        engine = MagicMock()
        conn = MagicMock()
        engine.connect.return_value.__enter__.return_value = conn
        engine.connect.return_value.__exit__.return_value = None
        with patch("value_screener.interfaces.runs.get_engine", return_value=engine):
            with patch("value_screener.interfaces.runs.ScreeningRepository") as Repo:
                Repo.return_value.get_run.return_value = _row(
                    meta={
                        "post_pipeline_phase": "ai_running",
                        "post_pipeline_started_at": datetime.now(timezone.utc).isoformat(),
                    },
                )
                client = TestClient(app)
                r = client.post("/api/v1/runs/18/post-pipeline")
        self.assertEqual(r.status_code, 409)

    @patch("value_screener.interfaces.runs.run_post_full_batch_pipeline_background")
    def test_202_when_busy_but_stale(self, _mock_bg: MagicMock) -> None:
        engine = MagicMock()
        conn = MagicMock()
        engine.connect.return_value.__enter__.return_value = conn
        engine.connect.return_value.__exit__.return_value = None
        old = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        with patch("value_screener.interfaces.runs.get_engine", return_value=engine):
            with patch("value_screener.interfaces.runs.ScreeningRepository") as Repo:
                Repo.return_value.get_run.return_value = _row(
                    meta={
                        "post_pipeline_phase": "ai_running",
                        "post_pipeline_started_at": old,
                    },
                )
                client = TestClient(app)
                r = client.post("/api/v1/runs/18/post-pipeline")
        self.assertEqual(r.status_code, 202)

    @patch("value_screener.interfaces.runs.run_post_full_batch_pipeline_background")
    def test_202_when_third_lens_transient_phase(self, _mock_bg: MagicMock) -> None:
        """third_lens_* 不再视为锁定，避免按钮长期置灰。"""
        engine = MagicMock()
        conn = MagicMock()
        engine.connect.return_value.__enter__.return_value = conn
        engine.connect.return_value.__exit__.return_value = None
        with patch("value_screener.interfaces.runs.get_engine", return_value=engine):
            with patch("value_screener.interfaces.runs.ScreeningRepository") as Repo:
                Repo.return_value.get_run.return_value = _row(
                    meta={"post_pipeline_phase": "third_lens_done"},
                )
                client = TestClient(app)
                r = client.post("/api/v1/runs/18/post-pipeline")
        self.assertEqual(r.status_code, 202)

    @patch("value_screener.interfaces.runs.run_post_full_batch_pipeline_background")
    def test_202_accepts_success_run(self, _mock_bg: MagicMock) -> None:
        engine = MagicMock()
        conn = MagicMock()
        engine.connect.return_value.__enter__.return_value = conn
        engine.connect.return_value.__exit__.return_value = None
        with patch("value_screener.interfaces.runs.get_engine", return_value=engine):
            with patch("value_screener.interfaces.runs.ScreeningRepository") as Repo:
                Repo.return_value.get_run.return_value = _row(meta={"post_pipeline_phase": "done"})
                client = TestClient(app)
                r = client.post("/api/v1/runs/18/post-pipeline")
        self.assertEqual(r.status_code, 202)
        body = r.json()
        self.assertEqual(body.get("run_id"), 18)
        self.assertEqual(body.get("status"), "accepted")
