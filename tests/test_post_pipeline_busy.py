from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from value_screener.application.post_full_batch_pipeline import is_post_pipeline_busy


class IsPostPipelineBusyTests(unittest.TestCase):
    def test_empty_meta(self) -> None:
        self.assertFalse(is_post_pipeline_busy(None))
        self.assertFalse(is_post_pipeline_busy({}))

    def test_done_not_busy(self) -> None:
        self.assertFalse(is_post_pipeline_busy({"post_pipeline_phase": "done"}))

    def test_third_lens_done_not_busy(self) -> None:
        self.assertFalse(is_post_pipeline_busy({"post_pipeline_phase": "third_lens_done"}))

    def test_starting_without_timestamps_not_busy(self) -> None:
        self.assertFalse(is_post_pipeline_busy({"post_pipeline_phase": "starting"}))

    def test_ai_running_recent_is_busy(self) -> None:
        meta = {
            "post_pipeline_phase": "ai_running",
            "post_pipeline_started_at": datetime.now(timezone.utc).isoformat(),
        }
        self.assertTrue(is_post_pipeline_busy(meta))

    def test_ai_running_stale_not_busy(self) -> None:
        old = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        meta = {"post_pipeline_phase": "ai_running", "post_pipeline_started_at": old}
        self.assertFalse(is_post_pipeline_busy(meta))

    def test_ai_running_old_started_recent_activity_still_busy(self) -> None:
        old_started = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        recent_act = datetime.now(timezone.utc).isoformat()
        meta = {
            "post_pipeline_phase": "ai_running",
            "post_pipeline_started_at": old_started,
            "post_pipeline_activity_at": recent_act,
        }
        self.assertTrue(is_post_pipeline_busy(meta))

    def test_custom_stale_minutes(self) -> None:
        started = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
        meta = {"post_pipeline_phase": "ai_running", "post_pipeline_started_at": started}
        with patch.dict("os.environ", {"VALUE_SCREENER_POST_PIPELINE_STALE_MINUTES": "5"}, clear=False):
            self.assertFalse(is_post_pipeline_busy(meta))
