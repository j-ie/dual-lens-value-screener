from __future__ import annotations

import os
import unittest
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

import httpx

from value_screener.application.company_ai_analysis import (
    CompanyAiDetailError,
    CompanyAiTimeoutError,
    _is_llm_timeout,
    build_analysis_context,
    context_hash_for,
)
from value_screener.infrastructure.settings import CompanyAiAnalysisSettings
from value_screener.interfaces.main import app


class CompanyAiTimeoutDetectionTests(unittest.TestCase):
    def test_is_llm_timeout_httpx_read(self) -> None:
        self.assertTrue(_is_llm_timeout(httpx.ReadTimeout("Request timed out.")))

    def test_is_llm_timeout_message_fallback(self) -> None:
        self.assertTrue(_is_llm_timeout(RuntimeError("Request timed out.")))

    def test_is_llm_timeout_chain(self) -> None:
        inner = httpx.ReadTimeout("x")
        outer = ValueError("wrap")
        outer.__cause__ = inner
        self.assertTrue(_is_llm_timeout(outer))


class CompanyAiContextTests(unittest.TestCase):
    def test_context_hash_stable(self) -> None:
        detail = {
            "run": {"id": 1},
            "run_snapshot": {"symbol": "600519.SH"},
            "reference": None,
            "financials": {"income": [], "balance": [], "cashflow": []},
            "live_quote": {"ok": False, "fetched_at": "t", "error": None, "data": None},
            "dcf": None,
        }
        ctx = build_analysis_context(detail)
        a = context_hash_for(ctx)
        b = context_hash_for(ctx)
        self.assertEqual(a, b)
        self.assertEqual(len(a), 64)


class CompanyAiRouteTests(unittest.TestCase):
    def test_ai_disabled_returns_503_without_db(self) -> None:
        with patch("value_screener.interfaces.runs.get_engine", return_value=MagicMock()):
            with patch.dict(os.environ, {"VALUE_SCREENER_AI_ENABLED": "0"}, clear=False):
                client = TestClient(app)
                r = client.post("/api/v1/runs/1/companies/600519.SH/ai-analysis")
        self.assertEqual(r.status_code, 503)

    def test_ai_enabled_incomplete_config_returns_503(self) -> None:
        with patch("value_screener.interfaces.runs.get_engine", return_value=MagicMock()):
            env = {
                "VALUE_SCREENER_AI_ENABLED": "1",
                "VALUE_SCREENER_AI_API_KEY": "",
                "VALUE_SCREENER_AI_BASE_URL": "https://example.com/v1",
                "VALUE_SCREENER_AI_MODEL": "ep-x",
            }
            with patch.dict(os.environ, env, clear=False):
                client = TestClient(app)
                r = client.post("/api/v1/runs/1/companies/600519.SH/ai-analysis")
        self.assertEqual(r.status_code, 503)

    @patch("value_screener.application.company_ai_analysis.CompanyAiAnalysisApplicationService.analyze")
    def test_success_shape(self, analyze_mock) -> None:
        analyze_mock.return_value = {
            "summary": "摘要",
            "key_metrics_commentary": "指标",
            "risks": "风险",
            "alignment_with_scores": "分数",
            "narrative_markdown": "## 正文",
            "ai_score": 72.5,
            "ai_score_rationale": "理由",
            "opportunity_score": 68.0,
            "opportunity_score_rationale": "机会简述",
            "meta": {
                "context_hash": "aa" * 32,
                "prompt_version": "v4",
                "model": "ep-test",
                "generated_at": "2026-04-04T00:00:00+00:00",
                "cached": False,
                "analysis_date": "2026-04-04",
            },
        }
        with patch("value_screener.interfaces.runs.get_engine", return_value=MagicMock()):
            with patch.dict(
                os.environ,
                {
                    "VALUE_SCREENER_AI_ENABLED": "1",
                    "VALUE_SCREENER_AI_API_KEY": "k",
                    "VALUE_SCREENER_AI_BASE_URL": "https://example.com/v1",
                    "VALUE_SCREENER_AI_MODEL": "ep-x",
                },
                clear=False,
            ):
                client = TestClient(app)
                r = client.post("/api/v1/runs/1/companies/600519.SH/ai-analysis")
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertEqual(body["summary"], "摘要")
        self.assertEqual(body["meta"]["prompt_version"], "v4")
        self.assertEqual(body["ai_score"], 72.5)
        self.assertEqual(body["opportunity_score"], 68.0)

    @patch("value_screener.application.company_ai_analysis.CompanyAiAnalysisApplicationService.analyze")
    def test_timeout_returns_504(self, analyze_mock) -> None:
        analyze_mock.side_effect = CompanyAiTimeoutError("模型响应超时（测试）")
        with patch("value_screener.interfaces.runs.get_engine", return_value=MagicMock()):
            with patch.dict(
                os.environ,
                {
                    "VALUE_SCREENER_AI_ENABLED": "1",
                    "VALUE_SCREENER_AI_API_KEY": "k",
                    "VALUE_SCREENER_AI_BASE_URL": "https://example.com/v1",
                    "VALUE_SCREENER_AI_MODEL": "ep-x",
                },
                clear=False,
            ):
                client = TestClient(app)
                r = client.post("/api/v1/runs/1/companies/600519.SH/ai-analysis")
        self.assertEqual(r.status_code, 504)
        self.assertEqual(r.json()["detail"], "模型响应超时（测试）")

    @patch("value_screener.application.company_ai_analysis.CompanyAiAnalysisApplicationService.analyze")
    def test_detail_error_run_not_found(self, analyze_mock) -> None:
        analyze_mock.side_effect = CompanyAiDetailError("run_not_found")
        with patch("value_screener.interfaces.runs.get_engine", return_value=MagicMock()):
            with patch.dict(
                os.environ,
                {
                    "VALUE_SCREENER_AI_ENABLED": "1",
                    "VALUE_SCREENER_AI_API_KEY": "k",
                    "VALUE_SCREENER_AI_BASE_URL": "https://example.com/v1",
                    "VALUE_SCREENER_AI_MODEL": "ep-x",
                },
                clear=False,
            ):
                client = TestClient(app)
                r = client.post("/api/v1/runs/999/companies/600519.SH/ai-analysis")
        self.assertEqual(r.status_code, 404)


class CompanyAiSettingsTests(unittest.TestCase):
    def test_is_ready_requires_all(self) -> None:
        with patch.dict(
            os.environ,
            {
                "VALUE_SCREENER_AI_ENABLED": "1",
                "VALUE_SCREENER_AI_API_KEY": "x",
                "VALUE_SCREENER_AI_BASE_URL": "https://b",
                "VALUE_SCREENER_AI_MODEL": "m",
            },
            clear=False,
        ):
            os.environ.pop("VALUE_SCREENER_AI_TIMEOUT_SECONDS", None)
            s = CompanyAiAnalysisSettings.from_env()
            self.assertTrue(s.is_ready())
            self.assertEqual(s.timeout_seconds, 240.0)
        with patch.dict(
            os.environ,
            {
                "VALUE_SCREENER_AI_ENABLED": "1",
                "VALUE_SCREENER_AI_API_KEY": "x",
                "VALUE_SCREENER_AI_BASE_URL": "",
                "VALUE_SCREENER_AI_MODEL": "",
            },
            clear=False,
        ):
            s2 = CompanyAiAnalysisSettings.from_env()
            self.assertFalse(s2.is_ready())
