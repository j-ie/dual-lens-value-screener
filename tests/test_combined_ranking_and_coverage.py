from __future__ import annotations

import os
import unittest
from unittest.mock import MagicMock, patch

from value_screener.domain.assessment_coverage import (
    combined_linear_score,
    dual_lens_coverage_ok,
)
from value_screener.domain.combined_ranking_params import CombinedRankingParams, snapshot_ttl_seconds
from value_screener.domain.snapshot import StockFinancialSnapshot
from value_screener.infrastructure.result_cache import (
    RESULT_CACHE_ENRICH_VER,
    cache_key,
    valuation_filters_cache_fingerprint,
)
from value_screener.infrastructure.screening_repository import ScreeningRepository
from value_screener.infrastructure.settings import PostFullBatchPipelineSettings


class CombinedRankingParamsTests(unittest.TestCase):
    def test_from_env_defaults(self) -> None:
        with patch.dict(
            os.environ,
            {
                "VALUE_SCREENER_COMBINED_WEIGHT_BUFFETT": "",
                "VALUE_SCREENER_COMBINED_WEIGHT_GRAHAM": "",
            },
            clear=False,
        ):
            p = CombinedRankingParams.from_env()
        self.assertAlmostEqual(p.weight_buffett, 0.5)
        self.assertAlmostEqual(p.weight_graham, 0.5)
        self.assertEqual(p.tiebreak, "min_dim")

    def test_from_env_rejects_bad_sum(self) -> None:
        with patch.dict(
            os.environ,
            {
                "VALUE_SCREENER_COMBINED_WEIGHT_BUFFETT": "0.3",
                "VALUE_SCREENER_COMBINED_WEIGHT_GRAHAM": "0.3",
            },
            clear=False,
        ):
            with self.assertRaises(ValueError):
                CombinedRankingParams.from_env()


class CoverageTests(unittest.TestCase):
    def test_dual_lens_coverage_requires_both(self) -> None:
        g = {"score": 1.0, "market_cap_to_ncav": 1.0}
        b = {"score": 1.0}
        self.assertFalse(dual_lens_coverage_ok(g, b))
        b2 = {"score": 1.0, "roe": 0.1}
        self.assertTrue(dual_lens_coverage_ok(g, b2))

    def test_combined_linear(self) -> None:
        v = combined_linear_score(80.0, 60.0, weight_buffett=0.5, weight_graham=0.5)
        self.assertEqual(v, 70.0)


class SnapshotSerdeTests(unittest.TestCase):
    def test_roundtrip_model_dump_validate(self) -> None:
        s = StockFinancialSnapshot(
            symbol="000001.SZ",
            market_cap=1e10,
            total_equity=5e9,
            net_income_ttm=1e8,
            data_source="tushare",
            financials_end_date="20241231",
        )
        payload = s.model_dump(mode="json")
        s2 = StockFinancialSnapshot.model_validate(payload)
        self.assertEqual(s2.symbol, s.symbol)
        self.assertEqual(s2.financials_end_date, "20241231")


class CacheKeyTests(unittest.TestCase):
    def test_cache_key_includes_fingerprint(self) -> None:
        k = cache_key(1, 1, 20, "combined", "desc", filter_fingerprint="wb=0.5|wg=0.5")
        self.assertIn(RESULT_CACHE_ENRICH_VER, k)
        self.assertIn("combined", k)
        self.assertIn("wb=0.5", k)

    def test_valuation_filters_fingerprint_nonempty(self) -> None:
        fp = valuation_filters_cache_fingerprint(1e9, 5e11, 1.0, 5.0)
        self.assertIn("mcmin:", fp)
        self.assertIn("dvmin:", fp)


class PageResultsCombinedTests(unittest.TestCase):
    def test_page_results_combined_requires_ranking(self) -> None:
        conn = MagicMock()
        repo = ScreeningRepository(MagicMock())
        with self.assertRaises(ValueError):
            repo.page_results(
                conn,
                1,
                sort_key="combined",
                order="desc",
                page=1,
                page_size=10,
                ranking=None,
            )


class ListTopSymbolsByCombinedTests(unittest.TestCase):
    def test_limit_zero_skips_query(self) -> None:
        conn = MagicMock()
        repo = ScreeningRepository(MagicMock())
        p = CombinedRankingParams.from_env()
        out = repo.list_top_symbols_by_combined(conn, 99, ranking=p, limit=0)
        self.assertEqual(out, [])
        conn.execute.assert_not_called()

    def test_weighted_coverage_only_limit_zero(self) -> None:
        conn = MagicMock()
        repo = ScreeningRepository(MagicMock())
        p = CombinedRankingParams.from_env()
        out = repo.list_top_symbols_weighted_desc_coverage_only(conn, 99, ranking=p, limit=0)
        self.assertEqual(out, [])
        conn.execute.assert_not_called()


class PostFullBatchPipelineSettingsTests(unittest.TestCase):
    def test_top_n_and_sleep_from_env(self) -> None:
        with patch.dict(
            os.environ,
            {
                "VALUE_SCREENER_POST_FULL_BATCH_AI_TOP_N": "50",
                "VALUE_SCREENER_POST_FULL_BATCH_AI_SLEEP_SECONDS": "0",
            },
            clear=False,
        ):
            s = PostFullBatchPipelineSettings.from_env()
        self.assertEqual(s.ai_top_n, 50)
        self.assertEqual(s.ai_sleep_seconds, 0.0)
        self.assertTrue(s.attach_third_lens)


class SnapshotTtlTests(unittest.TestCase):
    def test_snapshot_ttl_default(self) -> None:
        with patch.dict(os.environ, {"VALUE_SCREENER_SNAPSHOT_TTL_SECONDS": ""}, clear=False):
            self.assertEqual(snapshot_ttl_seconds(), 86400)
