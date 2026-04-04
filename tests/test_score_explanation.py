from __future__ import annotations

import unittest

from value_screener.domain.score_explanation import build_score_explanation_zh
from value_screener.infrastructure.result_cache import RESULT_CACHE_ENRICH_VER, cache_key


class ScoreExplanationTests(unittest.TestCase):
    def test_build_explanation_contains_scores_and_provenance(self) -> None:
        graham = {
            "score": 55.5,
            "market_cap_to_ncav": 1.2,
            "current_ratio": 1.5,
            "price_to_book": 2.0,
            "notes": {"net_net_tendency": True},
        }
        buffett = {
            "score": 80.0,
            "roe": 0.15,
            "debt_to_equity": 0.4,
            "ocf_to_net_income": 1.1,
            "notes": {},
        }
        provenance = {
            "data_source": "tushare",
            "trade_cal_date": "20260401",
            "financials_end_date": "20241231",
        }
        text = build_score_explanation_zh(graham, buffett, provenance)
        self.assertIn("tushare", text)
        self.assertIn("20260401", text)
        self.assertIn("巴菲特", text)
        self.assertIn("格雷厄姆", text)
        self.assertIn("80.0", text)
        self.assertIn("55.5", text)

    def test_cache_key_includes_enrich_version(self) -> None:
        key = cache_key(1, 2, 20, "buffett", "desc")
        self.assertIn(RESULT_CACHE_ENRICH_VER, key)


if __name__ == "__main__":
    unittest.main()
