from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from value_screener.domain.third_lens import (
    combine_third_lens_subscores,
    earnings_yield_ratio,
    final_triple_linear,
    industry_bucket,
    percentile_rank_0_100,
    revenue_yoy_from_two_annual,
)
from value_screener.domain.triple_composite_params import ThirdLensSubWeights, TripleCompositeParams


class TestThirdLensPure(unittest.TestCase):
    def test_industry_bucket(self) -> None:
        self.assertEqual(industry_bucket(None), "")
        self.assertEqual(industry_bucket("  银行  "), "银行")

    def test_percentile_rank(self) -> None:
        self.assertAlmostEqual(percentile_rank_0_100([1.0, 2.0, 3.0], 2.0), 50.0)
        self.assertEqual(percentile_rank_0_100([], 1.0), 50.0)

    def test_revenue_yoy(self) -> None:
        self.assertAlmostEqual(revenue_yoy_from_two_annual(120.0, 100.0) or 0, 0.2)
        self.assertIsNone(revenue_yoy_from_two_annual(120.0, 0.0))

    def test_ep(self) -> None:
        self.assertAlmostEqual(earnings_yield_ratio(1e9, 1e10) or 0, 0.1)
        self.assertIsNone(earnings_yield_ratio(-1, 1e10))

    def test_combine_subscores(self) -> None:
        t, m = combine_third_lens_subscores(80.0, 60.0, 0.5, 0.5)
        self.assertEqual(t, 70.0)
        t2, m2 = combine_third_lens_subscores(80.0, None, 0.5, 0.5)
        self.assertEqual(t2, 80.0)
        self.assertTrue(m2.get("third_lens_growth_only"))
        t3, m3 = combine_third_lens_subscores(None, None, 0.5, 0.5)
        self.assertIsNone(t3)

    def test_final_triple_renorm(self) -> None:
        f, d = final_triple_linear(80.0, 60.0, None, 1 / 3, 1 / 3, 1 / 3)
        self.assertTrue(d.get("triple_renormalized"))
        self.assertAlmostEqual(f, 70.0)
        f2, d2 = final_triple_linear(80.0, 60.0, 50.0, 0.2, 0.3, 0.5)
        self.assertFalse(d2.get("triple_renormalized"))
        self.assertAlmostEqual(f2, 59.0)


class TestTripleCompositeParams(unittest.TestCase):
    def test_from_env_defaults(self) -> None:
        with patch.dict(
            os.environ,
            {
                "VALUE_SCREENER_TRIPLE_WEIGHT_BUFFETT": "",
                "VALUE_SCREENER_TRIPLE_WEIGHT_GRAHAM": "",
                "VALUE_SCREENER_TRIPLE_WEIGHT_THIRD": "",
            },
            clear=False,
        ):
            p = TripleCompositeParams.from_env()
            self.assertAlmostEqual(p.weight_buffett + p.weight_graham + p.weight_third, 1.0)

    def test_from_env_rejects_bad_sum(self) -> None:
        with patch.dict(
            os.environ,
            {
                "VALUE_SCREENER_TRIPLE_WEIGHT_BUFFETT": "0.5",
                "VALUE_SCREENER_TRIPLE_WEIGHT_GRAHAM": "0.5",
                "VALUE_SCREENER_TRIPLE_WEIGHT_THIRD": "0.5",
            },
            clear=True,
        ):
            with self.assertRaises(ValueError):
                TripleCompositeParams.from_env()

    def test_sub_weights(self) -> None:
        with patch.dict(
            os.environ,
            {
                "VALUE_SCREENER_THIRD_LENS_WEIGHT_GROWTH": "",
                "VALUE_SCREENER_THIRD_LENS_WEIGHT_VALUATION": "",
            },
            clear=False,
        ):
            w = ThirdLensSubWeights.from_env()
            self.assertAlmostEqual(w.weight_growth + w.weight_valuation, 1.0)


if __name__ == "__main__":
    unittest.main()
