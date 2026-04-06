"""市盈率 TTM 等估值派生纯函数。"""

from __future__ import annotations

import unittest

from value_screener.domain.valuation_metrics import compute_pe_ttm


class TestComputePeTtm(unittest.TestCase):
    def test_positive_earnings(self) -> None:
        self.assertAlmostEqual(compute_pe_ttm(100.0, 10.0) or 0, 10.0)

    def test_none_or_non_positive_ni(self) -> None:
        self.assertIsNone(compute_pe_ttm(100.0, None))
        self.assertIsNone(compute_pe_ttm(100.0, 0.0))
        self.assertIsNone(compute_pe_ttm(100.0, -1.0))

    def test_non_positive_mcap(self) -> None:
        self.assertIsNone(compute_pe_ttm(0.0, 10.0))


if __name__ == "__main__":
    unittest.main()
