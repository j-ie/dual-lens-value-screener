"""领域层 DCF 纯函数测试。"""

from __future__ import annotations

import unittest

from value_screener.domain.dcf import DcfInputs, DcfSkipped, compute_dcf


class ComputeDcfTest(unittest.TestCase):
    def test_golden_path_positive_value(self) -> None:
        inp = DcfInputs(
            base_fcf=100.0,
            wacc=0.10,
            stage1_growth=0.0,
            terminal_growth=0.02,
            forecast_years=2,
            net_debt=0.0,
            shares_outstanding=100.0,
            wacc_terminal_epsilon=0.0005,
        )
        out = compute_dcf(inp)
        self.assertNotIsInstance(out, DcfSkipped)
        assert not isinstance(out, DcfSkipped)
        self.assertAlmostEqual(out.value_per_share, 12.27273, places=3)
        self.assertGreater(out.enterprise_value, 0.0)

    def test_wacc_not_above_terminal_returns_skipped(self) -> None:
        inp = DcfInputs(
            base_fcf=100.0,
            wacc=0.05,
            stage1_growth=0.0,
            terminal_growth=0.0499,
            forecast_years=5,
            net_debt=0.0,
            shares_outstanding=100.0,
            wacc_terminal_epsilon=0.0005,
        )
        out = compute_dcf(inp)
        self.assertIsInstance(out, DcfSkipped)
        assert isinstance(out, DcfSkipped)
        self.assertEqual(out.code, "wacc_terminal_spread")

    def test_invalid_shares(self) -> None:
        inp = DcfInputs(
            base_fcf=100.0,
            wacc=0.10,
            stage1_growth=0.02,
            terminal_growth=0.02,
            forecast_years=3,
            net_debt=10.0,
            shares_outstanding=0.0,
            wacc_terminal_epsilon=0.0005,
        )
        out = compute_dcf(inp)
        self.assertIsInstance(out, DcfSkipped)
        assert isinstance(out, DcfSkipped)
        self.assertEqual(out.code, "invalid_shares")


if __name__ == "__main__":
    unittest.main()
