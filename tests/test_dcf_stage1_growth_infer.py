"""预测期增长率由年报 CAGR 推断。"""

from __future__ import annotations

import dataclasses
import unittest

from value_screener.application.dcf_stage1_growth_infer import infer_stage1_growth_from_annual_statements
from value_screener.domain.dcf_sector_policy import DcfSectorKind
from value_screener.infrastructure.settings import DcfValuationSettings

_BASE = DcfValuationSettings(
    enabled=True,
    default_wacc=0.09,
    default_stage1_growth=0.02,
    default_terminal_growth=0.025,
    default_cyclical_stage1_growth=0.015,
    default_cyclical_terminal_growth=0.02,
    infer_stage1_enabled=True,
    infer_stage1_max_annuals=5,
    infer_stage1_min_span_years=1,
    forecast_years=5,
    wacc_terminal_epsilon=0.0005,
    ttm_periods_max=4,
    sync_since_years=5,
    annual_stale_days=550,
    wacc_min=0.04,
    wacc_max=0.25,
    stage1_g_min=-0.05,
    stage1_g_max=0.20,
    terminal_g_min=-0.02,
    terminal_g_max=0.06,
    daily_basic_timeout_seconds=12.0,
    financial_ni_base_scale=0.35,
)


class InferStage1GrowthTest(unittest.TestCase):
    def test_cyclical_skips_infer(self) -> None:
        g, src, w = infer_stage1_growth_from_annual_statements(
            sector_kind=DcfSectorKind.CYCLICAL,
            income_rows=[
                {"end_date": "20211231", "n_income_attr_p": 100.0, "report_type": "1"},
                {"end_date": "20231231", "n_income_attr_p": 200.0, "report_type": "1"},
            ],
            cashflow_rows=[],
            settings=_BASE,
        )
        self.assertIsNone(g)
        self.assertIsNone(src)
        self.assertEqual(w, [])

    def test_net_income_cagr_two_year_span(self) -> None:
        """2021→2023 跨度 2 年：144/100 的几何平均年化 = 20%。"""
        g, src, _w = infer_stage1_growth_from_annual_statements(
            sector_kind=DcfSectorKind.FINANCIAL,
            income_rows=[
                {"end_date": "20211231", "n_income_attr_p": 100.0, "report_type": "1"},
                {"end_date": "20231231", "n_income_attr_p": 144.0, "report_type": "1"},
            ],
            cashflow_rows=[],
            settings=_BASE,
        )
        self.assertEqual(src, "inferred_net_income_cagr")
        self.assertIsNotNone(g)
        assert g is not None
        self.assertAlmostEqual(g, 0.2, places=6)

    def test_falls_back_to_ocf_when_no_income(self) -> None:
        g, src, _w = infer_stage1_growth_from_annual_statements(
            sector_kind=DcfSectorKind.GENERAL,
            income_rows=[],
            cashflow_rows=[
                {"end_date": "20221231", "n_cashflow_act": 1000.0, "report_type": "1"},
                {"end_date": "20231231", "n_cashflow_act": 1100.0, "report_type": "1"},
            ],
            settings=_BASE,
        )
        self.assertEqual(src, "inferred_ocf_cagr")
        self.assertIsNotNone(g)
        assert g is not None
        self.assertAlmostEqual(g, 0.1, places=6)

    def test_disabled_returns_none(self) -> None:
        s = dataclasses.replace(_BASE, infer_stage1_enabled=False)
        g, src, _w = infer_stage1_growth_from_annual_statements(
            sector_kind=DcfSectorKind.GENERAL,
            income_rows=[
                {"end_date": "20221231", "n_income_attr_p": 100.0, "report_type": "1"},
                {"end_date": "20231231", "n_income_attr_p": 200.0, "report_type": "1"},
            ],
            cashflow_rows=[],
            settings=s,
        )
        self.assertIsNone(g)
        self.assertIsNone(src)


if __name__ == "__main__":
    unittest.main()
