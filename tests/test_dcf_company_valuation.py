"""DCF 聚合与 payload 构建测试（无网络）。"""

from __future__ import annotations

import dataclasses
import unittest
from datetime import date

from value_screener.application.dcf_company_valuation import (
    aggregate_ocf_and_capex_proxy_ttm,
    build_company_dcf_payload,
)
from value_screener.infrastructure.settings import DcfValuationSettings


_DCF_BASE = DcfValuationSettings(
    enabled=True,
    default_wacc=0.10,
    default_stage1_growth=0.0,
    default_terminal_growth=0.02,
    forecast_years=2,
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
    financial_ni_base_scale=1.0,
)


def _dcf_settings(**overrides: object) -> DcfValuationSettings:
    return dataclasses.replace(_DCF_BASE, **overrides)


class AggregateCashflowTest(unittest.TestCase):
    def test_missing_investing_warns_and_fcf_equals_ocf(self) -> None:
        rows = [
            {"end_date": "20231231", "n_cashflow_act": 50.0, "n_cash_flows_inv_act": None},
            {"end_date": "20230930", "n_cashflow_act": 50.0, "n_cash_flows_inv_act": None},
        ]
        ocf, cap, fcf, w = aggregate_ocf_and_capex_proxy_ttm(
            rows,
            max_periods=4,
            as_of=date(2024, 6, 1),
            annual_stale_days=550,
        )
        self.assertEqual(ocf, 50.0)
        self.assertIsNone(cap)
        self.assertEqual(fcf, 50.0)
        self.assertTrue(any("未扣除资本开支" in x for x in w))

    def test_capex_proxy_deducted(self) -> None:
        rows = [
            {"end_date": "20231231", "n_cashflow_act": 100.0, "n_cash_flows_inv_act": -30.0},
        ]
        ocf, cap, fcf, w = aggregate_ocf_and_capex_proxy_ttm(rows, max_periods=4, as_of=date(2024, 6, 1))
        self.assertEqual(ocf, 100.0)
        self.assertEqual(cap, 30.0)
        self.assertEqual(fcf, 70.0)

    def test_four_annual_rows_use_latest_only(self) -> None:
        rows = [
            {"end_date": "20231231", "n_cashflow_act": 10.0, "n_cash_flows_inv_act": None},
            {"end_date": "20221231", "n_cashflow_act": 20.0, "n_cash_flows_inv_act": None},
            {"end_date": "20211231", "n_cashflow_act": 30.0, "n_cash_flows_inv_act": None},
            {"end_date": "20201231", "n_cashflow_act": 40.0, "n_cash_flows_inv_act": None},
        ]
        ocf, _cap, fcf, w = aggregate_ocf_and_capex_proxy_ttm(rows, max_periods=4, as_of=date(2024, 6, 1))
        self.assertEqual(ocf, 10.0)
        self.assertEqual(fcf, 10.0)
        self.assertTrue(any("年报" in x for x in w))

    def test_quarterly_ttm_when_latest_annual_stale(self) -> None:
        """最近年报过旧时优先用四季度单季还原 TTM（as_of 需足够晚以使 1231 年报超龄）。"""
        rows = [
            {"end_date": "20221231", "n_cashflow_act": 999.0, "n_cash_flows_inv_act": None},
            {"end_date": "20230331", "n_cashflow_act": 10.0, "n_cash_flows_inv_act": -1.0},
            {"end_date": "20230630", "n_cashflow_act": 22.0, "n_cash_flows_inv_act": -4.0},
            {"end_date": "20230930", "n_cashflow_act": 36.0, "n_cash_flows_inv_act": -9.0},
            {"end_date": "20231231", "n_cashflow_act": 50.0, "n_cash_flows_inv_act": -12.0},
            {"end_date": "20240331", "n_cashflow_act": 8.0, "n_cash_flows_inv_act": -2.0},
            {"end_date": "20240630", "n_cashflow_act": 18.0, "n_cash_flows_inv_act": -5.0},
            {"end_date": "20240930", "n_cashflow_act": 30.0, "n_cash_flows_inv_act": -8.0},
        ]
        ocf, cap, fcf, w = aggregate_ocf_and_capex_proxy_ttm(
            rows,
            max_periods=4,
            as_of=date(2027, 6, 1),
            annual_stale_days=550,
        )
        self.assertEqual(ocf, 44.0)
        self.assertAlmostEqual(cap, 11.0, places=6)
        self.assertAlmostEqual(fcf, 33.0, places=6)
        self.assertTrue(any("四季度" in x for x in w))


class BuildCompanyDcfPayloadTest(unittest.TestCase):
    def test_ok_with_mock_shares(self) -> None:
        base = _dcf_settings()
        cf = [{"end_date": "20231231", "n_cashflow_act": 100.0, "n_cash_flows_inv_act": -20.0}]
        # 净债务为 0：负债与现金相等，便于断言每股价值数量级
        bal = [{"end_date": "20231231", "total_liab": 200.0, "money_cap": 200.0}]
        payload = build_company_dcf_payload(
            cashflow_rows=cf,
            balance_rows=bal,
            settings=base,
            wacc_override=None,
            stage1_override=None,
            terminal_override=None,
            fetch_total_shares=lambda: 1_000_000.0,
        )
        self.assertTrue(payload["ok"])
        self.assertIsNotNone(payload["values"])
        assert payload["values"] is not None
        self.assertIn("value_per_share", payload["values"])
        # 基期 FCF=80（100 经营 − 20 投资流出代理），净债务 0，股本 1e6
        self.assertAlmostEqual(payload["values"]["value_per_share"], 0.0009826446, places=5)

    def test_shares_fetch_failure(self) -> None:
        base = _dcf_settings()
        cf = [{"end_date": "20231231", "n_cashflow_act": 100.0, "n_cash_flows_inv_act": None}]
        bal = [{"end_date": "20231231", "total_liab": 100.0, "money_cap": 0.0}]

        def boom() -> float:
            raise RuntimeError("network")

        payload = build_company_dcf_payload(
            cashflow_rows=cf,
            balance_rows=bal,
            settings=base,
            wacc_override=None,
            stage1_override=None,
            terminal_override=None,
            fetch_total_shares=boom,
        )
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["skip_reason"], "shares_unavailable")

    def test_financial_uses_ni_and_interest_bearing_debt(self) -> None:
        """银行：归母净利润作基期 + payload 有息负债 − 现金，避免负债合计含存款。"""
        base = _dcf_settings()
        cf = [{"end_date": "20231231", "n_cashflow_act": 999.0, "n_cash_flows_inv_act": None}]
        inc = [{"end_date": "20231231", "n_income_attr_p": 80.0, "report_type": "1"}]
        bal = [
            {
                "end_date": "20231231",
                "total_liab": 1_000_000.0,
                "money_cap": 100.0,
                "payload": {"st_borrow": 50.0, "lt_borrow": 50.0},
            }
        ]
        payload = build_company_dcf_payload(
            cashflow_rows=cf,
            balance_rows=bal,
            settings=base,
            wacc_override=None,
            stage1_override=None,
            terminal_override=None,
            fetch_total_shares=lambda: 1_000_000.0,
            industry="银行",
            income_rows=inc,
        )
        self.assertTrue(payload["ok"])
        assert payload["assumptions"] is not None
        self.assertEqual(payload["assumptions"]["fcf_base_source"], "financial_net_income_annual")
        self.assertEqual(payload["assumptions"]["dcf_sector_kind"], "financial")
        self.assertAlmostEqual(payload["values"]["value_per_share"], 0.0009826446, places=5)

    def test_financial_ignores_large_interest_debt_in_equity_bridge(self) -> None:
        """金融业有息代理再大也不应从 EV 中扣减，否则每股易为负。"""
        base = _dcf_settings()
        cf = [{"end_date": "20231231", "n_cashflow_act": 100.0, "n_cash_flows_inv_act": None}]
        inc = [{"end_date": "20231231", "n_income_attr_p": 80.0, "report_type": "1"}]
        bal = [
            {
                "end_date": "20231231",
                "total_liab": 9e12,
                "money_cap": 100.0,
                "payload": {"st_borrow": 5e11, "lt_borrow": 6e11},
            }
        ]
        payload = build_company_dcf_payload(
            cashflow_rows=cf,
            balance_rows=bal,
            settings=base,
            wacc_override=None,
            stage1_override=None,
            terminal_override=None,
            fetch_total_shares=lambda: 1_000_000.0,
            industry="银行",
            income_rows=inc,
        )
        self.assertTrue(payload["ok"])
        assert payload["assumptions"] is not None
        self.assertEqual(payload["assumptions"]["net_debt"], 0.0)
        self.assertGreater(payload["assumptions"]["balance_sheet_net_debt_proxy"], 1e12)
        self.assertTrue(payload["assumptions"]["financial_equity_direct_bridge"])
        assert payload["values"] is not None
        self.assertGreater(payload["values"]["value_per_share"], 0.0)
        self.assertAlmostEqual(payload["values"]["equity_value"], payload["values"]["enterprise_value"], places=2)

    def test_financial_ni_scale_reduces_per_share_linearly(self) -> None:
        base_one = _dcf_settings(financial_ni_base_scale=1.0)
        base_half = _dcf_settings(financial_ni_base_scale=0.5)
        cf = [{"end_date": "20231231", "n_cashflow_act": 1.0, "n_cash_flows_inv_act": None}]
        inc = [{"end_date": "20231231", "n_income_attr_p": 100.0, "report_type": "1"}]
        bal = [
            {
                "end_date": "20231231",
                "total_liab": 1e6,
                "money_cap": 0.0,
                "payload": {"st_borrow": 1.0, "lt_borrow": 1.0},
            }
        ]
        kwargs = dict(
            cashflow_rows=cf,
            balance_rows=bal,
            wacc_override=None,
            stage1_override=None,
            terminal_override=None,
            fetch_total_shares=lambda: 1_000_000.0,
            industry="银行",
            income_rows=inc,
        )
        p1 = build_company_dcf_payload(settings=base_one, **kwargs)
        p2 = build_company_dcf_payload(settings=base_half, **kwargs)
        assert p1["values"] and p2["values"] and p1["assumptions"] and p2["assumptions"]
        self.assertAlmostEqual(float(p2["assumptions"]["base_fcf"]), float(p1["assumptions"]["base_fcf"]) * 0.5)
        self.assertAlmostEqual(float(p2["values"]["equity_value"]), float(p1["values"]["equity_value"]) * 0.5, delta=1.0)


if __name__ == "__main__":
    unittest.main()
