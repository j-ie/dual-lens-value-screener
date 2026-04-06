"""DCF 按需财报同步判定（无网络）。"""

from __future__ import annotations

import unittest

from value_screener.application.dcf_financial_sync import dcf_financials_need_tushare_refresh


class DcfFinancialSyncTest(unittest.TestCase):
    def test_empty_cashflow_triggers_refresh(self) -> None:
        bal = [{"end_date": "20231231", "total_liab": 100.0, "money_cap": 0.0}]
        self.assertTrue(dcf_financials_need_tushare_refresh([], bal))

    def test_all_investing_missing_triggers_refresh(self) -> None:
        cf = [
            {"end_date": "20231231", "n_cashflow_act": 10.0, "n_cash_flows_inv_act": None},
            {"end_date": "20230930", "n_cashflow_act": 10.0, "n_cash_flows_inv_act": None},
        ]
        bal = [{"end_date": "20231231", "total_liab": 100.0, "money_cap": 0.0}]
        self.assertTrue(dcf_financials_need_tushare_refresh(cf, bal))

    def test_complete_data_no_refresh(self) -> None:
        cf = [
            {"end_date": "20231231", "n_cashflow_act": 10.0, "n_cash_flows_inv_act": -2.0},
        ]
        bal = [{"end_date": "20231231", "total_liab": 100.0, "money_cap": 0.0}]
        self.assertFalse(dcf_financials_need_tushare_refresh(cf, bal))


if __name__ == "__main__":
    unittest.main()
