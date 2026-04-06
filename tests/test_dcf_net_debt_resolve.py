"""行业分档净债务解析。"""

from __future__ import annotations

import unittest

from value_screener.application.dcf_net_debt_resolve import resolve_net_debt_for_sector
from value_screener.domain.dcf_sector_policy import DcfSectorKind


class DcfNetDebtResolveTest(unittest.TestCase):
    def test_general_total_liab(self) -> None:
        rows = [{"end_date": "20231231", "total_liab": 500.0, "money_cap": 200.0}]
        net, method, _w = resolve_net_debt_for_sector(rows, DcfSectorKind.GENERAL)
        self.assertEqual(net, 300.0)
        self.assertEqual(method, "total_liab_minus_money_cap")

    def test_financial_sums_payload_fields(self) -> None:
        rows = [
            {
                "end_date": "20231231",
                "total_liab": 9_999_999.0,
                "money_cap": 100.0,
                "payload": {"st_borrow": 40.0, "lt_borrow": 60.0},
            }
        ]
        net, method, _w = resolve_net_debt_for_sector(rows, DcfSectorKind.FINANCIAL)
        self.assertEqual(net, 0.0)
        self.assertEqual(method, "financial_interest_bearing_minus_money_cap")

    def test_financial_missing_interest_fallback_zero(self) -> None:
        rows = [{"end_date": "20231231", "total_liab": 1e6, "money_cap": 0.0, "payload": {}}]
        net, method, _w = resolve_net_debt_for_sector(rows, DcfSectorKind.FINANCIAL)
        self.assertEqual(net, 0.0)
        self.assertEqual(method, "financial_interest_debt_missing_fallback_zero")

    def test_real_estate_contract_liab(self) -> None:
        rows = [
            {
                "end_date": "20231231",
                "total_liab": 1000.0,
                "money_cap": 50.0,
                "payload": {"contract_liab": 400.0},
            }
        ]
        net, method, _w = resolve_net_debt_for_sector(rows, DcfSectorKind.REAL_ESTATE)
        self.assertEqual(net, 550.0)
        self.assertEqual(method, "real_estate_liab_minus_contract_liab_minus_cash")


if __name__ == "__main__":
    unittest.main()
