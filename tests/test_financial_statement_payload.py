from __future__ import annotations

import math
import unittest

from value_screener.application.financial_statement_payload import (
    cashflow_scalars,
    merge_core_columns_with_payload,
    sanitize_financial_row,
    to_float_or_none,
)


class TestFinancialStatementPayload(unittest.TestCase):
    def test_sanitize_nan_to_none(self) -> None:
        row = {"ts_code": "000001.SZ", "end_date": "20231231", "total_revenue": float("nan")}
        out = sanitize_financial_row(row)
        self.assertIsNone(out["total_revenue"])

    def test_to_float_or_none(self) -> None:
        self.assertIsNone(to_float_or_none(None))
        self.assertIsNone(to_float_or_none(float("nan")))
        self.assertAlmostEqual(to_float_or_none("1.5"), 1.5)
        self.assertIsNone(to_float_or_none("x"))

    def test_merge_core_columns_with_payload_fills_none_from_payload(self) -> None:
        row = {
            "ts_code": "000001.SZ",
            "end_date": "20231231",
            "total_revenue": None,
            "payload": {"total_revenue": 1.0e8, "revenue": 9.0e7, "st_borr": 1.0},
        }
        m = merge_core_columns_with_payload(row)
        self.assertEqual(m.get("total_revenue"), 1.0e8)
        self.assertEqual(m.get("revenue"), 9.0e7)
        self.assertEqual(m.get("st_borr"), 1.0)
        self.assertNotIn("payload", m)

    def test_cashflow_scalars_maps_tushare_investing_field(self) -> None:
        row = {"n_cashflow_act": 1.0, "n_cashflow_inv_act": -2.0}
        out = cashflow_scalars(row)
        self.assertEqual(out["n_cash_flows_inv_act"], -2.0)


if __name__ == "__main__":
    unittest.main()
