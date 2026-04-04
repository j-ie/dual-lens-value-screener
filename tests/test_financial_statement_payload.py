from __future__ import annotations

import math
import unittest

from value_screener.application.financial_statement_payload import sanitize_financial_row, to_float_or_none


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


if __name__ == "__main__":
    unittest.main()
