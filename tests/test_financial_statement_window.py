from __future__ import annotations

import unittest
from datetime import date

from value_screener.application.financial_statement_window import (
    end_date_in_window,
    statement_api_date_bounds,
)


class TestStatementApiDateBounds(unittest.TestCase):
    def test_three_year_window_jan1_to_today(self) -> None:
        start, end = statement_api_date_bounds(since_years=3, today=date(2026, 4, 4))
        self.assertEqual(start, "20230101")
        self.assertEqual(end, "20260404")

    def test_five_year_window_jan1_to_today(self) -> None:
        start, end = statement_api_date_bounds(since_years=5, today=date(2026, 4, 4))
        self.assertEqual(start, "20210101")
        self.assertEqual(end, "20260404")

    def test_default_since_years_matches_five_year_window(self) -> None:
        start, end = statement_api_date_bounds(today=date(2026, 4, 4))
        self.assertEqual(start, "20210101")
        self.assertEqual(end, "20260404")

    def test_one_year(self) -> None:
        start, end = statement_api_date_bounds(since_years=1, today=date(2026, 12, 31))
        self.assertEqual(start, "20250101")
        self.assertEqual(end, "20261231")

    def test_invalid_since_years(self) -> None:
        with self.assertRaises(ValueError):
            statement_api_date_bounds(since_years=0)


class TestEndDateInWindow(unittest.TestCase):
    def test_inclusive_bounds(self) -> None:
        self.assertTrue(end_date_in_window("20230331", start="20230101", end="20261231"))
        self.assertTrue(end_date_in_window("20230101", start="20230101", end="20261231"))
        self.assertTrue(end_date_in_window("20261231", start="20230101", end="20261231"))

    def test_outside_window(self) -> None:
        self.assertFalse(end_date_in_window("20221231", start="20230101", end="20261231"))
        self.assertFalse(end_date_in_window("20270101", start="20230101", end="20261231"))

    def test_invalid_end_date(self) -> None:
        self.assertFalse(end_date_in_window("", start="20230101", end="20261231"))
        self.assertFalse(end_date_in_window("202331", start="20230101", end="20261231"))
        self.assertFalse(end_date_in_window("abcdefgh", start="20230101", end="20261231"))

    def test_quarter_and_year_end_dates_in_three_year_window(self) -> None:
        """季报末 0331/0630/0930/1231 与年报 1231、半年报常用 0630 均在窗内时应为 True。"""

        start, end = "20230101", "20260404"
        for ed in (
            "20230331",
            "20230630",
            "20230930",
            "20231231",
            "20240331",
            "20240630",
            "20240930",
            "20241231",
            "20250331",
            "20250630",
            "20250930",
            "20251231",
            "20260331",
        ):
            with self.subTest(end_date=ed):
                self.assertTrue(end_date_in_window(ed, start=start, end=end))


if __name__ == "__main__":
    unittest.main()
