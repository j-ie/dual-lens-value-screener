from __future__ import annotations

import unittest

from value_screener.application.sync_financial_statements import (
    _filter_window,
    _merged_end_date_suffix_counts,
)


class TestFilterWindow(unittest.TestCase):
    def test_keeps_quarterly_and_annual_inside_window(self) -> None:
        rows = [
            {"end_date": "20230331", "report_type": "1"},
            {"end_date": "20230630"},
            {"end_date": "20230930"},
            {"end_date": "20231231"},
            {"end_date": "20221231"},
            {"end_date": "invalid"},
        ]
        out = _filter_window(rows, start="20230101", end="20261231")
        ends = {r["end_date"] for r in out}
        self.assertEqual(ends, {"20230331", "20230630", "20230930", "20231231"})

    def test_does_not_filter_by_report_type(self) -> None:
        rows = [
            {"end_date": "20240630", "report_type": "2"},
            {"end_date": "20240630", "report_type": "1"},
        ]
        out = _filter_window(rows, start="20240101", end="20261231")
        self.assertEqual(len(out), 2)


class TestMergedEndDateSuffixCounts(unittest.TestCase):
    def test_unique_periods_by_mmdd(self) -> None:
        inc = [{"end_date": "20230331"}, {"end_date": "20230630"}]
        bal = [{"end_date": "20230331"}]
        cf = [{"end_date": "20230930"}]
        hist = _merged_end_date_suffix_counts(inc, bal, cf)
        self.assertEqual(hist, {"0331": 1, "0630": 1, "0930": 1})


if __name__ == "__main__":
    unittest.main()
