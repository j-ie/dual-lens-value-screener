"""财报同步任务参数指纹与续跑下标。"""

from __future__ import annotations

import unittest
from datetime import date

from value_screener.application.financial_statement_job_params import (
    default_scheduled_date,
    financial_statement_job_params_hash,
    universe_fingerprint,
)
from value_screener.application.sync_financial_statements import resume_start_index


class TestParamsHash(unittest.TestCase):
    def test_stable(self) -> None:
        a = financial_statement_job_params_hash(
            since_years=3,
            max_symbols=10,
            api_start="20230101",
            api_end="20261231",
        )
        b = financial_statement_job_params_hash(
            since_years=3,
            max_symbols=10,
            api_start="20230101",
            api_end="20261231",
        )
        self.assertEqual(a, b)
        self.assertEqual(len(a), 32)

    def test_max_symbols_none_differs(self) -> None:
        with_limit = financial_statement_job_params_hash(
            since_years=3,
            max_symbols=10,
            api_start="20230101",
            api_end="20261231",
        )
        full = financial_statement_job_params_hash(
            since_years=3,
            max_symbols=None,
            api_start="20230101",
            api_end="20261231",
        )
        self.assertNotEqual(with_limit, full)


class TestUniverseFingerprint(unittest.TestCase):
    def test_order_matters(self) -> None:
        a = universe_fingerprint(["000001.SZ", "000002.SZ"])
        b = universe_fingerprint(["000002.SZ", "000001.SZ"])
        self.assertNotEqual(a, b)


class TestResumeStartIndex(unittest.TestCase):
    def test_no_resume(self) -> None:
        syms = ["000001.SZ", "000002.SZ"]
        self.assertEqual(
            resume_start_index(syms, resume=False, cursor_ts_code="000002.SZ"),
            0,
        )

    def test_cursor_next_symbol(self) -> None:
        syms = ["000001.SZ", "000002.SZ", "000003.SZ"]
        self.assertEqual(
            resume_start_index(syms, resume=True, cursor_ts_code="000002.SZ"),
            1,
        )

    def test_missing_cursor(self) -> None:
        syms = ["000001.SZ"]
        self.assertEqual(
            resume_start_index(syms, resume=True, cursor_ts_code="999999.SZ"),
            0,
        )


class TestDefaultScheduledDate(unittest.TestCase):
    def test_returns_date(self) -> None:
        d = default_scheduled_date(tz_name="Asia/Shanghai")
        self.assertIsInstance(d, date)


if __name__ == "__main__":
    unittest.main()
