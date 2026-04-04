"""ts_code 格式校验。"""

from __future__ import annotations

import unittest

from value_screener.domain.ts_code_format import is_valid_ts_code


class TsCodeFormatTests(unittest.TestCase):
    def test_valid(self) -> None:
        self.assertTrue(is_valid_ts_code("600519.SH"))
        self.assertTrue(is_valid_ts_code("000001.sz"))
        self.assertTrue(is_valid_ts_code("920001.BJ"))

    def test_invalid(self) -> None:
        self.assertFalse(is_valid_ts_code("600519"))
        self.assertFalse(is_valid_ts_code("600519.SS"))
        self.assertFalse(is_valid_ts_code(""))
        self.assertFalse(is_valid_ts_code("6005191.SH"))


if __name__ == "__main__":
    unittest.main()
