import unittest

from value_screener.infrastructure.symbol_normalize import to_ts_code


class SymbolNormalizeTest(unittest.TestCase):
    def test_sh_main_board(self) -> None:
        self.assertEqual(to_ts_code("600519"), "600519.SH")

    def test_sz(self) -> None:
        self.assertEqual(to_ts_code("000001"), "000001.SZ")

    def test_already_suffixed(self) -> None:
        self.assertEqual(to_ts_code("300750.SZ"), "300750.SZ")


if __name__ == "__main__":
    unittest.main()
