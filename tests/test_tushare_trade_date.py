"""TuShare 交易日解析：与接口返回顺序无关，应取最近开市日。"""

import unittest
from unittest.mock import MagicMock

import pandas as pd

from value_screener.infrastructure.tushare_provider import TushareAShareProvider


class TushareLatestTradeDateTest(unittest.TestCase):
    def test_latest_open_trade_date_uses_max_when_cal_descending(self) -> None:
        """trade_cal 常为降序（新→旧），[-1] 会得到 20200102 类最早日期，应取 max。"""
        cal_df = pd.DataFrame(
            {
                "cal_date": ["20260403", "20260402", "20200102"],
                "is_open": [1, 1, 1],
            }
        )
        mock_pro = MagicMock()
        mock_pro.trade_cal.return_value = cal_df

        provider = object.__new__(TushareAShareProvider)
        provider._pro = mock_pro

        self.assertEqual(provider._latest_open_trade_date(), "20260403")

    def test_is_open_accepts_string_one(self) -> None:
        cal_df = pd.DataFrame(
            {
                "cal_date": ["20260401", "20260328"],
                "is_open": ["1", "1"],
            }
        )
        mock_pro = MagicMock()
        mock_pro.trade_cal.return_value = cal_df

        provider = object.__new__(TushareAShareProvider)
        provider._pro = mock_pro

        self.assertEqual(provider._latest_open_trade_date(), "20260401")


if __name__ == "__main__":
    unittest.main()
