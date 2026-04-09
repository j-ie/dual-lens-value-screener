"""TuShare 交易日解析：与接口返回顺序无关，应取最近开市日。"""

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from value_screener.infrastructure.tushare_provider import (
    TushareAShareProvider,
    _DailyBasicMaps,
)


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

    def test_resolve_effective_daily_basic_falls_back_when_latest_empty(self) -> None:
        """日历最新日为开市日但 daily_basic 未落库时，应使用上一有数据会话。"""
        maps = _DailyBasicMaps(
            mv_wan={"000001.SZ": 1.0},
            dv_ratio={},
            dv_ttm={},
            spot_dv_pct={},
        )

        def fake_open(self, *, max_sessions: int = 60) -> list[str]:
            del self, max_sessions
            return ["20260409", "20260408"]

        def fake_try(self, trade_date: str) -> _DailyBasicMaps | None:
            del self
            if trade_date == "20260409":
                return None
            if trade_date == "20260408":
                return maps
            return None

        provider = object.__new__(TushareAShareProvider)
        with patch.object(TushareAShareProvider, "_open_trade_dates_descending", fake_open), patch.object(
            TushareAShareProvider,
            "_try_load_daily_basic_maps",
            fake_try,
        ):
            td, got = provider._resolve_effective_daily_basic_session()
        self.assertEqual(td, "20260408")
        self.assertIs(got, maps)


if __name__ == "__main__":
    unittest.main()
