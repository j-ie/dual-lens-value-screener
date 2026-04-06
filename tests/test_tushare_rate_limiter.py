"""TuShare 全局限流与分钟错误识别。"""

from __future__ import annotations

import unittest

from value_screener.infrastructure.tushare_rate_limiter import (
    TushareRateLimiter,
    is_tushare_minute_rate_limit_error,
    sleep_until_next_minute_wall_clock,
)


class TestMinuteRateLimitDetection(unittest.TestCase):
    def test_chinese_message(self) -> None:
        self.assertTrue(
            is_tushare_minute_rate_limit_error(
                Exception("抱歉，您每分钟最多访问该接口200次"),
            )
        )

    def test_unrelated_error(self) -> None:
        self.assertFalse(is_tushare_minute_rate_limit_error(Exception("network timeout")))


class TestSleepUntilNextMinute(unittest.TestCase):
    def test_sleeps_until_boundary(self) -> None:
        sleeps: list[float] = []
        fake_time = {"t": 100.0}  # 1:40 from epoch minute

        def time_fn() -> float:
            return fake_time["t"]

        def sleep_fn(d: float) -> None:
            sleeps.append(d)
            fake_time["t"] += d

        sleep_until_next_minute_wall_clock(
            time_fn=time_fn,
            sleep_fn=sleep_fn,
            jitter_seconds=0.05,
        )
        self.assertEqual(len(sleeps), 1)
        self.assertGreater(sleeps[0], 15.0)


class TestTushareRateLimiter(unittest.TestCase):
    def test_calendar_minute_cap_utc(self) -> None:
        """同一 UTC 自然分钟内超过上限须 sleep 跨分钟。"""

        t = [0.0]
        sleeps: list[float] = []

        def time_fn() -> float:
            return t[0]

        def sleep_fn(d: float) -> None:
            sleeps.append(d)
            t[0] += d

        lim = TushareRateLimiter(
            2,
            zone_name="UTC",
            time_fn=time_fn,
            sleep_fn=sleep_fn,
            boundary_jitter_seconds=0.0,
        )
        lim.acquire()
        lim.acquire()
        lim.acquire()
        self.assertTrue(sleeps, "第三拍应跨到下一自然分钟")


if __name__ == "__main__":
    unittest.main()
