"""TuShare 调用：按「自然分钟」的全局限流与分钟配额错误退避。

TuShare 文案「每分钟最多 N 次」在服务端通常按**日历分钟**计数，而非任意连续 60 秒滑动窗口。
若使用滑动窗口，在分钟边界附近可能在数秒内连续打满两个窗口，仍触发平台限流。
本实现按指定时区（默认与财报调度日一致：Asia/Shanghai）的 wall-clock 分钟桶计数。
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta
from typing import Callable
from zoneinfo import ZoneInfo


def is_tushare_minute_rate_limit_error(exc: BaseException) -> bool:
    """识别 TuShare「每分钟最多访问」类返回（中英文）。"""

    text = str(exc)
    lowered = text.lower()
    return (
        "每分钟" in text
        or "per minute" in lowered
        or ("too many" in lowered and "minute" in lowered)
    )


def sleep_until_next_minute_wall_clock(
    *,
    time_fn: Callable[[], float] = time.time,
    sleep_fn: Callable[[float], None] = time.sleep,
    jitter_seconds: float = 0.05,
) -> None:
    """睡到下一自然分钟边界后少量抖动，避免与窗口边界齐发。"""

    wall = time_fn()
    next_boundary = int(wall // 60) * 60 + 60
    delay = max(0.0, next_boundary - wall + max(0.0, jitter_seconds))
    sleep_fn(delay)


class TushareRateLimiter:
    """
    按时区「自然分钟」计数：同一分钟内最多 max_calls_per_minute 次 acquire。
    与 TuShare「每分钟 N 次」口径对齐，避免滑动 60s 在整点两侧叠加超配额。
    """

    def __init__(
        self,
        max_calls_per_minute: int = 200,
        *,
        zone_name: str = "Asia/Shanghai",
        time_fn: Callable[[], float] = time.time,
        sleep_fn: Callable[[float], None] = time.sleep,
        boundary_jitter_seconds: float = 0.08,
    ) -> None:
        if max_calls_per_minute < 1:
            raise ValueError("max_calls_per_minute 至少为 1")
        self._max = int(max_calls_per_minute)
        self._zone = ZoneInfo(zone_name)
        self._time_fn = time_fn
        self._sleep = sleep_fn
        self._boundary_jitter = max(0.0, float(boundary_jitter_seconds))
        self._lock = threading.Lock()
        self._bucket_key: str | None = None
        self._count_in_bucket = 0

    def _minute_bucket_key(self, wall: float) -> str:
        dt = datetime.fromtimestamp(wall, tz=self._zone)
        return dt.strftime("%Y-%m-%d %H:%M")

    def _seconds_until_next_minute(self, wall: float) -> float:
        dt = datetime.fromtimestamp(wall, tz=self._zone)
        nxt = dt.replace(second=0, microsecond=0) + timedelta(minutes=1)
        return max(0.0, nxt.timestamp() - wall + self._boundary_jitter)

    def acquire(self) -> None:
        """阻塞直到当前自然分钟内计数未超上限，并记录一次调用。"""

        while True:
            with self._lock:
                wall = self._time_fn()
                key = self._minute_bucket_key(wall)
                if self._bucket_key != key:
                    self._bucket_key = key
                    self._count_in_bucket = 0
                if self._count_in_bucket < self._max:
                    self._count_in_bucket += 1
                    return
                wait = self._seconds_until_next_minute(wall)
            if wait > 0:
                self._sleep(wait)
