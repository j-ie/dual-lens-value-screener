from __future__ import annotations

from datetime import date, datetime, timezone

# 与默认同步窗口 `statement_api_date_bounds(since_years=5)` 下「满季报」规模一致（约 5×4 个报告期）。
DEFAULT_FINANCIAL_PERIODS_LIMIT = 20


def statement_api_date_bounds(*, since_years: int = 5, today: date | None = None) -> tuple[str, str]:
    """
    TuShare `start_date` / `end_date`（YYYYMMDD）：从 (今年 - since_years) 年 1 月 1 日起至今天。
    """

    if since_years < 1:
        raise ValueError("since_years 至少为 1")
    d = today or date.today()
    start_year = d.year - since_years
    start = f"{start_year:04d}0101"
    end = d.strftime("%Y%m%d")
    return start, end


def end_date_in_window(end_date_str: str, *, start: str, end: str) -> bool:
    """报告期是否在闭区间 [start, end]（字符串 YYYYMMDD 比较）。"""

    s = (end_date_str or "").strip()
    if len(s) != 8 or not s.isdigit():
        return False
    return start <= s <= end


def utc_now() -> datetime:
    return datetime.now(timezone.utc)
