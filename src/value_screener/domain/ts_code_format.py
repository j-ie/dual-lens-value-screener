from __future__ import annotations

import re

_TS_CODE_PATTERN = re.compile(r"^\d{6}\.(SH|SZ|BJ)$", re.IGNORECASE)


def is_valid_ts_code(value: str) -> bool:
    """TuShare A 股 ts_code：6 位数字 + 交易所后缀。"""

    s = str(value).strip().upper()
    return bool(_TS_CODE_PATTERN.fullmatch(s))
