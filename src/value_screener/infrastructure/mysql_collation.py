"""MySQL 跨表字符串比较：统一 COLLATE，避免 utf8mb4_unicode_ci 与 general_ci 混用触发 1267。"""

from __future__ import annotations

from typing import Any

TS_CODE_COLLATION = "utf8mb4_unicode_ci"


def ts_code_equals(left: Any, right: Any) -> Any:
    return left.collate(TS_CODE_COLLATION) == right.collate(TS_CODE_COLLATION)
