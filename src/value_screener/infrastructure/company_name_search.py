"""公司名称（及代码）模糊检索：LIKE 子串匹配，转义 % _ \\。"""

from __future__ import annotations

from typing import Any

from sqlalchemy import ColumnElement, func, or_


def normalized_company_search_term(raw: str | None) -> str | None:
    """去首尾空白；空串视为未筛选。"""

    if raw is None:
        return None
    s = str(raw).strip()
    return s if s else None


def escape_sql_like_fragment(text: str) -> str:
    """避免用户输入中的通配符改变 LIKE 语义。"""

    return text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def company_display_and_code_match_clause(
    *,
    ref_name_col: Any,
    ref_fullname_col: Any,
    ts_code_col: Any,
    term: str,
) -> ColumnElement[bool]:
    """
    匹配证券简称、公司全称或 ts_code 子串（OR）。
    term 须为非空规范化字符串。
    """

    esc = escape_sql_like_fragment(term)
    pat = f"%{esc}%"
    like_kw = {"escape": "\\"}
    return or_(
        func.coalesce(ref_name_col, "").like(pat, **like_kw),
        func.coalesce(ref_fullname_col, "").like(pat, **like_kw),
        ts_code_col.like(pat, **like_kw),
    )


__all__ = [
    "company_display_and_code_match_clause",
    "escape_sql_like_fragment",
    "normalized_company_search_term",
]
