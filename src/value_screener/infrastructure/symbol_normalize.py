from __future__ import annotations


def to_ts_code(symbol: str) -> str:
    """
    将 600519 / 600519.SH / sh600519 规范为 Tushare 风格 ts_code。
    """

    s = symbol.strip().upper()
    for suf in (".SH", ".SZ", ".BJ"):
        if s.endswith(suf):
            return s
    raw = s.replace("SH", "").replace("SZ", "").replace("BJ", "")
    if raw.startswith("SH") or raw.startswith("SZ") or raw.startswith("BJ"):
        raw = raw[2:]
    digits = "".join(c for c in raw if c.isdigit())
    if len(digits) < 6:
        return symbol.strip().upper()
    digits = digits[:6]
    if digits.startswith("6") or digits.startswith("68"):
        return f"{digits}.SH"
    if digits.startswith("0") or digits.startswith("3"):
        return f"{digits}.SZ"
    if digits.startswith("4") or digits.startswith("8") or digits.startswith("92"):
        return f"{digits}.BJ"
    return f"{digits}.SZ"


def to_ak_symbol(ts_code: str) -> str:
    """AkShare 东方财富类接口常用 6 位代码。"""

    return ts_code.split(".")[0]
