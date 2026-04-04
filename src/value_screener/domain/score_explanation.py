from __future__ import annotations

from typing import Any


def _fmt_num(v: Any) -> str | None:
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return str(v)
    if abs(f) >= 1000 or abs(f - round(f)) < 1e-6:
        return f"{f:.2f}"
    return f"{f:.4f}"


def build_score_explanation_zh(
    graham: dict[str, Any],
    buffett: dict[str, Any],
    provenance: dict[str, Any] | None,
) -> str:
    """
    基于已有 Graham/Buffett 评估 JSON 与 provenance 生成简短中文说明（不改变计分逻辑）。
    """

    lines: list[str] = []

    prov = provenance or {}
    ds = prov.get("data_source")
    td = prov.get("trade_cal_date")
    fd = prov.get("financials_end_date")
    if ds or td or fd:
        head: list[str] = []
        if ds:
            head.append(f"数据源 {ds}")
        if td:
            head.append(f"市值锚定交易日 {td}")
        if fd:
            head.append(f"财报截止期 {fd}")
        lines.append("；".join(head) + "。")

    b_parts: list[str] = []
    roe = _fmt_num(buffett.get("roe"))
    if roe is not None:
        b_parts.append(f"ROE≈{roe}")
    de = _fmt_num(buffett.get("debt_to_equity"))
    if de is not None:
        b_parts.append(f"负债/权益≈{de}")
    ocf = _fmt_num(buffett.get("ocf_to_net_income"))
    if ocf is not None:
        b_parts.append(f"经营现金流/净利润≈{ocf}")
    b_notes = buffett.get("notes") or {}
    if isinstance(b_notes, dict):
        if b_notes.get("debt_proxy_crude"):
            b_parts.append("杠杆为负债合计/权益粗算")
        if b_notes.get("weak_cash_conversion"):
            b_parts.append("现金流转化偏弱")
    if b_parts:
        lines.append("巴菲特维度：" + "，".join(b_parts) + f"（分 {buffett.get('score')}）。")

    g_parts: list[str] = []
    mnc = _fmt_num(graham.get("market_cap_to_ncav"))
    if mnc is not None:
        g_parts.append(f"市值/NCAV≈{mnc}")
    cr = _fmt_num(graham.get("current_ratio"))
    if cr is not None:
        g_parts.append(f"流动比率≈{cr}")
    ptb = _fmt_num(graham.get("price_to_book"))
    if ptb is not None:
        g_parts.append(f"市净≈{ptb}")
    g_notes = graham.get("notes") or {}
    if isinstance(g_notes, dict):
        if g_notes.get("net_net_tendency"):
            g_parts.append("偏净流动资产折价倾向")
        if g_notes.get("ncav_non_positive"):
            g_parts.append("NCAV 非正")
    if g_parts:
        lines.append("格雷厄姆维度：" + "，".join(g_parts) + f"（分 {graham.get('score')}）。")

    if not lines:
        return f"巴菲特分 {buffett.get('score')}，格雷厄姆分 {graham.get('score')}。"

    return "".join(lines)
