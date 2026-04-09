"""
DCF 基期经营现金流与资本开支代理：年报单期优先，否则按 A 股「年内累计」口径还原最近四季度 TTM。

避免将多期年报或混排报告期简单相加导致基数虚高。
"""

from __future__ import annotations

from datetime import date
from typing import Any

from value_screener.application.financial_statement_payload import investing_cashflow_net_from_row, to_float_or_none

_QUARTER_SUFFIXES = ("0331", "0630", "0930", "1231")


def _sort_by_end_date_desc(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda r: str(r.get("end_date") or ""), reverse=True)


def _report_type_rank(report_type: Any) -> tuple[int, str]:
    s = str(report_type).strip() if report_type is not None else ""
    if s == "1":
        return (0, s)
    return (1, s)


def merge_statement_rows_by_end_date(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """
    同一报告期多条记录时优先合并报表（report_type=1），供现金流量表、利润表等复用。
    """

    return _merge_rows_by_end_date([dict(x) for x in rows])


def _merge_rows_by_end_date(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """同一报告期多条记录时优先合并报表（report_type=1）。"""
    buckets: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        ed = str(row.get("end_date") or "").strip()
        if len(ed) != 8 or not ed.isdigit():
            continue
        buckets.setdefault(ed, []).append(row)
    merged: dict[str, dict[str, Any]] = {}
    for ed, lst in buckets.items():
        lst.sort(key=lambda r: _report_type_rank(r.get("report_type")))
        merged[ed] = dict(lst[0])
    return merged


def _days_since_end_date(as_of: date, end_date_str: str) -> int | None:
    if len(end_date_str) != 8 or not end_date_str.isdigit():
        return None
    try:
        ed = date(int(end_date_str[:4]), int(end_date_str[4:6]), int(end_date_str[6:8]))
    except ValueError:
        return None
    return (as_of - ed).days


def _extract_quarterly_increments(
    by_ed: dict[str, dict[str, Any]],
) -> list[tuple[str, float, float | None]]:
    """
    自年内累计现金流量还原单季值，按报告期升序排列。

    某年缺少 0331 锚点则跳过该年（避免误拆累计口径）。
    """
    years = sorted(
        {int(ed[:4]) for ed in by_ed if len(ed) == 8 and ed[4:] in _QUARTER_SUFFIXES}
    )
    singles: list[tuple[str, float, float | None]] = []
    for y in years:
        k1, k2, k3, k4 = f"{y}0331", f"{y}0630", f"{y}0930", f"{y}1231"
        r1, r2, r3, r4 = by_ed.get(k1), by_ed.get(k2), by_ed.get(k3), by_ed.get(k4)
        o1 = to_float_or_none(r1.get("n_cashflow_act")) if r1 else None
        o2 = to_float_or_none(r2.get("n_cashflow_act")) if r2 else None
        o3 = to_float_or_none(r3.get("n_cashflow_act")) if r3 else None
        o4 = to_float_or_none(r4.get("n_cashflow_act")) if r4 else None
        i1 = investing_cashflow_net_from_row(r1) if r1 else None
        i2 = investing_cashflow_net_from_row(r2) if r2 else None
        i3 = investing_cashflow_net_from_row(r3) if r3 else None
        i4 = investing_cashflow_net_from_row(r4) if r4 else None

        if o1 is None:
            continue
        singles.append((k1, o1, i1))
        if o2 is None:
            continue
        inv_q2 = (i2 - i1) if (i2 is not None and i1 is not None) else None
        singles.append((k2, o2 - o1, inv_q2))
        if o3 is None:
            continue
        inv_q3 = (i3 - i2) if (i3 is not None and i2 is not None) else None
        singles.append((k3, o3 - o2, inv_q3))
        if o4 is None:
            continue
        inv_q4 = (i4 - i3) if (i4 is not None and i3 is not None) else None
        singles.append((k4, o4 - o3, inv_q4))
    return singles


def _latest_annual_row(by_ed: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    candidates = [ed for ed in by_ed if ed.endswith("1231")]
    if not candidates:
        return None
    best_ed = max(candidates)
    row = dict(by_ed[best_ed])
    row["end_date"] = best_ed
    return row


def _capex_from_inv_increment(inv_inc: float | None) -> tuple[float, bool]:
    """单季投资现金流增量 → 资本开支代理；若无增量则无法扣减。"""
    if inv_inc is None:
        return 0.0, False
    return max(0.0, -float(inv_inc)), True


def aggregate_ocf_and_capex_proxy_ttm(
    cashflow_rows: list[dict[str, Any]],
    *,
    max_periods: int,
    as_of: date | None = None,
    annual_stale_days: int = 550,
) -> tuple[float | None, float | None, float | None, list[str]]:
    """
    返回 (ocf_ttm_or_annual_proxy, capex_proxy_ttm_or_annual, fcf_base, warnings)。

    max_periods：兼容旧参数；若实际返回行数少于该值则提示期数不足。
    """
    warnings: list[str] = []
    ref = as_of or date.today()
    stale_days = max(30, int(annual_stale_days))

    merged = _merge_rows_by_end_date([dict(x) for x in cashflow_rows])
    if not merged:
        return None, None, None, ["缺少现金流量表或报告期字段无效，无法估算基期现金流"]

    if len(cashflow_rows) < max_periods:
        warnings.append(
            f"现金流量表仅 {len(cashflow_rows)} 期（少于配置的 {max_periods} 期上限），"
            "季度还原或年报选择可能不完整"
        )

    singles = _extract_quarterly_increments(merged)
    annual_row = _latest_annual_row(merged)
    annual_ed = str(annual_row.get("end_date") or "") if annual_row else ""
    annual_age = _days_since_end_date(ref, annual_ed) if annual_ed else None
    annual_stale = annual_row is None or annual_age is None or annual_age > stale_days

    def _from_annual(row: dict[str, Any], *, stale_note: bool) -> tuple[float | None, float | None, float | None, list[str]]:
        ed = str(row.get("end_date") or "")
        ocf = to_float_or_none(row.get("n_cashflow_act"))
        if ocf is None:
            return None, None, None, ["年报缺少经营现金流 n_cashflow_act"]
        inv = investing_cashflow_net_from_row(row)
        wloc: list[str] = []
        if stale_note:
            wloc.append(
                f"最近一期年报报告期为 {ed}，距今 {annual_age} 天，已超过 {stale_days} 天阈值，"
                "仍采用年报全年口径作基期（建议核对是否应改用季度 TTM）"
            )
        if inv is None:
            wloc.append("缺少投资活动现金流（n_cashflow_inv_act），未扣除资本开支代理，基期现金流等于年报经营现金流")
            return float(ocf), None, float(ocf), wloc
        cap = max(0.0, -float(inv))
        return float(ocf), cap, float(ocf) - cap, wloc

    def _from_quarters_ttm(last4: list[tuple[str, float, float | None]]) -> tuple[float, float | None, float, list[str]]:
        ocf_sum = sum(q[1] for q in last4)
        capex_sum = 0.0
        inv_periods = 0
        inv_missing = 0
        for _ed, _o, inv_q in last4:
            cap_q, ok = _capex_from_inv_increment(inv_q)
            if ok:
                inv_periods += 1
                capex_sum += cap_q
            elif inv_q is None:
                inv_missing += 1
        wloc: list[str] = [
            f"基期采用最近四季度（单季还原）经营现金流 TTM，报告期：{last4[0][0]}～{last4[-1][0]}"
        ]
        if inv_missing > 0:
            wloc.append(
                "部分季度缺少投资活动现金流增量，资本开支代理不完整；"
                "未扣减部分按 0 处理，基期现金流可能偏高"
            )
            return (
                ocf_sum,
                capex_sum if inv_periods > 0 else None,
                ocf_sum - capex_sum,
                wloc,
            )
        if inv_periods == 0:
            wloc.append("四季度均无投资活动现金流增量，未扣除资本开支代理")
            return ocf_sum, None, ocf_sum, wloc
        return ocf_sum, capex_sum, ocf_sum - capex_sum, wloc

    # 1）近期年报优先（非陈旧）
    if annual_row is not None and not annual_stale:
        ocf, cap, fcf, wextra = _from_annual(annual_row, stale_note=False)
        warnings.extend(wextra)
        if ocf is None:
            return None, None, None, warnings
        warnings.insert(0, f"基期采用最近一期年报（{annual_ed}）全年经营现金流量净额")
        return ocf, cap, fcf, warnings

    # 2）季度 TTM（至少 4 个单季）
    if len(singles) >= 4:
        last4 = singles[-4:]
        ocf, cap, fcf, wextra = _from_quarters_ttm(last4)
        warnings.extend(wextra)
        return ocf, cap, fcf, warnings

    # 3）陈旧年报仍优于「多期乱加」
    if annual_row is not None:
        ocf, cap, fcf, wextra = _from_annual(annual_row, stale_note=True)
        warnings.extend(wextra)
        if ocf is None:
            return None, None, None, warnings
        warnings.insert(0, f"基期采用最近一期年报（{annual_ed}）全年经营现金流量净额")
        return ocf, cap, fcf, warnings

    # 4）仅取最近一条报告期（绝不把多期简单相加）
    latest = _sort_by_end_date_desc([{**dict(r), "end_date": ed} for ed, r in merged.items()])[0]
    ed = str(latest.get("end_date") or "")
    ocf = to_float_or_none(latest.get("n_cashflow_act"))
    if ocf is None:
        return None, None, None, warnings + ["最近一期现金流量表缺少 n_cashflow_act"]
    inv = investing_cashflow_net_from_row(latest)
    warnings.append(
        f"无法组成四季度 TTM 且无可用年报，基期仅取最近一期（{ed}）经营现金流；"
        "若该期为季报累计值，DCF 基数可能失真，请补全财报或扩大同步窗口"
    )
    if inv is None:
        warnings.append("缺少投资活动现金流（n_cashflow_inv_act），未扣除资本开支代理")
        return float(ocf), None, float(ocf), warnings
    cap = max(0.0, -float(inv))
    return float(ocf), cap, float(ocf) - cap, warnings
