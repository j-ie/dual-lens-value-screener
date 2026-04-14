"""
根据近年年报序列粗算预测期增长率 g（CAGR），供未手动覆盖 stage1 时替代固定默认。

优先：利润表年报归母净利润 n_income_attr_p；不足时：现金流量表年报 n_cashflow_act。
强周期行业不推断（避免景气顶外推）；结果仍经 DcfValuationSettings 钳制。
"""

from __future__ import annotations

from typing import Any

from value_screener.application.dcf_cashflow_aggregate import merge_statement_rows_by_end_date
from value_screener.application.financial_statement_payload import to_float_or_none
from value_screener.domain.dcf_sector_policy import DcfSectorKind
from value_screener.infrastructure.settings import DcfValuationSettings


def _annual_1231_series(
    merged: dict[str, dict[str, Any]],
    field: str,
) -> list[tuple[str, float]]:
    out: list[tuple[str, float]] = []
    for ed, row in merged.items():
        if len(ed) != 8 or not ed.isdigit() or not ed.endswith("1231"):
            continue
        v = to_float_or_none(row.get(field))
        if v is None or float(v) <= 0:
            continue
        out.append((ed, float(v)))
    out.sort(key=lambda x: x[0])
    return out


def _cagr_from_annuals(
    points: list[tuple[str, float]],
    *,
    max_years: int,
    min_span_years: int,
) -> tuple[float | None, str | None]:
    """
    取时间序列中最晚一段（至多 max_years 个年报点），用首尾算几何年化增长率。
    """

    if len(points) < 2:
        return None, None
    tail = points[-max_years:] if len(points) > max_years else points
    if len(tail) < 2:
        return None, None
    ed0, v0 = tail[0]
    ed1, v1 = tail[-1]
    y0 = int(ed0[:4])
    y1 = int(ed1[:4])
    span = y1 - y0
    if span < min_span_years:
        return None, None
    if v0 <= 0 or v1 <= 0:
        return None, None
    g = (v1 / v0) ** (1.0 / float(span)) - 1.0
    return float(g), f"{ed0}→{ed1}"


def infer_stage1_growth_from_annual_statements(
    *,
    sector_kind: DcfSectorKind,
    income_rows: list[dict[str, Any]],
    cashflow_rows: list[dict[str, Any]],
    settings: DcfValuationSettings,
) -> tuple[float | None, str | None, list[str]]:
    """
    返回 (未钳制的推断 g、来源码、warnings)。无法推断时 (None, None, warnings)。
    """

    warnings: list[str] = []
    if not settings.infer_stage1_enabled:
        return None, None, warnings
    if sector_kind is DcfSectorKind.CYCLICAL:
        return None, None, warnings

    max_n = max(2, min(settings.infer_stage1_max_annuals, 20))
    min_span = max(1, min(settings.infer_stage1_min_span_years, 10))

    if income_rows:
        merged_i = merge_statement_rows_by_end_date([dict(x) for x in income_rows])
        ni_pts = _annual_1231_series(merged_i, "n_income_attr_p")
        g, span_desc = _cagr_from_annuals(ni_pts, max_years=max_n, min_span_years=min_span)
        if g is not None:
            warnings.append(
                f"预测期增长率 g 由近年年报归母净利润 CAGR 粗推（{span_desc}，未钳制前约 {g:.4f}），"
                "已按配置上下限截断；单一路径外推，不构成业绩预测"
            )
            return g, "inferred_net_income_cagr", warnings

    merged_c = merge_statement_rows_by_end_date([dict(x) for x in cashflow_rows])
    ocf_pts = _annual_1231_series(merged_c, "n_cashflow_act")
    g2, span_desc2 = _cagr_from_annuals(ocf_pts, max_years=max_n, min_span_years=min_span)
    if g2 is not None:
        warnings.append(
            f"预测期增长率 g 由近年年报经营现金流量净额 CAGR 粗推（{span_desc2}，未钳制前约 {g2:.4f}），"
            "已按配置上下限截断；与 FCFF 代理口径可能不完全一致"
        )
        return g2, "inferred_ocf_cagr", warnings

    return None, None, warnings
