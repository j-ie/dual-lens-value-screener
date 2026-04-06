"""
公司详情 DCF：从财报摘要行聚合基期现金流、按行业分档解析净债务，并结合 TuShare 股本计算估值。
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any, Callable

from value_screener.application.dcf_cashflow_aggregate import aggregate_ocf_and_capex_proxy_ttm
from value_screener.application.dcf_income_for_valuation import latest_annual_n_income_attr_p
from value_screener.application.dcf_net_debt_resolve import resolve_net_debt_for_sector
from value_screener.domain.dcf import (
    DcfInputs,
    DcfResult,
    DcfSkipped,
    compute_dcf,
    dcf_result_to_public_dict,
)
from value_screener.domain.dcf_sector_policy import DcfSectorKind, resolve_dcf_sector_kind
from value_screener.infrastructure.settings import DcfValuationSettings

logger = logging.getLogger(__name__)


def resolve_effective_dcf_params(
    base: DcfValuationSettings,
    *,
    wacc_override: float | None,
    stage1_override: float | None,
    terminal_override: float | None,
) -> tuple[float, float, float, int, float] | str:
    """
    合并环境默认与查询覆盖（已钳制）。

    返回 (wacc, stage1, terminal, years, epsilon) 或错误信息字符串（422 用）。
    """

    w = base.clamp_wacc(wacc_override if wacc_override is not None else base.default_wacc)
    g1 = base.clamp_stage1(stage1_override if stage1_override is not None else base.default_stage1_growth)
    gt = base.clamp_terminal(terminal_override if terminal_override is not None else base.default_terminal_growth)
    eps = base.wacc_terminal_epsilon
    if w <= gt + eps:
        return (
            f"WACC（{w}）须大于永续增长率（{gt}）至少 {eps}；请调低 g_terminal 或提高 wacc"
        )
    return w, g1, gt, base.forecast_years, eps


def build_company_dcf_payload(
    *,
    cashflow_rows: list[dict[str, Any]],
    balance_rows: list[dict[str, Any]],
    settings: DcfValuationSettings,
    wacc_override: float | None,
    stage1_override: float | None,
    terminal_override: float | None,
    fetch_total_shares: Callable[[], float],
    industry: str | None = None,
    income_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """
    构建可 JSON 化的 DCF 块（ok=true/false）。

    fetch_total_shares: 由调用方注入（通常含 TuShare I/O），失败应抛异常。
    """

    warnings: list[str] = []
    sector_kind = resolve_dcf_sector_kind(industry)
    notes: list[str] = [
        "简化 FCFF：基期优先取最近年报全年或四季度单季还原 TTM，再减投资现金流出代理；"
        "折现率为单一路径 WACC；不构成投资建议。",
    ]
    if sector_kind is DcfSectorKind.FINANCIAL:
        notes.append(
            "金融业：有息负债 proxy 仅参考；折现现值按股权理解且不扣减该 proxy。"
            "年报归母净利润先乘以系数 financial_ni_base_scale（默认 0.35，环境变量"
            " VALUE_SCREENER_DCF_FINANCIAL_NI_BASE_SCALE）再进入折现，粗代理派息与资本留存；"
            "全额利润直接折现会系统性高估。WACC 仍为统一口径，非严谨股权成本。"
        )
    elif sector_kind is DcfSectorKind.REAL_ESTATE:
        notes.append(
            "地产股：在可得时从负债中扣除合同负债，削弱预收房款对「净债务」代理的高估。"
        )

    resolved = resolve_effective_dcf_params(
        settings,
        wacc_override=wacc_override,
        stage1_override=stage1_override,
        terminal_override=terminal_override,
    )
    if isinstance(resolved, str):
        return {
            "ok": False,
            "skip_reason": "invalid_params",
            "message": resolved,
            "warnings": warnings,
            "notes": notes,
            "assumptions": None,
            "values": None,
        }

    wacc, g1, gt, years, eps = resolved

    income_list = income_rows or []
    ocf_ttm: float | None
    capex_proxy: float | None
    fcf_base: float | None
    w_cf: list[str]
    fcf_source: str
    financial_reported_ni: float | None = None
    financial_ni_scale_applied: float | None = None

    if sector_kind is DcfSectorKind.FINANCIAL and income_list:
        ni, _ed_ni, w_ni = latest_annual_n_income_attr_p(
            income_list,
            as_of=date.today(),
            annual_stale_days=settings.annual_stale_days,
        )
        if ni is not None:
            ni_r = float(ni)
            scale = float(settings.financial_ni_base_scale)
            financial_reported_ni = ni_r
            financial_ni_scale_applied = scale
            fcf_base = ni_r * scale
            ocf_ttm = ni_r
            capex_proxy = None
            w_cf = list(w_ni)
            w_cf.append(
                f"金融业折现基数 = 年报归母净利 × 系数 {scale:.4f}（环境变量 "
                "VALUE_SCREENER_DCF_FINANCIAL_NI_BASE_SCALE，默认粗代理派息与留存）；"
                "若用全额净利折现将显著高估内在价值"
            )
            fcf_source = "financial_net_income_annual"
        else:
            ocf_ttm, capex_proxy, fcf_base, w_cf = aggregate_ocf_and_capex_proxy_ttm(
                cashflow_rows,
                max_periods=settings.ttm_periods_max,
                as_of=date.today(),
                annual_stale_days=settings.annual_stale_days,
            )
            warnings.extend(w_ni)
            fcf_source = "cashflow_ttm_or_annual"
    else:
        ocf_ttm, capex_proxy, fcf_base, w_cf = aggregate_ocf_and_capex_proxy_ttm(
            cashflow_rows,
            max_periods=settings.ttm_periods_max,
            as_of=date.today(),
            annual_stale_days=settings.annual_stale_days,
        )
        fcf_source = "cashflow_ttm_or_annual"

    warnings.extend(w_cf)

    net_debt_proxy, nd_method, w_nd = resolve_net_debt_for_sector(balance_rows, sector_kind)
    warnings.extend(w_nd)

    if fcf_base is None:
        return {
            "ok": False,
            "skip_reason": "insufficient_cashflow",
            "message": "无法从现金流量表或利润表汇总基期现金流",
            "warnings": warnings,
            "notes": notes,
            "assumptions": None,
            "values": None,
        }

    if net_debt_proxy is None:
        return {
            "ok": False,
            "skip_reason": "insufficient_balance",
            "message": "无法从资产负债表估算净债务",
            "warnings": warnings,
            "notes": notes,
            "assumptions": None,
            "values": None,
        }

    nd_proxy_f = float(net_debt_proxy)
    financial_equity_direct = sector_kind is DcfSectorKind.FINANCIAL
    if financial_equity_direct:
        net_debt_applied = 0.0
        warnings.append(
            "金融业：折现基数为利润表/现金流代理，折现现值按股权价值理解；"
            "已从公式中略去 EV−有息净负债 扣减（否则有息负债规模常与利润基数不匹配，易出现负每股价值）。"
            "balance_sheet_net_debt_proxy 为表内有息类科目粗算，仅供参考。"
        )
    else:
        net_debt_applied = nd_proxy_f

    try:
        shares = float(fetch_total_shares())
    except Exception as exc:  # noqa: BLE001
        logger.warning("dcf total_shares: %s", exc)
        return {
            "ok": False,
            "skip_reason": "shares_unavailable",
            "message": f"无法获取总股本：{exc}",
            "warnings": warnings,
            "notes": notes,
            "assumptions": None,
            "values": None,
        }

    inp = DcfInputs(
        base_fcf=float(fcf_base),
        wacc=wacc,
        stage1_growth=g1,
        terminal_growth=gt,
        forecast_years=years,
        net_debt=float(net_debt_applied),
        shares_outstanding=shares,
        wacc_terminal_epsilon=eps,
    )
    out = compute_dcf(inp)
    assumptions = {
        "wacc": wacc,
        "stage1_growth": g1,
        "terminal_growth": gt,
        "forecast_years": years,
        "wacc_terminal_epsilon": eps,
        "base_fcf": round(fcf_base, 4),
        "ocf_ttm_proxy": round(ocf_ttm, 4) if ocf_ttm is not None else None,
        "capex_proxy_ttm": round(capex_proxy, 4) if capex_proxy is not None else None,
        "net_debt": round(net_debt_applied, 4),
        "balance_sheet_net_debt_proxy": round(nd_proxy_f, 4),
        "financial_equity_direct_bridge": financial_equity_direct,
        "net_debt_method": nd_method,
        "shares_outstanding": round(shares, 2),
        "dcf_sector_kind": sector_kind.value,
        "fcf_base_source": fcf_source,
        "financial_reported_n_income": round(financial_reported_ni, 4)
        if financial_reported_ni is not None
        else None,
        "financial_ni_base_scale": round(financial_ni_scale_applied, 4)
        if financial_ni_scale_applied is not None
        else None,
    }

    if isinstance(out, DcfSkipped):
        return {
            "ok": False,
            "skip_reason": out.code,
            "message": out.message,
            "warnings": warnings,
            "notes": notes,
            "assumptions": assumptions,
            "values": None,
        }

    assert isinstance(out, DcfResult)
    return {
        "ok": True,
        "skip_reason": None,
        "message": None,
        "warnings": warnings,
        "notes": notes,
        "assumptions": assumptions,
        "values": dcf_result_to_public_dict(out),
    }
