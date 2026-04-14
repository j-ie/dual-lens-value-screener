"""
简化两阶段 FCFF 折现（领域纯函数）。

口径：预测期按固定增长率复利；终值 Gordon；企业价值减净债务得股权价值。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# 与 `build_company_dcf_payload` 写入的 `assumptions.dcf_model_revision` 一致；口径变更时请递增。
DCF_MODEL_REVISION = "2026.04-dcf-v1"


@dataclass(frozen=True, slots=True)
class DcfInputs:
    """DCF 计算输入（金额与股本单位须自洽，如人民币元 + 股数）。"""

    base_fcf: float
    wacc: float
    stage1_growth: float
    terminal_growth: float
    forecast_years: int
    net_debt: float
    shares_outstanding: float
    wacc_terminal_epsilon: float


@dataclass(frozen=True, slots=True)
class DcfResult:
    """成功时的估值结果。"""

    enterprise_value: float
    equity_value: float
    value_per_share: float
    pv_forecast: float
    pv_terminal: float
    terminal_value_nominal: float


@dataclass(frozen=True, slots=True)
class DcfSkipped:
    """无法计算时的可序列化原因。"""

    code: str
    message: str


def compute_dcf(inp: DcfInputs) -> DcfResult | DcfSkipped:
    """
    计算 EV、股权价值与每股价值。

    FCF_t = base_fcf * (1 + stage1_growth)^t，t = 1..n；
    终值：FCF_{n+1} = FCF_n * (1 + terminal_growth)，TV = FCF_{n+1} / (WACC - g_terminal)。
    """

    if inp.shares_outstanding <= 0:
        return DcfSkipped("invalid_shares", "总股本须为正数")
    if inp.forecast_years < 1:
        return DcfSkipped("invalid_horizon", "预测年数须至少为 1")
    eps = max(float(inp.wacc_terminal_epsilon), 1e-9)
    if inp.wacc <= inp.terminal_growth + eps:
        return DcfSkipped(
            "wacc_terminal_spread",
            f"WACC 须大于永续增长率至少 {eps:.6f}（当前 WACC={inp.wacc}，g_terminal={inp.terminal_growth}）",
        )

    g1 = float(inp.stage1_growth)
    gt = float(inp.terminal_growth)
    w = float(inp.wacc)
    n = int(inp.forecast_years)
    f0 = float(inp.base_fcf)

    pv_forecast = 0.0
    for t in range(1, n + 1):
        fcf_t = f0 * ((1.0 + g1) ** t)
        pv_forecast += fcf_t / ((1.0 + w) ** t)

    fcf_n = f0 * ((1.0 + g1) ** n)
    fcf_n1 = fcf_n * (1.0 + gt)
    denom = w - gt
    terminal_value_nominal = fcf_n1 / denom
    pv_terminal = terminal_value_nominal / ((1.0 + w) ** n)

    enterprise_value = pv_forecast + pv_terminal
    equity_value = enterprise_value - float(inp.net_debt)
    per_share = equity_value / float(inp.shares_outstanding)

    return DcfResult(
        enterprise_value=round(enterprise_value, 4),
        equity_value=round(equity_value, 4),
        value_per_share=round(per_share, 6),
        pv_forecast=round(pv_forecast, 4),
        pv_terminal=round(pv_terminal, 4),
        terminal_value_nominal=round(terminal_value_nominal, 4),
    )


def dcf_result_to_public_dict(res: DcfResult) -> dict[str, Any]:
    return {
        "enterprise_value": res.enterprise_value,
        "equity_value": res.equity_value,
        "value_per_share": res.value_per_share,
        "pv_forecast": res.pv_forecast,
        "pv_terminal": res.pv_terminal,
        "terminal_value_nominal": res.terminal_value_nominal,
    }
