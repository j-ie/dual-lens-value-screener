"""
按行业分档从资产负债表解析 DCF 用净债务（应用层）。

金融业使用 payload 内 TuShare 有息类科目求和；地产股尝试扣除合同负债。
"""

from __future__ import annotations

from typing import Any

from value_screener.application.financial_statement_payload import to_float_or_none
from value_screener.domain.dcf_sector_policy import DcfSectorKind

# TuShare balancesheet 常见有息/类有息负债字段（不同报表模板可能仅部分有值）
_FINANCIAL_INTEREST_FIELD_KEYS: tuple[str, ...] = (
    "st_borrow",
    "lt_borrow",
    "bond_payable",
    "bonds_payable",
    "non_cur_liab_due_1y",
    "cb_borr",
    "borrow_fund",
    "loan_oth_bank",
)


def _sort_by_end_date_desc(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda r: str(r.get("end_date") or ""), reverse=True)


def flatten_balance_row_for_dcf(row: dict[str, Any]) -> dict[str, Any]:
    """
    合并顶层标量列与 payload 全量字段，便于读取 TuShare 扩展科目。

    顶层非空优先；顶层为 None 时用 payload 补全。
    """

    out = {k: v for k, v in row.items() if k != "payload"}
    payload = row.get("payload")
    if not isinstance(payload, dict):
        return out
    for key, val in payload.items():
        if key not in out or out[key] is None:
            out[key] = val
    return out


def _sum_financial_interest_bearing_debt(flat: dict[str, Any]) -> tuple[float, bool]:
    """
    返回 (有息负债合计, 是否至少命中任一科目非空)。

    各字段按非负加总；缺失视为 0。
    """

    total = 0.0
    any_present = False
    for key in _FINANCIAL_INTEREST_FIELD_KEYS:
        raw = to_float_or_none(flat.get(key))
        if raw is not None:
            any_present = True
            total += max(0.0, float(raw))
    return total, any_present


def resolve_net_debt_for_sector(
    balance_rows: list[dict[str, Any]],
    sector_kind: DcfSectorKind,
) -> tuple[float | None, str | None, list[str]]:
    """
    按行业分档估算净债务，返回 (net_debt, method_code, warnings)。
    """

    warnings: list[str] = []
    if not balance_rows:
        return None, None, ["无资产负债表数据，无法估算净债务"]

    latest = flatten_balance_row_for_dcf(_sort_by_end_date_desc([dict(x) for x in balance_rows])[0])
    money = to_float_or_none(latest.get("money_cap"))
    money_v = float(money) if money is not None else 0.0

    if sector_kind is DcfSectorKind.FINANCIAL:
        gross, found = _sum_financial_interest_bearing_debt(latest)
        net = gross - money_v
        if not found:
            warnings.append(
                "金融业未识别到有息负债明细科目（请确认 balancesheet 已同步且含 payload）；"
                "balance_sheet_net_debt_proxy 记为 0，仅供侧栏参考"
            )
            return 0.0, "financial_interest_debt_missing_fallback_zero", warnings
        warnings.append(
            "金融业：有息类科目合计 − 货币资金记入 balance_sheet_net_debt_proxy（不含存款等经营性负债），"
            "粗代理；股权折现路径中不再用于 EV→E 扣减"
        )
        return net, "financial_interest_bearing_minus_money_cap", warnings

    if sector_kind is DcfSectorKind.REAL_ESTATE:
        total_liab = to_float_or_none(latest.get("total_liab"))
        if total_liab is None:
            return None, None, ["资产负债表缺少 total_liab，无法估算净债务"]
        contract = to_float_or_none(latest.get("contract_liab"))
        if contract is not None and contract > 0:
            net = float(total_liab) - float(contract) - money_v
            warnings.append(
                "地产股净债务 ≈ 负债合计 − 合同负债 − 货币资金（合同负债代理预收房款，非精确）"
            )
            return net, "real_estate_liab_minus_contract_liab_minus_cash", warnings
        warnings.append(
            "地产股未识别到合同负债 contract_liab，净债务仍用负债合计 − 货币资金，可能高估杠杆"
        )
        net = float(total_liab) - money_v
        return net, "real_estate_total_liab_minus_money_cap_fallback", warnings

    total_liab = to_float_or_none(latest.get("total_liab"))
    if total_liab is None:
        return None, None, ["资产负债表缺少 total_liab，无法估算净债务"]
    net = float(total_liab) - money_v
    warnings.append("净债务采用 total_liab − money_cap 粗代理，非精确有息净负债")
    return net, "total_liab_minus_money_cap", warnings
