from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from value_screener.domain.dcf_sector_policy import DcfSectorKind


class InvestmentDecision(str, Enum):
    BUY = "buy"
    WATCHLIST = "watchlist"
    CAUTIOUS = "cautious"
    REJECT = "reject"

    @property
    def label_zh(self) -> str:
        labels = {
            InvestmentDecision.BUY: "可买",
            InvestmentDecision.WATCHLIST: "跟踪",
            InvestmentDecision.CAUTIOUS: "谨慎",
            InvestmentDecision.REJECT: "排除",
        }
        return labels[self]


@dataclass(frozen=True, slots=True)
class RiskFlag:
    code: str
    severity: int
    message: str


@dataclass(frozen=True, slots=True)
class ModuleScore:
    score: int
    reasons: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class CompanyFinancials:
    name: str
    sector_kind: DcfSectorKind = DcfSectorKind.GENERAL
    revenue: tuple[float, ...] = ()
    net_profit: tuple[float, ...] = ()
    non_recurring_net_profit: tuple[float, ...] = ()
    gross_margin: tuple[float, ...] = ()
    net_margin: tuple[float, ...] = ()
    expense_ratio: tuple[float, ...] = ()
    operating_profit: tuple[float, ...] = ()
    operating_cashflow: tuple[float, ...] = ()
    free_cashflow: tuple[float, ...] = ()
    cash: tuple[float, ...] = ()
    short_debt: tuple[float, ...] = ()
    interest_bearing_debt: tuple[float, ...] = ()
    accounts_receivable: tuple[float, ...] = ()
    inventory: tuple[float, ...] = ()
    goodwill: tuple[float, ...] = ()
    net_assets: tuple[float, ...] = ()
    asset_liability_ratio: tuple[float, ...] = ()
    roe: tuple[float, ...] = ()
    roic: tuple[float, ...] = ()
    pe: float | None = None
    pb: float | None = None


@dataclass(frozen=True, slots=True)
class AnalysisResult:
    company_name: str
    total_score: int
    module_scores: dict[str, int]
    decision: InvestmentDecision
    decision_label_zh: str
    is_undervalued: bool
    reasons: tuple[str, ...] = ()
    risk_flags: tuple[RiskFlag, ...] = ()
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class WorthBuyDecision:
    is_worth_buy: bool
    label_zh: str
    reason_codes: tuple[str, ...] = ()


def resolve_worth_buy_decision(result: AnalysisResult) -> WorthBuyDecision:
    hard_risk_count = int(result.metadata.get("hard_risk_count", len(result.risk_flags)) or 0)
    reason_codes: list[str] = []
    if result.decision is InvestmentDecision.BUY:
        reason_codes.append("decision_buy")
    elif result.decision is InvestmentDecision.WATCHLIST:
        reason_codes.append("decision_watchlist")
    elif result.decision is InvestmentDecision.CAUTIOUS:
        reason_codes.append("decision_cautious")
    else:
        reason_codes.append("decision_not_buy")
    if result.is_undervalued:
        reason_codes.append("undervalued")
    else:
        reason_codes.append("not_undervalued")
    if hard_risk_count <= 1:
        reason_codes.append("risk_acceptable")
    else:
        reason_codes.append("risk_too_high")
    ok = (
        result.decision is InvestmentDecision.BUY
        and result.is_undervalued
        and hard_risk_count <= 1
    )
    if ok:
        return WorthBuyDecision(is_worth_buy=True, label_zh="值得买入", reason_codes=tuple(reason_codes))
    if result.decision in (InvestmentDecision.BUY, InvestmentDecision.WATCHLIST, InvestmentDecision.CAUTIOUS):
        return WorthBuyDecision(
            is_worth_buy=False,
            label_zh="谨慎观察",
            reason_codes=tuple(reason_codes),
        )
    return WorthBuyDecision(
        is_worth_buy=False,
        label_zh="不建议买入",
        reason_codes=tuple(reason_codes) if reason_codes else ("not_buy",),
    )


class InvestmentQualityAnalyzer:
    def analyze(self, company: CompanyFinancials) -> AnalysisResult:
        module_outputs = {
            "growth": self._score_growth(company),
            "profitability": self._score_profitability(company),
            "balance_sheet": self._score_balance_sheet(company),
            "cashflow": self._score_cashflow(company),
            "return_metrics": self._score_return_metrics(company),
            "valuation": self._score_valuation(company),
        }
        module_scores = {k: v.score for k, v in module_outputs.items()}
        reasons = tuple(reason for v in module_outputs.values() for reason in v.reasons)
        total_score = sum(module_scores.values())
        risk_flags = self._collect_risk_flags(company)
        decision = self._resolve_decision(total_score, len(risk_flags), company.sector_kind)
        valuation_score = module_scores["valuation"]
        quality_score = total_score - valuation_score
        is_undervalued = self._is_undervalued(
            valuation_score,
            quality_score,
            len(risk_flags),
            company.sector_kind,
        )
        return AnalysisResult(
            company_name=company.name,
            total_score=total_score,
            module_scores=module_scores,
            decision=decision,
            decision_label_zh=decision.label_zh,
            is_undervalued=is_undervalued,
            reasons=reasons,
            risk_flags=risk_flags,
            metadata={"hard_risk_count": len(risk_flags)},
        )

    @staticmethod
    def _latest(series: tuple[float, ...], default: float = 0.0) -> float:
        if not series:
            return default
        return float(series[-1])

    @staticmethod
    def _growth_rate(series: tuple[float, ...]) -> float | None:
        if len(series) < 2:
            return None
        prev = float(series[-2])
        curr = float(series[-1])
        if prev == 0:
            return None
        return (curr - prev) / abs(prev)

    @staticmethod
    def _is_non_decreasing(series: tuple[float, ...]) -> bool:
        if len(series) < 2:
            return False
        return all(series[i] <= series[i + 1] for i in range(len(series) - 1))

    def _score_growth(self, company: CompanyFinancials) -> ModuleScore:
        score = 0
        reasons: list[str] = []
        rev_g = self._growth_rate(company.revenue)
        np_g = self._growth_rate(company.net_profit)
        nr_g = self._growth_rate(company.non_recurring_net_profit)
        if self._is_non_decreasing(company.revenue):
            score += 2
            reasons.append("营收趋势连续增长")
        if self._is_non_decreasing(company.net_profit):
            score += 2
            reasons.append("净利润趋势连续增长")
        if rev_g is not None and rev_g > 0.1:
            score += 1
        if np_g is not None and np_g > 0.1:
            score += 1
        if rev_g is not None and np_g is not None and rev_g > 0 and np_g <= 0:
            score -= 2
            reasons.append("收入增长但利润未同步增长")
        if nr_g is not None and np_g is not None and nr_g < np_g * 0.5:
            score -= 2
            reasons.append("扣非净利润增速显著弱于净利润")
        return ModuleScore(score=score, reasons=tuple(reasons))

    def _score_profitability(self, company: CompanyFinancials) -> ModuleScore:
        score = 0
        reasons: list[str] = []
        if self._is_non_decreasing(company.gross_margin):
            score += 2
            reasons.append("毛利率稳定或提升")
        elif len(company.gross_margin) >= 2 and company.gross_margin[-1] < company.gross_margin[0]:
            score -= 2
            reasons.append("毛利率整体下滑")
        if len(company.expense_ratio) >= 2 and company.expense_ratio[-1] <= company.expense_ratio[0]:
            score += 1
            reasons.append("费用率控制较好")
        elif len(company.expense_ratio) >= 2 and company.expense_ratio[-1] > company.expense_ratio[0] * 1.2:
            score -= 2
            reasons.append("费用率恶化明显")
        if self._is_non_decreasing(company.operating_profit):
            score += 2
            reasons.append("营业利润趋势改善")
        elif len(company.operating_profit) >= 2:
            score -= 1
            reasons.append("营业利润波动较大")
        if len(company.net_margin) >= 2 and company.net_margin[-1] >= company.net_margin[0]:
            score += 1
        elif len(company.net_margin) >= 2:
            score -= 1
            reasons.append("净利率走弱")
        return ModuleScore(score=score, reasons=tuple(reasons))

    def _score_balance_sheet(self, company: CompanyFinancials) -> ModuleScore:
        score = 0
        reasons: list[str] = []
        rev_g = self._growth_rate(company.revenue)
        ar_g = self._growth_rate(company.accounts_receivable)
        inv_g = self._growth_rate(company.inventory)
        cash = self._latest(company.cash)
        short_debt = self._latest(company.short_debt)
        if cash >= short_debt:
            score += 2
            reasons.append("现金可覆盖短期债务")
        else:
            score -= 2
            reasons.append("现金覆盖短债能力不足")
        if rev_g is not None and ar_g is not None:
            if ar_g <= rev_g:
                score += 1
            else:
                score -= 2
                reasons.append("应收账款增速快于收入增速")
        if rev_g is not None and inv_g is not None:
            if inv_g <= rev_g:
                score += 1
            else:
                score -= 2
                reasons.append("存货增速快于收入增速")
        net_assets = self._latest(company.net_assets)
        goodwill = self._latest(company.goodwill)
        if net_assets > 0:
            ratio = goodwill / net_assets
            if ratio < 0.2:
                score += 1
            elif ratio > 0.5:
                score -= 2
                reasons.append("商誉占净资产比重过高")
        alr = self._latest(company.asset_liability_ratio)
        if alr <= 60:
            score += 1
        elif alr > 75:
            score -= 2
            reasons.append("资产负债率偏高")
        return ModuleScore(score=score, reasons=tuple(reasons))

    def _score_cashflow(self, company: CompanyFinancials) -> ModuleScore:
        score = 0
        reasons: list[str] = []
        ocf3 = company.operating_cashflow[-3:]
        if len(ocf3) == 3 and all(x > 0 for x in ocf3):
            score += 3
            reasons.append("近三期经营现金流持续为正")
        elif len(ocf3) == 3:
            score -= 2
            reasons.append("经营现金流存在不稳定或为负")
        np_latest = self._latest(company.net_profit)
        ocf_latest = self._latest(company.operating_cashflow)
        if np_latest != 0:
            ratio = ocf_latest / np_latest
            if ratio >= 0.8:
                score += 2
                reasons.append("经营现金流与净利润匹配良好")
            elif ratio < 0.5:
                score -= 2
                reasons.append("经营现金流明显弱于净利润")
        fcf2 = company.free_cashflow[-2:]
        if len(fcf2) == 2 and all(x > 0 for x in fcf2):
            score += 2
            reasons.append("自由现金流表现良好")
        elif len(fcf2) == 2:
            score -= 1
            reasons.append("自由现金流偏弱")
        return ModuleScore(score=score, reasons=tuple(reasons))

    def _score_return_metrics(self, company: CompanyFinancials) -> ModuleScore:
        score = 0
        reasons: list[str] = []
        roe_last = company.roe[-3:] if company.roe else ()
        roic_last = company.roic[-3:] if company.roic else ()
        avg_roe = sum(roe_last) / len(roe_last) if roe_last else None
        avg_roic = sum(roic_last) / len(roic_last) if roic_last else None
        if avg_roe is not None:
            if avg_roe >= 15:
                score += 2
                reasons.append("ROE处于较高水平")
            elif avg_roe < 8:
                score -= 2
                reasons.append("ROE偏低")
        if avg_roic is not None:
            if avg_roic >= 10:
                score += 2
                reasons.append("ROIC水平较好")
            elif avg_roic < 5:
                score -= 2
                reasons.append("ROIC偏低")
        if avg_roe is not None and avg_roic is not None and avg_roe > 15 and avg_roic < 8:
            score -= 1
            reasons.append("ROE较高但ROIC偏低，需关注杠杆因素")
        return ModuleScore(score=score, reasons=tuple(reasons))

    def _score_valuation(self, company: CompanyFinancials) -> ModuleScore:
        score = 0
        reasons: list[str] = []
        if company.sector_kind == DcfSectorKind.FINANCIAL:
            if company.pb is not None:
                if company.pb < 1:
                    score += 1
                    reasons.append("PB处于较低区间")
                elif company.pb > 2:
                    score -= 1
                    reasons.append("PB偏高")
        elif company.sector_kind == DcfSectorKind.CYCLICAL:
            if company.pe is not None:
                if company.pe < 8:
                    score += 1
                    reasons.append("周期口径下PE处于低位")
                elif company.pe > 25:
                    score -= 2
                    reasons.append("周期口径下PE偏高")
        else:
            if company.pe is not None:
                if company.pe < 15:
                    score += 2
                    reasons.append("PE处于合理偏低区间")
                elif company.pe > 40:
                    score -= 2
                    reasons.append("PE偏高，估值透支风险较大")
        return ModuleScore(score=score, reasons=tuple(reasons))

    def _collect_risk_flags(self, company: CompanyFinancials) -> tuple[RiskFlag, ...]:
        flags: list[RiskFlag] = []
        if self._latest(company.cash) < self._latest(company.short_debt):
            flags.append(RiskFlag("cash_short_debt_mismatch", 2, "现金小于短期债务"))
        if self._latest(company.operating_cashflow) < 0:
            flags.append(RiskFlag("negative_operating_cashflow", 2, "最近一期经营现金流为负"))
        ar_g = self._growth_rate(company.accounts_receivable)
        rev_g = self._growth_rate(company.revenue)
        if ar_g is not None and rev_g is not None and ar_g > rev_g + 0.2:
            flags.append(RiskFlag("receivable_growth_too_fast", 2, "应收账款增速显著高于收入增速"))
        inv_g = self._growth_rate(company.inventory)
        if inv_g is not None and rev_g is not None and inv_g > rev_g + 0.2:
            flags.append(RiskFlag("inventory_growth_too_fast", 2, "存货增速显著高于收入增速"))
        np_latest = self._latest(company.net_profit)
        nr_latest = self._latest(company.non_recurring_net_profit)
        if np_latest > 0 and nr_latest < np_latest * 0.5:
            flags.append(RiskFlag("weak_non_recurring_profit", 2, "扣非净利润明显低于净利润"))
        net_assets = self._latest(company.net_assets)
        if net_assets > 0 and self._latest(company.goodwill) / net_assets > 0.5:
            flags.append(RiskFlag("high_goodwill_ratio", 2, "商誉占净资产比例过高"))
        return tuple(flags)

    @staticmethod
    def _resolve_decision(
        total_score: int,
        hard_risk_count: int,
        sector_kind: DcfSectorKind,
    ) -> InvestmentDecision:
        if sector_kind is DcfSectorKind.CYCLICAL and hard_risk_count >= 2:
            return InvestmentDecision.REJECT
        if hard_risk_count >= 3:
            return InvestmentDecision.REJECT
        buy_threshold = 11 if sector_kind is DcfSectorKind.CYCLICAL else 10
        if total_score >= buy_threshold and hard_risk_count == 0:
            return InvestmentDecision.BUY
        if total_score >= 5:
            if hard_risk_count >= 2:
                return InvestmentDecision.CAUTIOUS
            return InvestmentDecision.WATCHLIST
        if total_score >= 0:
            return InvestmentDecision.CAUTIOUS
        return InvestmentDecision.REJECT

    @staticmethod
    def _is_undervalued(
        valuation_score: int,
        quality_score: int,
        hard_risk_count: int,
        sector_kind: DcfSectorKind,
    ) -> bool:
        q_floor = 10 if sector_kind is DcfSectorKind.CYCLICAL else 8
        return valuation_score > 0 and quality_score >= q_floor and hard_risk_count == 0

