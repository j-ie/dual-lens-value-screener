from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel, Field, ValidationError, field_validator

from value_screener.infrastructure.settings import CompanyAiAnalysisSettings

PROMPT_VERSION = "investment-master-v1"

_SYSTEM_PROMPT_ZH = """你是“投资大师”研究助手，只能基于输入 JSON 输出价值投资风格结论。

硬性规则：
1. 先风险后收益，先讨论安全边际，再讨论潜在回报。
2. 必须区分事实与判断：facts 只写输入中可验证字段，judgments 写推断。
3. 必须输出仓位建议、买入触发区间、退出条件、反方观点。
4. 不得使用“必涨/稳赚/无风险”等确定性措辞，必须使用条件化表达。
5. 禁止编造输入 JSON 中不存在的数据。
6. 输出中文，面向个人投研复盘，不构成投资建议。
"""


class BuyTriggerZone(BaseModel):
    ideal: str = Field(description="理想买入区间或条件")
    acceptable: str = Field(description="可接受买入区间或条件")


class InvestmentMasterSummary(BaseModel):
    style: Literal["value-investing"] = "value-investing"
    conclusion: str = Field(description="一句话结论")
    valuation_view: str = Field(description="估值与安全边际判断")
    position_advice: str = Field(description="仓位建议，例如观察仓/试探仓/标准仓")
    buy_trigger_zone: BuyTriggerZone
    exit_conditions: list[str] = Field(min_length=1, description="退出条件列表")
    counter_arguments: list[str] = Field(min_length=1, description="反方观点列表")
    watch_items: list[str] = Field(min_length=1, description="后续跟踪清单")
    facts: list[str] = Field(min_length=1, description="事实依据列表")
    judgments: list[str] = Field(min_length=1, description="分析判断列表")
    confidence: Literal["high", "medium", "low"] = "medium"
    disclaimer: str = "仅供个人投研复盘，不构成投资建议。"

    @field_validator("position_advice")
    @classmethod
    def validate_position_advice(cls, v: str) -> str:
        s = v.strip()
        if not s:
            raise ValueError("position_advice 不能为空")
        return s


class InvestmentMasterSummaryError(Exception):
    pass


class InvestmentMasterUnavailableError(InvestmentMasterSummaryError):
    pass


@dataclass(frozen=True, slots=True)
class InvestmentMasterSummaryResult:
    summary: InvestmentMasterSummary
    prompt_version: str
    model: str


def _invoke_structured_summary(*, settings: CompanyAiAnalysisSettings, context_json: str) -> InvestmentMasterSummary:
    try:
        from langchain_core.prompts import ChatPromptTemplate
        from langchain_openai import ChatOpenAI
    except ImportError as exc:
        raise InvestmentMasterUnavailableError(
            "未安装 AI 依赖：请执行 pip install -e \".[ai]\"",
        ) from exc

    llm = ChatOpenAI(
        model=settings.model or "",
        api_key=settings.api_key,
        base_url=settings.base_url,
        timeout=settings.timeout_seconds,
        max_retries=0,
    )
    structured = llm.with_structured_output(InvestmentMasterSummary)
    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", _SYSTEM_PROMPT_ZH),
            (
                "human",
                "请基于以下 JSON 输出结构化结论，注意必须包含仓位建议、买入触发区间、退出条件与反方观点：\n{context_json}",
            ),
        ],
    )
    chain = prompt | structured
    try:
        out = chain.invoke({"context_json": context_json})
    except Exception as exc:  # noqa: BLE001
        raise InvestmentMasterSummaryError("AI 总结生成失败，请稍后重试") from exc
    if not isinstance(out, InvestmentMasterSummary):
        raise InvestmentMasterSummaryError("AI 返回结构不合法")
    return out


def summarize_investment_master(
    context: dict[str, Any],
    *,
    invoke_fn: Any = None,
) -> InvestmentMasterSummaryResult:
    settings = CompanyAiAnalysisSettings.from_env()
    if not settings.is_ready():
        raise InvestmentMasterUnavailableError("AI 配置未就绪，请检查 VALUE_SCREENER_AI_* 环境变量")

    context_json = json.dumps(context, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    invoker = invoke_fn or _invoke_structured_summary
    try:
        summary = invoker(settings=settings, context_json=context_json)
    except ValidationError as exc:
        raise InvestmentMasterSummaryError(f"AI 输出结构校验失败: {exc}") from exc
    return InvestmentMasterSummaryResult(
        summary=summary,
        prompt_version=PROMPT_VERSION,
        model=settings.model or "",
    )
