from __future__ import annotations

import hashlib
import json
import logging
from datetime import date, datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from pydantic import BaseModel, Field
from sqlalchemy.engine import Engine

from value_screener.application.company_detail_query import CompanyDetailQueryService
from value_screener.application.investment_quality_run_context import (
    compute_investment_quality_for_run_symbol,
)
from value_screener.domain.company_ai_dcf_snapshot import dcf_snapshot_for_persistence
from value_screener.infrastructure.ai_analysis_cache import (
    ai_analysis_cache_key,
    ai_cache_get,
    ai_cache_set,
)
from value_screener.infrastructure.company_ai_analysis_repository import CompanyAiAnalysisRepository
from value_screener.infrastructure.settings import CompanyAiAnalysisSettings

logger = logging.getLogger(__name__)

PROMPT_VERSION = "v5"

_SYSTEM_PROMPT_ZH = """你是一位资深财务分析师与投资顾问（教学/信息用途）。用户将提供一段 JSON，
其中包含某次股票筛选批跑的冻结快照 run_snapshot、公司主数据 reference、三大报表摘要 financials、
独立拉取的行情 live_quote，以及可选的简化 DCF 块 dcf（若存在）。

硬性规则：
1. run_snapshot 中的分数、解释与 provenance 仅反映该次批跑所用数据时点；live_quote 的拉取时刻见其中的 fetched_at，可能与快照不一致。
2. 若 JSON 含 dcf 字段：其为基于固定假设的机械化折现估算，高度依赖参数与财报代理口径；dcf.ok 为 false 时不得编造每股内在价值或 EV 数字。
3. 你必须输出 ai_score：0～100 的浮点数，仅表示在**仅依据所给 JSON** 前提下，对「信息完整度、叙事与规则分数（巴菲特/格雷厄姆/第三套/三元等）及 dcf（若有）之间**一致性**」的综合主观判断；**不得**用 ai_score 表达标的是否便宜、是否值得买入；**不是**投资建议，也**不等于**官方基本面打分。
4. 你必须输出 opportunity_score：0～100 的浮点数，表示在**仅依据所给 JSON** 前提下，对「当前价格相对于所给基本面与估值信号，是否可能具备较显著安全边际 / 是否接近所谓黄金坑机会」的**主观倾向**（非买卖指令）。须综合 run_snapshot 中各规则分、live_quote 与 reference/financials 中可引用的估值相关事实。若 dcf.ok 为 true：**银行/保险等金融业**（或可由 JSON 中行业、主营业务描述合理推断为金融企业）时，须认知 DCF 为粗代理，**不得**仅因「DCF 每股价值低于市价」就将 opportunity_score 打到极低（如低于 40），除非 Graham/市净率/盈利收益率等 JSON 内字段共同支持「显著高估」叙事；非金融企业则可对 DCF 与市价偏离给予更高权重但仍须与规则分交叉校验。
5. ai_score_rationale、opportunity_score_rationale 各用 1～2 句中文简述对应给分理由（仅引用已给字段）。
6. alignment_with_scores 须显式讨论：规则筛分分数、dcf（若 dcf.ok 为 true 且有数值）与叙述之间的关系或张力；若 dcf.ok 为 false，须说明无法使用 DCF 数值结论；并简要说明 ai_score（一致性）与 opportunity_score（机会倾向）是否因同一组张力而分化。
7. 仅可使用 JSON 中出现的数字与事实；不得编造未提供的财务数值、股价或预测。
8. 若某项指标在 JSON 中缺失或未披露，须明确写「提供数据中未包含该项」，不得推测具体数字。
   若 run_snapshot 中已出现 `pe_ttm`、`net_income_ttm`、`market_cap` 等键且值为数字，则视为已提供，可引用并说明其为批跑冻结时点口径（非实时行情）。
9. 输出须客观，避免「必涨」「稳赚」「必买」等承诺式表述。
10. 回答面向专业投资者教育场景，不构成任何投资建议。
11. 若 JSON 含 `investment_quality`：其为系统规则引擎输出的结构化价值判断（含结论、模块分、风险标记等），**非**大模型生成；你必须在摘要、`investment_quality_commentary`、与规则筛分对照及完整叙述中显式讨论该块与 Buffett/Graham/第三套/三元等分数的关系或张力，不得编造该块中未出现的字段或数值。若不含该键或值为 null，则写明「本次上下文未提供规则化价值判断数据」。"""


class CompanyAiDetailError(Exception):
    """详情聚合失败（与 company detail 一致的错误码）。"""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class CompanyAiUnavailableError(Exception):
    """功能未启用或依赖/配置缺失。"""

    pass


class CompanyAiUpstreamError(Exception):
    """模型调用或解析失败。"""

    pass


class CompanyAiTimeoutError(CompanyAiUpstreamError):
    """上游模型在配置的超时时间内未返回完整响应（区别于 4xx/5xx 等业务错误）。"""

    pass


def _is_llm_timeout(exc: BaseException) -> bool:
    """识别 HTTP/SDK 层的读超时，含 __cause__ 链上的包装异常。"""

    chain: list[BaseException] = []
    cur: BaseException | None = exc
    seen: set[int] = set()
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        chain.append(cur)
        cur = cur.__cause__

    for item in chain:
        try:
            import httpx

            if isinstance(item, httpx.TimeoutException):
                return True
        except ImportError:
            pass
        try:
            from openai import APITimeoutError

            if isinstance(item, APITimeoutError):
                return True
        except ImportError:
            pass
        if isinstance(item, TimeoutError):
            return True
        lowered = str(item).lower()
        if "timed out" in lowered or "read timeout" in lowered:
            return True
    return False


class LlmCompanyAnalysisSchema(BaseModel):
    """与模型结构化输出字段对齐（供 LangChain with_structured_output 使用）。"""

    summary: str = Field(description="3～6 句中文要点摘要")
    key_metrics_commentary: str = Field(description="结合提供数据对关键财务与估值相关信息的评论，勿编造数字")
    risks: str = Field(description="主要风险与不确定性（基于已给信息）")
    alignment_with_scores: str = Field(
        description="筛分分数、investment_quality（若有）、dcf（若有且成功）与基本面叙述的对照；须说明与 Buffett/Graham/第三套/三元等的关系"
    )
    investment_quality_commentary: str = Field(
        default="",
        description="若上下文含 investment_quality：3～8 句中文解读规则结论、模块分与风险提示，并点出与行业/周期口径的衔接；不含则写未提供",
    )
    narrative_markdown: str = Field(description="完整叙述，可使用 Markdown 小标题与列表（勿重复粘贴 investment_quality 全文 JSON）")
    ai_score: float = Field(
        description="0～100，仅信息完整度与规则分/DCF/叙述一致性，非便宜度或买卖建议",
    )
    ai_score_rationale: str = Field(default="", description="1～2 句中文简述一致性给分理由")
    opportunity_score: float = Field(
        description="0～100，安全边际/黄金坑机会主观倾向，金融业勿单押简化DCF，非买卖建议",
    )
    opportunity_score_rationale: str = Field(default="", description="1～2 句中文简述机会倾向理由")


def _canonical_context_json(context: dict[str, Any]) -> str:
    return json.dumps(context, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def build_analysis_context(detail: dict[str, Any]) -> dict[str, Any]:
    """与详情 API 同源字段，附加混源说明，供模型与哈希使用。"""
    ctx: dict[str, Any] = {
        "data_mixing_note_zh": (
            "run_snapshot 来自该次 screening 批跑冻结结果；live_quote 为请求时刻附近独立拉取的行情，"
            "二者时间戳可能不一致。dcf 为可选简化估值块。investment_quality 为同次 run 下规则引擎价值判断（若提供）。"
            "禁止编造上下文中未出现的数值。"
        ),
        "run": detail.get("run"),
        "run_snapshot": detail.get("run_snapshot"),
        "reference": detail.get("reference"),
        "financials": detail.get("financials"),
        "live_quote": detail.get("live_quote"),
        "dcf": detail.get("dcf"),
    }
    if "investment_quality" in detail:
        ctx["investment_quality"] = detail.get("investment_quality")
    return ctx


def context_hash_for(context: dict[str, Any]) -> str:
    payload = _canonical_context_json(context)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _analysis_calendar_date_shanghai(now_utc: datetime) -> date:
    return now_utc.astimezone(ZoneInfo("Asia/Shanghai")).date()


def _clamp_unit_score(v: float, field_name: str) -> float:
    if v < 0.0 or v > 100.0:
        raise CompanyAiUpstreamError(f"模型返回的 {field_name} 超出 0～100 范围")
    return round(float(v), 4)


def _invoke_structured_llm(
    *,
    settings: CompanyAiAnalysisSettings,
    context_json: str,
) -> LlmCompanyAnalysisSchema:
    try:
        from langchain_core.prompts import ChatPromptTemplate
        from langchain_openai import ChatOpenAI
    except ImportError as exc:
        raise CompanyAiUnavailableError(
            "未安装 AI 依赖：请执行 pip install -e \".[ai]\" 后再启用 VALUE_SCREENER_AI_ENABLED",
        ) from exc

    llm = ChatOpenAI(
        model=settings.model or "",
        api_key=settings.api_key,
        base_url=settings.base_url,
        timeout=settings.timeout_seconds,
        max_retries=0,
    )
    structured = llm.with_structured_output(LlmCompanyAnalysisSchema)
    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", _SYSTEM_PROMPT_ZH),
            ("human", "以下为分析上下文（JSON 字符串）。请严格按系统规则输出：\n{context_json}"),
        ],
    )
    chain = prompt | structured
    try:
        out = chain.invoke({"context_json": context_json})
    except Exception as exc:  # noqa: BLE001
        logger.warning("company ai llm invoke failed: %s", exc)
        if _is_llm_timeout(exc):
            t = int(settings.timeout_seconds) if settings.timeout_seconds >= 1 else settings.timeout_seconds
            raise CompanyAiTimeoutError(
                f"模型响应超时（客户端等待上限 {t} 秒）。"
                "可调大环境变量 VALUE_SCREENER_AI_TIMEOUT_SECONDS（上限 600）后重试，"
                "或检查网络与模型端点负载。",
            ) from exc
        raise CompanyAiUpstreamError("模型调用失败或超时，请稍后重试") from exc
    if not isinstance(out, LlmCompanyAnalysisSchema):
        raise CompanyAiUpstreamError("模型返回格式异常")
    return out


class CompanyAiAnalysisApplicationService:
    """详情页触发的 AI 分析应用服务（领域分数仍由确定性规则计算，不由 LLM 改写）。

    输出中 `ai_score` 与 `opportunity_score` 的语义边界见 `domain.company_ai_score_semantics` 模块说明。
    """

    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        self._detail = CompanyDetailQueryService(engine)
        self._ai_repo = CompanyAiAnalysisRepository(engine)

    def analyze(self, run_id: int, ts_code: str, *, force_refresh: bool = False) -> dict[str, Any]:
        settings = CompanyAiAnalysisSettings.from_env()
        if not settings.is_ready():
            gaps = settings.readiness_gaps_zh()
            hint = "；".join(gaps) if gaps else "请检查 VALUE_SCREENER_AI_* 环境变量"
            raise CompanyAiUnavailableError(
                f"AI 分析不可用：{hint}。"
                " 若已写入项目根目录 .env，请重启 API 进程；"
                " 使用 scripts/dev.ps1 时会自动加载 .env 到 API 窗口。",
            )

        try:
            detail = self._detail.load(
                run_id,
                ts_code,
                include_financial_payload=False,
                financial_limit=12,
                include_dcf=True,
            )
        except ValueError as exc:
            raise CompanyAiDetailError("bad_ts_code") from exc

        err = detail.get("_error")
        if err == "run_not_found":
            raise CompanyAiDetailError("run_not_found")
        if err == "symbol_not_in_run":
            raise CompanyAiDetailError("symbol_not_in_run")

        iq_payload = compute_investment_quality_for_run_symbol(self._engine, run_id, ts_code)
        detail_with_iq = dict(detail)
        if iq_payload is not None:
            detail_with_iq["investment_quality"] = iq_payload
        else:
            detail_with_iq["investment_quality"] = None

        ctx = build_analysis_context(detail_with_iq)
        context_json = _canonical_context_json(ctx)
        ctx_hash = hashlib.sha256(context_json.encode("utf-8")).hexdigest()
        model_id = settings.model or ""
        cache_key = ai_analysis_cache_key(ctx_hash, model_id, PROMPT_VERSION)

        if (not force_refresh) and settings.cache_ttl_seconds > 0:
            cached = ai_cache_get(cache_key)
            if cached is not None:
                meta = dict(cached.get("meta") or {})
                meta["cached"] = True
                cached["meta"] = meta
                return cached

        parsed = _invoke_structured_llm(settings=settings, context_json=context_json)
        ai_score = _clamp_unit_score(parsed.ai_score, "ai_score")
        opportunity_score = _clamp_unit_score(parsed.opportunity_score, "opportunity_score")
        generated_at = datetime.now(timezone.utc)
        dcf_raw = detail.get("dcf")
        dcf_json, dcf_ok, dcf_headline = dcf_snapshot_for_persistence(dcf_raw)
        iq_commentary = (parsed.investment_quality_commentary or "").strip()
        narrative_combined = (parsed.narrative_markdown or "").strip()
        if iq_commentary:
            narrative_combined = (
                "## 价值判断（规则引擎）\n\n" + iq_commentary + "\n\n---\n\n" + narrative_combined
            )
        body: dict[str, Any] = {
            "summary": parsed.summary,
            "key_metrics_commentary": parsed.key_metrics_commentary,
            "risks": parsed.risks,
            "alignment_with_scores": parsed.alignment_with_scores,
            "investment_quality_commentary": iq_commentary,
            "narrative_markdown": narrative_combined,
            "ai_score": ai_score,
            "ai_score_rationale": (parsed.ai_score_rationale or "").strip(),
            "opportunity_score": opportunity_score,
            "opportunity_score_rationale": (parsed.opportunity_score_rationale or "").strip(),
            "dcf_snapshot": dcf_json,
            "meta": {
                "context_hash": ctx_hash,
                "prompt_version": PROMPT_VERSION,
                "model": model_id,
                "generated_at": generated_at.isoformat(),
                "cached": False,
            },
        }

        code = str(ts_code).strip()
        adate = _analysis_calendar_date_shanghai(generated_at)
        try:
            with self._engine.begin() as conn:
                self._ai_repo.upsert(
                    conn,
                    ts_code=code,
                    analysis_date=adate,
                    run_id=run_id,
                    ai_score=ai_score,
                    ai_score_rationale=body["ai_score_rationale"] or None,
                    opportunity_score=opportunity_score,
                    opportunity_score_rationale=body["opportunity_score_rationale"] or None,
                    summary=parsed.summary,
                    key_metrics_commentary=parsed.key_metrics_commentary,
                    risks=parsed.risks,
                    alignment_with_scores=parsed.alignment_with_scores,
                    narrative_markdown=parsed.narrative_markdown,
                    context_hash=ctx_hash,
                    prompt_version=PROMPT_VERSION,
                    model=model_id,
                    generated_at=generated_at,
                    now=generated_at,
                    dcf_json=dcf_json,
                    dcf_ok=dcf_ok,
                    dcf_headline=dcf_headline,
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("company ai persist failed: %s", exc)

        body["meta"]["analysis_date"] = adate.isoformat()
        if settings.cache_ttl_seconds > 0:
            ai_cache_set(cache_key, body, settings.cache_ttl_seconds)
        return body
