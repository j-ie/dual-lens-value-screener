from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class AShareIngestionSettings:
    """A 股拉数与批跑配置（环境变量 + 默认值）。

    tushare_max_calls_per_minute：平台文档中的每分钟上限；tushare_rpm_headroom：本地限流预留余量。
    batch_screen_persist_chunk_size：HTTP 异步批跑分块写入 screening_result 的标的数，0 表示仅结束时写入。
    """

    tushare_token: str | None
    primary_backend: str
    max_symbols: int | None
    request_sleep_seconds: float
    tushare_max_workers: int
    tushare_max_retries: int
    tushare_retry_backoff_seconds: float
    tushare_max_calls_per_minute: int
    tushare_rpm_headroom: int
    fs_sync_schedule_tz: str
    batch_screen_persist_chunk_size: int

    @classmethod
    def from_env(cls) -> AShareIngestionSettings:
        token = os.environ.get("TUSHARE_TOKEN", "").strip() or None
        primary = os.environ.get("VALUE_SCREENER_PRIMARY", "tushare").strip().lower()
        if primary not in {"tushare", "akshare"}:
            primary = "tushare"
        max_sym = os.environ.get("VALUE_SCREENER_MAX_SYMBOLS")
        max_symbols = int(max_sym) if max_sym and max_sym.isdigit() else None
        sleep_s = float(os.environ.get("VALUE_SCREENER_REQUEST_SLEEP", "0.12"))
        # 默认 4：未显式配置时启用有界并发，否则与旧版单线程无异、全市场仍极慢。
        # 限流敏感环境可设 VALUE_SCREENER_TUSHARE_MAX_WORKERS=1。
        workers_raw = os.environ.get("VALUE_SCREENER_TUSHARE_MAX_WORKERS", "4").strip()
        tushare_max_workers = int(workers_raw) if workers_raw.isdigit() else 4
        tushare_max_workers = max(1, min(tushare_max_workers, 64))
        retries_raw = os.environ.get("VALUE_SCREENER_TUSHARE_MAX_RETRIES", "2").strip()
        tushare_max_retries = int(retries_raw) if retries_raw.isdigit() else 2
        tushare_max_retries = max(0, min(tushare_max_retries, 10))
        backoff_s = float(os.environ.get("VALUE_SCREENER_TUSHARE_RETRY_BACKOFF", "0.5"))
        rpm_raw = os.environ.get("VALUE_SCREENER_TUSHARE_MAX_CALLS_PER_MINUTE", "200").strip()
        tushare_max_calls_per_minute = int(rpm_raw) if rpm_raw.isdigit() else 200
        tushare_max_calls_per_minute = max(1, min(tushare_max_calls_per_minute, 10_000))
        hr_raw = os.environ.get("VALUE_SCREENER_TUSHARE_RPM_HEADROOM", "15").strip()
        tushare_rpm_headroom = int(hr_raw) if hr_raw.isdigit() else 15
        tushare_rpm_headroom = max(0, min(tushare_rpm_headroom, tushare_max_calls_per_minute - 1))
        tz_raw = os.environ.get("VALUE_SCREENER_FS_SYNC_SCHEDULE_TZ", "Asia/Shanghai").strip()
        fs_sync_schedule_tz = tz_raw if tz_raw else "Asia/Shanghai"
        ch_raw = os.environ.get("VALUE_SCREENER_BATCH_PERSIST_CHUNK_SIZE", "50").strip()
        batch_screen_persist_chunk_size = int(ch_raw) if ch_raw.isdigit() else 50
        batch_screen_persist_chunk_size = max(0, min(batch_screen_persist_chunk_size, 2000))
        return cls(
            tushare_token=token,
            primary_backend=primary,
            max_symbols=max_symbols,
            request_sleep_seconds=max(0.0, sleep_s),
            tushare_max_workers=tushare_max_workers,
            tushare_max_retries=tushare_max_retries,
            tushare_retry_backoff_seconds=max(0.0, backoff_s),
            tushare_max_calls_per_minute=tushare_max_calls_per_minute,
            tushare_rpm_headroom=tushare_rpm_headroom,
            fs_sync_schedule_tz=fs_sync_schedule_tz,
            batch_screen_persist_chunk_size=batch_screen_persist_chunk_size,
        )

    def tushare_rpm_effective_cap(self) -> int:
        """实际限流使用的每分钟次数（上限减余量，至少为 1）。"""

        return max(1, int(self.tushare_max_calls_per_minute) - int(self.tushare_rpm_headroom))


def _env_truthy(name: str) -> bool:
    v = os.environ.get(name, "").strip().lower()
    return v in {"1", "true", "yes", "on"}


@dataclass(frozen=True, slots=True)
class CompanyAiAnalysisSettings:
    """详情页 AI 分析（LangChain + 兼容 OpenAI 的模型端点，如火山方舟）。"""

    enabled: bool
    api_key: str | None
    base_url: str | None
    model: str | None
    cache_ttl_seconds: int
    timeout_seconds: float

    @classmethod
    def from_env(cls) -> CompanyAiAnalysisSettings:
        api_key = os.environ.get("VALUE_SCREENER_AI_API_KEY", "").strip() or None
        base_url = os.environ.get("VALUE_SCREENER_AI_BASE_URL", "").strip() or None
        model = os.environ.get("VALUE_SCREENER_AI_MODEL", "").strip() or None
        ttl_raw = os.environ.get("VALUE_SCREENER_AI_CACHE_TTL_SECONDS", "3600").strip()
        try:
            cache_ttl = max(0, int(ttl_raw))
        except ValueError:
            cache_ttl = 3600
        # 结构化输出 + 较长 JSON 上下文在部分方舟/代理链路上常超过 120s，默认放宽至 240s（仍受上限 600 钳制）。
        timeout_raw = os.environ.get("VALUE_SCREENER_AI_TIMEOUT_SECONDS", "240").strip()
        try:
            timeout_s = float(timeout_raw)
        except ValueError:
            timeout_s = 240.0
        timeout_s = max(5.0, min(timeout_s, 600.0))
        return cls(
            enabled=_env_truthy("VALUE_SCREENER_AI_ENABLED"),
            api_key=api_key,
            base_url=base_url,
            model=model,
            cache_ttl_seconds=cache_ttl,
            timeout_seconds=timeout_s,
        )

    def is_ready(self) -> bool:
        return bool(
            self.enabled
            and self.api_key
            and self.base_url
            and self.model,
        )

    def readiness_gaps_zh(self) -> list[str]:
        """用于 503 提示（不含密钥）；说明缺哪几项或开关未打开。"""
        gaps: list[str] = []
        if not self.enabled:
            gaps.append("VALUE_SCREENER_AI_ENABLED 未设为 1 / true / yes / on（当前视为关闭）")
        if not self.api_key:
            gaps.append("缺少或为空：VALUE_SCREENER_AI_API_KEY")
        if not self.base_url:
            gaps.append("缺少或为空：VALUE_SCREENER_AI_BASE_URL（火山方舟多为 …/api/v3）")
        if not self.model:
            gaps.append("缺少或为空：VALUE_SCREENER_AI_MODEL（方舟一般为 ep-… 接入点 ID）")
        return gaps


def _env_float_default(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int_default(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


@dataclass(frozen=True, slots=True)
class DcfValuationSettings:
    """公司详情 DCF（简化 FCFF）：环境默认与查询参数钳制边界。"""

    enabled: bool
    default_wacc: float
    default_stage1_growth: float
    default_terminal_growth: float
    forecast_years: int
    wacc_terminal_epsilon: float
    ttm_periods_max: int
    sync_since_years: int
    annual_stale_days: int
    wacc_min: float
    wacc_max: float
    stage1_g_min: float
    stage1_g_max: float
    terminal_g_min: float
    terminal_g_max: float
    daily_basic_timeout_seconds: float
    financial_ni_base_scale: float

    @classmethod
    def from_env(cls) -> DcfValuationSettings:
        fin_scale_raw = _env_float_default("VALUE_SCREENER_DCF_FINANCIAL_NI_BASE_SCALE", 0.35)
        fin_scale = max(0.15, min(1.0, fin_scale_raw))
        return cls(
            enabled=_env_truthy("VALUE_SCREENER_DCF_ENABLED"),
            default_wacc=_env_float_default("VALUE_SCREENER_DCF_WACC", 0.09),
            default_stage1_growth=_env_float_default("VALUE_SCREENER_DCF_STAGE1_GROWTH", 0.02),
            default_terminal_growth=_env_float_default("VALUE_SCREENER_DCF_TERMINAL_GROWTH", 0.025),
            forecast_years=max(1, min(_env_int_default("VALUE_SCREENER_DCF_FORECAST_YEARS", 5), 20)),
            wacc_terminal_epsilon=max(1e-6, _env_float_default("VALUE_SCREENER_DCF_WACC_TERMINAL_EPSILON", 0.0005)),
            ttm_periods_max=max(1, min(_env_int_default("VALUE_SCREENER_DCF_TTM_PERIODS", 4), 12)),
            sync_since_years=max(1, min(_env_int_default("VALUE_SCREENER_DCF_SYNC_SINCE_YEARS", 5), 20)),
            annual_stale_days=max(30, min(_env_int_default("VALUE_SCREENER_DCF_ANNUAL_STALE_DAYS", 550), 4000)),
            wacc_min=_env_float_default("VALUE_SCREENER_DCF_WACC_MIN", 0.04),
            wacc_max=_env_float_default("VALUE_SCREENER_DCF_WACC_MAX", 0.25),
            stage1_g_min=_env_float_default("VALUE_SCREENER_DCF_STAGE1_G_MIN", -0.05),
            stage1_g_max=_env_float_default("VALUE_SCREENER_DCF_STAGE1_G_MAX", 0.20),
            terminal_g_min=_env_float_default("VALUE_SCREENER_DCF_TERMINAL_G_MIN", -0.02),
            terminal_g_max=_env_float_default("VALUE_SCREENER_DCF_TERMINAL_G_MAX", 0.06),
            daily_basic_timeout_seconds=max(
                1.0, _env_float_default("VALUE_SCREENER_DCF_DAILY_BASIC_TIMEOUT_SECONDS", 12.0)
            ),
            financial_ni_base_scale=fin_scale,
        )

    def clamp_wacc(self, v: float) -> float:
        return max(self.wacc_min, min(self.wacc_max, v))

    def clamp_stage1(self, v: float) -> float:
        return max(self.stage1_g_min, min(self.stage1_g_max, v))

    def clamp_terminal(self, v: float) -> float:
        return max(self.terminal_g_min, min(self.terminal_g_max, v))


@dataclass(frozen=True, slots=True)
class PostFullBatchPipelineSettings:
    """
    手动触发的后置流水线参数（第三套/三元、综合 Top N 的 DCF+AI）：由环境变量配置。
    批跑成功后不会在后台自动执行，须在「拉数任务」或 POST /runs/{id}/post-pipeline 手动触发。
    """

    attach_third_lens: bool
    ai_top_n: int
    ai_sleep_seconds: float

    @classmethod
    def from_env(cls) -> PostFullBatchPipelineSettings:
        attach_tl = not _env_truthy("VALUE_SCREENER_POST_FULL_BATCH_SKIP_THIRD_LENS")
        n_raw = os.environ.get("VALUE_SCREENER_POST_FULL_BATCH_AI_TOP_N", "1000").strip()
        try:
            ai_top_n = int(n_raw) if n_raw else 1000
        except ValueError:
            ai_top_n = 1000
        ai_top_n = max(0, min(ai_top_n, 10_000))
        sleep_raw = os.environ.get("VALUE_SCREENER_POST_FULL_BATCH_AI_SLEEP_SECONDS", "1.0").strip()
        try:
            ai_sleep = float(sleep_raw) if sleep_raw else 1.0
        except ValueError:
            ai_sleep = 1.0
        ai_sleep = max(0.0, min(ai_sleep, 60.0))
        return cls(
            attach_third_lens=attach_tl,
            ai_top_n=ai_top_n,
            ai_sleep_seconds=ai_sleep,
        )
