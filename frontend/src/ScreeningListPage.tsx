import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { Button, Card, Checkbox, Input, InputNumber, Progress, Select, Space, Table, Typography, message } from "antd";
import type { ColumnsType, TablePaginationConfig } from "antd/es/table";

type RunItem = {
  id: number;
  external_uuid: string;
  status: string;
  created_at: string;
  finished_at: string | null;
  universe_size: number | null;
  snapshot_ok: number | null;
  snapshot_failed: number | null;
  provider_label: string | null;
  progress_percent?: number | null;
  progress_current?: number | null;
  progress_total?: number | null;
  progress_phase?: string | null;
  progress_symbol?: string | null;
};

type PagedItem = {
  symbol: string;
  graham_score: number;
  buffett_score: number;
  combined_score?: number | null;
  third_lens_score?: number | null;
  third_lens?: Record<string, unknown> | null;
  final_triple_score?: number | null;
  coverage_ok?: boolean;
  graham: Record<string, unknown>;
  buffett: Record<string, unknown>;
  provenance: Record<string, unknown> | null;
  display_name: string;
  company_full_name: string | null;
  industry: string;
  region: string;
  score_explanation_zh: string;
  trade_cal_date: string | null;
  financials_end_date: string | null;
  data_source: string | null;
  ai_score?: number | null;
  opportunity_score?: number | null;
  ai_analysis_date?: string | null;
  ai_run_id?: number | null;
  ai_summary_preview?: string | null;
  market_cap?: number | null;
  dv_ratio?: number | null;
  dv_ttm?: number | null;
  investment_quality?: Record<string, unknown> | null;
  iq_decision?: string | null;
  iq_decision_label_zh?: string | null;
};

type Paged = {
  items: PagedItem[];
  total: number;
  page: number;
  page_size: number;
  sort: string;
  order: string;
};

const SORT_OPTIONS = [
  { value: "buffett_desc", label: "巴菲特分 ↓" },
  { value: "buffett_asc", label: "巴菲特分 ↑" },
  { value: "graham_desc", label: "格雷厄姆分 ↓" },
  { value: "graham_asc", label: "格雷厄姆分 ↑" },
  { value: "combined_desc", label: "综合分 ↓（双维 B+G+门槛，默认）" },
  { value: "combined_asc", label: "综合分 ↑" },
  { value: "third_lens_desc", label: "第三套分 ↓" },
  { value: "third_lens_asc", label: "第三套分 ↑" },
  { value: "triple_desc", label: "三元综合 ↓（B+G+第三套）" },
  { value: "triple_asc", label: "三元综合 ↑" },
  { value: "industry_desc", label: "行业 ↓" },
  { value: "industry_asc", label: "行业 ↑" },
  { value: "ai_score_desc", label: "AI 一致性分 ↓（持久化）" },
  { value: "ai_score_asc", label: "AI 一致性分 ↑（持久化）" },
  { value: "market_cap_desc", label: "市值 ↓" },
  { value: "market_cap_asc", label: "市值 ↑" },
  { value: "dividend_yield_desc", label: "股息率 ↓" },
  { value: "dividend_yield_asc", label: "股息率 ↑" },
];

const EMPTY = "—";

function formatMarketCapYuan(v: number | null | undefined): string {
  if (v === null || v === undefined || !Number.isFinite(v) || v <= 0) {
    return EMPTY;
  }
  const yi = v / 1e8;
  if (yi >= 0.01) {
    return `${yi.toFixed(2)} 亿`;
  }
  const wan = v / 1e4;
  if (wan >= 1) {
    return `${wan.toFixed(2)} 万`;
  }
  return `${Math.round(v)} 元`;
}

function formatDividendYieldPreferred(
  dvTtm: number | null | undefined,
  dvRatio: number | null | undefined,
): string {
  const pick = (x: number | null | undefined) =>
    x !== null && x !== undefined && Number.isFinite(x) ? x : null;
  const chosen = pick(dvTtm) ?? pick(dvRatio);
  if (chosen === null) {
    return EMPTY;
  }
  return `${chosen.toFixed(2)}%`;
}

/** 与后端 INDUSTRY_EMPTY_QUERY_VALUE 一致 */
const INDUSTRY_EMPTY = "__EMPTY__";
/** 与后端 IQ_DECISION_EMPTY_QUERY_VALUE 一致：批跑未写入价值质量结论 */
const IQ_DECISION_EMPTY = "__IQ_EMPTY__";

/** Facet 未就绪时仍可选，与服务端 `iq_decision` 一致 */
const IQ_DECISION_FILTER_FALLBACK: { value: string; label: string }[] = [
  { value: IQ_DECISION_EMPTY, label: "（未计算/旧 Run）" },
  { value: "buy", label: "可买" },
  { value: "watchlist", label: "跟踪" },
  { value: "cautious", label: "谨慎" },
  { value: "reject", label: "排除" },
];

function parseSortKey(combo: string): {
  sort:
    | "buffett"
    | "graham"
    | "combined"
    | "industry"
    | "third_lens"
    | "triple"
    | "ai_score"
    | "market_cap"
    | "dividend_yield";
  order: "asc" | "desc";
} {
  const m = combo.match(
    /^(buffett|graham|combined|industry|third_lens|triple|ai_score|market_cap|dividend_yield)_(asc|desc)$/,
  );
  if (!m) {
    return { sort: "combined", order: "desc" };
  }
  return {
    sort: m[1] as
      | "buffett"
      | "graham"
      | "combined"
      | "industry"
      | "third_lens"
      | "triple"
      | "ai_score"
      | "market_cap"
      | "dividend_yield",
    order: m[2] as "asc" | "desc",
  };
}

function formatRunLabel(r: RunItem): string {
  let tail =
    r.status === "running"
      ? "进行中…"
      : `ok=${r.snapshot_ok ?? "-"} fail=${r.snapshot_failed ?? "-"}`;
  if (
    r.status === "running" &&
    r.progress_total != null &&
    r.progress_total > 0 &&
    r.progress_percent != null
  ) {
    const cur = r.progress_current ?? 0;
    const sym = r.progress_symbol ? ` ${r.progress_symbol}` : "";
    tail = `${r.progress_percent}% (${cur}/${r.progress_total}${sym})`;
  }
  return `#${r.id} ${r.status} ${r.created_at} ${tail}`;
}

export default function ScreeningListPage() {
  const [searchParams] = useSearchParams();
  const [runs, setRuns] = useState<RunItem[]>([]);
  const [runId, setRunId] = useState<number | null>(null);
  const [sortCombo, setSortCombo] = useState("combined_desc");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [data, setData] = useState<Paged | null>(null);
  const [loading, setLoading] = useState(false);
  /** null = 全市场；数字 = 上限 */
  const [batchMax, setBatchMax] = useState<number | null>(null);
  const [batchLoading, setBatchLoading] = useState(false);
  const [selectedIndustries, setSelectedIndustries] = useState<string[]>([]);
  const [industryOptions, setIndustryOptions] = useState<{ value: string; label: string }[]>([]);
  const [hasAiOnly, setHasAiOnly] = useState(false);
  const [aiScoreMin, setAiScoreMin] = useState<number | null>(null);
  const [companyNameQ, setCompanyNameQ] = useState("");
  /** 亿元，提交时换算为后端「元」 */
  const [marketCapMinYi, setMarketCapMinYi] = useState<number | null>(null);
  const [marketCapMaxYi, setMarketCapMaxYi] = useState<number | null>(null);
  const [divYieldMin, setDivYieldMin] = useState<number | null>(null);
  const [divYieldMax, setDivYieldMax] = useState<number | null>(null);
  const [selectedIqDecisions, setSelectedIqDecisions] = useState<string[]>([]);
  const [iqDecisionOptions, setIqDecisionOptions] = useState<{ value: string; label: string }[]>([]);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const loadRuns = useCallback(async () => {
    try {
      const res = await fetch("/api/v1/runs?limit=100");
      if (!res.ok) {
        throw new Error(await res.text());
      }
      const list = (await res.json()) as RunItem[];
      setRuns(list);
      setRunId((prev) => (prev === null && list.length ? list[0].id : prev));
    } catch (e) {
      message.error(`加载 Run 列表失败：${e}`);
    }
  }, []);

  const { sort, order } = useMemo(() => parseSortKey(sortCombo), [sortCombo]);

  const loadPage = useCallback(async () => {
    if (runId === null) {
      return;
    }
    setLoading(true);
    try {
      const q = new URLSearchParams({
        page: String(page),
        page_size: String(pageSize),
        sort,
        order,
      });
      for (const ind of selectedIndustries) {
        q.append("industry", ind);
      }
      if (hasAiOnly) {
        q.set("has_ai_analysis", "true");
      }
      if (aiScoreMin !== null && Number.isFinite(aiScoreMin)) {
        q.set("ai_score_min", String(aiScoreMin));
      }
      const cn = companyNameQ.trim();
      if (cn) {
        q.set("company_name", cn);
      }
      if (marketCapMinYi !== null && Number.isFinite(marketCapMinYi) && marketCapMinYi >= 0) {
        q.set("market_cap_min", String(marketCapMinYi * 1e8));
      }
      if (marketCapMaxYi !== null && Number.isFinite(marketCapMaxYi) && marketCapMaxYi >= 0) {
        q.set("market_cap_max", String(marketCapMaxYi * 1e8));
      }
      if (divYieldMin !== null && Number.isFinite(divYieldMin) && divYieldMin >= 0) {
        q.set("dividend_yield_min", String(divYieldMin));
      }
      if (divYieldMax !== null && Number.isFinite(divYieldMax) && divYieldMax >= 0) {
        q.set("dividend_yield_max", String(divYieldMax));
      }
      for (const iqd of selectedIqDecisions) {
        q.append("iq_decision", iqd);
      }
      const res = await fetch(`/api/v1/runs/${runId}/results?${q.toString()}`);
      if (!res.ok) {
        throw new Error(await res.text());
      }
      setData((await res.json()) as Paged);
    } catch (e) {
      message.error(`加载分页失败：${e}`);
    } finally {
      setLoading(false);
    }
  }, [
    runId,
    page,
    pageSize,
    sort,
    order,
    selectedIndustries,
    hasAiOnly,
    aiScoreMin,
    companyNameQ,
    marketCapMinYi,
    marketCapMaxYi,
    divYieldMin,
    divYieldMax,
    selectedIqDecisions,
  ]);

  const loadIqDecisionFacets = useCallback(async () => {
    if (runId === null) {
      setIqDecisionOptions([]);
      return;
    }
    try {
      const res = await fetch(`/api/v1/runs/${runId}/result-iq-decisions`);
      if (!res.ok) {
        return;
      }
      const body = (await res.json()) as { iq_decisions: string[] };
      const labelFor = (v: string) => {
        if (v === IQ_DECISION_EMPTY) {
          return "（未计算/旧 Run）";
        }
        const map: Record<string, string> = {
          buy: "可买",
          watchlist: "跟踪",
          cautious: "谨慎",
          reject: "排除",
        };
        return map[v] ?? v;
      };
      const opts = (body.iq_decisions ?? []).map((v) => ({
        value: v,
        label: labelFor(v),
      }));
      setIqDecisionOptions(opts);
    } catch {
      setIqDecisionOptions([]);
    }
  }, [runId]);

  const loadIndustryFacets = useCallback(async () => {
    if (runId === null) {
      setIndustryOptions([]);
      return;
    }
    try {
      const res = await fetch(`/api/v1/runs/${runId}/result-industries`);
      if (!res.ok) {
        return;
      }
      const body = (await res.json()) as { industries: string[] };
      const opts = (body.industries ?? []).map((v) => ({
        value: v,
        label: v === INDUSTRY_EMPTY ? "（未分类/无行业）" : v,
      }));
      setIndustryOptions(opts);
    } catch {
      setIndustryOptions([]);
    }
  }, [runId]);

  const stopPoll = useCallback(() => {
    if (pollRef.current !== null) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
  }, []);

  const pollRunUntilTerminal = useCallback(
    async (id: number) => {
      stopPoll();
      const tick = async () => {
        try {
          const res = await fetch(`/api/v1/runs/${id}`);
          if (!res.ok) {
            return;
          }
          const r = (await res.json()) as RunItem;
          await loadRuns();
          if (r.status !== "running") {
            stopPoll();
            if (r.status === "failed") {
              message.error(`Run #${id} 失败，请检查服务端日志或 meta batch_error`);
            } else {
              message.success(`Run #${id} 已完成：成功 ${r.snapshot_ok ?? 0} / 请求 ${r.universe_size ?? 0}`);
            }
            setRunId(id);
            setPage(1);
            void loadPage();
          }
        } catch {
          /* 轮询忽略单次错误 */
        }
      };
      await tick();
      pollRef.current = setInterval(() => void tick(), 2000);
    },
    [loadRuns, loadPage, stopPoll],
  );

  const runBatchScreen = useCallback(async () => {
    setBatchLoading(true);
    try {
      const body =
        batchMax !== null && batchMax >= 1 ? { max_symbols: batchMax } : { max_symbols: null };
      const res = await fetch("/api/v1/runs/batch-screen", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const text = await res.text();
      if (!res.ok) {
        throw new Error(text || res.statusText);
      }
      const payload = JSON.parse(text) as {
        run_id: number;
        status?: string;
        message?: string;
      };
      message.info(payload.message ?? `已排队 Run #${payload.run_id}，后台执行中…`);
      await loadRuns();
      setRunId(payload.run_id);
      setPage(1);
      void pollRunUntilTerminal(payload.run_id);
    } catch (e) {
      message.error(`一键拉数失败：${e}`);
    } finally {
      setBatchLoading(false);
    }
  }, [batchMax, loadRuns, pollRunUntilTerminal]);

  useEffect(() => {
    void loadRuns();
  }, [loadRuns]);

  useEffect(() => {
    const raw = searchParams.get("runId");
    if (!raw) {
      return;
    }
    const n = parseInt(raw, 10);
    if (!Number.isNaN(n) && n > 0) {
      setRunId(n);
      setPage(1);
      setSelectedIndustries([]);
    }
  }, [searchParams]);

  useEffect(() => {
    void loadIndustryFacets();
  }, [loadIndustryFacets]);

  useEffect(() => {
    void loadIqDecisionFacets();
  }, [loadIqDecisionFacets]);

  useEffect(() => {
    void loadPage();
  }, [loadPage]);

  useEffect(() => () => stopPoll(), [stopPoll]);

  const columns: ColumnsType<PagedItem> = useMemo(
    () => [
      { title: "代码", dataIndex: "symbol", width: 108, fixed: "left" },
      {
        title: "名称",
        dataIndex: "display_name",
        width: 100,
        ellipsis: true,
        render: (_: string, row) => {
          const label = row.display_name ? row.display_name : EMPTY;
          const rid = row.ai_run_id ?? runId;
          if (rid === null) {
            return label;
          }
          return <Link to={`/runs/${rid}/companies/${encodeURIComponent(row.symbol)}`}>{label}</Link>;
        },
      },
      {
        title: "行业/地区",
        key: "industry_region",
        width: 140,
        ellipsis: true,
        render: (_: unknown, row) => {
          const a = [row.industry, row.region].filter(Boolean).join(" / ");
          return a || EMPTY;
        },
      },
      {
        title: "市值（批跑时点）",
        dataIndex: "market_cap",
        width: 120,
        align: "right",
        render: (_: number | null | undefined, row) => formatMarketCapYuan(row.market_cap),
      },
      {
        title: "股息率％",
        key: "dividend_yield",
        width: 100,
        align: "right",
        render: (_: unknown, row) => formatDividendYieldPreferred(row.dv_ttm, row.dv_ratio),
      },
      {
        title: "价值判断",
        key: "iq_label",
        width: 96,
        ellipsis: true,
        render: (_: unknown, row) =>
          row.iq_decision_label_zh
            ? String(row.iq_decision_label_zh)
            : row.iq_decision
              ? String(row.iq_decision)
              : EMPTY,
      },
      {
        title: "巴菲特分（规则）",
        dataIndex: "buffett_score",
        width: 96,
        align: "right",
        render: (v: number) => v.toFixed(2),
      },
      {
        title: "格雷厄姆分（规则）",
        dataIndex: "graham_score",
        width: 104,
        align: "right",
        render: (v: number) => v.toFixed(2),
      },
      {
        title: "综合分（双维）",
        dataIndex: "combined_score",
        width: 108,
        align: "right",
        render: (v: number | null | undefined) =>
          v !== null && v !== undefined && !Number.isNaN(v) ? v.toFixed(2) : EMPTY,
      },
      {
        title: "第三套分",
        dataIndex: "third_lens_score",
        width: 96,
        align: "right",
        render: (v: number | null | undefined) =>
          v !== null && v !== undefined && !Number.isNaN(v) ? v.toFixed(2) : EMPTY,
      },
      {
        title: "三元综合",
        dataIndex: "final_triple_score",
        width: 96,
        align: "right",
        render: (v: number | null | undefined) =>
          v !== null && v !== undefined && !Number.isNaN(v) ? v.toFixed(2) : EMPTY,
      },
      {
        title: "AI 一致性分",
        dataIndex: "ai_score",
        width: 102,
        align: "right",
        render: (v: number | null | undefined) =>
          v !== null && v !== undefined && !Number.isNaN(v) ? v.toFixed(2) : EMPTY,
      },
      {
        title: "机会倾向分",
        dataIndex: "opportunity_score",
        width: 96,
        align: "right",
        render: (v: number | null | undefined) =>
          v !== null && v !== undefined && !Number.isNaN(v) ? v.toFixed(2) : EMPTY,
      },
      {
        title: "AI 分析日",
        dataIndex: "ai_analysis_date",
        width: 110,
        render: (v: string | null | undefined) => (v ? String(v) : EMPTY),
      },
      {
        title: "数据覆盖",
        dataIndex: "coverage_ok",
        width: 88,
        render: (v: boolean | undefined) => (v === false ? "不足" : "足"),
      },
      {
        title: "数据时间",
        key: "dates",
        width: 200,
        ellipsis: true,
        render: (_: unknown, row) => {
          const parts = [
            row.trade_cal_date ? `市值日 ${row.trade_cal_date}` : "",
            row.financials_end_date ? `财报期 ${row.financials_end_date}` : "",
            row.data_source ? `源 ${row.data_source}` : "",
          ].filter(Boolean);
          return parts.length ? parts.join("；") : EMPTY;
        },
      },
      {
        title: "分数解释",
        dataIndex: "score_explanation_zh",
        ellipsis: true,
        render: (v: string) => (v ? v : EMPTY),
      },
    ],
    [runId],
  );

  const pagination: TablePaginationConfig = {
    current: page,
    pageSize,
    total: data?.total ?? 0,
    showSizeChanger: true,
    pageSizeOptions: [10, 20, 50, 100],
    onChange: (p, ps) => {
      setPage(p);
      setPageSize(ps || 20);
    },
  };

  const selectedRun = runs.find((r) => r.id === runId);

  const iqDecisionSelectOptions = useMemo(() => {
    if (iqDecisionOptions.length > 0) {
      return iqDecisionOptions;
    }
    return IQ_DECISION_FILTER_FALLBACK;
  }, [iqDecisionOptions]);

  return (
    <div>
      <Typography.Title level={3} className="vs-page-heading">
        双视角筛选结果
      </Typography.Title>
      <Typography.Paragraph type="secondary" className="vs-page-lead">
        选择 Run 与筛选条件；分页、排序均由服务端计算并返回。
      </Typography.Paragraph>
      <Card className="vs-surface-card">
        <div className="vs-toolbar-block">
          <div className="vs-toolbar-title">批量拉取</div>
          <Typography.Paragraph type="secondary" style={{ marginBottom: 14, maxWidth: 560 }}>
            TuShare 优先，失败整批切换 AkShare。提交后为异步任务（HTTP 202），完成后在此选择对应 Run。
          </Typography.Paragraph>
          <Space wrap size="middle" align="center">
            <InputNumber
              min={1}
              max={10000}
              placeholder="上限（空=全市场）"
              style={{ width: 168 }}
              value={batchMax ?? undefined}
              onChange={(v) => setBatchMax(v === null || v === undefined ? null : v)}
            />
            <Button type="primary" size="large" loading={batchLoading} onClick={() => void runBatchScreen()}>
              一键拉数并入库
            </Button>
          </Space>
        </div>
        <div className="vs-filter-stack">
          <div className="vs-filter-row">
            <div className="vs-filter-label">当前 Run</div>
            <div className="vs-filter-controls">
              <Select
                style={{ minWidth: 360, maxWidth: "100%", flex: "1 1 360px" }}
                value={runId ?? undefined}
                placeholder="请选择 screening_run"
                options={runs.map((r) => ({
                  value: r.id,
                  label: formatRunLabel(r),
                }))}
                onChange={(v) => {
                  setRunId(v);
                  setPage(1);
                  setSelectedIndustries([]);
                  setSelectedIqDecisions([]);
                  setCompanyNameQ("");
                  setMarketCapMinYi(null);
                  setMarketCapMaxYi(null);
                  setDivYieldMin(null);
                  setDivYieldMax(null);
                }}
              />
            </div>
          </div>
          <div className="vs-filter-row">
            <div className="vs-filter-label">行业</div>
            <div className="vs-filter-controls">
              <Select
                mode="multiple"
                allowClear
                placeholder="不筛选"
                style={{ minWidth: 260, maxWidth: "100%", flex: "1 1 260px" }}
                value={selectedIndustries}
                options={industryOptions}
                maxTagCount="responsive"
                onChange={(v) => {
                  setSelectedIndustries(v);
                  setPage(1);
                }}
              />
            </div>
          </div>
          <div className="vs-filter-row">
            <div className="vs-filter-label">公司名称</div>
            <div className="vs-filter-controls">
              <Input
                allowClear
                placeholder="简称、全称或代码，模糊匹配"
                style={{ minWidth: 220, maxWidth: "100%", flex: "1 1 220px" }}
                value={companyNameQ}
                maxLength={128}
                onChange={(e) => {
                  setCompanyNameQ(e.target.value);
                  setPage(1);
                }}
              />
            </div>
          </div>
          <div className="vs-filter-row">
            <div className="vs-filter-label">市值 / 股息</div>
            <div className="vs-filter-controls">
              <InputNumber
                min={0}
                placeholder="市值下限(亿)"
                style={{ width: 132 }}
                value={marketCapMinYi ?? undefined}
                onChange={(v) => {
                  setMarketCapMinYi(v === null || v === undefined ? null : v);
                  setPage(1);
                }}
              />
              <InputNumber
                min={0}
                placeholder="市值上限(亿)"
                style={{ width: 132 }}
                value={marketCapMaxYi ?? undefined}
                onChange={(v) => {
                  setMarketCapMaxYi(v === null || v === undefined ? null : v);
                  setPage(1);
                }}
              />
              <InputNumber
                min={0}
                max={100}
                placeholder="股息率下限(%)"
                style={{ width: 148 }}
                value={divYieldMin ?? undefined}
                onChange={(v) => {
                  setDivYieldMin(v === null || v === undefined ? null : v);
                  setPage(1);
                }}
              />
              <InputNumber
                min={0}
                max={100}
                placeholder="股息率上限(%)"
                style={{ width: 148 }}
                value={divYieldMax ?? undefined}
                onChange={(v) => {
                  setDivYieldMax(v === null || v === undefined ? null : v);
                  setPage(1);
                }}
              />
            </div>
          </div>
          <div className="vs-filter-row">
            <div className="vs-filter-label">AI · 价值 · 排序</div>
            <div className="vs-filter-controls" style={{ flexWrap: "wrap", gap: 8 }}>
              <Checkbox
                checked={hasAiOnly}
                onChange={(e) => {
                  setHasAiOnly(e.target.checked);
                  setPage(1);
                }}
              >
                仅已有 AI 分析
              </Checkbox>
              <InputNumber
                min={0}
                max={100}
                placeholder="最低 AI 分"
                style={{ width: 128 }}
                value={aiScoreMin ?? undefined}
                onChange={(v) => {
                  setAiScoreMin(v === null || v === undefined ? null : v);
                  setPage(1);
                }}
              />
              <Select
                mode="multiple"
                allowClear
                placeholder="价值判断（可买/跟踪/谨慎/排除）"
                style={{ minWidth: 220, maxWidth: "100%", flex: "1 1 220px" }}
                value={selectedIqDecisions}
                options={iqDecisionSelectOptions}
                maxTagCount="responsive"
                onChange={(v) => {
                  setSelectedIqDecisions(v);
                  setPage(1);
                }}
              />
              <Select
                style={{ minWidth: 240, maxWidth: "100%", flex: "1 1 240px" }}
                value={sortCombo}
                options={SORT_OPTIONS}
                onChange={(v) => {
                  setSortCombo(v);
                  setPage(1);
                }}
              />
            </div>
          </div>
        </div>
        {selectedRun?.status === "running" && (
          <div style={{ marginBottom: 20 }}>
            <Typography.Paragraph type="warning" style={{ marginBottom: 8 }}>
              后台任务进行中：已算分标的会按批写入数据库并出现在下方表格；进度条含「拉数」阶段时，行数可能暂时少于已处理只数。列表每 2 秒自动刷新。
            </Typography.Paragraph>
            {selectedRun.progress_total != null && selectedRun.progress_total > 0 ? (
              <Progress
                percent={Math.min(100, Math.max(0, selectedRun.progress_percent ?? 0))}
                status="active"
                format={(pct) =>
                  `${pct}% · ${selectedRun.progress_phase ?? "-"} · ${selectedRun.progress_current ?? 0}/${
                    selectedRun.progress_total
                  }${selectedRun.progress_symbol ? ` ${selectedRun.progress_symbol}` : ""}`
                }
              />
            ) : (
              <Typography.Text type="secondary">正在准备拉数，请稍候…</Typography.Text>
            )}
          </div>
        )}
        <Table<PagedItem>
          className="vs-data-table"
          bordered={false}
          rowKey="symbol"
          loading={loading}
          columns={columns}
          dataSource={data?.items ?? []}
          pagination={pagination}
          scroll={{ x: 1680 }}
        />
      </Card>
    </div>
  );
}
