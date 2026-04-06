import { lazy, Suspense, useEffect, useMemo, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { Alert, Button, Card, Descriptions, Space, Spin, Table, Typography, message } from "antd";
import type { ColumnsType } from "antd/es/table";
import { AiAnalysisSections } from "./components/AiAnalysisSections";
import { DcfSnapshotCard } from "./components/DcfSnapshotCard";

const IncomeComboChart = lazy(async () => {
  const m = await import("./IncomeComboChart");
  return { default: m.IncomeComboChart };
});

type LiveQuoteBlock = {
  ok: boolean;
  fetched_at: string;
  error: string | null;
  data: Record<string, unknown> | null;
};

type CompanyAiAnalysisMeta = {
  context_hash: string;
  prompt_version: string;
  model: string;
  generated_at: string;
  cached?: boolean;
  analysis_date?: string;
};

type CompanyAiAnalysisResponse = {
  summary: string;
  key_metrics_commentary: string;
  risks: string;
  alignment_with_scores: string;
  narrative_markdown: string;
  ai_score: number;
  ai_score_rationale?: string;
  opportunity_score: number;
  opportunity_score_rationale?: string;
  dcf_snapshot?: CompanyDcfBlock | null;
  meta: CompanyAiAnalysisMeta;
};

type PersistedAiAnalysisBlock = {
  ts_code: string;
  analysis_date: string;
  run_id: number | null;
  ai_score: number;
  ai_score_rationale: string | null;
  opportunity_score?: number | null;
  opportunity_score_rationale?: string | null;
  summary: string;
  key_metrics_commentary: string;
  risks: string;
  alignment_with_scores: string;
  narrative_markdown: string;
  dcf_snapshot: CompanyDcfBlock | null;
  dcf_ok: boolean | null;
  dcf_headline: string | null;
  context_hash: string;
  prompt_version: string;
  model: string;
  generated_at: string | null;
};

type CompanyDcfBlock = {
  ok: boolean;
  skip_reason?: string | null;
  message?: string | null;
  warnings?: string[];
  notes?: string[];
  assumptions?: Record<string, unknown> | null;
  values?: Record<string, unknown> | null;
};

type CompanyDetailResponse = {
  run_id: number;
  ts_code: string;
  run: {
    id: number;
    status: string;
    created_at: string | null;
    finished_at: string | null;
  };
  run_snapshot: Record<string, unknown>;
  reference: Record<string, unknown> | null;
  financials: {
    income: Record<string, unknown>[];
    balance: Record<string, unknown>[];
    cashflow: Record<string, unknown>[];
  };
  live_quote: LiveQuoteBlock;
  dcf?: CompanyDcfBlock | null;
  persisted_ai_analysis?: PersistedAiAnalysisBlock | null;
};

const EMPTY = "—";

function fmtNum(v: unknown): string {
  if (v === null || v === undefined) {
    return EMPTY;
  }
  if (typeof v === "number" && !Number.isNaN(v)) {
    return Number.isInteger(v) ? String(v) : v.toFixed(4);
  }
  return String(v);
}

export default function CompanyDetailPage() {
  const { runId, tsCode } = useParams<{ runId: string; tsCode: string }>();
  const navigate = useNavigate();
  const [loading, setLoading] = useState(true);
  const [detail, setDetail] = useState<CompanyDetailResponse | null>(null);
  const [aiLoading, setAiLoading] = useState(false);
  const [aiError, setAiError] = useState<string | null>(null);
  const [aiResult, setAiResult] = useState<CompanyAiAnalysisResponse | null>(null);
  const [dcfLoading, setDcfLoading] = useState(false);

  useEffect(() => {
    if (!runId || !tsCode) {
      return;
    }
    const rid = Number(runId);
    if (!Number.isFinite(rid)) {
      message.error("无效的 run id");
      return;
    }
    const code = decodeURIComponent(tsCode);
    let cancelled = false;
    setLoading(true);
    void (async () => {
      try {
        const q = new URLSearchParams({ include_persisted_ai: "true" });
        const res = await fetch(
          `/api/v1/runs/${rid}/companies/${encodeURIComponent(code)}/detail?${q.toString()}`,
        );
        if (res.status === 404) {
          const t = await res.text();
          message.error(t || "未找到");
          if (!cancelled) {
            setDetail(null);
          }
          return;
        }
        if (!res.ok) {
          throw new Error(await res.text());
        }
        const body = (await res.json()) as CompanyDetailResponse;
        if (!cancelled) {
          setDetail(body);
        }
      } catch (e) {
        message.error(`加载详情失败：${e}`);
        if (!cancelled) {
          setDetail(null);
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [runId, tsCode]);

  const displayAi = useMemo(() => {
    if (aiResult) {
      return {
        kind: "fresh" as const,
        data: {
          summary: aiResult.summary,
          key_metrics_commentary: aiResult.key_metrics_commentary,
          risks: aiResult.risks,
          alignment_with_scores: aiResult.alignment_with_scores,
          narrative_markdown: aiResult.narrative_markdown,
          ai_score: aiResult.ai_score,
          ai_score_rationale: aiResult.ai_score_rationale,
          opportunity_score: aiResult.opportunity_score,
          opportunity_score_rationale: aiResult.opportunity_score_rationale,
          meta: aiResult.meta,
        },
        dcf: aiResult.dcf_snapshot ?? null,
      };
    }
    const p = detail?.persisted_ai_analysis;
    if (p) {
      return {
        kind: "persisted" as const,
        data: {
          summary: p.summary,
          key_metrics_commentary: p.key_metrics_commentary,
          risks: p.risks,
          alignment_with_scores: p.alignment_with_scores,
          narrative_markdown: p.narrative_markdown,
          ai_score: p.ai_score,
          ai_score_rationale: p.ai_score_rationale ?? undefined,
          opportunity_score: p.opportunity_score,
          opportunity_score_rationale: p.opportunity_score_rationale ?? undefined,
          meta: {
            analysis_date: p.analysis_date,
            context_hash: p.context_hash,
            prompt_version: p.prompt_version,
            model: p.model,
            generated_at: p.generated_at ?? undefined,
          },
        },
        dcf: p.dcf_snapshot,
      };
    }
    return null;
  }, [aiResult, detail?.persisted_ai_analysis]);

  const loadDcfValuation = async () => {
    if (!runId || !tsCode) {
      return;
    }
    const rid = Number(runId);
    if (!Number.isFinite(rid)) {
      message.error("无效的 run id");
      return;
    }
    const code = decodeURIComponent(tsCode);
    setDcfLoading(true);
    try {
      const params = new URLSearchParams({ include_dcf: "1" });
      const res = await fetch(
        `/api/v1/runs/${rid}/companies/${encodeURIComponent(code)}/detail?${params.toString()}`,
      );
      if (!res.ok) {
        throw new Error(await res.text());
      }
      const body = (await res.json()) as CompanyDetailResponse;
      setDetail((prev) => (prev ? { ...prev, dcf: body.dcf ?? null } : prev));
    } catch (e) {
      message.error(`加载 DCF 失败：${e}`);
    } finally {
      setDcfLoading(false);
    }
  };

  const runAiAnalysis = async () => {
    if (!runId || !tsCode) {
      return;
    }
    const rid = Number(runId);
    if (!Number.isFinite(rid)) {
      message.error("无效的 run id");
      return;
    }
    const code = decodeURIComponent(tsCode);
    setAiLoading(true);
    setAiError(null);
    try {
      const res = await fetch(
        `/api/v1/runs/${rid}/companies/${encodeURIComponent(code)}/ai-analysis`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: "{}",
        },
      );
      if (res.status === 404) {
        const t = await res.json().catch(() => ({}));
        const d = (t as { detail?: string }).detail;
        setAiError(typeof d === "string" ? d : "未找到");
        return;
      }
      if (res.status === 503) {
        const t = await res.json().catch(() => ({}));
        const d = (t as { detail?: string }).detail;
        setAiError(typeof d === "string" ? d : "AI 功能未启用或未配置");
        return;
      }
      if (res.status === 502) {
        const t = await res.json().catch(() => ({}));
        const d = (t as { detail?: string }).detail;
        setAiError(typeof d === "string" ? d : "上游模型调用失败");
        return;
      }
      if (res.status === 504) {
        const t = await res.json().catch(() => ({}));
        const d = (t as { detail?: string }).detail;
        setAiError(typeof d === "string" ? d : "模型响应超时，请稍后重试或调大服务端超时配置");
        return;
      }
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(txt || `HTTP ${res.status}`);
      }
      const body = (await res.json()) as CompanyAiAnalysisResponse;
      setAiResult(body);
    } catch (e) {
      setAiError(`请求失败：${e}`);
    } finally {
      setAiLoading(false);
    }
  };

  const snap = detail?.run_snapshot;
  const incomeCols: ColumnsType<Record<string, unknown>> = [
    { title: "报告期", dataIndex: "end_date", width: 100 },
    { title: "营收", dataIndex: "total_revenue", render: fmtNum },
    { title: "归母净利", dataIndex: "n_income_attr_p", render: fmtNum },
    { title: "基本 EPS", dataIndex: "basic_eps", render: fmtNum },
  ];
  const balanceCols: ColumnsType<Record<string, unknown>> = [
    { title: "报告期", dataIndex: "end_date", width: 100 },
    { title: "总资产", dataIndex: "total_assets", render: fmtNum },
    { title: "总负债", dataIndex: "total_liab", render: fmtNum },
    { title: "股东权益", dataIndex: "total_hldr_eqy_exc_min_int", render: fmtNum },
  ];
  const cashCols: ColumnsType<Record<string, unknown>> = [
    { title: "报告期", dataIndex: "end_date", width: 100 },
    { title: "经营现金流", dataIndex: "n_cashflow_act", render: fmtNum },
    { title: "投资现金流", dataIndex: "n_cash_flows_inv_act", render: fmtNum },
    { title: "筹资现金流", dataIndex: "n_cash_flows_fnc_act", render: fmtNum },
  ];

  return (
    <div>
      <Space style={{ marginBottom: 16 }}>
        <Button type="link" onClick={() => navigate(-1)} style={{ padding: 0 }}>
          ← 返回列表
        </Button>
      </Space>
      <Typography.Title level={3} className="vs-page-heading">
        公司详情 {detail?.ts_code ? `· ${detail.ts_code}` : ""}
      </Typography.Title>
      <Typography.Paragraph type="secondary" className="vs-page-lead">
        本页聚合该次 Run 的冻结筛分快照、行情与财报摘要；AI 与 DCF 需手动触发。
      </Typography.Paragraph>

      <Card className="vs-surface-card" loading={loading} style={{ marginBottom: 16 }}>
        {!detail ? null : (
          <>
            <Typography.Title level={5}>筛选快照（Run #{detail.run.id}）</Typography.Title>
            <Typography.Paragraph type="secondary" style={{ marginBottom: 12 }}>
              以下分数与 provenance 均来自该次批跑结果，完成时间：{detail.run.finished_at ?? detail.run.created_at ?? EMPTY}
              ；与下方「行情」拉取时刻无关。
            </Typography.Paragraph>
            <Descriptions bordered size="small" column={2}>
              <Descriptions.Item label="代码">{String(snap?.symbol ?? EMPTY)}</Descriptions.Item>
              <Descriptions.Item label="名称">{String(snap?.display_name ?? EMPTY)}</Descriptions.Item>
              <Descriptions.Item label="巴菲特分">{fmtNum(snap?.buffett_score)}</Descriptions.Item>
              <Descriptions.Item label="格雷厄姆分">{fmtNum(snap?.graham_score)}</Descriptions.Item>
              <Descriptions.Item label="综合分（双维）">{fmtNum(snap?.combined_score)}</Descriptions.Item>
              <Descriptions.Item label="第三套分">{fmtNum(snap?.third_lens_score)}</Descriptions.Item>
              <Descriptions.Item label="三元综合">{fmtNum(snap?.final_triple_score)}</Descriptions.Item>
              <Descriptions.Item label="数据覆盖">{snap?.coverage_ok === false ? "不足" : "足"}</Descriptions.Item>
              <Descriptions.Item label="数据时间" span={2}>
                {[
                  snap?.trade_cal_date ? `市值日 ${String(snap.trade_cal_date)}` : "",
                  snap?.financials_end_date ? `财报期 ${String(snap.financials_end_date)}` : "",
                  snap?.data_source ? `源 ${String(snap.data_source)}` : "",
                ]
                  .filter(Boolean)
                  .join("；") || EMPTY}
              </Descriptions.Item>
              <Descriptions.Item label="分数解释" span={2}>
                {String(snap?.score_explanation_zh ?? EMPTY)}
              </Descriptions.Item>
            </Descriptions>

            <Typography.Title level={5} style={{ marginTop: 24 }}>
              AI 分析（手动触发）
            </Typography.Title>
            <Alert
              type="warning"
              showIcon
              style={{ marginBottom: 12 }}
              message="免责声明"
              description="以下由大模型根据本页已展示的结构化数据生成，仅供信息参考，不构成投资建议。模型可能遗漏或误解信息，请勿作为唯一决策依据。"
            />
            <Space wrap style={{ marginBottom: 12 }}>
              <Button type="primary" loading={aiLoading} onClick={() => void runAiAnalysis()}>
                生成 AI 分析
              </Button>
              {aiResult?.meta?.cached ? (
                <Typography.Text type="secondary">本次结果来自缓存</Typography.Text>
              ) : null}
            </Space>
            {aiError ? (
              <Typography.Paragraph type="danger" style={{ marginBottom: 12 }}>
                {aiError}
              </Typography.Paragraph>
            ) : null}
            {displayAi?.kind === "persisted" ? (
              <Alert
                type="info"
                showIcon
                style={{ marginBottom: 16 }}
                message="已展示最新落库分析"
                description="以下为数据库中该代码最近一次成功落库的结构化结果（含分析时点 DCF 快照）。点击「生成 AI 分析」可刷新当日记录。"
              />
            ) : null}
            {displayAi ? (
              <div style={{ marginBottom: 8 }}>
                <AiAnalysisSections
                  data={displayAi.data}
                  dcfExtra={<DcfSnapshotCard dcf={displayAi.dcf} />}
                />
              </div>
            ) : null}

            <Typography.Title level={5} style={{ marginTop: 24 }}>
              简化 DCF 估值（手动触发）
            </Typography.Title>
            <Alert
              type="warning"
              showIcon
              style={{ marginBottom: 12 }}
              message="免责声明"
              description="以下为基于财报摘要与固定假设的机械化折现估算，投资活动现金流仅作资本开支粗代理；结果高度依赖参数与数据质量，仅供学习参考，不构成投资建议。"
            />
            <Space wrap style={{ marginBottom: 12 }}>
              <Button loading={dcfLoading} onClick={() => void loadDcfValuation()}>
                加载 DCF 估值
              </Button>
            </Space>
            {detail.dcf ? (
              detail.dcf.ok && detail.dcf.values && detail.dcf.assumptions ? (
                <div style={{ marginBottom: 16 }}>
                  <Descriptions bordered size="small" column={2} title="假设与结果摘要">
                    <Descriptions.Item label="WACC">
                      {fmtNum(detail.dcf.assumptions.wacc)}
                    </Descriptions.Item>
                    <Descriptions.Item label="预测期增长率 g">
                      {fmtNum(detail.dcf.assumptions.stage1_growth)}
                    </Descriptions.Item>
                    <Descriptions.Item label="永续增长率 g_terminal">
                      {fmtNum(detail.dcf.assumptions.terminal_growth)}
                    </Descriptions.Item>
                    <Descriptions.Item label="预测年数">
                      {String(detail.dcf.assumptions.forecast_years ?? EMPTY)}
                    </Descriptions.Item>
                    <Descriptions.Item label="基期折现基数（代理）" span={2}>
                      {fmtNum(detail.dcf.assumptions.base_fcf)}
                    </Descriptions.Item>
                    {detail.dcf.assumptions.financial_reported_n_income != null &&
                    detail.dcf.assumptions.financial_reported_n_income !== undefined ? (
                      <>
                        <Descriptions.Item label="年报归母净利（折现前）" span={2}>
                          {fmtNum(detail.dcf.assumptions.financial_reported_n_income)}
                        </Descriptions.Item>
                        <Descriptions.Item label="金融业折现基数系数" span={2}>
                          {fmtNum(detail.dcf.assumptions.financial_ni_base_scale)}
                        </Descriptions.Item>
                      </>
                    ) : null}
                    <Descriptions.Item label="每股内在价值（元）" span={2}>
                      {fmtNum(detail.dcf.values.value_per_share)}
                    </Descriptions.Item>
                    <Descriptions.Item label="企业价值 EV" span={2}>
                      {fmtNum(detail.dcf.values.enterprise_value)}
                    </Descriptions.Item>
                    {detail.dcf.assumptions.financial_equity_direct_bridge === true ? (
                      <Descriptions.Item label="表内有息净负债（参考，未参与扣减）" span={2}>
                        {fmtNum(detail.dcf.assumptions.balance_sheet_net_debt_proxy)}
                      </Descriptions.Item>
                    ) : null}
                  </Descriptions>
                  {(() => {
                    const closeRaw = detail.live_quote.ok
                      ? detail.live_quote.data?.close
                      : undefined;
                    const close =
                      typeof closeRaw === "number"
                        ? closeRaw
                        : typeof closeRaw === "string"
                          ? Number(closeRaw)
                          : NaN;
                    const intrinsic = Number(detail.dcf.values?.value_per_share);
                    if (
                      !Number.isFinite(close) ||
                      close <= 0 ||
                      !Number.isFinite(intrinsic) ||
                      intrinsic <= 0
                    ) {
                      return null;
                    }
                    const pct = ((intrinsic - close) / close) * 100;
                    return (
                      <Typography.Paragraph style={{ marginTop: 8 }}>
                        相对最新收盘价（{fmtNum(close)}）：内在价值偏离约{" "}
                        <Typography.Text strong>{pct.toFixed(2)}%</Typography.Text>
                        （正值表示估算高于现价，负值表示低于现价）
                      </Typography.Paragraph>
                    );
                  })()}
                  {detail.dcf.warnings && detail.dcf.warnings.length > 0 ? (
                    <Typography.Paragraph type="secondary" style={{ marginTop: 8 }}>
                      提示：{detail.dcf.warnings.join("；")}
                    </Typography.Paragraph>
                  ) : null}
                </div>
              ) : (
                <Typography.Paragraph type="secondary">
                  未能完成估值
                  {detail.dcf.skip_reason ? `（${detail.dcf.skip_reason}）` : ""}
                  {detail.dcf.message ? `：${detail.dcf.message}` : ""}
                </Typography.Paragraph>
              )
            ) : null}

            <Typography.Title level={5} style={{ marginTop: 24 }}>
              行情（TuShare daily，独立拉取）
            </Typography.Title>
            <Typography.Paragraph type="secondary">
              刷新时间：{detail.live_quote.fetched_at}
              {!detail.live_quote.ok && detail.live_quote.error
                ? ` · 失败：${detail.live_quote.error}`
                : ""}
            </Typography.Paragraph>
            {detail.live_quote.ok && detail.live_quote.data ? (
              <Descriptions bordered size="small" column={2}>
                {Object.entries(detail.live_quote.data).map(([k, v]) => (
                  <Descriptions.Item key={k} label={k}>
                    {fmtNum(v)}
                  </Descriptions.Item>
                ))}
              </Descriptions>
            ) : (
              <Typography.Text type="secondary">暂无行情数据</Typography.Text>
            )}

            <Typography.Title level={5} style={{ marginTop: 24 }}>
              公司主数据（security_reference）
            </Typography.Title>
            {!detail.reference || Object.keys(detail.reference).length === 0 ? (
              <Typography.Text type="secondary">无参考表数据，可执行 sync-reference</Typography.Text>
            ) : (
              <Descriptions bordered size="small" column={2}>
                {Object.entries(detail.reference).map(([k, v]) => (
                  <Descriptions.Item key={k} label={k}>
                    {v === null || v === undefined ? EMPTY : String(v)}
                  </Descriptions.Item>
                ))}
              </Descriptions>
            )}

            <Typography.Title level={5} style={{ marginTop: 24 }}>
              利润表（摘要）
            </Typography.Title>
            <Suspense
              fallback={
                <div style={{ marginBottom: 16 }}>
                  <Spin size="small" /> <Typography.Text type="secondary"> 加载图表…</Typography.Text>
                </div>
              }
            >
              <IncomeComboChart income={detail.financials.income} />
            </Suspense>
            <Table
              size="small"
              rowKey={(_, i) => `inc-${i}`}
              columns={incomeCols}
              dataSource={detail.financials.income}
              pagination={false}
              locale={{ emptyText: "暂无数据（可执行 sync-financial-statements）" }}
            />

            <Typography.Title level={5} style={{ marginTop: 24 }}>
              资产负债表（摘要）
            </Typography.Title>
            <Table
              size="small"
              rowKey={(_, i) => `bal-${i}`}
              columns={balanceCols}
              dataSource={detail.financials.balance}
              pagination={false}
              locale={{ emptyText: "暂无数据" }}
            />

            <Typography.Title level={5} style={{ marginTop: 24 }}>
              现金流量表（摘要）
            </Typography.Title>
            <Table
              size="small"
              rowKey={(_, i) => `cf-${i}`}
              columns={cashCols}
              dataSource={detail.financials.cashflow}
              pagination={false}
              locale={{ emptyText: "暂无数据" }}
            />
          </>
        )}
      </Card>
    </div>
  );
}
