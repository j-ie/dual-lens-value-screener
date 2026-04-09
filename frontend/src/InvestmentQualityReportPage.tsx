import { useCallback, useMemo, useRef, useState } from "react";
import { Alert, Button, Card, Input, Select, Space, Table, Tag, Typography, message } from "antd";
import type { ColumnsType } from "antd/es/table";

type IqRun = {
  id: number;
  status: string;
  created_at: string;
};

type IqResultItem = {
  symbol: string;
  display_name: string;
  industry: string;
  market_cap: number | null;
  investment_quality: {
    total_score: number;
    decision_label_zh: string;
    is_undervalued: boolean;
    module_scores: Record<string, number>;
    reasons: string[];
    risk_flags: Array<{ code: string; severity: number; message: string }>;
  };
};

type IqPaged = {
  items: IqResultItem[];
  total: number;
};

type AiSummary = {
  conclusion: string;
  valuation_view: string;
  position_advice: string;
  buy_trigger_zone: { ideal: string; acceptable: string };
  exit_conditions: string[];
  counter_arguments: string[];
  watch_items: string[];
  facts: string[];
  judgments: string[];
  confidence: "high" | "medium" | "low";
  disclaimer: string;
};

function confidenceTag(v: string) {
  if (v === "high") return <Tag color="success">高</Tag>;
  if (v === "low") return <Tag color="error">低</Tag>;
  return <Tag color="processing">中</Tag>;
}

export default function InvestmentQualityReportPage() {
  const [symbol, setSymbol] = useState("");
  const [industry, setIndustry] = useState<string | undefined>(undefined);
  const [runId, setRunId] = useState<number | undefined>(undefined);
  const [runs, setRuns] = useState<IqRun[]>([]);
  const [results, setResults] = useState<IqPaged | null>(null);
  const [selectedRow, setSelectedRow] = useState<IqResultItem | null>(null);
  const [singleSummary, setSingleSummary] = useState<AiSummary | null>(null);
  const [runSummary, setRunSummary] = useState<AiSummary | null>(null);
  const [loadingSingle, setLoadingSingle] = useState(false);
  const [loadingRuns, setLoadingRuns] = useState(false);
  const [loadingResults, setLoadingResults] = useState(false);
  const [loadingRunSummary, setLoadingRunSummary] = useState(false);
  const runListCacheRef = useRef<IqRun[] | null>(null);
  const runResultsCacheRef = useRef<Record<number, IqPaged>>({});
  const singleSummaryCacheRef = useRef<Record<string, AiSummary>>({});
  const runSummaryCacheRef = useRef<Record<string, AiSummary>>({});

  const loadRuns = useCallback(async () => {
    if (runListCacheRef.current) {
      setRuns(runListCacheRef.current);
      if (!runId && runListCacheRef.current.length > 0) {
        setRunId(runListCacheRef.current[0].id);
      }
      return;
    }
    setLoadingRuns(true);
    try {
      const res = await fetch("/api/v1/investment-quality/runs?limit=100");
      if (!res.ok) throw new Error(await res.text());
      const data = (await res.json()) as IqRun[];
      setRuns(data);
      runListCacheRef.current = data;
      if (!runId && data.length > 0) {
        setRunId(data[0].id);
      }
    } catch (e) {
      message.error(`加载任务失败：${e}`);
    } finally {
      setLoadingRuns(false);
    }
  }, [runId]);

  const loadResults = useCallback(async () => {
    if (!runId) return;
    const hit = runResultsCacheRef.current[runId];
    if (hit) {
      setResults(hit);
      setSelectedRow(hit.items[0] ?? null);
      return;
    }
    setLoadingResults(true);
    try {
      const res = await fetch(`/api/v1/investment-quality/runs/${runId}/results?page=1&page_size=50`);
      if (!res.ok) throw new Error(await res.text());
      const data = (await res.json()) as IqPaged;
      setResults(data);
      runResultsCacheRef.current[runId] = data;
      setSelectedRow(data.items[0] ?? null);
    } catch (e) {
      message.error(`加载批量报告失败：${e}`);
    } finally {
      setLoadingResults(false);
    }
  }, [runId]);

  const generateSingleSummary = useCallback(async () => {
    const code = symbol.trim();
    if (!code) {
      message.warning("请输入股票代码");
      return;
    }
    const singleKey = `${code}|${industry ?? ""}`;
    const singleHit = singleSummaryCacheRef.current[singleKey];
    if (singleHit) {
      setSingleSummary(singleHit);
      return;
    }
    setLoadingSingle(true);
    try {
      const res = await fetch("/api/v1/investment-quality/ai-summary", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ symbol: code, industry: industry ?? null }),
      });
      if (!res.ok) throw new Error(await res.text());
      const body = (await res.json()) as { ai_summary: AiSummary };
      setSingleSummary(body.ai_summary);
      singleSummaryCacheRef.current[singleKey] = body.ai_summary;
    } catch (e) {
      message.error(`生成单公司报告失败：${e}`);
    } finally {
      setLoadingSingle(false);
    }
  }, [symbol, industry]);

  const generateRunSummary = useCallback(async () => {
    if (!runId || !selectedRow) {
      message.warning("请先选择 run 与标的");
      return;
    }
    const runKey = `${runId}|${selectedRow.symbol}`;
    const runHit = runSummaryCacheRef.current[runKey];
    if (runHit) {
      setRunSummary(runHit);
      return;
    }
    setLoadingRunSummary(true);
    try {
      const res = await fetch("/api/v1/investment-quality/ai-summary", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ run_id: runId, symbol: selectedRow.symbol }),
      });
      if (!res.ok) throw new Error(await res.text());
      const body = (await res.json()) as { ai_summary: AiSummary };
      setRunSummary(body.ai_summary);
      runSummaryCacheRef.current[runKey] = body.ai_summary;
    } catch (e) {
      message.error(`生成批量报告总结失败：${e}`);
    } finally {
      setLoadingRunSummary(false);
    }
  }, [runId, selectedRow]);

  const columns: ColumnsType<IqResultItem> = useMemo(
    () => [
      { title: "代码", dataIndex: "symbol", width: 120 },
      { title: "名称", dataIndex: "display_name", width: 140, render: (v: string) => v || "—" },
      { title: "行业", dataIndex: "industry", width: 120, render: (v: string) => v || "—" },
      {
        title: "结论",
        key: "decision",
        width: 100,
        render: (_: unknown, r) => r.investment_quality.decision_label_zh,
      },
      {
        title: "总分",
        key: "score",
        width: 80,
        render: (_: unknown, r) => r.investment_quality.total_score,
      },
      {
        title: "低估",
        key: "undervalued",
        width: 80,
        render: (_: unknown, r) => (r.investment_quality.is_undervalued ? "是" : "否"),
      },
    ],
    [],
  );

  const renderSummaryCard = (title: string, summary: AiSummary | null) => (
    <Card className="vs-surface-card" title={title}>
      {!summary ? (
        <Typography.Text type="secondary">暂无总结，请先生成。</Typography.Text>
      ) : (
        <Space direction="vertical" style={{ width: "100%" }}>
          <Alert
            type="info"
            message={summary.conclusion}
            description={
              <div>
                置信度：{confidenceTag(summary.confidence)}，仓位建议：{summary.position_advice}
              </div>
            }
          />
          <Typography.Text>
            <b>估值判断：</b>
            {summary.valuation_view}
          </Typography.Text>
          <Typography.Text>
            <b>买入触发：</b>理想 {summary.buy_trigger_zone.ideal}；可接受 {summary.buy_trigger_zone.acceptable}
          </Typography.Text>
          <Typography.Text>
            <b>退出条件：</b>
            {summary.exit_conditions.join("；")}
          </Typography.Text>
          <Typography.Text>
            <b>反方观点：</b>
            {summary.counter_arguments.join("；")}
          </Typography.Text>
          <Typography.Text>
            <b>事实依据：</b>
            {summary.facts.join("；")}
          </Typography.Text>
          <Typography.Text>
            <b>判断结论：</b>
            {summary.judgments.join("；")}
          </Typography.Text>
          <Typography.Text type="secondary">{summary.disclaimer}</Typography.Text>
        </Space>
      )}
    </Card>
  );

  return (
    <div>
      <Typography.Title level={3} className="vs-page-heading">
        投资质量报告
      </Typography.Title>
      <Card className="vs-surface-card" style={{ marginBottom: 16 }} title="单公司报告">
        <Space wrap>
          <Input
            style={{ width: 220 }}
            placeholder="输入代码，如 600519.SH"
            value={symbol}
            onChange={(e) => setSymbol(e.target.value)}
          />
          <Select
            allowClear
            placeholder="行业（可选）"
            value={industry}
            onChange={setIndustry}
            style={{ width: 180 }}
            options={[
              { label: "一般工商业", value: "一般工商业" },
              { label: "银行", value: "银行" },
              { label: "保险", value: "保险" },
              { label: "全国地产", value: "全国地产" },
            ]}
          />
          <Button loading={loadingSingle} type="primary" onClick={() => void generateSingleSummary()}>
            生成单公司报告
          </Button>
        </Space>
      </Card>
      {renderSummaryCard("单公司 AI 总结（价值投资风格）", singleSummary)}

      <Card className="vs-surface-card" style={{ marginTop: 16 }} title="批量任务报告">
        <Space wrap style={{ marginBottom: 12 }}>
          <Button loading={loadingRuns} onClick={() => void loadRuns()}>
            刷新任务
          </Button>
          <Select
            style={{ width: 360 }}
            placeholder="选择 run"
            value={runId}
            options={runs.map((x) => ({ value: x.id, label: `#${x.id} ${x.status} ${x.created_at}` }))}
            onChange={(v) => setRunId(v)}
          />
          <Button loading={loadingResults} onClick={() => void loadResults()}>
            加载报告列表
          </Button>
          <Button type="primary" loading={loadingRunSummary} onClick={() => void generateRunSummary()}>
            生成选中标的总结
          </Button>
        </Space>
        <Table<IqResultItem>
          rowKey="symbol"
          dataSource={results?.items ?? []}
          loading={loadingResults}
          columns={columns}
          pagination={{ pageSize: 10 }}
          onRow={(record) => ({
            onClick: () => setSelectedRow(record),
          })}
          rowClassName={(record) => (selectedRow?.symbol === record.symbol ? "ant-table-row-selected" : "")}
        />
      </Card>

      <div style={{ marginTop: 16 }}>
        {renderSummaryCard(
          `批量任务 AI 总结（${selectedRow ? selectedRow.symbol : "请先选择标的"}）`,
          runSummary,
        )}
      </div>
    </div>
  );
}
