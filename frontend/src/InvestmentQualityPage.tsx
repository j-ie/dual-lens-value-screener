import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Button, Card, Input, InputNumber, Select, Space, Table, Tag, Typography, message } from "antd";
import type { ColumnsType } from "antd/es/table";

type SingleIq = {
  symbol: string;
  investment_quality: {
    total_score: number;
    decision: string;
    decision_label_zh: string;
    is_undervalued: boolean;
    module_scores: Record<string, number>;
    reasons: string[];
    risk_flags: Array<{ code: string; severity: number; message: string }>;
  };
};

type IqRun = {
  id: number;
  status: string;
  created_at: string;
  finished_at: string | null;
  progress_percent?: number | null;
};

type IqResultItem = {
  symbol: string;
  display_name: string;
  industry: string;
  market_cap: number | null;
  investment_quality: {
    total_score: number;
    decision: string;
    decision_label_zh: string;
    is_undervalued: boolean;
    reasons: string[];
    risk_flags: Array<{ code: string; severity: number; message: string }>;
  };
};

type IqPaged = {
  items: IqResultItem[];
  total: number;
};

function statusTag(status: string) {
  if (status === "running") return <Tag color="processing">进行中</Tag>;
  if (status === "success") return <Tag color="success">成功</Tag>;
  if (status === "failed") return <Tag color="error">失败</Tag>;
  return <Tag>{status}</Tag>;
}

export default function InvestmentQualityPage() {
  const [symbol, setSymbol] = useState("");
  const [industry, setIndustry] = useState<string | undefined>(undefined);
  const [single, setSingle] = useState<SingleIq | null>(null);
  const [singleLoading, setSingleLoading] = useState(false);
  const [batchMax, setBatchMax] = useState<number | null>(null);
  const [batchLoading, setBatchLoading] = useState(false);
  const [runs, setRuns] = useState<IqRun[]>([]);
  const [runId, setRunId] = useState<number | null>(null);
  const [results, setResults] = useState<IqPaged | null>(null);
  const [resultsLoading, setResultsLoading] = useState(false);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const loadRuns = useCallback(async () => {
    const res = await fetch("/api/v1/investment-quality/runs?limit=100");
    if (!res.ok) throw new Error(await res.text());
    const data = (await res.json()) as IqRun[];
    setRuns(data);
    setRunId((prev) => (prev === null && data.length > 0 ? data[0].id : prev));
  }, []);

  const loadResults = useCallback(async () => {
    if (runId === null) return;
    setResultsLoading(true);
    try {
      const res = await fetch(`/api/v1/investment-quality/runs/${runId}/results?page=1&page_size=50`);
      if (!res.ok) throw new Error(await res.text());
      setResults((await res.json()) as IqPaged);
    } catch (e) {
      message.error(`加载结果失败：${e}`);
    } finally {
      setResultsLoading(false);
    }
  }, [runId]);

  useEffect(() => {
    void loadRuns();
  }, [loadRuns]);

  useEffect(() => {
    void loadResults();
  }, [loadResults]);

  useEffect(() => {
    if (pollRef.current) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
    if (!runs.some((x) => x.status === "running")) return;
    pollRef.current = setInterval(() => {
      void loadRuns();
      void loadResults();
    }, 2500);
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, [runs, loadRuns, loadResults]);

  const runSingle = useCallback(async () => {
    const code = symbol.trim();
    if (!code) {
      message.warning("请先输入股票代码");
      return;
    }
    setSingleLoading(true);
    try {
      const body = {
        symbol: code,
        industry: industry ?? null,
      };
      const res = await fetch("/api/v1/investment-quality/single", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!res.ok) throw new Error(await res.text());
      setSingle((await res.json()) as SingleIq);
    } catch (e) {
      message.error(`单公司计算失败：${e}`);
    } finally {
      setSingleLoading(false);
    }
  }, [symbol, industry]);

  const triggerBatch = useCallback(async () => {
    setBatchLoading(true);
    try {
      const res = await fetch("/api/v1/investment-quality/runs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ max_symbols: batchMax }),
      });
      if (!res.ok) throw new Error(await res.text());
      const data = (await res.json()) as { run_id: number };
      message.success(`已创建任务 #${data.run_id}`);
      await loadRuns();
      setRunId(data.run_id);
      await loadResults();
    } catch (e) {
      message.error(`创建任务失败：${e}`);
    } finally {
      setBatchLoading(false);
    }
  }, [batchMax, loadRuns, loadResults]);

  const columns: ColumnsType<IqResultItem> = useMemo(
    () => [
      { title: "代码", dataIndex: "symbol", width: 120 },
      { title: "名称", dataIndex: "display_name", width: 120, render: (v: string) => v || "—" },
      { title: "行业", dataIndex: "industry", width: 120, render: (v: string) => v || "—" },
      {
        title: "总分",
        key: "score",
        width: 80,
        align: "right",
        render: (_: unknown, r) => r.investment_quality.total_score,
      },
      {
        title: "结论",
        key: "decision",
        width: 100,
        render: (_: unknown, r) => r.investment_quality.decision_label_zh,
      },
      {
        title: "低估",
        key: "undervalued",
        width: 80,
        align: "center",
        render: (_: unknown, r) => (r.investment_quality.is_undervalued ? "是" : "否"),
      },
      {
        title: "风险提示",
        key: "risk",
        render: (_: unknown, r) =>
          r.investment_quality.risk_flags.length
            ? r.investment_quality.risk_flags.map((x) => x.message).join("；")
            : "—",
      },
    ],
    [],
  );

  return (
    <div>
      <Typography.Title level={3} className="vs-page-heading">
        投资质量分析
      </Typography.Title>
      <Card className="vs-surface-card" style={{ marginBottom: 16 }}>
        <Typography.Title level={5}>单公司计算</Typography.Title>
        <Space wrap>
          <Input
            placeholder="输入代码，如 600519.SH"
            value={symbol}
            onChange={(e) => setSymbol(e.target.value)}
            style={{ width: 220 }}
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
          <Button type="primary" loading={singleLoading} onClick={() => void runSingle()}>
            立即计算
          </Button>
        </Space>
        {single && (
          <div className="vs-reading" style={{ marginTop: 16 }}>
            <Typography.Text>结论：{single.investment_quality.decision_label_zh}</Typography.Text>
            <br />
            <Typography.Text>总分：{single.investment_quality.total_score}</Typography.Text>
            <br />
            <Typography.Text>是否低估：{single.investment_quality.is_undervalued ? "是" : "否"}</Typography.Text>
          </div>
        )}
      </Card>

      <Card className="vs-surface-card">
        <Typography.Title level={5}>批量任务计算</Typography.Title>
        <Space wrap style={{ marginBottom: 12 }}>
          <InputNumber
            min={1}
            max={10000}
            placeholder="上限（空=全市场）"
            value={batchMax ?? undefined}
            onChange={(v) => setBatchMax(v ?? null)}
          />
          <Button type="primary" loading={batchLoading} onClick={() => void triggerBatch()}>
            发起任务
          </Button>
          <Select
            style={{ width: 360 }}
            value={runId ?? undefined}
            options={runs.map((r) => ({
              value: r.id,
              label: `#${r.id} ${r.status} ${r.created_at}`,
            }))}
            onChange={setRunId}
          />
          {runId !== null && statusTag(runs.find((x) => x.id === runId)?.status ?? "")}
        </Space>
        <Table<IqResultItem>
          className="vs-data-table"
          bordered={false}
          rowKey="symbol"
          loading={resultsLoading}
          columns={columns}
          dataSource={results?.items ?? []}
          pagination={{ pageSize: 20, showSizeChanger: true }}
          scroll={{ x: 1100 }}
        />
      </Card>
    </div>
  );
}

