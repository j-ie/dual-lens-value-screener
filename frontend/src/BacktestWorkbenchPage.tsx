import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Line } from "@ant-design/plots";
import {
  Alert,
  Button,
  Card,
  Col,
  Descriptions,
  Form,
  Input,
  Popconfirm,
  Row,
  Select,
  Space,
  Statistic,
  Table,
  Tag,
  Tooltip,
  Typography,
  message,
} from "antd";
import type { ColumnsType } from "antd/es/table";

type BacktestListItem = {
  id: number;
  external_uuid: string;
  strategy_name: string;
  status: string;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  error?: string;
  config: {
    strategy_name?: string;
    start_date: string;
    end_date: string;
    rebalance_frequency: string;
    holding_period_days: number;
    top_n?: number | null;
    top_quantile?: number | null;
    benchmark?: string;
    transaction_cost_bps?: number;
    filters?: { symbols?: string[] };
  };
};

type BacktestDetail = {
  job: {
    id: number;
    status: string;
    config: {
      strategy_name: string;
      start_date: string;
      end_date: string;
      rebalance_frequency: string;
      holding_period_days: number;
      top_n?: number | null;
      top_quantile?: number | null;
      benchmark: string;
      transaction_cost_bps: number;
      filters?: { symbols?: string[] };
    };
    meta?: { error?: string };
  };
  result: null | {
    summary: Record<string, unknown>;
    metrics: Record<string, number>;
    curve: null | { points: Array<Record<string, unknown>> };
    diagnostics: null | Record<string, unknown>;
  };
};

type BacktestCoverage = {
  ok: boolean;
  start_date: string | null;
  end_date: string | null;
};

const STATUS_OPTIONS = [
  { label: "全部状态", value: "all" },
  { label: "待执行", value: "pending" },
  { label: "执行中", value: "running" },
  { label: "成功", value: "success" },
  { label: "失败", value: "failed" },
];

const TEMPLATE_OPTIONS = [
  { label: "自定义", value: "custom" },
  { label: "稳健（长持低换手）", value: "steady" },
  { label: "平衡（默认推荐）", value: "balanced" },
  { label: "进取（更高调仓）", value: "aggressive" },
];

const STRATEGY_OPTIONS = [
  { label: "investment_quality_score", value: "investment_quality_score" },
  { label: "investment_quality_buy_only", value: "investment_quality_buy_only" },
];

const REBALANCE_OPTIONS = [
  { label: "周度", value: "weekly" },
  { label: "月度", value: "monthly" },
  { label: "季度", value: "quarterly" },
];

const HOLDING_DAYS_OPTIONS = [5, 10, 20, 30, 60, 90, 120].map((x) => ({
  label: `${x} 天`,
  value: x,
}));

const PICK_MODE_OPTIONS = [
  { label: "Top-N 固定持仓", value: "top_n" },
  { label: "Top-Quantile 分位选股", value: "top_quantile" },
];

const TOP_N_OPTIONS = [10, 20, 30, 50, 100, 200].map((x) => ({ label: `Top ${x}`, value: x }));
const QUANTILE_OPTIONS = [0.1, 0.15, 0.2, 0.25, 0.3].map((x) => ({
  label: `前 ${(x * 100).toFixed(0)}%`,
  value: x,
}));
const COST_OPTIONS = [5, 10, 15, 20, 30, 50].map((x) => ({ label: `${x} bps`, value: x }));

function helpLabel(title: string, help: string) {
  return (
    <Space size={6}>
      <span>{title}</span>
      <Tooltip title={help}>
        <Tag bordered={false} color="blue" style={{ marginInlineEnd: 0, cursor: "help" }}>
          ?
        </Tag>
      </Tooltip>
    </Space>
  );
}

function statusTag(status: string) {
  if (status === "running") {
    return <Tag color="processing">执行中</Tag>;
  }
  if (status === "success") {
    return <Tag color="success">成功</Tag>;
  }
  if (status === "failed") {
    return <Tag color="error">失败</Tag>;
  }
  if (status === "pending") {
    return <Tag color="default">待执行</Tag>;
  }
  return <Tag>{status}</Tag>;
}

function toPercent(v: number | undefined): string {
  if (typeof v !== "number" || Number.isNaN(v)) {
    return "—";
  }
  return `${(v * 100).toFixed(2)}%`;
}

export default function BacktestWorkbenchPage() {
  const [form] = Form.useForm();
  const [statusFilter, setStatusFilter] = useState<string>("all");
  const [loading, setLoading] = useState<boolean>(false);
  const [submitting, setSubmitting] = useState<boolean>(false);
  const [executingId, setExecutingId] = useState<number | null>(null);
  const [jobs, setJobs] = useState<BacktestListItem[]>([]);
  const [selectedJobId, setSelectedJobId] = useState<number | null>(null);
  const [detail, setDetail] = useState<BacktestDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState<boolean>(false);
  const [coverage, setCoverage] = useState<BacktestCoverage | null>(null);
  const [coverageLoading, setCoverageLoading] = useState<boolean>(false);
  const [deletingId, setDeletingId] = useState<number | null>(null);
  const detailCardRef = useRef<HTMLDivElement | null>(null);
  const pickMode = Form.useWatch("pick_mode", form) as "top_n" | "top_quantile" | undefined;

  const loadJobs = useCallback(async () => {
    setLoading(true);
    try {
      const qs = statusFilter === "all" ? "" : `&status=${encodeURIComponent(statusFilter)}`;
      const res = await fetch(`/api/v1/backtests?limit=200${qs}`);
      if (!res.ok) {
        throw new Error(await res.text());
      }
      setJobs((await res.json()) as BacktestListItem[]);
    } catch (e) {
      message.error(`加载回测任务失败：${e}`);
    } finally {
      setLoading(false);
    }
  }, [statusFilter]);

  const loadCoverage = useCallback(async () => {
    setCoverageLoading(true);
    try {
      const res = await fetch("/api/v1/backtests/coverage");
      if (!res.ok) {
        throw new Error(await res.text());
      }
      setCoverage((await res.json()) as BacktestCoverage);
    } catch (e) {
      message.warning(`获取回测可用区间失败：${e}`);
      setCoverage(null);
    } finally {
      setCoverageLoading(false);
    }
  }, []);

  const loadDetail = useCallback(async (jobId: number) => {
    setDetailLoading(true);
    try {
      const res = await fetch(`/api/v1/backtests/${jobId}`);
      if (!res.ok) {
        throw new Error(await res.text());
      }
      setDetail((await res.json()) as BacktestDetail);
      setSelectedJobId(jobId);
    } catch (e) {
      message.error(`加载回测详情失败：${e}`);
    } finally {
      setDetailLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadJobs();
    void loadCoverage();
  }, [loadJobs, loadCoverage]);

  useEffect(() => {
    if (!detail?.job.id) {
      return;
    }
    const timer = window.setTimeout(() => {
      const el = detailCardRef.current;
      if (el && typeof el.scrollIntoView === "function") {
        el.scrollIntoView({ behavior: "smooth", block: "start" });
      }
    }, 0);
    return () => window.clearTimeout(timer);
  }, [detail?.job.id]);

  useEffect(() => {
    const hasRunning = jobs.some((x) => x.status === "running");
    if (!hasRunning) {
      return;
    }
    const timer = setInterval(() => {
      void loadJobs();
      if (selectedJobId) {
        void loadDetail(selectedJobId);
      }
    }, 3000);
    return () => clearInterval(timer);
  }, [jobs, loadJobs, loadDetail, selectedJobId]);

  const submitCreate = useCallback(async () => {
    try {
      const values = await form.validateFields();
      if (coverage?.start_date && coverage?.end_date) {
        const st = String(values.start_date ?? "");
        const ed = String(values.end_date ?? "");
        const cst = String(coverage.start_date).replace(/(\d{4})(\d{2})(\d{2})/, "$1-$2-$3");
        const ced = String(coverage.end_date).replace(/(\d{4})(\d{2})(\d{2})/, "$1-$2-$3");
        if (st < cst || ed > ced) {
          message.error(`回测日期超出可用区间：${cst} ~ ${ced}`);
          return;
        }
      }
      const rawSymbols = String(values.symbolsText ?? "").trim();
      const symbols = rawSymbols
        ? rawSymbols
            .split(/[\s,]+/)
            .map((x: string) => x.trim())
            .filter(Boolean)
        : [];
      setSubmitting(true);
      const payload = {
        strategy_name: values.strategy_name,
        start_date: values.start_date,
        end_date: values.end_date,
        rebalance_frequency: values.rebalance_frequency,
        holding_period_days: values.holding_period_days,
        top_n: values.pick_mode === "top_n" ? values.top_n ?? null : null,
        top_quantile: values.pick_mode === "top_quantile" ? values.top_quantile ?? null : null,
        benchmark: values.benchmark,
        transaction_cost_bps: values.transaction_cost_bps,
        filters: symbols.length > 0 ? { symbols } : {},
        run_sync: false,
      };
      const res = await fetch("/api/v1/backtests", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!res.ok) {
        throw new Error(await res.text());
      }
      const body = (await res.json()) as { job_id: number };
      message.success(`已创建回测任务 #${body.job_id}`);
      await loadJobs();
      await loadDetail(body.job_id);
    } catch (e) {
      if (e instanceof Error) {
        message.error(`创建任务失败：${e.message}`);
      }
    } finally {
      setSubmitting(false);
    }
  }, [coverage, form, loadDetail, loadJobs]);

  const applyTemplate = useCallback(
    (tpl: string) => {
      if (tpl === "steady") {
        form.setFieldsValue({
          rebalance_frequency: "quarterly",
          holding_period_days: 90,
          pick_mode: "top_quantile",
          top_n: undefined,
          top_quantile: 0.15,
          transaction_cost_bps: 10,
        });
        return;
      }
      if (tpl === "balanced") {
        form.setFieldsValue({
          rebalance_frequency: "monthly",
          holding_period_days: 30,
          pick_mode: "top_quantile",
          top_n: undefined,
          top_quantile: 0.2,
          transaction_cost_bps: 15,
        });
        return;
      }
      if (tpl === "aggressive") {
        form.setFieldsValue({
          rebalance_frequency: "weekly",
          holding_period_days: 10,
          pick_mode: "top_n",
          top_n: 30,
          top_quantile: undefined,
          transaction_cost_bps: 20,
        });
      }
    },
    [form],
  );

  const deleteJob = useCallback(
    async (jobId: number) => {
      setDeletingId(jobId);
      try {
        const res = await fetch(`/api/v1/backtests/${jobId}`, { method: "DELETE" });
        if (!res.ok) {
          throw new Error(await res.text());
        }
        message.success(`已删除任务 #${jobId}`);
        if (selectedJobId === jobId) {
          setSelectedJobId(null);
          setDetail(null);
        }
        await loadJobs();
      } catch (e) {
        message.error(`删除失败：${e}`);
      } finally {
        setDeletingId(null);
      }
    },
    [loadJobs, selectedJobId],
  );

  const executeJob = useCallback(
    async (jobId: number) => {
      setExecutingId(jobId);
      try {
        const res = await fetch(`/api/v1/backtests/${jobId}/execute`, { method: "POST" });
        if (!res.ok) {
          throw new Error(await res.text());
        }
        message.success(`任务 #${jobId} 已开始执行`);
        await loadJobs();
        await loadDetail(jobId);
      } catch (e) {
        message.error(`执行任务失败：${e}`);
      } finally {
        setExecutingId(null);
      }
    },
    [loadDetail, loadJobs],
  );

  const rerunWithConfig = useCallback((job: BacktestListItem) => {
    const symbols = Array.isArray(job.config.filters?.symbols) ? job.config.filters?.symbols ?? [] : [];
    const mode: "top_n" | "top_quantile" = job.config.top_n != null ? "top_n" : "top_quantile";
    form.setFieldsValue({
      config_template: "custom",
      strategy_name: job.config.strategy_name ?? job.strategy_name,
      start_date: job.config.start_date,
      end_date: job.config.end_date,
      rebalance_frequency: job.config.rebalance_frequency,
      holding_period_days: job.config.holding_period_days,
      pick_mode: mode,
      top_n: job.config.top_n ?? undefined,
      top_quantile: job.config.top_quantile ?? undefined,
      benchmark: job.config.benchmark ?? "000300.SH",
      transaction_cost_bps: job.config.transaction_cost_bps ?? 15,
      symbolsText: symbols.join("\n"),
    });
    message.info(`已复制任务 #${job.id} 参数，可直接提交新任务`);
  }, [form]);

  const columns: ColumnsType<BacktestListItem> = useMemo(
    () => [
      { title: "ID", dataIndex: "id", width: 80, align: "right" },
      { title: "状态", dataIndex: "status", width: 100, render: (s: string) => statusTag(s) },
      { title: "策略", dataIndex: "strategy_name", width: 180 },
      { title: "起止", key: "range", width: 220, render: (_: unknown, r) => `${r.config.start_date} ~ ${r.config.end_date}` },
      { title: "调仓", key: "freq", width: 100, render: (_: unknown, r) => r.config.rebalance_frequency },
      { title: "创建时间", dataIndex: "created_at", width: 220 },
      {
        title: "操作",
        key: "actions",
        width: 280,
        render: (_: unknown, r) => (
          <Space wrap>
            <Button
              size="small"
              type="link"
              onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
                void loadDetail(r.id);
              }}
            >
              查看详情
            </Button>
            {(r.status === "pending" || r.status === "failed") && (
              <Button
                size="small"
                type="link"
                loading={executingId === r.id}
                onClick={(e) => {
                  e.preventDefault();
                  e.stopPropagation();
                  void executeJob(r.id);
                }}
              >
                执行
              </Button>
            )}
            <Button
              size="small"
              type="link"
              onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
                rerunWithConfig(r);
              }}
            >
              复制参数重跑
            </Button>
            {r.status === "running" ? (
              <Tooltip title="执行中不可删除">
                <Button size="small" type="link" danger disabled>
                  删除
                </Button>
              </Tooltip>
            ) : (
              <Popconfirm
                title="确认删除该回测任务？"
                description="删除后不可恢复，关联结果一并清除。"
                okText="删除"
                cancelText="取消"
                okButtonProps={{ danger: true }}
                onConfirm={() => {
                  void deleteJob(r.id);
                }}
              >
                <Button size="small" type="link" danger loading={deletingId === r.id}>
                  删除
                </Button>
              </Popconfirm>
            )}
          </Space>
        ),
      },
    ],
    [deleteJob, deletingId, executeJob, executingId, loadDetail, rerunWithConfig],
  );

  const chartData = useMemo(() => {
    const points = detail?.result?.curve?.points;
    if (!Array.isArray(points)) {
      return [];
    }
    const out: Array<{ date: string; type: string; value: number }> = [];
    for (const p of points) {
      const d = String(p.date ?? "");
      const nav = Number(p.portfolio_nav ?? 0);
      const b = Number(p.benchmark_nav ?? 0);
      out.push({ date: d, type: "策略净值", value: nav });
      out.push({ date: d, type: "基准净值", value: b });
    }
    return out;
  }, [detail]);

  return (
    <div>
      <Typography.Title level={3} className="vs-page-heading">
        回测工作台
      </Typography.Title>
      <Typography.Paragraph type="secondary" className="vs-page-lead">
        在这里创建回测任务、跟踪状态、查看结果，并支持复制历史参数快速重跑。
      </Typography.Paragraph>
      <Alert
        type="info"
        showIcon
        style={{ marginBottom: 12 }}
        message={
          coverage?.start_date && coverage?.end_date
            ? `可用回测行情区间：${String(coverage.start_date).replace(/(\d{4})(\d{2})(\d{2})/, "$1-$2-$3")} ~ ${String(coverage.end_date).replace(/(\d{4})(\d{2})(\d{2})/, "$1-$2-$3")}`
            : "暂未获取到可用回测区间（请检查 TuShare 配置）"
        }
        description="创建任务前将校验日期范围，超出区间会阻止提交。"
      />

      <Row gutter={[16, 16]}>
        <Col span={24} xl={10}>
          <Card title="新建回测任务" className="vs-surface-card">
            <Form
              form={form}
              layout="vertical"
              initialValues={{
                config_template: "balanced",
                strategy_name: "investment_quality_score",
                rebalance_frequency: "monthly",
                holding_period_days: 30,
                pick_mode: "top_quantile",
                top_quantile: 0.2,
                benchmark: "000300.SH",
                transaction_cost_bps: 15,
              }}
            >
              <Row gutter={12}>
                <Col span={24}>
                  <Form.Item
                    name="config_template"
                    label={helpLabel(
                      "参数模板",
                      "快速套用常见回测配置。稳健=低换手长持，平衡=通用默认，进取=高频更激进。",
                    )}
                    rules={[{ required: true, message: "请选择模板" }]}
                  >
                    <Select options={TEMPLATE_OPTIONS} onChange={(v) => applyTemplate(String(v))} />
                  </Form.Item>
                </Col>
                <Col span={24}>
                  <Form.Item
                    name="strategy_name"
                    label={helpLabel("策略名称", "选择评分策略标识，决定后端按哪套规则生成信号。")}
                    rules={[{ required: true, message: "请选择策略名称" }]}
                  >
                    <Select options={STRATEGY_OPTIONS} />
                  </Form.Item>
                </Col>
                <Col span={12}>
                  <Form.Item
                    name="start_date"
                    label={helpLabel("开始日期", "回测起始日期，格式 YYYY-MM-DD。")}
                    rules={[{ required: true, message: "请输入开始日期" }]}
                  >
                    <Input placeholder="YYYY-MM-DD" />
                  </Form.Item>
                </Col>
                <Col span={12}>
                  <Form.Item
                    name="end_date"
                    label={helpLabel("结束日期", "回测结束日期，建议与起始日期至少间隔 3 个月。")}
                    rules={[{ required: true, message: "请输入结束日期" }]}
                  >
                    <Input placeholder="YYYY-MM-DD" />
                  </Form.Item>
                </Col>
                <Col span={12}>
                  <Form.Item
                    name="rebalance_frequency"
                    label={helpLabel("调仓频率", "决定多频繁更新持仓。频率越高，交易成本影响越大。")}
                    rules={[{ required: true }]}
                  >
                    <Select options={REBALANCE_OPTIONS} />
                  </Form.Item>
                </Col>
                <Col span={12}>
                  <Form.Item
                    name="holding_period_days"
                    label={helpLabel("持有天数", "每次调仓后持仓评估窗口，常用 10/20/30 天。")}
                    rules={[{ required: true }]}
                  >
                    <Select options={HOLDING_DAYS_OPTIONS} />
                  </Form.Item>
                </Col>
                <Col span={12}>
                  <Form.Item
                    name="pick_mode"
                    label={helpLabel("选股模式", "Top-N 与 Top-Quantile 二选一，避免参数冲突。")}
                    rules={[{ required: true }]}
                  >
                    <Select options={PICK_MODE_OPTIONS} />
                  </Form.Item>
                </Col>
                <Col span={12}>
                  {pickMode === "top_n" ? (
                    <Form.Item
                      name="top_n"
                      label={helpLabel("Top-N", "固定持仓只数，例如 Top 30。适合样本量稳定场景。")}
                      rules={[{ required: true, message: "请选择 Top-N" }]}
                    >
                      <Select options={TOP_N_OPTIONS} />
                    </Form.Item>
                  ) : (
                    <Form.Item
                      name="top_quantile"
                      label={helpLabel("Top-Quantile", "按分位选股。0.2 代表每期选择评分前 20%。")}
                      rules={[{ required: true, message: "请选择 Top-Quantile" }]}
                    >
                      <Select options={QUANTILE_OPTIONS} />
                    </Form.Item>
                  )}
                </Col>
                <Col span={12}>
                  <Form.Item
                    name="benchmark"
                    label={helpLabel("基准代码", "用于计算超额收益的对照指数，如 000300.SH。")}
                  >
                    <Input placeholder="000300.SH" />
                  </Form.Item>
                </Col>
                <Col span={12}>
                  <Form.Item
                    name="transaction_cost_bps"
                    label={helpLabel("交易成本(bps)", "每次调仓的双边成本估计，值越大，净值越保守。")}
                  >
                    <Select options={COST_OPTIONS} />
                  </Form.Item>
                </Col>
                <Col span={24}>
                  <Form.Item
                    name="symbolsText"
                    label={helpLabel("股票池（可选）", "限制回测标的范围。留空表示用全量可用样本。")}
                  >
                    <Input.TextArea rows={4} placeholder="000001.SZ&#10;000002.SZ" />
                  </Form.Item>
                </Col>
              </Row>
              <Space>
                <Button type="primary" loading={submitting} onClick={() => void submitCreate()}>
                  创建任务
                </Button>
                <Button onClick={() => form.resetFields()}>重置</Button>
              </Space>
            </Form>
          </Card>
        </Col>

        <Col span={24} xl={14}>
          <Card
            title="任务列表"
            className="vs-surface-card"
            extra={
              <Space>
                <Select
                  value={statusFilter}
                  style={{ width: 140 }}
                  options={STATUS_OPTIONS}
                  onChange={(v) => setStatusFilter(String(v))}
                />
                <Button loading={loading} onClick={() => void loadJobs()}>
                  刷新
                </Button>
                <Button loading={coverageLoading} onClick={() => void loadCoverage()}>
                  刷新区间
                </Button>
              </Space>
            }
          >
            <Table<BacktestListItem>
              className="vs-data-table"
              bordered={false}
              rowKey="id"
              loading={loading}
              columns={columns}
              dataSource={jobs}
              size="small"
              pagination={{ pageSize: 8 }}
              scroll={{ x: 1080 }}
            />
          </Card>
        </Col>
      </Row>

      <div ref={detailCardRef} style={{ scrollMarginTop: 72 }}>
        <Card title="结果详情" className="vs-surface-card" style={{ marginTop: 16 }} loading={detailLoading}>
          {!detail && <Typography.Text type="secondary">请选择一个任务查看详情。</Typography.Text>}
          {detail && (
            <Space direction="vertical" style={{ width: "100%" }} size={16}>
              <Space>
                <Typography.Text strong>任务 #{detail.job.id}</Typography.Text>
                {statusTag(detail.job.status)}
              </Space>
              {detail.job.status === "failed" && (
                <Alert
                  type="error"
                  showIcon
                  message="任务执行失败"
                  description={detail.job.meta?.error || "请检查参数后复制重跑"}
                />
              )}
              {detail.result ? (
                <>
                  <Row gutter={[12, 12]}>
                    <Col span={24} md={8} lg={6}>
                      <Statistic title="年化收益" value={toPercent(detail.result.metrics.annualized_return)} />
                    </Col>
                    <Col span={24} md={8} lg={6}>
                      <Statistic title="最大回撤" value={toPercent(detail.result.metrics.max_drawdown)} />
                    </Col>
                    <Col span={24} md={8} lg={6}>
                      <Statistic title="夏普" value={Number(detail.result.metrics.sharpe ?? 0).toFixed(3)} />
                    </Col>
                    <Col span={24} md={8} lg={6}>
                      <Statistic title="超额收益" value={toPercent(detail.result.metrics.excess_return)} />
                    </Col>
                    <Col span={24} md={8} lg={6}>
                      <Statistic title="换手率" value={toPercent(detail.result.metrics.turnover)} />
                    </Col>
                    <Col span={24} md={8} lg={6}>
                      <Statistic title="IC 均值" value={Number(detail.result.diagnostics?.ic_mean ?? 0).toFixed(3)} />
                    </Col>
                    <Col span={24} md={8} lg={6}>
                      <Statistic
                        title="RankIC 均值"
                        value={Number(detail.result.diagnostics?.rank_ic_mean ?? 0).toFixed(3)}
                      />
                    </Col>
                  </Row>
                  {chartData.length > 0 ? (
                    <Line
                      data={chartData}
                      xField="date"
                      yField="value"
                      seriesField="type"
                      smooth
                      height={280}
                      legend={{ position: "top" }}
                    />
                  ) : (
                    <Alert type="info" showIcon message="暂无净值曲线数据" />
                  )}
                  {Array.isArray(detail.result.diagnostics?.quantile_returns) &&
                  detail.result.diagnostics?.quantile_returns.length > 0 ? (
                    <Table
                      className="vs-data-table"
                      bordered={false}
                      size="small"
                      rowKey={(r) => String((r as { date?: string }).date ?? "")}
                      columns={[
                        { title: "日期", dataIndex: "date", key: "date", width: 120 },
                        {
                          title: "分层收益",
                          key: "layers",
                          render: (_: unknown, row: { layers?: Array<{ layer: number; return: number }> }) => {
                            const layers = Array.isArray(row.layers) ? row.layers : [];
                            return layers
                              .map((x) => `L${x.layer}: ${toPercent(Number(x.return ?? 0))}`)
                              .join(" | ");
                          },
                        },
                      ]}
                      dataSource={detail.result.diagnostics.quantile_returns as Array<{
                        date: string;
                        layers?: Array<{ layer: number; return: number }>;
                      }>}
                      pagination={false}
                    />
                  ) : (
                    <Alert type="warning" showIcon message="暂无分层收益诊断数据（可检查样本与时间区间）" />
                  )}
                  <Descriptions size="small" column={1} bordered>
                    <Descriptions.Item label="回测引擎路径">
                      {String(detail.result.diagnostics?.engine_path ?? "unknown")}
                    </Descriptions.Item>
                    <Descriptions.Item label="样本覆盖统计">
                      {JSON.stringify(detail.result.diagnostics?.coverage ?? {}, null, 0)}
                    </Descriptions.Item>
                    <Descriptions.Item label="剔除原因分布">
                      {JSON.stringify(detail.result.diagnostics?.exclusion_reasons ?? {}, null, 0)}
                    </Descriptions.Item>
                  </Descriptions>
                </>
              ) : (
                <Alert type="info" showIcon message="当前任务尚未产出结果。可点击任务列表中的“执行”或等待运行完成。" />
              )}
            </Space>
          )}
        </Card>
      </div>
    </div>
  );
}

