import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Button, Card, InputNumber, Progress, Select, Space, Table, Typography, message } from "antd";
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
  { value: "buffett_desc", label: "巴菲特分 ↓（默认）" },
  { value: "buffett_asc", label: "巴菲特分 ↑" },
  { value: "graham_desc", label: "格雷厄姆分 ↓" },
  { value: "graham_asc", label: "格雷厄姆分 ↑" },
];

const EMPTY = "—";

function parseSortKey(combo: string): { sort: "buffett" | "graham"; order: "asc" | "desc" } {
  const [sort, order] = combo.split("_") as ["buffett" | "graham", "asc" | "desc"];
  return { sort, order };
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

export default function App() {
  const [runs, setRuns] = useState<RunItem[]>([]);
  const [runId, setRunId] = useState<number | null>(null);
  const [sortCombo, setSortCombo] = useState("buffett_desc");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [data, setData] = useState<Paged | null>(null);
  const [loading, setLoading] = useState(false);
  /** null = 全市场；数字 = 上限 */
  const [batchMax, setBatchMax] = useState<number | null>(null);
  const [batchLoading, setBatchLoading] = useState(false);
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
  }, [runId, page, pageSize, sort, order]);

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
    void loadPage();
  }, [loadPage]);

  useEffect(() => () => stopPoll(), [stopPoll]);

  const columns: ColumnsType<PagedItem> = [
    { title: "代码", dataIndex: "symbol", width: 108, fixed: "left" },
    {
      title: "名称",
      dataIndex: "display_name",
      width: 100,
      ellipsis: true,
      render: (v: string) => (v ? v : EMPTY),
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
      title: "巴菲特分",
      dataIndex: "buffett_score",
      width: 96,
      render: (v: number) => v.toFixed(2),
    },
    {
      title: "格雷厄姆分",
      dataIndex: "graham_score",
      width: 104,
      render: (v: number) => v.toFixed(2),
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
  ];

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

  return (
    <div style={{ padding: 24, maxWidth: 1400, margin: "0 auto" }}>
      <Typography.Title level={3} style={{ marginTop: 0 }}>
        双视角筛选结果
      </Typography.Title>
      <Card>
        <Space wrap style={{ marginBottom: 16 }} align="center">
          <Typography.Text type="secondary">TuShare 优先，失败整批切 AkShare；批跑为异步 202</Typography.Text>
          <span>上限（空=全市场）：</span>
          <InputNumber
            min={1}
            max={10000}
            placeholder="全市场"
            value={batchMax ?? undefined}
            onChange={(v) => setBatchMax(v === null || v === undefined ? null : v)}
          />
          <Button type="primary" loading={batchLoading} onClick={() => void runBatchScreen()}>
            一键拉数并入库
          </Button>
        </Space>
        <Space wrap style={{ marginBottom: 16 }}>
          <span>选择 Run：</span>
          <Select
            style={{ minWidth: 420 }}
            value={runId ?? undefined}
            placeholder="请选择 screening_run"
            options={runs.map((r) => ({
              value: r.id,
              label: formatRunLabel(r),
            }))}
            onChange={(v) => {
              setRunId(v);
              setPage(1);
            }}
          />
          <span>排序：</span>
          <Select
            style={{ minWidth: 220 }}
            value={sortCombo}
            options={SORT_OPTIONS}
            onChange={(v) => {
              setSortCombo(v);
              setPage(1);
            }}
          />
        </Space>
        {selectedRun?.status === "running" && (
          <div style={{ marginBottom: 16 }}>
            <Typography.Paragraph type="warning" style={{ marginBottom: 8 }}>
              当前 Run 仍在后台执行，结果表可能暂时为空；列表将每 2 秒自动刷新。
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
          rowKey="symbol"
          loading={loading}
          columns={columns}
          dataSource={data?.items ?? []}
          pagination={pagination}
          scroll={{ x: 1200 }}
        />
      </Card>
    </div>
  );
}
