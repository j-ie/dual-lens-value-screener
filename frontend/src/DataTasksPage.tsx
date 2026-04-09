import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { Button, Card, Popconfirm, Space, Table, Tag, Tooltip, Typography, message } from "antd";
import type { ColumnsType } from "antd/es/table";

/** 与后端 is_post_pipeline_busy 一致：仅这两段视为「仍在跑」 */
const POST_PIPELINE_BUSY_PHASES = new Set(["starting", "ai_running"]);

/** 与 VALUE_SCREENER_POST_PIPELINE_STALE_MINUTES 默认 45 对齐 */
const POST_PIPELINE_STALE_MS = 45 * 60 * 1000;

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
  post_pipeline_phase?: string | null;
  post_pipeline_started_at?: string | null;
  /** 与后端 meta.post_pipeline_activity_at 一致；每处理一只标的会刷新，用于长耗时 Top N 不误判僵尸 */
  post_pipeline_activity_at?: string | null;
  post_pipeline_ai_index?: number | null;
  post_pipeline_ai_total?: number | null;
  post_pipeline_ai_symbol?: string | null;
  post_pipeline_ai_ok?: number | null;
  post_pipeline_ai_failed?: number | null;
  post_pipeline_ai_skip_reason?: string | null;
  post_pipeline_ai_symbol_pick?: string | null;
  post_pipeline_finished_at?: string | null;
  investment_quality_summary?: {
    worth_buy_count?: number | null;
    analyzed_count?: number | null;
    label_zh?: string | null;
  } | null;
};

function statusTag(status: string) {
  if (status === "running") {
    return <Tag color="processing">进行中</Tag>;
  }
  if (status === "success") {
    return <Tag color="success">成功</Tag>;
  }
  if (status === "failed") {
    return <Tag color="error">失败</Tag>;
  }
  return <Tag>{status}</Tag>;
}

function formatProgress(r: RunItem): string {
  if (r.status !== "running") {
    return "—";
  }
  if (r.progress_total != null && r.progress_total > 0) {
    const cur = r.progress_current ?? 0;
    const sym = r.progress_symbol ? ` ${r.progress_symbol}` : "";
    return `${r.progress_percent ?? 0}%（${cur}/${r.progress_total}${sym}）`;
  }
  return r.progress_phase ? String(r.progress_phase) : "准备中…";
}

/** 与后端 _post_pipeline_staleness_reference 一致：取 started_at 与 activity_at 中较新的时间戳（毫秒） */
function postPipelineStalenessReferenceMs(r: RunItem): number | null {
  const ms: number[] = [];
  for (const s of [r.post_pipeline_activity_at, r.post_pipeline_started_at]) {
    if (!s) {
      continue;
    }
    const t = Date.parse(s);
    if (!Number.isNaN(t)) {
      ms.push(t);
    }
  }
  if (ms.length === 0) {
    return null;
  }
  return Math.max(...ms);
}

function isPostPipelineStale(r: RunItem): boolean {
  const ref = postPipelineStalenessReferenceMs(r);
  if (ref == null) {
    return false;
  }
  return Date.now() - ref > POST_PIPELINE_STALE_MS;
}

function isPostPipelineBusy(r: RunItem): boolean {
  const p = (r.post_pipeline_phase ?? "").trim();
  if (!POST_PIPELINE_BUSY_PHASES.has(p)) {
    return false;
  }
  if (postPipelineStalenessReferenceMs(r) == null) {
    return false;
  }
  if (isPostPipelineStale(r)) {
    return false;
  }
  return true;
}

function formatPostPipeline(r: RunItem): string {
  const p = r.post_pipeline_phase;
  if (!p) {
    return "未运行";
  }
  if (p === "done") {
    const ok = r.post_pipeline_ai_ok;
    const fail = r.post_pipeline_ai_failed;
    const bits: string[] = [];
    if (ok != null || fail != null) {
      bits.push(`AI 成功 ${ok ?? "—"} / 失败 ${fail ?? "—"}`);
    }
    if (r.post_pipeline_ai_skip_reason) {
      bits.push(`说明：${r.post_pipeline_ai_skip_reason}`);
    }
    if (r.post_pipeline_ai_symbol_pick && r.post_pipeline_ai_symbol_pick !== "combined_gates") {
      bits.push(`选股：${r.post_pipeline_ai_symbol_pick}`);
    }
    if (bits.length) {
      return `完成（${bits.join("；")}）`;
    }
    return "完成";
  }
  if (p === "ai_running" && (r.post_pipeline_ai_total ?? 0) > 0) {
    const sym = r.post_pipeline_ai_symbol ? ` ${r.post_pipeline_ai_symbol}` : "";
    return `AI 分析 ${r.post_pipeline_ai_index ?? 0}/${r.post_pipeline_ai_total}${sym}`;
  }
  if (POST_PIPELINE_BUSY_PHASES.has(p) && !isPostPipelineStale(r)) {
    return p;
  }
  if (POST_PIPELINE_BUSY_PHASES.has(p) && isPostPipelineStale(r)) {
    return `${p}（已超时，可再次点「后置任务」重试）`;
  }
  return p;
}

function formatWorthBuySummary(r: RunItem): string {
  const s = r.investment_quality_summary;
  if (!s) {
    return "—";
  }
  const buy = s.worth_buy_count;
  const total = s.analyzed_count;
  if (typeof buy === "number" && typeof total === "number" && total >= 0) {
    return `值得买入 ${buy}/${total}`;
  }
  return s.label_zh ?? "—";
}

export default function DataTasksPage() {
  const [runs, setRuns] = useState<RunItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [actionId, setActionId] = useState<number | null>(null);
  const [postPipeId, setPostPipeId] = useState<number | null>(null);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const loadRuns = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch("/api/v1/runs?limit=200");
      if (!res.ok) {
        throw new Error(await res.text());
      }
      setRuns((await res.json()) as RunItem[]);
    } catch (e) {
      message.error(`加载任务列表失败：${e}`);
    } finally {
      setLoading(false);
    }
  }, []);

  const stopPoll = useCallback(() => {
    if (pollRef.current !== null) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
  }, []);

  useEffect(() => {
    void loadRuns();
  }, [loadRuns]);

  useEffect(() => {
    const hasRunning = runs.some((r) => r.status === "running");
    const hasPostBusy = runs.some((r) => isPostPipelineBusy(r));
    stopPoll();
    if (!hasRunning && !hasPostBusy) {
      return;
    }
    pollRef.current = setInterval(() => void loadRuns(), 2500);
    return stopPoll;
  }, [runs, loadRuns, stopPoll]);

  const requeue = useCallback(
    async (runId: number) => {
      setActionId(runId);
      try {
        const res = await fetch(`/api/v1/runs/${runId}/requeue-batch-screen`, { method: "POST" });
        const text = await res.text();
        if (!res.ok) {
          throw new Error(text || res.statusText);
        }
        const body = JSON.parse(text) as { run_id: number; message?: string };
        message.success(body.message ?? `已排队新 Run #${body.run_id}`);
        await loadRuns();
      } catch (e) {
        message.error(`继续拉数失败：${e}`);
      } finally {
        setActionId(null);
      }
    },
    [loadRuns],
  );

  const triggerPostPipeline = useCallback(
    async (runId: number) => {
      setPostPipeId(runId);
      try {
        const res = await fetch(`/api/v1/runs/${runId}/post-pipeline`, { method: "POST" });
        const text = await res.text();
        if (res.status === 409) {
          message.warning(text || "后置任务已在执行中");
          return;
        }
        if (!res.ok) {
          throw new Error(text || res.statusText);
        }
        const body = JSON.parse(text) as { message?: string };
        message.success(body.message ?? "已排队后置任务（第三套/三元、Top N DCF+AI）");
        await loadRuns();
      } catch (e) {
        message.error(`触发失败：${e}`);
      } finally {
        setPostPipeId(null);
      }
    },
    [loadRuns],
  );

  const removeRun = useCallback(
    async (runId: number) => {
      setActionId(runId);
      try {
        const res = await fetch(`/api/v1/runs/${runId}`, { method: "DELETE" });
        if (!res.ok) {
          const t = await res.text();
          throw new Error(t || res.statusText);
        }
        message.success(`已删除 Run #${runId}`);
        await loadRuns();
      } catch (e) {
        message.error(`删除失败：${e}`);
      } finally {
        setActionId(null);
      }
    },
    [loadRuns],
  );

  const columns: ColumnsType<RunItem> = useMemo(
    () => [
      { title: "ID", dataIndex: "id", width: 72, fixed: "left" },
      {
        title: "状态",
        dataIndex: "status",
        width: 100,
        render: (s: string) => statusTag(s),
      },
      {
        title: "创建时间",
        dataIndex: "created_at",
        width: 200,
        ellipsis: true,
      },
      {
        title: "结束时间",
        dataIndex: "finished_at",
        width: 200,
        render: (v: string | null) => v ?? "—",
      },
      {
        title: "进度",
        key: "progress",
        width: 220,
        ellipsis: true,
        render: (_: unknown, r) => formatProgress(r),
      },
      {
        title: "规模 / 成功 / 失败",
        key: "counts",
        width: 160,
        render: (_: unknown, r) =>
          `${r.universe_size ?? "—"} / ${r.snapshot_ok ?? "—"} / ${r.snapshot_failed ?? "—"}`,
      },
      {
        title: "数据源",
        dataIndex: "provider_label",
        width: 120,
        ellipsis: true,
        render: (v: string | null) => v ?? "—",
      },
      {
        title: "是否值得买入",
        key: "worth_buy",
        width: 180,
        ellipsis: true,
        render: (_: unknown, r) => (
          <Tooltip title="口径：投资质量结论=可买 且 低估=true 且风险可控时计入“值得买入”">
            <Typography.Text>{formatWorthBuySummary(r)}</Typography.Text>
          </Tooltip>
        ),
      },
      {
        title: "后置流水线",
        key: "post_pipeline",
        width: 280,
        ellipsis: true,
        render: (_: unknown, r) => {
          const txt = formatPostPipeline(r);
          return (
            <Tooltip title={txt}>
              <Typography.Text type={isPostPipelineBusy(r) ? undefined : "secondary"} ellipsis>
                {txt}
              </Typography.Text>
            </Tooltip>
          );
        },
      },
      {
        title: "操作",
        key: "actions",
        fixed: "right",
        width: 380,
        render: (_: unknown, r) => (
          <Space size="small" wrap>
            <Link to={`/?runId=${r.id}`}>
              <Button size="small" type="link">
                查看结果
              </Button>
            </Link>
            {r.status === "success" ? (
              <Popconfirm
                title="运行后置流水线？"
                description="将刷新第三套/三元综合分，并对综合分 Top N（见环境变量 VALUE_SCREENER_POST_FULL_BATCH_AI_TOP_N）逐只计算 DCF 并调用 AI 落库，耗时与费用较高。"
                okText="开始"
                cancelText="取消"
                onConfirm={() => void triggerPostPipeline(r.id)}
              >
                <Button
                  size="small"
                  type="link"
                  loading={postPipeId === r.id}
                  disabled={isPostPipelineBusy(r)}
                >
                  后置任务
                </Button>
              </Popconfirm>
            ) : null}
            <Button
              size="small"
              type="link"
              loading={actionId === r.id}
              onClick={() => void requeue(r.id)}
            >
              继续拉数
            </Button>
            <Popconfirm
              title="确定删除该 Run？"
              description={
                r.status === "running"
                  ? "状态为「进行中」多为僵尸任务，可删。将删除本批全部筛选结果，以及曾绑定该 Run 的落库 AI 分析记录。全市场共享的财报快照/三大表数据不会删。若确定后台仍在跑该批次，删除可能导致其写库失败（一般仅日志）。"
                  : "将删除本批全部筛选结果，以及曾绑定该 Run 的落库 AI 分析记录。全市场共享的财报快照等不会删。"
              }
              okText="删除"
              cancelText="取消"
              okButtonProps={{ danger: true }}
              onConfirm={() => void removeRun(r.id)}
            >
              <Button size="small" type="link" danger>
                删除
              </Button>
            </Popconfirm>
          </Space>
        ),
      },
    ],
    [actionId, postPipeId, requeue, removeRun, triggerPostPipeline],
  );

  return (
    <div>
      <Typography.Title level={3} className="vs-page-heading">
        拉数任务
      </Typography.Title>
      <Typography.Paragraph type="secondary" className="vs-page-lead">
        管理「一键拉数并入库」产生的批次。批跑成功后，可点「后置任务」手动排队：刷新第三套/三元综合分，并对综合高分 Top N 跑 DCF 与 AI 分析落库（依赖 AI/DCF
        环境变量）。「后置任务」置灰表示当前 Run 在元数据里仍处于后置流水线的 starting / ai_running，且最近活动时间（含逐只 AI 进度）在
        VALUE_SCREENER_POST_PIPELINE_STALE_MINUTES（默认 45）分钟内；整批 Top N 可能需数小时，只要仍在跑会保持置灰。若 API 重启导致后台线程消失、进度长时间不更新，超过上述分钟数后会自动解锁以便重试。
      </Typography.Paragraph>
      <Card className="vs-surface-card">
        <Space style={{ marginBottom: 16 }}>
          <Button onClick={() => void loadRuns()} loading={loading}>
            刷新列表
          </Button>
        </Space>
        <Table<RunItem>
          className="vs-data-table"
          bordered={false}
          rowKey="id"
          loading={loading}
          columns={columns}
          dataSource={runs}
          pagination={{ pageSize: 20, showSizeChanger: true, pageSizeOptions: [10, 20, 50, 100] }}
          scroll={{ x: 1480 }}
        />
      </Card>
    </div>
  );
}
