import { lazy, Suspense, useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { Button, Card, Descriptions, Space, Spin, Table, Typography, message } from "antd";
import type { ColumnsType } from "antd/es/table";

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
        const res = await fetch(
          `/api/v1/runs/${rid}/companies/${encodeURIComponent(code)}/detail`,
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
    <div style={{ padding: 24, maxWidth: 1200, margin: "0 auto" }}>
      <Space style={{ marginBottom: 16 }}>
        <Button type="link" onClick={() => navigate(-1)} style={{ padding: 0 }}>
          ← 返回列表
        </Button>
      </Space>
      <Typography.Title level={3} style={{ marginTop: 0 }}>
        公司详情 {detail?.ts_code ? `· ${detail.ts_code}` : ""}
      </Typography.Title>

      <Card loading={loading} style={{ marginBottom: 16 }}>
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
