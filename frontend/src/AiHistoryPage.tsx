import { useCallback, useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { Card, Input, InputNumber, Select, Spin, Table, Typography, message } from "antd";
import type { ColumnsType, TablePaginationConfig } from "antd/es/table";
import { AiAnalysisSections } from "./components/AiAnalysisSections";
import { DcfSnapshotCard, type DcfBlock } from "./components/DcfSnapshotCard";

type AiListItem = {
  id: number;
  ts_code: string;
  analysis_date: string;
  run_id: number | null;
  ai_score: number;
  ai_score_rationale: string | null;
  opportunity_score?: number | null;
  opportunity_score_rationale?: string | null;
  summary_preview: string;
  generated_at: string | null;
  display_name: string;
  industry: string;
  dcf_ok?: boolean | null;
  dcf_headline?: string | null;
};

type CompanyAiDetailApi = {
  id: number;
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
  dcf_snapshot: Record<string, unknown> | null;
  dcf_ok: boolean | null;
  dcf_headline: string | null;
  context_hash: string;
  prompt_version: string;
  model: string;
  generated_at: string | null;
};

function AiHistoryExpanded({ analysisId }: { analysisId: number }) {
  const [detail, setDetail] = useState<CompanyAiDetailApi | null>(null);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setErr(null);
    void (async () => {
      try {
        const res = await fetch(`/api/v1/company-ai-analyses/${analysisId}`);
        if (!res.ok) {
          throw new Error(await res.text());
        }
        const body = (await res.json()) as CompanyAiDetailApi;
        if (!cancelled) {
          setDetail(body);
        }
      } catch (e) {
        if (!cancelled) {
          setErr(String(e));
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
  }, [analysisId]);

  if (loading) {
    return (
      <div style={{ padding: 16 }}>
        <Spin size="small" /> <Typography.Text type="secondary"> 加载完整记录…</Typography.Text>
      </div>
    );
  }
  if (err) {
    return (
      <Typography.Paragraph type="danger" style={{ padding: 16, margin: 0 }}>
        {err}
      </Typography.Paragraph>
    );
  }
  if (!detail) {
    return null;
  }

  const dcf = detail.dcf_snapshot as DcfBlock | null | undefined;

  return (
    <div style={{ padding: "8px 16px 24px", background: "rgba(0,0,0,0.02)", borderRadius: 12 }}>
      <AiAnalysisSections
        data={{
          summary: detail.summary,
          key_metrics_commentary: detail.key_metrics_commentary,
          risks: detail.risks,
          alignment_with_scores: detail.alignment_with_scores,
          narrative_markdown: detail.narrative_markdown,
          ai_score: detail.ai_score,
          ai_score_rationale: detail.ai_score_rationale ?? undefined,
          opportunity_score: detail.opportunity_score,
          opportunity_score_rationale: detail.opportunity_score_rationale ?? undefined,
          meta: {
            analysis_date: detail.analysis_date,
            context_hash: detail.context_hash,
            prompt_version: detail.prompt_version,
            model: detail.model,
            generated_at: detail.generated_at ?? undefined,
          },
        }}
        dcfExtra={<DcfSnapshotCard dcf={dcf} title="DCF（落库快照）" />}
      />
    </div>
  );
}

type Paged = {
  items: AiListItem[];
  total: number;
  page: number;
  page_size: number;
  sort: string;
  order: string;
};

const SORT_OPTIONS = [
  { value: "analysis_date_desc", label: "分析日期 ↓" },
  { value: "analysis_date_asc", label: "分析日期 ↑" },
  { value: "ai_score_desc", label: "一致性分 ↓" },
  { value: "ai_score_asc", label: "一致性分 ↑" },
  { value: "opportunity_score_desc", label: "机会倾向 ↓" },
  { value: "opportunity_score_asc", label: "机会倾向 ↑" },
  { value: "ts_code_desc", label: "代码 ↓" },
  { value: "ts_code_asc", label: "代码 ↑" },
];

const EMPTY = "—";
const INDUSTRY_EMPTY = "__EMPTY__";

function parseSortCombo(combo: string): { sort: string; order: "asc" | "desc" } {
  const m = combo.match(/^(analysis_date|ai_score|opportunity_score|ts_code)_(asc|desc)$/);
  if (!m) {
    return { sort: "opportunity_score", order: "desc" };
  }
  return { sort: m[1], order: m[2] as "asc" | "desc" };
}

export default function AiHistoryPage() {
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [sortCombo, setSortCombo] = useState("opportunity_score_desc");
  const [data, setData] = useState<Paged | null>(null);
  const [loading, setLoading] = useState(false);
  const [dateFrom, setDateFrom] = useState("");
  const [dateTo, setDateTo] = useState("");
  const [aiMin, setAiMin] = useState<number | null>(null);
  const [selectedIndustries, setSelectedIndustries] = useState<string[]>([]);
  const [industryOptions, setIndustryOptions] = useState<{ value: string; label: string }[]>([]);
  const [companyNameQ, setCompanyNameQ] = useState("");

  const { sort, order } = useMemo(() => parseSortCombo(sortCombo), [sortCombo]);

  const loadIndustries = useCallback(async () => {
    try {
      const res = await fetch("/api/v1/company-ai-analyses/industries");
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
  }, []);

  const loadPage = useCallback(async () => {
    setLoading(true);
    try {
      const q = new URLSearchParams({
        page: String(page),
        page_size: String(pageSize),
        sort,
        order,
      });
      const df = dateFrom.trim();
      const dt = dateTo.trim();
      if (df) {
        q.set("analysis_date_from", df);
      }
      if (dt) {
        q.set("analysis_date_to", dt);
      }
      if (aiMin !== null && Number.isFinite(aiMin)) {
        q.set("ai_score_min", String(aiMin));
      }
      for (const ind of selectedIndustries) {
        q.append("industry", ind);
      }
      const cn = companyNameQ.trim();
      if (cn) {
        q.set("company_name", cn);
      }
      const res = await fetch(`/api/v1/company-ai-analyses?${q.toString()}`);
      if (!res.ok) {
        throw new Error(await res.text());
      }
      setData((await res.json()) as Paged);
    } catch (e) {
      message.error(`加载失败：${e}`);
    } finally {
      setLoading(false);
    }
  }, [page, pageSize, sort, order, dateFrom, dateTo, aiMin, selectedIndustries, companyNameQ]);

  useEffect(() => {
    void loadIndustries();
  }, [loadIndustries]);

  useEffect(() => {
    void loadPage();
  }, [loadPage]);

  const columns: ColumnsType<AiListItem> = useMemo(
    () => [
      { title: "代码", dataIndex: "ts_code", width: 110, fixed: "left" },
      {
        title: "名称",
        dataIndex: "display_name",
        width: 100,
        ellipsis: true,
        render: (v: string, row) => {
          const label = v ? v : EMPTY;
          if (row.run_id != null) {
            return (
              <Link to={`/runs/${row.run_id}/companies/${encodeURIComponent(row.ts_code)}`}>{label}</Link>
            );
          }
          return label;
        },
      },
      { title: "行业", dataIndex: "industry", width: 100, ellipsis: true, render: (v: string) => v || EMPTY },
      {
        title: "分析日",
        dataIndex: "analysis_date",
        width: 110,
      },
      {
        title: "一致性分",
        dataIndex: "ai_score",
        width: 88,
        render: (v: number) => (Number.isFinite(v) ? v.toFixed(2) : EMPTY),
      },
      {
        title: "机会倾向",
        dataIndex: "opportunity_score",
        width: 88,
        render: (v: number | null | undefined) =>
          v !== null && v !== undefined && Number.isFinite(v) ? v.toFixed(2) : EMPTY,
      },
      {
        title: "DCF 状态",
        key: "dcf_ok",
        width: 92,
        render: (_: unknown, row: AiListItem) => {
          if (row.dcf_ok === true) {
            return "可用";
          }
          if (row.dcf_ok === false) {
            return "不可用";
          }
          return EMPTY;
        },
      },
      {
        title: "DCF 摘要",
        dataIndex: "dcf_headline",
        width: 220,
        ellipsis: true,
        render: (v: string | null | undefined) => (v ? String(v) : EMPTY),
      },
      {
        title: "摘要",
        dataIndex: "summary_preview",
        ellipsis: true,
        render: (v: string) => v || EMPTY,
      },
      {
        title: "生成时间(UTC)",
        dataIndex: "generated_at",
        width: 200,
        ellipsis: true,
        render: (v: string | null) => v ?? EMPTY,
      },
    ],
    [],
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

  return (
    <div>
      <Typography.Title level={3} className="vs-page-heading">
        AI 分析历史
      </Typography.Title>
      <Typography.Paragraph type="secondary" className="vs-page-lead">
        已落库记录按上海日历日唯一；点选行首展开可查看完整结构化内容与 DCF 快照。仅供复盘，不构成投资建议。
      </Typography.Paragraph>
      <Card className="vs-surface-card">
        <div className="vs-filter-stack">
          <div className="vs-filter-row">
            <div className="vs-filter-label">分析日期</div>
            <div className="vs-filter-controls">
              <Input
                style={{ width: 148 }}
                placeholder="起 · YYYY-MM-DD"
                value={dateFrom}
                onChange={(e) => {
                  setDateFrom(e.target.value);
                  setPage(1);
                }}
              />
              <Input
                style={{ width: 148 }}
                placeholder="止 · YYYY-MM-DD"
                value={dateTo}
                onChange={(e) => {
                  setDateTo(e.target.value);
                  setPage(1);
                }}
              />
            </div>
          </div>
          <div className="vs-filter-row">
            <div className="vs-filter-label">分数与排序</div>
            <div className="vs-filter-controls">
              <InputNumber
                min={0}
                max={100}
                placeholder="最低 AI 分"
                style={{ width: 132 }}
                value={aiMin ?? undefined}
                onChange={(v) => {
                  setAiMin(v === null || v === undefined ? null : v);
                  setPage(1);
                }}
              />
              <Select
                style={{ minWidth: 200 }}
                value={sortCombo}
                options={SORT_OPTIONS}
                onChange={(v) => {
                  setSortCombo(v);
                  setPage(1);
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
                style={{ minWidth: 280, maxWidth: "100%", flex: "1 1 280px" }}
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
        </div>
        <Table<AiListItem>
          className="vs-data-table"
          bordered={false}
          rowKey="id"
          loading={loading}
          columns={columns}
          dataSource={data?.items ?? []}
          pagination={pagination}
          scroll={{ x: 1280 }}
          expandable={{
            expandedRowRender: (row) => <AiHistoryExpanded analysisId={row.id} />,
            rowExpandable: () => true,
          }}
        />
      </Card>
    </div>
  );
}
