import type { ReactNode } from "react";
import ReactMarkdown from "react-markdown";
import { Descriptions, Typography } from "antd";

export type AiStructured = {
  summary: string;
  key_metrics_commentary: string;
  risks: string;
  alignment_with_scores: string;
  /** 模型对规则引擎价值判断的专门解读；旧落库记录可能为空 */
  investment_quality_commentary?: string;
  narrative_markdown: string;
  ai_score: number;
  ai_score_rationale?: string;
  opportunity_score?: number | null;
  opportunity_score_rationale?: string | null;
  meta?: {
    analysis_date?: string;
    context_hash?: string;
    prompt_version?: string;
    model?: string;
    generated_at?: string;
    cached?: boolean;
  };
};

const EMPTY = "—";

function fmtNum(v: number): string {
  if (!Number.isFinite(v)) {
    return EMPTY;
  }
  return v.toFixed(2);
}

type Props = {
  data: AiStructured;
  /** 与本次分析一并展示的 DCF 卡片（由父组件传入） */
  dcfExtra?: ReactNode;
};

export function AiAnalysisSections({ data, dcfExtra }: Props) {
  const meta = data.meta;

  return (
    <div>
      <Descriptions bordered size="small" column={2} style={{ marginBottom: 16 }}>
        <Descriptions.Item label="信号一致性分（大模型）" span={2}>
          <Typography.Text type="secondary" style={{ fontSize: 12, display: "block", marginBottom: 4 }}>
            衡量信息完整度与规则分、DCF、叙述是否自洽；不表示是否便宜或是否该买。
          </Typography.Text>
          {fmtNum(data.ai_score)}
        </Descriptions.Item>
        <Descriptions.Item label="一致性简述" span={2}>
          {data.ai_score_rationale ? String(data.ai_score_rationale) : EMPTY}
        </Descriptions.Item>
        <Descriptions.Item label="机会倾向分（黄金坑/安全边际）" span={2}>
          <Typography.Text type="secondary" style={{ fontSize: 12, display: "block", marginBottom: 4 }}>
            在已给数据下对安全边际是否充裕的主观倾向；非买卖指令。金融业简化 DCF 单独不应压至极低分。
          </Typography.Text>
          {data.opportunity_score != null && Number.isFinite(data.opportunity_score)
            ? fmtNum(data.opportunity_score)
            : EMPTY}
        </Descriptions.Item>
        <Descriptions.Item label="机会倾向简述" span={2}>
          {data.opportunity_score_rationale ? String(data.opportunity_score_rationale) : EMPTY}
        </Descriptions.Item>
        {meta?.analysis_date ? (
          <Descriptions.Item label="落库分析日" span={2}>
            {String(meta.analysis_date)}
          </Descriptions.Item>
        ) : null}
      </Descriptions>

      {dcfExtra}

      <div className="vs-reading">
        {data.investment_quality_commentary ? (
          <>
            <div className="vs-section-title">价值判断（规则引擎 · 模型解读）</div>
            <Typography.Paragraph style={{ whiteSpace: "pre-wrap", marginBottom: 16 }}>
              {data.investment_quality_commentary}
            </Typography.Paragraph>
          </>
        ) : null}

        <div className="vs-section-title">摘要</div>
        <Typography.Paragraph style={{ marginBottom: 0 }}>{data.summary}</Typography.Paragraph>

        <div className="vs-section-title">关键指标与业务</div>
        <Typography.Paragraph style={{ whiteSpace: "pre-wrap", marginBottom: 0 }}>
          {data.key_metrics_commentary}
        </Typography.Paragraph>

        <div className="vs-section-title">风险与不确定性</div>
        <Typography.Paragraph style={{ whiteSpace: "pre-wrap", marginBottom: 0 }}>{data.risks}</Typography.Paragraph>

        <div className="vs-section-title">与规则筛分及 DCF 的对照</div>
        <Typography.Paragraph style={{ whiteSpace: "pre-wrap", marginBottom: 0 }}>
          {data.alignment_with_scores}
        </Typography.Paragraph>

        <div className="vs-section-title">完整叙述</div>
        <div className="ai-narrative-md">
          <ReactMarkdown>{data.narrative_markdown}</ReactMarkdown>
        </div>
      </div>

      {meta ? (
        <Typography.Paragraph type="secondary" style={{ marginTop: 16, fontSize: 12 }}>
          模型：{meta.model ?? EMPTY} · 提示版本 {meta.prompt_version ?? EMPTY} · 上下文哈希{" "}
          {meta.context_hash ? `${meta.context_hash.slice(0, 16)}…` : EMPTY} · 生成时间{" "}
          {meta.generated_at ?? EMPTY}
          {meta.cached ? " · 来自缓存" : ""}
        </Typography.Paragraph>
      ) : null}
    </div>
  );
}
