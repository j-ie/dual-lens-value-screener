import { Descriptions, Typography } from "antd";

export type DcfBlock = {
  ok: boolean;
  skip_reason?: string | null;
  message?: string | null;
  warnings?: string[];
  notes?: string[];
  assumptions?: Record<string, unknown> | null;
  values?: Record<string, unknown> | null;
};

const EMPTY = "—";

function fmtNum(v: unknown): string {
  if (v === null || v === undefined) {
    return EMPTY;
  }
  if (typeof v === "number" && !Number.isNaN(v)) {
    return Number.isInteger(v) ? String(v) : String(v);
  }
  return String(v);
}

type Props = {
  dcf: DcfBlock | null | undefined;
  title?: string;
};

export function DcfSnapshotCard({ dcf, title = "DCF（分析时点快照）" }: Props) {
  if (!dcf) {
    return (
      <div className="vs-dcf-card">
        <Typography.Text type="secondary">无 DCF 快照数据</Typography.Text>
      </div>
    );
  }

  if (dcf.ok && dcf.values && dcf.assumptions) {
    return (
      <div className="vs-dcf-card">
        <Typography.Title level={5} style={{ marginTop: 0, fontWeight: 600 }}>
          {title}
        </Typography.Title>
        <Descriptions bordered size="small" column={2}>
          <Descriptions.Item label="WACC">{fmtNum(dcf.assumptions.wacc)}</Descriptions.Item>
          <Descriptions.Item label="预测期 g">{fmtNum(dcf.assumptions.stage1_growth)}</Descriptions.Item>
          <Descriptions.Item label="永续 g">{fmtNum(dcf.assumptions.terminal_growth)}</Descriptions.Item>
          <Descriptions.Item label="预测年数">{fmtNum(dcf.assumptions.forecast_years)}</Descriptions.Item>
          <Descriptions.Item label="每股内在价值（估算）" span={2}>
            {fmtNum(dcf.values.value_per_share)}
          </Descriptions.Item>
          <Descriptions.Item label="企业价值 EV" span={2}>
            {fmtNum(dcf.values.enterprise_value)}
          </Descriptions.Item>
        </Descriptions>
        {dcf.warnings && dcf.warnings.length > 0 ? (
          <Typography.Paragraph type="secondary" style={{ marginTop: 12, marginBottom: 0 }}>
            提示：{dcf.warnings.join("；")}
          </Typography.Paragraph>
        ) : null}
      </div>
    );
  }

  return (
    <div className="vs-dcf-card">
      <Typography.Title level={5} style={{ marginTop: 0, fontWeight: 600 }}>
        {title}
      </Typography.Title>
      <Typography.Text type="secondary">
        未能完成估值
        {dcf.skip_reason ? `（${dcf.skip_reason}）` : ""}
        {dcf.message ? `：${dcf.message}` : ""}
      </Typography.Text>
    </div>
  );
}
