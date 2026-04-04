import { memo, useMemo } from "react";
import { DualAxes } from "@ant-design/plots";
import type { DualAxesConfig } from "@ant-design/plots";
import { Typography } from "antd";

type IncomeRow = Record<string, unknown>;

function parseFiniteNumber(v: unknown): number | null {
  if (v === null || v === undefined) {
    return null;
  }
  if (typeof v === "number" && Number.isFinite(v)) {
    return v;
  }
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

/** 升序报告期 + 柱线数据；不满足出图条件时 show 为 false。 */
export function prepareIncomeDualAxesData(income: IncomeRow[]): {
  barData: { period: string; revenue: number }[];
  lineData: { period: string; profit: number }[];
  show: boolean;
} {
  const parsed = income
    .map((r) => {
      const raw = r.end_date;
      if (raw === null || raw === undefined) {
        return null;
      }
      const period = String(raw).trim();
      if (period.length !== 8) {
        return null;
      }
      const revenue = parseFiniteNumber(r.total_revenue);
      const profit = parseFiniteNumber(r.n_income_attr_p);
      if (revenue === null && profit === null) {
        return null;
      }
      return { period, revenue, profit };
    })
    .filter((x): x is NonNullable<typeof x> => x !== null);

  parsed.sort((a, b) => a.period.localeCompare(b.period));

  const barData = parsed
    .filter((r) => r.revenue !== null)
    .map((r) => ({ period: r.period, revenue: r.revenue as number }));
  const lineData = parsed
    .filter((r) => r.profit !== null)
    .map((r) => ({ period: r.period, profit: r.profit as number }));

  const distinctPeriods = new Set(parsed.map((r) => r.period));
  const show =
    distinctPeriods.size >= 2 && barData.length >= 1 && lineData.length >= 1;

  return { barData, lineData, show };
}

type Props = {
  income: IncomeRow[];
};

export const IncomeComboChart = memo(function IncomeComboChart({ income }: Props) {
  const { barData, lineData, show } = useMemo(() => prepareIncomeDualAxesData(income), [income]);

  const config = useMemo((): DualAxesConfig => {
    return {
      autoFit: true,
      height: 320,
      legend: { color: { position: "top", layout: { justifyContent: "center" } } },
      children: [
        {
          data: barData,
          type: "interval",
          xField: "period",
          yField: "revenue",
          axis: { y: { title: "营业收入" } },
          style: { maxWidth: 28 },
          interaction: { elementHighlight: { background: true } },
        },
        {
          data: lineData,
          type: "line",
          xField: "period",
          yField: "profit",
          axis: { y: { position: "right", title: "归母净利润" } },
          style: { lineWidth: 2 },
          interaction: { tooltip: { crosshairs: false } },
        },
      ],
    };
  }, [barData, lineData]);

  if (!show) {
    return null;
  }

  return (
    <div style={{ marginBottom: 16 }}>
      <Typography.Paragraph type="secondary" style={{ marginBottom: 8 }}>
        趋势（报告期由旧到新）：柱为营收，线为归母净利；双轴刻度独立，与算分快照无关。
      </Typography.Paragraph>
      <DualAxes {...config} />
    </div>
  );
});
