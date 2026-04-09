import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import InvestmentQualityReportPage from "./InvestmentQualityReportPage";

describe("InvestmentQualityReportPage", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("can generate single company AI summary", async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes("/api/v1/investment-quality/ai-summary")) {
        return new Response(
          JSON.stringify({
            ai_summary: {
              conclusion: "结论：继续跟踪。",
              valuation_view: "估值中性，等待更好安全边际。",
              position_advice: "观察仓",
              buy_trigger_zone: { ideal: "回撤20%", acceptable: "回撤10%" },
              exit_conditions: ["基本面恶化"],
              counter_arguments: ["行业竞争加剧"],
              watch_items: ["现金流质量"],
              facts: ["总分来自规则计算"],
              judgments: ["当前更适合观察"],
              confidence: "medium",
              disclaimer: "仅供个人投研复盘，不构成投资建议。",
            },
          }),
          { status: 200 },
        );
      }
      return new Response(JSON.stringify({ items: [], total: 0 }), { status: 200 });
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<InvestmentQualityReportPage />);
    fireEvent.change(screen.getByPlaceholderText("输入代码，如 600519.SH"), {
      target: { value: "600519.SH" },
    });
    fireEvent.click(screen.getByRole("button", { name: "生成单公司报告" }));

    await waitFor(() => {
      expect(screen.getByText(/结论：继续跟踪/)).toBeInTheDocument();
      expect(screen.getByText(/观察仓/)).toBeInTheDocument();
    });
  });
});
