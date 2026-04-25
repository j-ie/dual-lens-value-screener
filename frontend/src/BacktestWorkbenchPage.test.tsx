import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router-dom";

import BacktestWorkbenchPage from "./BacktestWorkbenchPage";

describe("BacktestWorkbenchPage", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("renders list and supports detail interaction", async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.includes("/api/v1/backtests/coverage")) {
        return new Response(JSON.stringify({ ok: true, start_date: "20240101", end_date: "20241231" }), {
          status: 200,
        });
      }
      if (url.includes("/api/v1/backtests?limit=200")) {
        return new Response(
          JSON.stringify([
            {
              id: 1,
              external_uuid: "u1",
              strategy_name: "investment_quality_score",
              status: "pending",
              created_at: "2026-04-01T00:00:00Z",
              started_at: null,
              finished_at: null,
              error: "",
              config: {
                strategy_name: "investment_quality_score",
                start_date: "2024-01-01",
                end_date: "2024-12-31",
                rebalance_frequency: "monthly",
                holding_period_days: 20,
                top_n: null,
                top_quantile: 0.2,
                benchmark: "000300.SH",
                transaction_cost_bps: 15,
                filters: {},
              },
            },
          ]),
          { status: 200 },
        );
      }
      if (url.includes("/api/v1/backtests/1")) {
        return new Response(
          JSON.stringify({
            job: {
              id: 1,
              status: "pending",
              config: {
                strategy_name: "investment_quality_score",
                start_date: "2024-01-01",
                end_date: "2024-12-31",
                rebalance_frequency: "monthly",
                holding_period_days: 20,
                top_n: null,
                top_quantile: 0.2,
                benchmark: "000300.SH",
                transaction_cost_bps: 15,
                filters: {},
              },
              meta: {},
            },
            result: null,
          }),
          { status: 200 },
        );
      }
      if (init?.method === "POST" && url.endsWith("/api/v1/backtests")) {
        return new Response(JSON.stringify({ job_id: 2, external_uuid: "u2", status: "pending" }), {
          status: 202,
        });
      }
      if (init?.method === "POST" && url.includes("/execute")) {
        return new Response(JSON.stringify({ job_id: 1, external_uuid: "u1", status: "running" }), {
          status: 200,
        });
      }
      return new Response("not-found", { status: 404 });
    });
    vi.stubGlobal("fetch", fetchMock);

    render(
      <MemoryRouter>
        <BacktestWorkbenchPage />
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByText("回测工作台")).toBeInTheDocument();
      expect(screen.getAllByText("investment_quality_score").length).toBeGreaterThan(0);
      expect(screen.getByText(/可用回测行情区间/)).toBeInTheDocument();
    });

    await userEvent.click(screen.getByText("查看详情"));
    await waitFor(() => {
      expect(screen.getByText("当前任务尚未产出结果。可点击任务列表中的“执行”或等待运行完成。")).toBeInTheDocument();
    });

    await waitFor(() => {
      const hitDetail = fetchMock.mock.calls.some((args) => String(args[0]).includes("/api/v1/backtests/1"));
      expect(hitDetail).toBe(true);
    });
  });
});

