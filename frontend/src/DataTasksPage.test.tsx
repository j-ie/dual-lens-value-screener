import { render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router-dom";

import DataTasksPage from "./DataTasksPage";

describe("DataTasksPage", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("renders worth-buy summary column", async () => {
    const fetchMock = vi.fn(async () => {
      return new Response(
        JSON.stringify([
          {
            id: 1,
            external_uuid: "u-1",
            status: "success",
            created_at: "2026-04-08T00:00:00Z",
            finished_at: "2026-04-08T00:10:00Z",
            universe_size: 100,
            snapshot_ok: 98,
            snapshot_failed: 2,
            provider_label: "tushare",
            investment_quality_summary: {
              worth_buy_count: 12,
              analyzed_count: 98,
              label_zh: "值得买入 12/98",
            },
          },
        ]),
        { status: 200 },
      );
    });
    vi.stubGlobal("fetch", fetchMock);

    render(
      <MemoryRouter>
        <DataTasksPage />
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getAllByText("是否值得买入").length).toBeGreaterThan(0);
      expect(screen.getByText("值得买入 12/98")).toBeInTheDocument();
    });
  });
});
