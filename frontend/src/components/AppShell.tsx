import type { ReactNode } from "react";
import { Layout, Segmented, Typography } from "antd";
import { useLocation, useNavigate } from "react-router-dom";

const { Content } = Layout;

function mainSegmentFromPath(
  pathname: string,
): "screening" | "ai-history" | "data-tasks" | "backtests" {
  if (pathname.startsWith("/investment-quality-report") || pathname.startsWith("/investment-quality")) {
    return "data-tasks";
  }
  if (pathname.startsWith("/backtests")) {
    return "backtests";
  }
  if (pathname.startsWith("/ai-history")) {
    return "ai-history";
  }
  if (pathname.startsWith("/data-tasks")) {
    return "data-tasks";
  }
  return "screening";
}

export function AppShell({ children }: { children: ReactNode }) {
  const loc = useLocation();
  const navigate = useNavigate();
  const active = mainSegmentFromPath(loc.pathname);

  return (
    <Layout className="vs-layout-root">
      <header className="vs-sticky-top">
        <div className="vs-top-inner">
          <div className="vs-top-row">
            <Typography.Title level={4} className="vs-brand-title">
              价值筛选
            </Typography.Title>
            <Typography.Text type="secondary" className="vs-brand-sub">
              双视角评分 · AI 解读 · DCF 快照
            </Typography.Text>
          </div>
          <div className="vs-segmented-wrap">
            <Segmented
              className="vs-main-segmented"
              size="large"
              value={active}
              onChange={(v) => {
                const key = String(v);
                if (key === "screening") {
                  navigate("/");
                } else if (key === "data-tasks") {
                  navigate("/data-tasks");
                } else if (key === "backtests") {
                  navigate("/backtests");
                } else {
                  navigate("/ai-history");
                }
              }}
              options={[
                { label: "筛选结果", value: "screening" },
                { label: "拉数任务", value: "data-tasks" },
                { label: "回测工作台", value: "backtests" },
                { label: "AI 分析历史", value: "ai-history" },
              ]}
            />
          </div>
        </div>
      </header>
      <Content className="vs-layout-content">
        <div className="vs-app-content">
          <div className="vs-workspace">{children}</div>
        </div>
      </Content>
    </Layout>
  );
}
