import React from "react";
import ReactDOM from "react-dom/client";
import { ConfigProvider } from "antd";
import zhCN from "antd/locale/zh_CN";
import App from "./App";
import "./global.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <ConfigProvider
      locale={zhCN}
      theme={{
        token: {
          colorPrimary: "#0066cc",
          colorBgLayout: "transparent",
          colorBgContainer: "#ffffff",
          colorText: "#1d1d1f",
          colorTextSecondary: "#6e6e73",
          borderRadius: 12,
          colorBorderSecondary: "rgba(0,0,0,0.08)",
          fontFamily:
            '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, "PingFang SC", "Noto Sans SC", "Noto Sans", sans-serif',
          fontSize: 14,
          lineHeight: 1.5,
        },
        components: {
          Layout: {
            bodyBg: "transparent",
            headerBg: "transparent",
            footerBg: "transparent",
          },
          Table: {
            headerBg: "transparent",
            headerColor: "rgba(0,0,0,0.45)",
            rowHoverBg: "rgba(0,0,0,0.03)",
            borderColor: "transparent",
            headerSplitColor: "transparent",
            cellPaddingBlock: 16,
            cellPaddingInline: 12,
          },
          Card: {
            borderRadiusLG: 18,
          },
          Segmented: {
            trackBg: "rgba(0,0,0,0.06)",
            itemColor: "rgba(0,0,0,0.55)",
            itemHoverColor: "#1d1d1f",
            itemSelectedBg: "#ffffff",
            itemSelectedColor: "#1d1d1f",
          },
          Button: {
            primaryShadow: "0 2px 8px rgba(0, 102, 204, 0.22)",
          },
          Input: {
            activeBorderColor: "#0066cc",
            hoverBorderColor: "rgba(0,0,0,0.2)",
          },
          Select: {
            optionSelectedBg: "rgba(0,102,204,0.08)",
          },
        },
      }}
    >
      <App />
    </ConfigProvider>
  </React.StrictMode>,
);
