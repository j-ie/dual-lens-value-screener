## Why

当前“公司详情”页仅支持 AI 与 DCF 手动触发，缺少“投资质量判断”入口，导致新逻辑无法在最常用的详情场景内直接验证。同时“拉数任务”页缺少“是否值得买入”的聚合视图，运营上无法快速筛出可优先复盘的 run。

## What Changes

- 在公司详情页新增“价值质量判断”模块，支持手动触发并展示结论、低估状态、模块分、理由与风险提示。
- 在拉数任务页新增“是否值得买入”列，展示每个 run 的投资价值聚合结果（如值得买入数量/覆盖数量）。
- 在后端补充 run 级投资质量聚合字段，避免前端 N+1 请求。
- 统一“是否值得买入”判定口径为领域规则，确保详情页、任务页、报告页一致。

## Capabilities

### New Capabilities
- `investment-quality-detail-and-worth-buy`: 在详情页提供投资质量手动评估入口，并在任务页提供 run 级值得买入聚合展示。

### Modified Capabilities
- None.

## Impact

- 前端：`frontend/src/CompanyDetailPage.tsx`、`frontend/src/DataTasksPage.tsx`。
- 后端接口：`src/value_screener/interfaces/investment_quality.py`、`src/value_screener/interfaces/runs.py`（或对应 run 列表接口文件）。
- 领域与应用：新增/复用“值得买入”判定与 run 级聚合逻辑（domain + application/repository）。
- 测试：补充详情页触发与任务页列展示的前后端测试。
