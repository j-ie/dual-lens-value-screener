## 1. 领域规则与接口契约

- [x] 1.1 新增/复用领域层 worth-buy 判定函数，统一输出 `is_worth_buy`、`worth_buy_label_zh`、`worth_buy_reason_codes`
- [x] 1.2 将 worth-buy 字段接入投资质量单标的结果构建流程，确保详情页可直接消费
- [x] 1.3 扩展 run 列表接口返回 `investment_quality_summary`（含 worth_buy_count/analyzed_count 等聚合字段）

## 2. 公司详情页改造

- [x] 2.1 在 `CompanyDetailPage` 新增“价值质量判断（手动触发）”区块与 loading/error 状态
- [x] 2.2 复用 `/api/v1/investment-quality/single` 实现详情页手动触发并渲染结构化结果
- [x] 2.3 在详情页展示 worth-buy 相关字段与中文标签，保持与投资质量页口径一致

## 3. 拉数任务页新增“是否值得买入”列

- [x] 3.1 在 `DataTasksPage` 的 run 表格增加“是否值得买入”列及空值兜底显示
- [x] 3.2 将 run 级聚合值格式化为可读展示（如 `worth_buy_count / analyzed_count`）并增加 tooltip 说明
- [x] 3.3 确保新增列不影响现有轮询、后置任务和批次管理交互

## 4. 测试与验证

- [x] 4.1 增加后端测试覆盖 worth-buy 字段输出与 run 聚合接口字段完整性
- [x] 4.2 增加前端测试覆盖详情页手动触发与任务页新列展示
- [x] 4.3 进行页面级 smoke 验证，确认投资质量页、报告页、任务页兼容
