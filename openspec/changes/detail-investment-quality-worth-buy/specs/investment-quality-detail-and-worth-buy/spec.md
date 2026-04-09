## ADDED Requirements

### Requirement: Company detail page SHALL support manual investment-quality evaluation
系统在公司详情页 MUST 提供“价值质量判断（手动触发）”能力，用户可对当前公司代码触发投资质量计算，并看到结构化结果。

#### Scenario: Manual trigger in company detail succeeds
- **WHEN** 用户在公司详情页点击“立即评估”并且后端成功返回投资质量结果
- **THEN** 页面 MUST 展示 `decision_label_zh`、`is_undervalued`、`total_score`、`module_scores`、`reasons` 与 `risk_flags`

#### Scenario: Manual trigger in company detail fails
- **WHEN** 用户在公司详情页触发投资质量计算且接口调用失败
- **THEN** 页面 MUST 展示可理解的错误提示并允许用户再次触发

### Requirement: Worth-buy decision SHALL be a unified domain rule output
系统 MUST 在投资质量输出中包含统一口径的“是否值得买入”判定字段，供详情页、任务页与报告页复用。

#### Scenario: Domain output includes worth-buy fields
- **WHEN** 系统完成任一标的的投资质量计算
- **THEN** 返回结果 MUST 包含 `is_worth_buy`、`worth_buy_label_zh` 与 `worth_buy_reason_codes`

### Requirement: Data tasks page SHALL display run-level worth-buy summary
系统在拉数任务页 MUST 新增“是否值得买入”列，用于展示 run 级聚合结果，并避免逐行二次请求。

#### Scenario: Run list shows worth-buy summary for analyzed run
- **WHEN** 用户打开拉数任务页且某 run 已有投资质量分析结果
- **THEN** 页面 MUST 在该行显示 worth-buy 聚合值（例如 `worth_buy_count / analyzed_count`）

#### Scenario: Run list handles missing worth-buy summary
- **WHEN** run 不存在投资质量聚合数据（例如历史 run）
- **THEN** 页面 MUST 显示空值占位且不影响其余列渲染
