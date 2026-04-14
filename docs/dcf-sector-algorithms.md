# 简化 DCF：按 `DcfSectorKind` 的算法规格

本文描述公司详情 API 中 `build_company_dcf_payload` / `compute_dcf` 的**意图口径**，与代码中的 `DcfSectorKind`（`value_screener.domain.dcf_sector_policy`）对齐。数值结果以程序为准；变更算法时请同步更新本文与 **`DCF_MODEL_REVISION`**（`value_screener.domain.dcf`）。

**免责声明**：以下为教学式机械化估算，不构成投资建议。

## 公共部分

- **折现内核**：两阶段 FCFF——预测期按固定 `stage1_growth` 复利；终值 Gordon；`enterprise_value = PV(预测) + PV(终值)`；`equity_value = EV − net_debt_applied`（金融业见下）；`value_per_share = equity_value / shares`。
- **载荷审计**：成功或带 `assumptions` 的失败响应均含 `assumptions.dcf_model_revision`，便于日志、前端与后续 LLM 对照。
- **预测期 g 来源**（未传查询覆盖时）：见 `assumptions.stage1_growth_source`（`inferred_net_income_cagr` / `inferred_ocf_cagr` / `default_sector` / `default_cyclical` / `query_override`）；推断逻辑见 `dcf_stage1_growth_infer.py`，可用环境变量关闭推断。

## `general`（一般工商业）

- **现金流基数**：现金流量表年报或 TTM 代理（`aggregate_ocf_and_capex_proxy_ttm`），经营现金流减投资流出代理为 FCF 基数。
- **净债务**：优先 **有息类科目合计 − 货币资金**；若无明细则 **负债合计 − 货币资金**（`dcf_net_debt_resolve`）。
- **EV→E**：扣减上述净债务。

## `financial`（银行、证券、保险等）

- **现金流基数**：最近年报 **归母净利润 × financial_ni_base_scale**（默认 0.35），粗代理派息与留存。
- **净债务 proxy**：表内 **有息科目 − 货币资金** 写入 `balance_sheet_net_debt_proxy`，**不参与** EV→E 扣减（`net_debt_applied = 0`），避免与利润基数错配导致负每股价值。
- **EV→E**：折现结果按 **股权价值** 理解，不再扣有息 proxy。

## `real_estate`（地产链）

- **现金流基数**：同 `general`（现金流 TTM/年报代理）。
- **净债务**：**负债合计 − 合同负债（若可得）− 货币资金**；无 `contract_liab` 时回退为负债合计 − 货币资金并告警。
- **EV→E**：扣减上述净债务。

## `cyclical`（强周期）

- **现金流基数**：同 `general`。
- **净债务**：同 `general`（当前实现未单独改有息优先策略以外的周期逻辑）。
- **默认增长**：未覆盖查询参数时使用 **更保守** 的 `default_cyclical_stage1_growth` / `default_cyclical_terminal_growth`（环境变量可配）。
- **推断 g**：**关闭** 历史 CAGR 推断，避免景气顶外推。

## 黄金回归测试（合成数据）

| 分档 | 主要覆盖 | 测试模块 |
|------|----------|----------|
| GENERAL 有息 / 回退 | `resolve_net_debt_for_sector`、`build_company_dcf_payload` | `tests/test_dcf_net_debt_resolve.py`、`tests/test_dcf_company_valuation.py` |
| FINANCIAL | 归母净利基数、股权桥、大负债 proxy | `tests/test_dcf_company_valuation.py` |
| REAL_ESTATE | 合同负债净债务 | `tests/test_dcf_net_debt_resolve.py`、`tests/test_dcf_company_valuation.py`（`test_real_estate_contract_liab_payload`） |
| CYCLICAL | 保守默认 g、周期说明 | `tests/test_dcf_company_valuation.py` |
| 推断 stage1 | CAGR | `tests/test_dcf_stage1_growth_infer.py` |
| 行业映射 | TuShare 行业 → 分档 | `tests/test_dcf_sector_policy.py` |
