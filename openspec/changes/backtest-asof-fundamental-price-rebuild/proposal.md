## Why

当前回测引擎依赖 `financial_snapshot.fetched_at` 作为历史时点来源，导致可回测区间受“近期是否跑过快照任务”限制，无法支撑多年策略研究。要验证分数策略的长期有效性，必须改为“财报 + 历史行情”按时点重建，摆脱快照沉淀长度约束。

## What Changes

- 将回测时点构建从快照日期驱动改为历史交易日驱动，支持长区间回测。
- 引入财报 `as-of` 重建能力：按交易日 T 读取 T 前可见财报（含公告可见性规则）。
- 引入历史行情/估值口径重建能力：按交易日计算收益、基准对比与调仓绩效。
- 增强回测诊断与错误提示，输出样本覆盖率、可用日期覆盖和缺失原因分布。
- 对回测 API/前端增加数据可用性提示，避免用户提交超出数据覆盖区间的任务。

## Capabilities

### New Capabilities
- `backtest-asof-reconstruction`: 基于历史财报与历史行情做时点一致重建并驱动回测执行。

### Modified Capabilities
- （无）

## Impact

- Affected code:
  - `src/value_screener/application/`：重构回测引擎时点构建与收益计算流程。
  - `src/value_screener/infrastructure/`：新增历史行情读取仓储与财报 as-of 查询能力。
  - `src/value_screener/domain/`：补充时点重建、样本过滤与诊断领域对象。
  - `src/value_screener/interfaces/`：增强回测任务接口返回的数据覆盖提示与错误语义。
  - `frontend/src/`：增加日期覆盖提示与可用区间反馈。
- Data model:
  - 可能新增历史行情缓存/日线表（若现有表不足以支撑日频收益回放）。
- Dependencies:
  - 复用现有财报表 `fs_income/fs_balance/fs_cashflow` 与已有数据同步链路；必要时补充日线行情拉取任务。
