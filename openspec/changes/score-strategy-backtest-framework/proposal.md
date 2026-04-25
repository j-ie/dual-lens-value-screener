## Why

当前投资质量分数策略缺少统一、可复现、无未来函数的回测框架，无法稳定验证分数与未来收益之间的有效性，也难以对调参结果进行客观评估。随着策略能力扩展，需要尽快建立标准化回测闭环，支撑策略迭代与上线决策。

## What Changes

- 新增分数策略回测能力，支持按历史时点重建特征并执行组合回测。
- 新增分层验证能力，支持按分数分位输出分层收益与单调性结果。
- 新增指标评估能力，统一输出年化收益、超额收益、回撤、换手、IC/RankIC 等核心指标。
- 新增回测配置能力，支持调仓频率、持有期、交易成本、股票池过滤与基准设置。
- 新增结果落库与查询能力，支持回测任务元数据、参数、关键指标与曲线摘要持久化。

## Capabilities

### New Capabilities
- `score-strategy-backtest`: 以时点一致数据执行分数策略回测，输出策略净值与评估指标。

### Modified Capabilities
- （无）

## Impact

- Affected code:
  - `src/value_screener/application/`：新增回测编排与指标计算应用服务。
  - `src/value_screener/domain/`：新增回测领域对象、组合构建与绩效评估规则。
  - `src/value_screener/infrastructure/`：新增历史行情/特征读取与结果持久化仓储。
  - `src/value_screener/interfaces/`：新增回测任务触发与结果查询接口（CLI/API）。
- Data model:
  - 新增回测任务与结果表（任务参数、状态、关键指标、时间序列摘要）。
- Dependencies:
  - 复用现有投资质量分数计算逻辑与历史财务数据源，不引入外部服务依赖作为首版前置条件。
