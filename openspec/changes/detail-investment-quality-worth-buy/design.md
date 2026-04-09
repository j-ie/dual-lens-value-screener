## Context

现有系统已具备投资质量算法与独立投资质量页面，但公司详情页仍缺少投资质量手动触发入口，导致“从 run 结果进入单公司复盘”的主路径不完整。与此同时，拉数任务列表只展示 run 进度与后置流水线状态，缺少“值不值得买入”的业务聚合视角，无法快速定位高价值 run。

该改造同时涉及前端页面、后端接口返回结构、领域判定口径与 run 聚合查询，属于跨模块改动，需要先明确统一判定规则与接口契约。

## Goals / Non-Goals

**Goals:**
- 在公司详情页增加投资质量手动触发与结构化展示，支持与 AI、DCF 并行使用。
- 在拉数任务页新增“是否值得买入”列，展示 run 级聚合值（例如 worth_buy_count / total_count）。
- 将“是否值得买入”定义为领域判定，输出稳定机器字段与中文标签字段，供多页面复用。
- 保持现有投资质量页与报告页接口兼容，不破坏现有工作流。

**Non-Goals:**
- 不改造投资质量打分算法本身（总分与模块分逻辑不变）。
- 不在本次引入新的行情/财务数据源。
- 不在任务页引入复杂多维筛选面板，仅新增关键列展示。

## Decisions

### Decision 1: 详情页直接复用投资质量单公司接口
- **Choice:** 公司详情页调用现有 `POST /api/v1/investment-quality/single`，使用详情页的 `ts_code`（及可选行业）进行手动评估。
- **Rationale:** 降低接口重复建设，确保与投资质量页口径一致。
- **Alternative:** 新增详情页专用接口。Rejected，因为会产生重复契约与维护成本。

### Decision 2: “是否值得买入”定义为领域规则并标准化输出
- **Choice:** 在领域层提供 `is_worth_buy` 判定与 `worth_buy_label_zh`、`worth_buy_reason_codes`。
- **Rationale:** 避免前端散落业务 if-else，确保详情页/任务页/报告页一致。
- **Alternative:** 前端自行按 decision 与低估标记推断。Rejected，因为规则难以统一和演进。

### Decision 3: 任务页采用 run 级聚合字段避免 N+1
- **Choice:** 在 run 列表接口中返回 `investment_quality_summary`（如 `worth_buy_count`、`analyzed_count`）。
- **Rationale:** 任务页主表展示必须低延迟，不应逐行再查 results。
- **Alternative:** 前端逐 run 请求 results 计算。Rejected，因为性能差且请求风暴风险高。

### Decision 4: v1 以“数量+标签”呈现任务页买入信息
- **Choice:** 新增列显示“值得买入 X / Y”，并提供 tooltip 说明判定口径。
- **Rationale:** 信息密度高、改动小、便于运营快速判断。
- **Alternative:** 直接显示明细名单。Rejected，因为会挤占表格空间且影响可读性。

## Risks / Trade-offs

- **[Risk] 不同页面读取字段不一致** -> **Mitigation:** 统一使用领域输出字段命名，新增契约测试覆盖。
- **[Risk] run 级聚合查询增加数据库压力** -> **Mitigation:** 仅对当前分页 run 计算聚合，必要时在 repository 做轻量缓存。
- **[Risk] 用户误将“值得买入”视为确定建议** -> **Mitigation:** 详情与任务页均保留免责声明，并展示风险标记摘要。
- **[Risk] 历史 run 缺失投资质量字段** -> **Mitigation:** 对缺失数据回退显示 `—`，不阻断页面渲染。

## Migration Plan

1. 增加领域层“worth-buy 判定”输出模型与辅助函数。
2. 将该字段接入投资质量结果构建流程，确保单公司与批量结果均可产出。
3. 扩展 run 列表接口返回聚合字段，兼容旧 run（可空）。
4. 前端详情页增加“价值质量判断（手动触发）”区块。
5. 前端拉数任务页增加“是否值得买入”列和说明。
6. 增加前后端测试并进行 UI smoke 验证。
7. 回滚策略：前端隐藏新增区块/列，后端保持字段向后兼容不影响旧页面。

## Open Questions

- v1 的判定阈值是否需要环境变量化（如高风险条数阈值）？
- run 聚合是否只统计 `status=success` run，还是允许 running run 展示实时估算值？
- 任务页列是否需要下钻跳转到“投资质量报告”并自动带 run 过滤？
