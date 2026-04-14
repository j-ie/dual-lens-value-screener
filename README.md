# dual-lens-value-screener

格雷厄姆 / 巴菲特双视角筛选：FastAPI、`batch-screen` 全 A 拉数（TuShare + AkShare 主备）、**MySQL 持久化历史 Run**、**Redis 分页缓存**、**Vite + React 结果页**。

OpenSpec：`openspec/changes/dual-lens-value-screener/`、`openspec/changes/screening-results-mysql-redis-ui/`；已归档见 `openspec/changes/archive/`。

- 公式与评分梳理（格雷厄姆/巴菲特/第三透镜及仓库内对照说明）：[`doc/公式与评分梳理.md`](doc/公式与评分梳理.md)。

## 安装

```bash
cd dual-lens-value-screener
pip install -e ".[a-share]"
# 详情页 AI 分析（LangChain + 兼容 OpenAI 的模型端点，如字节火山方舟）需额外：
# pip install -e ".[a-share,ai]"
```

- `TUSHARE_TOKEN`：TuShare 必填（[tushare.pro](https://tushare.pro)）。
- `VALUE_SCREENER_PRIMARY`：`tushare` | `akshare`。
- `VALUE_SCREENER_REQUEST_SLEEP`：逐标的间隔秒数，默认 `0.12`（每只标的拉取前 `sleep` 一次；并发时各 worker 独立执行该间隔）。
- `VALUE_SCREENER_TUSHARE_MAX_WORKERS`：TuShare `fetch_snapshots` 并发 worker 数，**默认 `4`**（未配置环境变量时也会并发拉取）。若需完全顺序拉取（与最旧行为一致）请设为 `1`。可调到 `8` 等在**不突破 TuShare 账号每分钟限额**的前提下进一步缩短墙钟时间；过高易限流，需与 `REQUEST_SLEEP` 联调。上限钳制 `64`。服务启动日志会打印一行 `TuShare 拉数并发: max_workers=...` 便于确认是否生效。
- `VALUE_SCREENER_TUSHARE_MAX_RETRIES`：单标的 `_fetch_one` 遇异常时的额外重试次数，默认 `2`（共最多 3 次尝试）；`0` 表示不重试。
- `VALUE_SCREENER_TUSHARE_RETRY_BACKOFF`：重试基础退避秒数，默认 `0.5`，实际等待为 `backoff * 2^attempt`。
- 并发模式下进度回调 `progress_current` 按**完成只数**递增（完成顺序与标的顺序无关）；返回的 `results` / `failures` 仍与请求标的顺序一致。
- 约 2000 积分档：默认已是 `MAX_WORKERS=4` + `REQUEST_SLEEP=0.12`；若仍慢可试 `MAX_WORKERS=8`。若出现频繁限流/重试日志，改为 `MAX_WORKERS=2` 或略增大 `REQUEST_SLEEP`（以 [tushare.pro](https://tushare.pro) 当前 RPM 为准）。
- 若主数据源为 **AkShare**（`VALUE_SCREENER_PRIMARY=akshare` 或 TuShare 不可用），当前实现仍为**单线程逐只**，不会变快；提速请优先保证 TuShare 为主且 token 有效。
- `VALUE_SCREENER_AKSHARE_USE_SYSTEM_PROXY`：设为 `1` 时 AkShare 请求保留 `HTTP(S)_PROXY`；默认不设置（临时去掉环境代理，避免失效代理导致东财 `ProxyError`）。
- `DATABASE_URL`：本机常见 `mysql+pymysql://root:root@127.0.0.1:3306/value_screener`；若用仓库内 `docker-compose` 则为 `screener:screener`（见 `.env.example`）。
- `REDIS_URL`：如 `redis://127.0.0.1:6379/0`（可选，不配置则跳过缓存）。
- `CACHE_TTL_SECONDS`：分页缓存 TTL，默认 `120`。
- **财务快照复用（减重复拉财报）**：配置 `DATABASE_URL` 且未禁用缓存时，批跑/拉数会写入表 `financial_snapshot`，在 TTL 内同标的优先读库。
  - `VALUE_SCREENER_SNAPSHOT_TTL_SECONDS`：快照有效秒数，默认 `86400`（24h）；`<=0` 表示不命中缓存（仍会尝试写入）。
  - `VALUE_SCREENER_SNAPSHOT_CACHE_ENABLED`：设为 `0` / `false` / `no` 时关闭 DB 快照层（始终直连数据源）。
- **综合排序与门槛**（`GET .../results?sort=combined`；权重之和须约等于 1，否则进程启动失败）：
  - `VALUE_SCREENER_COMBINED_WEIGHT_BUFFETT`、`VALUE_SCREENER_COMBINED_WEIGHT_GRAHAM`：默认各 `0.5`。
  - `VALUE_SCREENER_GATE_MIN_BUFFETT`、`VALUE_SCREENER_GATE_MIN_GRAHAM`、`VALUE_SCREENER_GATE_MIN_COMBINED`：可选最低分门槛，留空表示不启用该维。
  - `VALUE_SCREENER_COMBINED_TIEBREAK`：`min_dim`（默认，按 `min(B,G)`）或 `sum_bg`（按 `B+G`）。
- **第三套分与三元综合**（与上面「双维 combined」独立；需迁移 `006_third_lens` 且已落库 `fs_income` 等）：
  - `VALUE_SCREENER_TRIPLE_WEIGHT_BUFFETT`、`VALUE_SCREENER_TRIPLE_WEIGHT_GRAHAM`、`VALUE_SCREENER_TRIPLE_WEIGHT_THIRD`：默认各 `1/3`，三者之和须约等于 1，否则 API 启动失败。
  - `VALUE_SCREENER_THIRD_LENS_WEIGHT_GROWTH`、`VALUE_SCREENER_THIRD_LENS_WEIGHT_VALUATION`：第三套内部「行业内营收增速分位」与「E/P 分位」权重，默认各 `0.5`，之和须约等于 1。
  - 新批跑写入的 `provenance_json` 含 `market_cap`（元），供第三套估值子分使用；历史 Run 若无该字段，估值子分可能为空，三元综合会对 B/G 权重重归一。
  - **混合存储**（迁移 **`011_run_hybrid_facts`**）：`screening_result` 另含 `run_fact_json`（与当次评分一致的快照字段）、可空列 `market_cap` 与 **`pe_ttm`（市盈率 TTM = 市值 / 归属净利润 TTM；亏损或非正利润为 NULL）**，便于排序与详情/AI 上下文引用；与 `provenance_json.market_cap` 同批写入时保持一致。
  - 对某次 Run 计算并回填：`python -m value_screener.cli attach-third-lens --run-id <id>`。
  - 分页 API `sort` 增加 `third_lens`、`triple`（三元综合）；Redis 缓存键已随排序与三元权重指纹变化。
- `REFERENCE_SYNC_API_ENABLED`：设为 `1` / `true` 时允许 `POST /api/v1/reference/sync-stock-basic` 将 `stock_basic` 同步到 MySQL（默认关闭）。
- **公司详情行情缓存**（可选）：`VALUE_SCREENER_DETAIL_QUOTE_TTL_SECONDS` 为 `live_quote` 进程内缓存秒数，默认 `60`；`0` 表示不缓存。`VALUE_SCREENER_DETAIL_QUOTE_TIMEOUT_SECONDS` 为单次 TuShare `daily` 拉取超时，默认 `12`。
- **详情页 AI 分析**（可选）：需 `pip install -e ".[ai]"` 且配置齐全后生效。
  - `VALUE_SCREENER_AI_ENABLED`：设为 `1` / `true` 时启用 `POST .../ai-analysis`。
  - `VALUE_SCREENER_AI_API_KEY`、`VALUE_SCREENER_AI_BASE_URL`、`VALUE_SCREENER_AI_MODEL`：字节火山方舟等兼容 OpenAI Chat Completions 的网关；`BASE_URL` 一般为区域 API 根（如 `https://ark.cn-beijing.volces.com/api/v3`，以控制台为准），`MODEL` 常为推理接入点 ID（如 `ep-...`）。
  - `VALUE_SCREENER_AI_CACHE_TTL_SECONDS`：成功结果缓存秒数，默认 `3600`；`0` 禁用。已配置 `REDIS_URL` 时写入 Redis，否则进程内字典。
  - `VALUE_SCREENER_AI_TIMEOUT_SECONDS`：单次模型调用超时，默认 `120`，范围约 `5～600`。
  - **合规**：AI 输出仅供参考，不构成投资建议；算分仍由确定性规则计算，不由 LLM 改写。
  - **上下文与 DCF**：服务端聚合详情时会 **`include_dcf=True`** 拉取与详情 API 同结构的 `dcf`（若 `VALUE_SCREENER_DCF_ENABLED=1` 且 TuShare 可用）。可能增加单次分析耗时与 TuShare 调用；`dcf.ok=false` 时仅作说明，模型不得编造数值。提示词版本为 `PROMPT_VERSION`（当前 **v4**），与 Redis 缓存键绑定，升级后旧缓存自动失效。
  - **持久化**（需迁移 **`007_company_ai_analysis`**，另见 **`008_company_ai_dcf`**）：成功且校验通过的结构化结果会 **upsert** 到表 `company_ai_analysis`，唯一键 **`(ts_code, analysis_date)`**。`analysis_date` 为 **Asia/Shanghai** 当日日历日（`YYYY-MM-DD`），与 Run 无关；含 **`ai_score`（0～100）**、可选 **`ai_score_rationale`**、摘要与叙述等；并写入当次上下文中的 **`dcf_json`** 及摘要列 **`dcf_ok` / `dcf_headline`**。校验失败返回 **422**，不落库。
  - **排错**：未就绪时接口会返回 **503**，`detail` 中会列出具体缺哪几项（不暴露密钥）。须**同时**满足：`ENABLED` 为真，且 `API_KEY`、`BASE_URL`、`MODEL` 均非空。仅改 `.env.example` 无效，应复制为项目根目录 **`.env`** 并**重启 uvicorn**。用 **`scripts/dev.ps1`** 启动时，API 子窗口会先加载根目录 `.env`（含 AI 变量）；若手动 `uvicorn`，请确保进程能读到上述环境变量。
- **公司详情简化 DCF**（可选）：默认关闭。
  - `VALUE_SCREENER_DCF_ENABLED`：设为 `1` / `true` / `yes` / `on` 时允许 `GET .../detail?include_dcf=1` 计算并返回 `dcf`。
  - `VALUE_SCREENER_DCF_WACC`、`VALUE_SCREENER_DCF_STAGE1_GROWTH`、`VALUE_SCREENER_DCF_TERMINAL_GROWTH`：小数形式默认折现与增长假设。
  - `VALUE_SCREENER_DCF_FORECAST_YEARS`：预测年数（1～20，默认 5）。
  - `VALUE_SCREENER_DCF_WACC_TERMINAL_EPSILON`：WACC 与永续增长率最小利差（防止分母过小）。
  - `VALUE_SCREENER_DCF_TTM_PERIODS`：现金流量表用于滚动加总的最多期数（默认 4）。
  - `VALUE_SCREENER_DCF_WACC_MIN` / `_MAX`、`_STAGE1_G_*`、`_TERMINAL_G_*`：API 查询参数覆盖时的钳制边界。
  - `VALUE_SCREENER_DCF_DAILY_BASIC_TIMEOUT_SECONDS`：拉取 `daily_basic` 股本超时秒数。
  - **分行业口径与回归说明**（算法规格、`dcf_model_revision`、黄金测试索引）：[`docs/dcf-sector-algorithms.md`](docs/dcf-sector-algorithms.md)。

## Docker（MySQL + Redis）

```bash
docker compose up -d
```

根目录 `docker-compose.yml`：MySQL `3306`（库 `value_screener` / 用户 `screener` / 密码 `screener`），Redis `6379`。

## 数据库迁移

在**项目根目录** `dual-lens-value-screener` 下执行（需已 `pip install -e .` 或 `pip install -e ".[a-share]"`，以便安装 Alembic 依赖）。

**Windows PowerShell**（若直接敲 `alembic` 提示无法识别，请一律用下面的 `python -m` 形式）：

```powershell
$env:DATABASE_URL = "mysql+pymysql://root:root@127.0.0.1:3306/value_screener"
python -m alembic upgrade head
```

**cmd / bash**：

```bash
set DATABASE_URL=mysql+pymysql://root:root@127.0.0.1:3306/value_screener
python -m alembic upgrade head
```

（若已将 `Scripts` 加入 PATH，也可使用 `alembic upgrade head`。）

## 一键开发（Windows）

```powershell
.\scripts\dev.ps1
```

假定本机 MySQL/Redis 已就绪（不执行 compose）、尝试 `alembic upgrade`，并新开两个窗口分别运行 **uvicorn :8000** 与 **Vite :5173**。

## HTTP API

```bash
uvicorn value_screener.interfaces.main:app --reload --host 0.0.0.0 --port 8000
```

- `GET /health`
- `GET /v1/examples`、`POST /v1/screen`（手工快照算分）
- `POST /api/v1/runs/batch-screen`：**202 Accepted**，异步批跑入库（主 TuShare / 备 AkShare）。body 可省略或 `{"max_symbols":null}` 表示**全市场**；`{"max_symbols":500}` 表示最多处理 500 只。响应含 `run_id`，请轮询 `GET /api/v1/runs` 或 `GET /api/v1/runs/{id}` 直至 `status` 非 `running`。
- `GET /api/v1/runs`：最近 screening 批次
- `GET /api/v1/runs/{id}`：单个 Run 状态（供轮询）
- `GET /api/v1/runs/{id}/results?page=&page_size=&sort=buffett|graham|combined|third_lens|triple|industry|ai_score&order=asc|desc`：服务端分页排序；`combined` 仍为**双维** B+G 加权及门槛；`triple` 为 **B+G+第三套** 三元综合（列 `final_triple_score`）；`third_lens` 按第三套分列排序；**`ai_score`** 按每条结果关联的**最新一条**落库 AI 分析排序（无则视为 NULL）。可选筛选：**`has_ai_analysis=true`**（仅保留已有 AI 记录）、**`ai_score_min`**（下限，含边界）。Redis 分页键含 AI 相关指纹（`enrich_v4`）。
- `GET /api/v1/runs/{id}/result-industries`：返回该 run 结果集中去重行业列表（JSON `industries`，空行业为 `__EMPTY__`），供前端筛选下拉。
- `GET /api/v1/runs/{id}/companies/{ts_code}/detail`：单公司详情聚合。`ts_code` 为 TuShare 格式（如 `600519.SH`）。响应含 `run_snapshot`（该 run 冻结筛分结果）、`reference`（`security_reference`）、`financials`（`fs_income` / `fs_balance` / `fs_cashflow` 最近若干期摘要）、`live_quote`（独立拉取的 TuShare `daily` 最近一根 K 线，与算分时刻无关）。查询参数：`include_financial_payload`（默认 false）、`financial_limit`（每表条数，默认 12，上限 48）、**`include_persisted_ai=true`**（附带该代码**最新一条**落库 AI 分析，含 `dcf_snapshot`）。
- **简化 DCF（可选）**：`include_dcf=1` 时在 JSON 中增加 `dcf` 对象（需 `VALUE_SCREENER_DCF_ENABLED=1` 且配置 `TUSHARE_TOKEN`）。基于现金流量表滚动汇总代理基期自由现金流、资产负债表粗算净债务，并经 TuShare `daily_basic` 取总股本折算每股价值。可选查询覆盖（范围受环境钳制）：`dcf_wacc`、`dcf_stage1_growth`、`dcf_terminal_growth`。`dcf.ok=false` 时见 `skip_reason` / `message`；结果含 `warnings` / `notes`。**仅供辅助分析，不构成投资建议。**
- `POST /api/v1/runs/{id}/companies/{ts_code}/ai-analysis`：详情页触发的 AI 分析（body 可为 `{}`）。与 detail 同源结构化上下文送入大模型（**含 `dcf` 块**，规则同上）；未启用或配置不全返回 **503**；上游失败 **502**。响应含 `summary`、`key_metrics_commentary`、`risks`、`alignment_with_scores`、`ai_score`、`ai_score_rationale`、`narrative_markdown`、**`dcf_snapshot`**（与落库 `dcf_json` 一致）及 `meta`（`context_hash`、`prompt_version`、`model`、`generated_at`、`cached`、`analysis_date`）。
- `GET /api/v1/company-ai-analyses`：按落库记录分页浏览 AI 历史；支持 `sort`（`analysis_date` | `ai_score` | `ts_code`）、`order`、`analysis_date_from` / `analysis_date_to`、`ai_score_min`、**`industry`** 多选、**`include_dcf=true`**（本页每条含完整 `dcf`）等（详见 OpenAPI）。默认项含 **`dcf_ok` / `dcf_headline`** 摘要列。
- `GET /api/v1/company-ai-analyses/industries`：当前 AI 历史集中去重行业列表（与列表筛选下拉对齐）。
- `GET /api/v1/company-ai-analyses/{id}`：单条落库记录完整结构化字段与 `dcf_snapshot`（供前端展开行等）。
- `POST /api/v1/reference/sync-stock-basic`：将 TuShare `stock_basic` 同步至表 `security_reference`（需 `REFERENCE_SYNC_API_ENABLED=1`）

## 批跑 CLI

```bash
python -m value_screener.cli batch-screen --max-symbols 20 --primary akshare -o out.json --persist
```

`--persist` 将本次 `results` 写入 **新** `screening_run`（历史保留）。需已配置 `DATABASE_URL` 且已迁移。

前端「一键拉数并入库」异步批跑：默认按 `VALUE_SCREENER_BATCH_PERSIST_CHUNK_SIZE`（默认 **50** 只）分块拉数、算分并 **upsert** 至 `screening_result`，运行中即可在结果表分页看到已落库行；设为 **0** 则恢复为结束时一次性写入（运行中列表可能为空）。结果分页在 Run 为 `running` 时不走 Redis 缓存，避免长期命中空页。

同步证券主数据（`stock_basic` → `security_reference`，便于结果表展示名称/行业）：

```powershell
python -m value_screener.cli sync-reference
```

（若已将安装目录下的 `Scripts` 加入 PATH，也可使用 `value-screener sync-reference`。）

需 `TUSHARE_TOKEN` 与 `DATABASE_URL`，建议积分满足 TuShare `stock_basic` 要求。

### 三大财报历史（MySQL）

表 `fs_income`、`fs_balance`、`fs_cashflow`：每行一条报告期（`end_date`，YYYYMMDD），唯一键 `(ts_code, end_date)`。除常用数值列外，`payload` 存 TuShare 接口返回的**整行 JSON**（字段以 TuShare `income` / `balancesheet` / `cashflow` 为准，接口升级时可不迁库仍能读全量）。

**报告频率（年报 / 半年报 / 季报）**：同步时 **不按** `report_type` 或 `end_date` 的月日后缀丢弃数据。只要 `end_date` 落在 `--since-years` 决定的闭区间 `[start, end]` 内（字符串 YYYYMMDD 比较；CLI 默认近 **5** 个日历年），TuShare 返回的季报末（如 `0331`、`0630`、`0930`、`1231`）、半年报、年报行 **一律 upsert**。同一 `end_date` 在不同表各一行；`report_type`、`comp_type` 等元数据写入列并存在于 `payload` 中，便于下游区分合并口径。

与 `financial_snapshot` 的区别：`financial_snapshot` 是批跑用的**时点 TTM 近似快照**（短 TTL 复用）；三张 `fs_*` 表是**按报告期存历史**，供后续深度分析或自建指标，二者互补。

CLI（需已 `alembic upgrade` 到含 `009_ingestion_job` 的版本）：

```powershell
python -m value_screener.cli sync-financial-statements --max-symbols 10
```

- 标的列表优先读 `security_reference` 中 `list_status=L`；若表为空则临时调 TuShare `stock_basic`（该请求也计入分钟限流）。
- **调度日与续跑**：默认调度日为 `VALUE_SCREENER_FS_SYNC_SCHEDULE_TZ`（默认 `Asia/Shanghai`）下的当天日历日；任务键为 `(job_type, scheduled_date, params_hash)`。单标的 **三表事务写库成功** 后推进游标；进程中断后同一调度日再次执行会从下一未完成标的继续。`--scheduled-date YYYY-MM-DD` 指定调度日；`--reset-job` 清除当日同参数游标后从头拉；`--no-resume` 忽略游标从第一只开始（仍写入同一 job 键）。当日任务已 `completed` 时默认跳过（见日志 `skipped_completed`）。
- **全局限流**：财报同步为 **单线程**；`TushareRateLimiter` 按 **`VALUE_SCREENER_FS_SYNC_SCHEDULE_TZ`（默认 `Asia/Shanghai`）的自然分钟** 计数（与 TuShare「每分钟 N 次」口径一致，避免滑动 60 秒在整点两侧叠加仍被平台拦）。`VALUE_SCREENER_TUSHARE_MAX_CALLS_PER_MINUTE` 默认 200；另设 **`VALUE_SCREENER_TUSHARE_RPM_HEADROOM`（默认 15）** 作为安全余量，实际每分钟最多发送 `上限 − 余量` 次（默认约 185）。每次 `income` / `balancesheet` / `cashflow` / `stock_basic` 调用前 `acquire`；若接口仍返回「每分钟最多…次」，睡到下一自然分钟再重试。未使用限流器时仍可用 `VALUE_SCREENER_REQUEST_SLEEP` 作间隔。**`batch-screen` 等快照批跑**若 `VALUE_SCREENER_TUSHARE_MAX_WORKERS>1`，仍可能并发打 TuShare，与财报 CLI 限流无关；全局限流需单进程低并发或后续为批跑接入同一限流器。
- 其它重试：`VALUE_SCREENER_TUSHARE_MAX_RETRIES`、`VALUE_SCREENER_TUSHARE_RETRY_BACKOFF`。`VALUE_SCREENER_TUSHARE_MAX_WORKERS` **不作用于** 本 CLI（批跑快照仍可用）。
- 单标的失败仅记日志与汇总，不中断其余标的；失败标的 **不推进游标**，下次续跑会重试该标的。
- 成功同步时日志含 `end_date_mmdd=`：为三表合并后的 **唯一报告期** 按 `MMDD` 后缀计数，便于核对季末/半年末是否落库（具体条数以 TuShare 披露为准）。
- TuShare 积分与分钟配额说明见仓库内 [`docs/tushare-积分与接口限制说明.md`](docs/tushare-积分与接口限制说明.md)。

对已有 `screening_run` 回填 **第三套分** 与 **三元综合**（需迁移 `006_third_lens`、已同步 `fs_income` 年报、参考表行业）：

```powershell
python -m alembic upgrade head
python -m value_screener.cli attach-third-lens --run-id 1
```

## 前端

```bash
cd frontend
npm install
npm run dev
```

开发态通过 Vite 代理访问 `/api/v1/*`。一键批跑为异步：提交后自动轮询 Run 状态；上限留空表示全市场。表格展示名称、行业/地区、**双维综合分**、**第三套分**、**三元综合分**、数据时间、中文分数解释；选择 Run、**行业多选筛选**、排序（含第三套/三元综合/行业）与分页均走后端。**名称**可点击进入公司详情页（路由 `/runs/:runId/companies/:tsCode`）：展示当前 Run 的冻结快照、独立行情区块、主数据与三大财报摘要。详情页利润表在表格上方提供 **营收（柱）+ 归母净利（线）双轴组合图**（`@ant-design/plots` 的 `DualAxes`，按需懒加载）；数据来自详情 API 的 `financials.income` 标量列，至少两期且柱/线各有有效点时才渲染。详情页提供 **「生成 AI 分析」**（`POST .../ai-analysis`）、免责声明与结构化结果展示（`react-markdown` 渲染叙述）；仅用户点击触发，不自动批跑；展示 **`ai_score`** / 简述 / **`analysis_date`**（与落库一致）。另提供 **「加载 DCF 估值」**：请求 `detail?include_dcf=1` 并展示假设摘要、每股内在价值及相对收盘价的偏离说明（需后端启用 DCF）。筛选结果表支持 **AI 分**列、仅已有 AI、最低 AI 分、按 AI 分排序；独立路由 **`/ai-history`** 浏览全库 AI 分析历史（服务端分页，与上述列表 API 对齐）。

新增独立路由 **`/investment-quality-report`**（导航“投资质量报告”），支持：

- 单公司模式：输入代码生成报告，并可调用 `POST /api/v1/investment-quality/ai-summary` 生成价值投资风格总结；
- 批量模式：选择 run 后加载结果列表，再对选中标的生成“投资大师”总结；
- 总结输出强校验字段：仓位建议、买入触发区间（ideal/acceptable）、退出条件、反方观点、事实依据、判断结论。

“投资大师”总结模板见 `docs/investment-master-skill.md`。

## 测试

```bash
pip install -e ".[dev,a-share]"
python -m unittest discover -s tests -p "test_*.py" -v
```
