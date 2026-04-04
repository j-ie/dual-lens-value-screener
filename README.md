# dual-lens-value-screener

格雷厄姆 / 巴菲特双视角筛选：FastAPI、`batch-screen` 全 A 拉数（TuShare + AkShare 主备）、**MySQL 持久化历史 Run**、**Redis 分页缓存**、**Vite + React 结果页**。

OpenSpec：`openspec/changes/dual-lens-value-screener/`、`openspec/changes/screening-results-mysql-redis-ui/`；已归档见 `openspec/changes/archive/`。

## 安装

```bash
cd dual-lens-value-screener
pip install -e ".[a-share]"
```

- `TUSHARE_TOKEN`：TuShare 必填（[tushare.pro](https://tushare.pro)）。
- `VALUE_SCREENER_PRIMARY`：`tushare` | `akshare`。
- `VALUE_SCREENER_REQUEST_SLEEP`：逐标的间隔秒数，默认 `0.12`。
- `VALUE_SCREENER_AKSHARE_USE_SYSTEM_PROXY`：设为 `1` 时 AkShare 请求保留 `HTTP(S)_PROXY`；默认不设置（临时去掉环境代理，避免失效代理导致东财 `ProxyError`）。
- `DATABASE_URL`：本机常见 `mysql+pymysql://root:root@127.0.0.1:3306/value_screener`；若用仓库内 `docker-compose` 则为 `screener:screener`（见 `.env.example`）。
- `REDIS_URL`：如 `redis://127.0.0.1:6379/0`（可选，不配置则跳过缓存）。
- `CACHE_TTL_SECONDS`：分页缓存 TTL，默认 `120`。
- `REFERENCE_SYNC_API_ENABLED`：设为 `1` / `true` 时允许 `POST /api/v1/reference/sync-stock-basic` 将 `stock_basic` 同步到 MySQL（默认关闭）。

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
- `GET /api/v1/runs/{id}/results?page=&page_size=&sort=buffett|graham&order=asc|desc`：服务端分页排序；条目含公司名称、行业/地区、中文分数解释、数据锚定时间等（默认 Buffett 降序 + `symbol` 升序 tie-break）
- `POST /api/v1/reference/sync-stock-basic`：将 TuShare `stock_basic` 同步至表 `security_reference`（需 `REFERENCE_SYNC_API_ENABLED=1`）

## 批跑 CLI

```bash
python -m value_screener.cli batch-screen --max-symbols 20 --primary akshare -o out.json --persist
```

`--persist` 将本次 `results` 写入 **新** `screening_run`（历史保留）。需已配置 `DATABASE_URL` 且已迁移。

同步证券主数据（`stock_basic` → `security_reference`，便于结果表展示名称/行业）：

```powershell
python -m value_screener.cli sync-reference
```

（若已将安装目录下的 `Scripts` 加入 PATH，也可使用 `value-screener sync-reference`。）

需 `TUSHARE_TOKEN` 与 `DATABASE_URL`，建议积分满足 TuShare `stock_basic` 要求。

## 前端

```bash
cd frontend
npm install
npm run dev
```

开发态通过 Vite 代理访问 `/api/v1/*`。一键批跑为异步：提交后自动轮询 Run 状态；上限留空表示全市场。表格展示名称、行业/地区、数据时间、中文分数解释；选择 Run、排序与分页均走后端。

## 测试

```bash
pip install -e ".[dev,a-share]"
python -m unittest discover -s tests -p "test_*.py" -v
```
