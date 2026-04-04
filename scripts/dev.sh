#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "==> skip docker compose (use local MySQL:3306 Redis:6379; match DATABASE_URL/REDIS_URL)"

export DATABASE_URL="${DATABASE_URL:-mysql+pymysql://root:root@127.0.0.1:3306/value_screener}"
export REDIS_URL="${REDIS_URL:-redis://127.0.0.1:6379/0}"
export PYTHONPATH="$ROOT/src"

echo "==> alembic upgrade"
sleep 2
alembic upgrade head || echo "warn: alembic failed, run manually when MySQL is ready"

echo "==> start API (8000) and Vite (5173) — run in two terminals:"
echo "  cd \"$ROOT\" && PYTHONPATH=src DATABASE_URL=\"$DATABASE_URL\" REDIS_URL=\"$REDIS_URL\" python -m uvicorn value_screener.interfaces.main:app --reload --host 0.0.0.0 --port 8000"
echo "  cd \"$ROOT/frontend\" && npm install && npm run dev"
