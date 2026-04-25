from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from math import sqrt
from typing import Any

from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, ProgrammingError

from value_screener.application.backtest_service import BacktestExecutor
from value_screener.application.screening_service import ScreeningApplicationService
from value_screener.domain.backtest import BacktestConfig
from value_screener.domain.snapshot import StockFinancialSnapshot
from value_screener.infrastructure.asof_fundamental_repository import AsOfFundamentalRepository
from value_screener.infrastructure.settings import AShareIngestionSettings
from value_screener.infrastructure.historical_price_repository import HistoricalPriceRepository
from value_screener.infrastructure.screening_schema import financial_snapshot


@dataclass(frozen=True, slots=True)
class SignalRow:
    symbol: str
    score: float
    decision: str
    market_cap: float


@dataclass(frozen=True, slots=True)
class FeatureBuildOutput:
    snapshots: list[StockFinancialSnapshot]
    exclusions: list[dict[str, str]]


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _to_utc_end_of_day(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=timezone.utc)


def _required_snapshot_fields(s: StockFinancialSnapshot) -> list[str]:
    miss: list[str] = []
    if s.market_cap <= 0:
        miss.append("market_cap")
    return miss


def _generate_rebalance_dates(start_date: date, end_date: date, freq: str) -> list[date]:
    if start_date > end_date:
        return []
    freq_key = (freq or "monthly").strip().lower()
    step_days = 30
    if freq_key in ("weekly", "week"):
        step_days = 7
    elif freq_key in ("quarterly", "quarter"):
        step_days = 90
    out: list[date] = []
    cur = start_date
    while cur <= end_date:
        out.append(cur)
        cur = cur + timedelta(days=step_days)
    if out[-1] != end_date:
        out.append(end_date)
    return out


def _mean(vals: list[float]) -> float:
    if not vals:
        return 0.0
    return sum(vals) / float(len(vals))


def _stdev(vals: list[float]) -> float:
    if len(vals) < 2:
        return 0.0
    mu = _mean(vals)
    var = sum((x - mu) ** 2 for x in vals) / float(len(vals) - 1)
    return sqrt(max(var, 0.0))


def _max_drawdown(nav_curve: list[float]) -> float:
    if not nav_curve:
        return 0.0
    peak = nav_curve[0]
    mdd = 0.0
    for v in nav_curve:
        peak = max(peak, v)
        if peak <= 0:
            continue
        dd = (peak - v) / peak
        mdd = max(mdd, dd)
    return mdd


def _corr(x: list[float], y: list[float]) -> float:
    n = min(len(x), len(y))
    if n < 2:
        return 0.0
    xx = x[:n]
    yy = y[:n]
    mx = _mean(xx)
    my = _mean(yy)
    cov = sum((xx[i] - mx) * (yy[i] - my) for i in range(n))
    sx = _stdev(xx)
    sy = _stdev(yy)
    if sx <= 0 or sy <= 0:
        return 0.0
    return cov / float((n - 1) * sx * sy)


def _rank(vals: list[float]) -> list[float]:
    pairs = sorted((v, i) for i, v in enumerate(vals))
    ranks = [0.0] * len(vals)
    for r, (_, idx) in enumerate(pairs, start=1):
        ranks[idx] = float(r)
    return ranks


class HistoricalFeatureBuilder:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def build_as_of(self, as_of: date, symbols: list[str] | None = None) -> FeatureBuildOutput:
        cutoff = _to_utc_end_of_day(as_of)
        stmt = (
            select(
                financial_snapshot.c.symbol,
                financial_snapshot.c.snapshot_json,
                financial_snapshot.c.fetched_at,
            )
            .where(financial_snapshot.c.fetched_at <= cutoff)
            .order_by(financial_snapshot.c.symbol.asc(), financial_snapshot.c.fetched_at.desc())
        )
        if symbols:
            stmt = stmt.where(financial_snapshot.c.symbol.in_(symbols))
        latest_by_symbol: dict[str, dict[str, Any]] = {}
        with self._engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()
        for row in rows:
            sym = str(row.symbol)
            if sym in latest_by_symbol:
                continue
            payload = row.snapshot_json if isinstance(row.snapshot_json, dict) else {}
            if not payload:
                continue
            latest_by_symbol[sym] = payload
        snaps: list[StockFinancialSnapshot] = []
        exclusions: list[dict[str, str]] = []
        for sym, payload in latest_by_symbol.items():
            try:
                snap = StockFinancialSnapshot.model_validate(payload)
            except Exception:  # noqa: BLE001
                exclusions.append({"symbol": sym, "reason": "invalid_snapshot_payload"})
                continue
            missing = _required_snapshot_fields(snap)
            if missing:
                exclusions.append({"symbol": sym, "reason": f"missing_fields:{','.join(missing)}"})
                continue
            snaps.append(snap)
        return FeatureBuildOutput(snapshots=snaps, exclusions=exclusions)

    def list_available_dates(self, start_date: date, end_date: date, symbols: list[str] | None = None) -> list[date]:
        stmt = select(financial_snapshot.c.fetched_at).order_by(financial_snapshot.c.fetched_at.asc())
        if symbols:
            stmt = stmt.where(financial_snapshot.c.symbol.in_(symbols))
        out: list[date] = []
        seen: set[date] = set()
        with self._engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()
        for row in rows:
            dt = row.fetched_at
            if dt is None:
                continue
            d = dt.date()
            if d < start_date or d > end_date:
                continue
            if d in seen:
                continue
            seen.add(d)
            out.append(d)
        return out

    def snapshot_date_bounds(self, symbols: list[str] | None = None) -> tuple[date | None, date | None]:
        stmt = select(financial_snapshot.c.fetched_at)
        if symbols:
            stmt = stmt.where(financial_snapshot.c.symbol.in_(symbols))
        with self._engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()
        if not rows:
            return None, None
        ds = [r.fetched_at.date() for r in rows if r.fetched_at is not None]
        if not ds:
            return None, None
        return min(ds), max(ds)


def _select_rebalance_dates_from_available(available: list[date], freq: str) -> list[date]:
    if not available:
        return []
    freq_key = (freq or "monthly").strip().lower()
    step_days = 30
    if freq_key in ("weekly", "week"):
        step_days = 7
    elif freq_key in ("quarterly", "quarter"):
        step_days = 90
    picked: list[date] = [available[0]]
    last = available[0]
    for d in available[1:]:
        if (d - last).days >= step_days:
            picked.append(d)
            last = d
    if picked[-1] != available[-1]:
        picked.append(available[-1])
    return picked


class InvestmentQualitySignalGenerator:
    def __init__(self) -> None:
        self._svc = ScreeningApplicationService()

    def generate(self, snapshots: list[StockFinancialSnapshot]) -> list[SignalRow]:
        rows = self._svc.screen(snapshots, parallel=False)
        out: list[SignalRow] = []
        for row in rows:
            iq = row.get("investment_quality")
            if not isinstance(iq, dict):
                continue
            try:
                score = float(iq.get("total_score"))
            except (TypeError, ValueError):
                continue
            sym = str(row.get("symbol") or "").strip()
            if not sym:
                continue
            snap = next((x for x in snapshots if x.symbol == sym), None)
            if snap is None:
                continue
            decision = str(iq.get("decision") or "")
            out.append(
                SignalRow(
                    symbol=sym,
                    score=score,
                    decision=decision,
                    market_cap=float(snap.market_cap),
                )
            )
        return out


class EqualWeightPortfolioConstructor:
    def select(self, signals: list[SignalRow], config: BacktestConfig) -> list[str]:
        if not signals:
            return []
        ordered = sorted(signals, key=lambda x: x.score, reverse=True)
        if config.top_n is not None and config.top_n > 0:
            return [x.symbol for x in ordered[: config.top_n]]
        q = config.top_quantile if config.top_quantile is not None else 0.2
        q = min(max(float(q), 0.01), 1.0)
        n = max(1, int(len(ordered) * q))
        return [x.symbol for x in ordered[:n]]


class DefaultBacktestExecutor(BacktestExecutor):
    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        self._feature_builder = HistoricalFeatureBuilder(engine)
        self._signal_gen = InvestmentQualitySignalGenerator()
        self._portfolio = EqualWeightPortfolioConstructor()
        self._asof_repo = AsOfFundamentalRepository(engine)

    @staticmethod
    def _d8_to_date(d8: str) -> date:
        return datetime.strptime(d8, "%Y%m%d").date()

    @staticmethod
    def _sample_dates_from_trade_dates(trade_dates: list[str], freq: str) -> list[date]:
        dates = [DefaultBacktestExecutor._d8_to_date(x) for x in trade_dates]
        return _select_rebalance_dates_from_available(dates, freq)

    def _run_legacy_snapshot_path(self, config: BacktestConfig) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
        start_d = _parse_date(config.start_date)
        end_d = _parse_date(config.end_date)
        symbols_filter = None
        if isinstance(config.filters, dict):
            raw_syms = config.filters.get("symbols")
            if isinstance(raw_syms, list):
                symbols_filter = [str(x).strip() for x in raw_syms if str(x).strip()]
        available_dates = self._feature_builder.list_available_dates(start_d, end_d, symbols=symbols_filter)
        data_min, data_max = self._feature_builder.snapshot_date_bounds(symbols=symbols_filter)
        dates = _select_rebalance_dates_from_available(available_dates, config.rebalance_frequency)
        if len(dates) < 2:
            dates = _generate_rebalance_dates(start_d, end_d, config.rebalance_frequency)
        if len(dates) < 2:
            raise ValueError("回测区间不足，至少需要两个调仓时点")

        nav = 1.0
        bench_nav = 1.0
        prev_holdings: set[str] = set()
        curve_points: list[dict[str, Any]] = []
        period_returns: list[float] = []
        turnover_series: list[float] = []
        ic_vals: list[float] = []
        rank_ic_vals: list[float] = []
        diagnostics_layers: list[dict[str, Any]] = []
        total_exclusions = 0

        for i in range(len(dates) - 1):
            cur_d = dates[i]
            nxt_d = dates[i + 1]
            current = self._feature_builder.build_as_of(cur_d, symbols=symbols_filter)
            future = self._feature_builder.build_as_of(nxt_d, symbols=symbols_filter)
            total_exclusions += len(current.exclusions)
            cur_map = {s.symbol: s for s in current.snapshots}
            nxt_map = {s.symbol: s for s in future.snapshots}
            common = [s for s in current.snapshots if s.symbol in nxt_map]
            if not common:
                continue
            signals = self._signal_gen.generate(common)
            picks_set = set(self._portfolio.select(signals, config))
            if not picks_set:
                continue
            turnover = 1.0
            if prev_holdings:
                inter = len(prev_holdings & picks_set)
                turnover = 1.0 - (float(inter) / float(max(len(prev_holdings), len(picks_set))))
            turnover_series.append(turnover)
            rets: list[float] = []
            score_for_ic: list[float] = []
            fwd_ret_for_ic: list[float] = []
            for s in signals:
                if s.symbol not in nxt_map:
                    continue
                curr_cap = float(cur_map[s.symbol].market_cap)
                next_cap = float(nxt_map[s.symbol].market_cap)
                if curr_cap <= 0:
                    continue
                r = (next_cap - curr_cap) / curr_cap
                score_for_ic.append(s.score)
                fwd_ret_for_ic.append(r)
                if s.symbol in picks_set:
                    rets.append(r)
            if not rets:
                continue
            net = _mean(rets) - turnover * (float(config.transaction_cost_bps) / 10000.0)
            period_returns.append(net)
            nav = nav * (1.0 + net)
            if score_for_ic and fwd_ret_for_ic:
                ic_vals.append(_corr(score_for_ic, fwd_ret_for_ic))
                rank_ic_vals.append(_corr(_rank(score_for_ic), _rank(fwd_ret_for_ic)))
            ordered = sorted(signals, key=lambda x: x.score, reverse=True)
            bucket_size = max(1, len(ordered) // 5)
            layer_rows: list[dict[str, Any]] = []
            for bi in range(5):
                st = bi * bucket_size
                ed = len(ordered) if bi == 4 else min(len(ordered), st + bucket_size)
                bucket = ordered[st:ed]
                b_rets: list[float] = []
                for row in bucket:
                    if row.symbol not in cur_map or row.symbol not in nxt_map:
                        continue
                    curr_cap = float(cur_map[row.symbol].market_cap)
                    next_cap = float(nxt_map[row.symbol].market_cap)
                    if curr_cap > 0:
                        b_rets.append((next_cap - curr_cap) / curr_cap)
                layer_rows.append({"layer": bi + 1, "return": _mean(b_rets) if b_rets else 0.0, "count": len(bucket)})
            diagnostics_layers.append({"date": nxt_d.isoformat(), "layers": layer_rows})
            curve_points.append(
                {
                    "date": nxt_d.isoformat(),
                    "portfolio_nav": nav,
                    "benchmark_nav": bench_nav,
                    "excess_nav": nav / bench_nav if bench_nav > 0 else 0.0,
                    "period_return": net,
                    "benchmark_return": 0.0,
                    "turnover": turnover,
                    "holdings_count": len(picks_set),
                }
            )
            prev_holdings = picks_set
        if not period_returns:
            data_range_hint = ""
            if data_min is not None and data_max is not None:
                data_range_hint = f"当前可用快照日期范围：{data_min.isoformat()} ~ {data_max.isoformat()}。"
            raise ValueError(
                "无有效收益序列，无法完成回测。请检查：1) 时间区间内是否有至少两期快照；"
                f"2) 股票池是否过窄；3) 可用样本是否被全部过滤。{data_range_hint}"
            )
        ann_factor = 252.0 / 30.0
        ret_std = _stdev(period_returns)
        sharpe = (_mean(period_returns) / ret_std) * sqrt(ann_factor) if ret_std > 0 else 0.0
        metrics = {
            "annualized_return": (nav ** (ann_factor / float(max(len(period_returns), 1)))) - 1.0,
            "max_drawdown": _max_drawdown([1.0] + [p["portfolio_nav"] for p in curve_points]),
            "sharpe": sharpe,
            "excess_return": nav - bench_nav,
            "turnover": _mean(turnover_series) if turnover_series else 0.0,
            "periods": len(period_returns),
            "benchmark_end_nav": bench_nav,
            "portfolio_end_nav": nav,
        }
        diagnostics = {
            "ic_mean": _mean(ic_vals) if ic_vals else 0.0,
            "rank_ic_mean": _mean(rank_ic_vals) if rank_ic_vals else 0.0,
            "ic_series": ic_vals,
            "rank_ic_series": rank_ic_vals,
            "quantile_returns": diagnostics_layers,
            "excluded_samples": total_exclusions,
            "engine_path": "legacy_snapshot",
        }
        summary = {
            "strategy_name": config.strategy_name,
            "start_date": config.start_date,
            "end_date": config.end_date,
            "rebalance_frequency": config.rebalance_frequency,
            "transaction_cost_bps": config.transaction_cost_bps,
            "symbols_filter_count": len(symbols_filter) if symbols_filter else None,
        }
        return summary, metrics, {"points": curve_points}, diagnostics

    def run(self, config: BacktestConfig) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
        if bool((config.extras or {}).get("use_legacy_snapshot_path")):
            return self._run_legacy_snapshot_path(config)
        settings = AShareIngestionSettings.from_env()
        token = (settings.tushare_token or "").strip()
        if not token:
            return self._run_legacy_snapshot_path(config)
        price_repo = HistoricalPriceRepository(token)
        symbols_filter: list[str] | None = None
        if isinstance(config.filters, dict):
            raw_syms = config.filters.get("symbols")
            if isinstance(raw_syms, list):
                symbols_filter = [str(x).strip() for x in raw_syms if str(x).strip()]
        trade_dates = price_repo.list_trade_dates(config.start_date, config.end_date)
        dates = self._sample_dates_from_trade_dates(trade_dates, config.rebalance_frequency)
        if len(dates) < 2:
            b0, b1 = price_repo.coverage_bounds()
            raise ValueError(
                f"无足够交易日用于回测。当前行情可用范围：{(b0 or 'unknown')} ~ {(b1 or 'unknown')}，"
                f"请求区间：{config.start_date} ~ {config.end_date}。"
            )

        nav = 1.0
        bench_nav = 1.0
        prev_holdings: set[str] = set()
        curve_points: list[dict[str, Any]] = []
        period_returns: list[float] = []
        turnover_series: list[float] = []
        ic_vals: list[float] = []
        rank_ic_vals: list[float] = []
        diagnostics_layers: list[dict[str, Any]] = []
        total_exclusions = 0
        coverage_total_symbols = 0
        coverage_ok_symbols = 0
        coverage_no_visible_fs = 0
        exclusion_reasons: dict[str, int] = {}

        for i in range(len(dates) - 1):
            cur_d = dates[i]
            nxt_d = dates[i + 1]
            cur_d8 = cur_d.strftime("%Y%m%d")
            nxt_d8 = nxt_d.strftime("%Y%m%d")
            cur_caps = price_repo.fetch_market_caps(cur_d8, symbols=symbols_filter)
            nxt_caps = price_repo.fetch_market_caps(nxt_d8, symbols=symbols_filter)
            try:
                with self._engine.connect() as conn:
                    cur_facts, cur_stat = self._asof_repo.build_asof_fact_map(conn, cur_d8, symbols=symbols_filter)
            except (OperationalError, ProgrammingError):
                return self._run_legacy_snapshot_path(config)
            coverage_total_symbols += int(cur_stat.get("total_symbols", 0))
            coverage_ok_symbols += int(cur_stat.get("ok_symbols", 0))
            coverage_no_visible_fs += int(cur_stat.get("no_visible_fs", 0))

            common_symbols = [s for s in cur_facts.keys() if s in cur_caps and s in nxt_caps]
            if not common_symbols:
                total_exclusions += max(1, int(cur_stat.get("total_symbols", 0)))
                exclusion_reasons["no_common_symbol_with_price"] = exclusion_reasons.get("no_common_symbol_with_price", 0) + 1
                continue
            snapshots: list[StockFinancialSnapshot] = []
            for sym in common_symbols:
                fact = cur_facts[sym]
                market_cap = float(cur_caps[sym])
                if market_cap <= 0:
                    continue
                try:
                    snap = StockFinancialSnapshot(
                        symbol=sym,
                        market_cap=market_cap,
                        total_current_assets=fact.get("total_current_assets"),
                        total_current_liabilities=fact.get("total_current_liabilities"),
                        total_liabilities=fact.get("total_liabilities"),
                        total_equity=fact.get("total_equity"),
                        net_income_ttm=fact.get("net_income_ttm"),
                        operating_cash_flow_ttm=fact.get("operating_cash_flow_ttm"),
                        revenue_ttm=fact.get("revenue_ttm"),
                        data_source="asof_fs_price",
                        trade_cal_date=cur_d8,
                        financials_end_date=str(fact.get("end_date_latest") or ""),
                    )
                except Exception:
                    total_exclusions += 1
                    exclusion_reasons["invalid_snapshot_build"] = exclusion_reasons.get("invalid_snapshot_build", 0) + 1
                    continue
                snapshots.append(snap)
            if not snapshots:
                continue

            signals = self._signal_gen.generate(snapshots)
            picks = self._portfolio.select(signals, config)
            picks_set = set(picks)
            if not picks_set:
                continue

            turnover = 1.0
            if prev_holdings:
                inter = len(prev_holdings & picks_set)
                turnover = 1.0 - (float(inter) / float(max(len(prev_holdings), len(picks_set))))
            turnover_series.append(turnover)

            rets: list[float] = []
            score_for_ic: list[float] = []
            fwd_ret_for_ic: list[float] = []
            for s in signals:
                if s.symbol not in nxt_caps or s.symbol not in cur_caps:
                    continue
                curr_cap = float(cur_caps[s.symbol])
                next_cap = float(nxt_caps[s.symbol])
                if curr_cap <= 0:
                    continue
                r = (next_cap - curr_cap) / curr_cap
                score_for_ic.append(s.score)
                fwd_ret_for_ic.append(r)
                if s.symbol in picks_set:
                    rets.append(r)

            if not rets:
                continue
            gross = _mean(rets)
            cost = turnover * (float(config.transaction_cost_bps) / 10000.0)
            net = gross - cost
            period_returns.append(net)
            nav = nav * (1.0 + net)

            bench_r = 0.0
            bench_map = config.extras.get("benchmark_returns") if isinstance(config.extras, dict) else None
            if isinstance(bench_map, dict):
                key = nxt_d.isoformat()
                raw = bench_map.get(key)
                try:
                    bench_r = float(raw)
                except (TypeError, ValueError):
                    bench_r = 0.0
            bench_nav = bench_nav * (1.0 + bench_r)

            if score_for_ic and fwd_ret_for_ic:
                ic_vals.append(_corr(score_for_ic, fwd_ret_for_ic))
                rank_ic_vals.append(_corr(_rank(score_for_ic), _rank(fwd_ret_for_ic)))

            # 分层收益：按当期全体可交易样本的分数分成 5 桶。
            ordered = sorted(signals, key=lambda x: x.score, reverse=True)
            bucket_size = max(1, len(ordered) // 5)
            layer_rows: list[dict[str, Any]] = []
            for bi in range(5):
                st = bi * bucket_size
                ed = len(ordered) if bi == 4 else min(len(ordered), st + bucket_size)
                bucket = ordered[st:ed]
                b_rets: list[float] = []
                for row in bucket:
                    if row.symbol not in cur_caps or row.symbol not in nxt_caps:
                        continue
                    curr_cap = float(cur_caps[row.symbol])
                    next_cap = float(nxt_caps[row.symbol])
                    if curr_cap <= 0:
                        continue
                    b_rets.append((next_cap - curr_cap) / curr_cap)
                layer_rows.append(
                    {
                        "layer": bi + 1,
                        "return": _mean(b_rets) if b_rets else 0.0,
                        "count": len(bucket),
                    }
                )
            diagnostics_layers.append({"date": nxt_d.isoformat(), "layers": layer_rows})

            curve_points.append(
                {
                    "date": nxt_d.isoformat(),
                    "portfolio_nav": nav,
                    "benchmark_nav": bench_nav,
                    "excess_nav": nav / bench_nav if bench_nav > 0 else 0.0,
                    "period_return": net,
                    "benchmark_return": bench_r,
                    "turnover": turnover,
                    "holdings_count": len(picks_set),
                }
            )
            prev_holdings = picks_set

        if not period_returns:
            b0, b1 = price_repo.coverage_bounds()
            data_range_hint = f"当前可用行情日期范围：{(b0 or 'unknown')} ~ {(b1 or 'unknown')}。"
            raise ValueError(
                "无有效收益序列，无法完成回测。请检查：1) 时间区间内是否有至少两期快照；"
                f"2) 股票池是否过窄；3) 可用样本是否被全部过滤。{data_range_hint}"
            )

        ann_factor = 252.0 / 30.0
        annualized_return = (nav ** (ann_factor / float(max(len(period_returns), 1)))) - 1.0
        excess_return = nav - bench_nav
        sharpe = 0.0
        ret_std = _stdev(period_returns)
        if ret_std > 0:
            sharpe = (_mean(period_returns) / ret_std) * sqrt(ann_factor)
        metrics = {
            "annualized_return": annualized_return,
            "max_drawdown": _max_drawdown([1.0] + [p["portfolio_nav"] for p in curve_points]),
            "sharpe": sharpe,
            "excess_return": excess_return,
            "turnover": _mean(turnover_series) if turnover_series else 0.0,
            "periods": len(period_returns),
            "benchmark_end_nav": bench_nav,
            "portfolio_end_nav": nav,
        }
        diagnostics = {
            "ic_mean": _mean(ic_vals) if ic_vals else 0.0,
            "rank_ic_mean": _mean(rank_ic_vals) if rank_ic_vals else 0.0,
            "ic_series": ic_vals,
            "rank_ic_series": rank_ic_vals,
            "quantile_returns": diagnostics_layers,
            "excluded_samples": total_exclusions,
            "coverage": {
                "total_symbols": coverage_total_symbols,
                "ok_symbols": coverage_ok_symbols,
                "no_visible_fs": coverage_no_visible_fs,
            },
            "exclusion_reasons": exclusion_reasons,
            "engine_path": "asof_fundamental_price",
        }
        summary = {
            "strategy_name": config.strategy_name,
            "start_date": config.start_date,
            "end_date": config.end_date,
            "rebalance_frequency": config.rebalance_frequency,
            "transaction_cost_bps": config.transaction_cost_bps,
            "symbols_filter_count": len(symbols_filter) if symbols_filter else None,
            "trade_date_count": len(dates),
        }
        curve = {"points": curve_points}
        return summary, metrics, curve, diagnostics


class SampleBacktestExecutor(BacktestExecutor):
    """固定样例执行器：用于验收链路与演示，不依赖数据库历史数据。"""

    def run(self, config: BacktestConfig) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
        points = [
            {
                "date": config.start_date,
                "portfolio_nav": 1.0,
                "benchmark_nav": 1.0,
                "excess_nav": 1.0,
                "period_return": 0.0,
                "benchmark_return": 0.0,
                "turnover": 0.0,
                "holdings_count": 20,
            },
            {
                "date": config.end_date,
                "portfolio_nav": 1.18,
                "benchmark_nav": 1.08,
                "excess_nav": 1.0925925926,
                "period_return": 0.18,
                "benchmark_return": 0.08,
                "turnover": 0.32,
                "holdings_count": 20,
            },
        ]
        summary = {
            "strategy_name": config.strategy_name,
            "start_date": config.start_date,
            "end_date": config.end_date,
            "rebalance_frequency": config.rebalance_frequency,
            "sample_mode": True,
        }
        metrics = {
            "annualized_return": 0.18,
            "max_drawdown": 0.05,
            "sharpe": 1.42,
            "excess_return": 0.10,
            "turnover": 0.32,
            "periods": 1,
            "benchmark_end_nav": 1.08,
            "portfolio_end_nav": 1.18,
        }
        diagnostics = {
            "ic_mean": 0.12,
            "rank_ic_mean": 0.18,
            "ic_series": [0.12],
            "rank_ic_series": [0.18],
            "quantile_returns": [
                {
                    "date": config.end_date,
                    "layers": [
                        {"layer": 1, "return": 0.11, "count": 50},
                        {"layer": 2, "return": 0.08, "count": 50},
                        {"layer": 3, "return": 0.06, "count": 50},
                        {"layer": 4, "return": 0.03, "count": 50},
                        {"layer": 5, "return": 0.01, "count": 50},
                    ],
                }
            ],
            "excluded_samples": 0,
        }
        return summary, metrics, {"points": points}, diagnostics

