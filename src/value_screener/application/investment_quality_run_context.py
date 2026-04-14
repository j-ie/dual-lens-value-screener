"""Run 维度投资质量计算：供详情 API 与 AI 分析上下文复用。"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy.engine import Engine

from value_screener.application.financial_statement_payload import merge_core_columns_with_payload
from value_screener.application.financial_statement_window import DEFAULT_FINANCIAL_PERIODS_LIMIT
from value_screener.application.investment_quality_view import attach_investment_quality_for_result_row
from value_screener.application.result_enrichment import enrich_screening_result_row
from value_screener.domain.combined_ranking_params import CombinedRankingParams
from value_screener.domain.investment_quality import InvestmentQualityAnalyzer
from value_screener.infrastructure.factory import build_composite_provider
from value_screener.infrastructure.fetch_types import SymbolFetchFailure
from value_screener.infrastructure.financial_statement_repository import FinancialStatementRepository
from value_screener.infrastructure.reference_repository import ReferenceMasterRepository
from value_screener.infrastructure.screening_repository import ScreeningRepository
from value_screener.infrastructure.settings import AShareIngestionSettings
from value_screener.infrastructure.symbol_normalize import to_ts_code

logger = logging.getLogger(__name__)


def _finite_non_neg_simple(v: object) -> float | None:
    """与 TuShare 口径一致：非负有限数，否则视为缺失。"""

    if v is None:
        return None
    try:
        import pandas as pd

        if isinstance(v, float) and pd.isna(v):
            return None
    except ImportError:
        pass
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    if x < 0:
        return None
    return x


def _bs_row_get(row: Any, key: str) -> Any:
    if row is None:
        return None
    if hasattr(row, "get"):
        return row.get(key)
    try:
        return row[key]
    except (KeyError, TypeError):
        return None


def _proxy_current_assets_from_bs_row(row: Any) -> float | None:
    """
    当 total_cur_assets 为空时，用流动资产分项求和（应收票据及应收账款与分项互斥）。
    适用于信托、证券等模板未填流动资产合计的情形（如 000563.SZ）。
    """

    total = 0.0
    has_any = False
    bill = _finite_non_neg_simple(_bs_row_get(row, "accounts_receiv_bill"))
    if bill is not None:
        total += bill
        has_any = True
    else:
        for col in ("notes_receiv", "accounts_receiv"):
            x = _finite_non_neg_simple(_bs_row_get(row, col))
            if x is not None:
                total += x
                has_any = True
    for col in (
        "money_cap",
        "trad_asset",
        "oth_receiv",
        "prepayment",
        "div_receiv",
        "int_receiv",
        "inventories",
        "nca_within_1y",
        "sett_rsrv",
        "loanto_oth_bank_fi",
        "pur_resale_fa",
        "oth_cur_assets",
        "receiv_financing",
        "contract_assets",
        "lending_funds",
        "cost_fin_assets",
        "fair_value_fin_assets",
    ):
        x = _finite_non_neg_simple(_bs_row_get(row, col))
        if x is not None:
            total += x
            has_any = True
    if not has_any or total <= 0:
        return None
    return total


def _proxy_current_liabilities_from_bs_row(row: Any) -> float | None:
    """
    当 total_cur_liab 为空时，用流动负债分项求和（应付票据及应付账款与应付款项互斥）。
    """

    total = 0.0
    has_any = False
    ap_all = _finite_non_neg_simple(_bs_row_get(row, "accounts_pay"))
    if ap_all is not None:
        total += ap_all
        has_any = True
    else:
        payables = _finite_non_neg_simple(_bs_row_get(row, "payables"))
        if payables is not None:
            total += payables
            has_any = True
        else:
            for col in ("notes_payable", "acct_payable"):
                x = _finite_non_neg_simple(_bs_row_get(row, col))
                if x is not None:
                    total += x
                    has_any = True
    for col in (
        "st_borr",
        "cb_borr",
        "loan_oth_bank",
        "trading_fl",
        "adv_receipts",
        "sold_for_repur_fa",
        "comm_payable",
        "payroll_payable",
        "taxes_payable",
        "int_payable",
        "div_payable",
        "oth_payable",
        "non_cur_liab_due_1y",
        "oth_cur_liab",
        "st_bonds_payable",
        "st_fin_payable",
        "contract_liab",
    ):
        x = _finite_non_neg_simple(_bs_row_get(row, col))
        if x is not None:
            total += x
            has_any = True
    if not has_any or total <= 0:
        return None
    return total


def merge_tushare_balancesheet_into_run_fact(
    ts_code: str,
    run_fact: dict[str, Any],
    settings: AShareIngestionSettings,
) -> dict[str, Any]:
    """
    快照仍缺资产负债表科目时，直接请求 TuShare balancesheet（多期扫描），仅填补空键。
    金融业等个别报表可能缺流动资产/负债列，尽量取最近一期有值的报告期。
    """

    token = (settings.tushare_token or "").strip()
    if not token:
        return run_fact
    need = {
        "total_current_assets",
        "total_current_liabilities",
        "total_liabilities",
        "total_equity",
    }
    if not any(run_fact.get(k) is None for k in need):
        return run_fact
    try:
        import tushare as ts

        ts.set_token(token)
        pro = ts.pro_api()
        bs = pro.balancesheet(
            ts_code=ts_code,
            fields=(
                "ts_code,end_date,ann_date,total_cur_assets,total_cur_liab,"
                "total_liab,total_hldr_eqy_exc_min_int,st_borr,lt_borr,"
                "money_cap,trad_asset,notes_receiv,accounts_receiv,accounts_receiv_bill,"
                "oth_receiv,prepayment,div_receiv,int_receiv,inventories,nca_within_1y,"
                "sett_rsrv,loanto_oth_bank_fi,pur_resale_fa,oth_cur_assets,receiv_financing,"
                "contract_assets,lending_funds,cost_fin_assets,fair_value_fin_assets,"
                "accounts_pay,payables,notes_payable,acct_payable,"
                "cb_borr,loan_oth_bank,trading_fl,adv_receipts,sold_for_repur_fa,"
                "comm_payable,payroll_payable,taxes_payable,int_payable,div_payable,"
                "oth_payable,non_cur_liab_due_1y,oth_cur_liab,st_bonds_payable,st_fin_payable,"
                "contract_liab"
            ),
            limit=DEFAULT_FINANCIAL_PERIODS_LIMIT,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("investment_quality tushare balancesheet %s: %s", ts_code, exc)
        return run_fact
    if bs is None or bs.empty:
        return run_fact
    try:
        work = bs.sort_values("end_date", ascending=False)
    except Exception:
        work = bs
    merged = dict(run_fact)
    key_map = {
        "total_current_assets": "total_cur_assets",
        "total_current_liabilities": "total_cur_liab",
        "total_liabilities": "total_liab",
        "total_equity": "total_hldr_eqy_exc_min_int",
    }
    for _, row in work.iterrows():
        for out_key, src_col in key_map.items():
            if merged.get(out_key) is not None:
                continue
            x = _finite_non_neg_simple(row.get(src_col))
            if out_key == "total_equity" and (x is None or x <= 0):
                continue
            if x is not None:
                merged[out_key] = x
        if merged.get("total_current_assets") is None:
            pa = _proxy_current_assets_from_bs_row(row)
            if pa is not None:
                merged["total_current_assets"] = pa
                logger.debug(
                    "investment_quality balancesheet proxy total_current_assets ts_code=%s end_date=%s",
                    ts_code,
                    _bs_row_get(row, "end_date"),
                )
        if merged.get("total_current_liabilities") is None:
            pl = _proxy_current_liabilities_from_bs_row(row)
            if pl is not None:
                merged["total_current_liabilities"] = pl
                logger.debug(
                    "investment_quality balancesheet proxy total_current_liabilities ts_code=%s end_date=%s",
                    ts_code,
                    _bs_row_get(row, "end_date"),
                )
        stb = _finite_non_neg_simple(_bs_row_get(row, "st_borr")) or _finite_non_neg_simple(
            _bs_row_get(row, "st_borrow")
        )
        ltb = _finite_non_neg_simple(_bs_row_get(row, "lt_borr")) or _finite_non_neg_simple(
            _bs_row_get(row, "lt_borrow")
        )
        if merged.get("interest_bearing_debt") is None and (stb is not None or ltb is not None):
            ibd = (stb or 0.0) + (ltb or 0.0)
            if ibd > 0:
                merged["interest_bearing_debt"] = ibd
        if all(merged.get(k) is not None for k in ("total_current_assets", "total_current_liabilities")):
            break
    return merged


def pick_float(*vals: Any) -> float | None:
    for v in vals:
        if v is None:
            continue
        try:
            x = float(v)
        except (TypeError, ValueError):
            continue
        if x > 0:
            return x
    return None


def sum_latest_n(rows: list[dict[str, Any]], key: str, n: int = 4) -> float | None:
    values: list[float] = []
    for row in rows[: max(1, n)]:
        v = row.get(key)
        if v is None:
            continue
        try:
            values.append(float(v))
        except (TypeError, ValueError):
            continue
    if not values:
        return None
    return float(sum(values))


def sum_latest_n_first_key(
    rows: list[dict[str, Any]],
    keys: tuple[str, ...],
    n: int = 4,
) -> float | None:
    """
    各报告期按 keys 顺序取第一个可用数值再求和。
    用于 TuShare 并存字段：如 total_revenue / revenue；n_cashflow_act / n_cash_flow_act（历史误写）。
    """

    values: list[float] = []
    for row in rows[: max(1, n)]:
        picked: float | None = None
        for key in keys:
            v = row.get(key)
            if v is None:
                continue
            try:
                picked = float(v)
            except (TypeError, ValueError):
                continue
            break
        if picked is not None:
            values.append(picked)
    if not values:
        return None
    return float(sum(values))


def _interest_bearing_debt_from_balance_row(bal: dict[str, Any]) -> float | None:
    """
    短期借款 + 长期借款（TuShare 字段名为 st_borr / lt_borr；兼容 st_borrow 别名）。
    读库行顶层列与 payload 全量 JSON（fs_balance 借款不在宽表列上）。
    """

    pl = bal.get("payload") if isinstance(bal.get("payload"), dict) else {}

    def first_non_neg(key_pairs: tuple[str, ...]) -> float | None:
        for k in key_pairs:
            for src in (bal, pl):
                if not isinstance(src, dict):
                    continue
                x = _finite_non_neg_simple(src.get(k))
                if x is not None:
                    return x
        return None

    stb = first_non_neg(("st_borr", "st_borrow"))
    ltb = first_non_neg(("lt_borr", "lt_borrow"))
    ibd: float | None = None
    if stb is not None and ltb is not None:
        ibd = float(stb + ltb)
    elif stb is not None:
        ibd = float(stb)
    elif ltb is not None:
        ibd = float(ltb)
    if ibd is not None and ibd <= 0:
        return None
    return ibd


def missing_required_iq_fields(run_fact: dict[str, Any]) -> list[str]:
    required = (
        "market_cap",
        "total_current_assets",
        "total_current_liabilities",
        "total_liabilities",
        "total_equity",
        "net_income_ttm",
        "operating_cash_flow_ttm",
        "revenue_ttm",
    )
    missing: list[str] = []
    for key in required:
        val = run_fact.get(key)
        if val is None:
            missing.append(key)
            continue
        try:
            num = float(val)
        except (TypeError, ValueError):
            missing.append(key)
            continue
        if key == "market_cap":
            if num <= 0:
                missing.append(key)
        elif key in ("total_equity", "revenue_ttm"):
            if num <= 0:
                missing.append(key)
    return missing


def hydrate_run_fact_from_db(
    *,
    conn: Any,
    ts_code: str,
    row: dict[str, Any],
) -> tuple[dict[str, Any], str | None]:
    run_fact = dict(row.get("run_fact_json") or {}) if isinstance(row.get("run_fact_json"), dict) else {}
    provenance = row.get("provenance_json") if isinstance(row.get("provenance_json"), dict) else {}
    fs_repo = FinancialStatementRepository(conn.engine)
    ref_repo = ReferenceMasterRepository(conn.engine)
    ref = ref_repo.fetch_one_by_ts_code(conn, ts_code) or {}
    industry = str(
        (
            ref.get("industry")
            or row.get("ref_industry")
            or row.get("industry")
            or (row.get("provenance_json") or {}).get("industry")
            or ""
        )
    ).strip() or None

    bal = fs_repo.list_recent_balance(conn, ts_code, limit=1)
    inc = fs_repo.list_recent_income(conn, ts_code, limit=4)
    cf = fs_repo.list_recent_cashflow(conn, ts_code, limit=4)
    bal0 = merge_core_columns_with_payload(bal[0]) if bal else {}
    inc_rows = [merge_core_columns_with_payload(r) for r in inc]
    cf_rows = [merge_core_columns_with_payload(r) for r in cf]

    run_fact.setdefault(
        "market_cap",
        pick_float(row.get("market_cap"), provenance.get("market_cap"), run_fact.get("market_cap")),
    )
    run_fact.setdefault("total_current_assets", bal0.get("total_cur_assets"))
    run_fact.setdefault("total_current_liabilities", bal0.get("total_cur_liab"))
    run_fact.setdefault("total_liabilities", bal0.get("total_liab"))
    run_fact.setdefault("total_equity", bal0.get("total_hldr_eqy_exc_min_int"))
    run_fact.setdefault("interest_bearing_debt", _interest_bearing_debt_from_balance_row(bal0))
    run_fact.setdefault("net_income_ttm", sum_latest_n(inc_rows, "n_income_attr_p", n=4))
    run_fact.setdefault(
        "revenue_ttm",
        sum_latest_n_first_key(inc_rows, ("total_revenue", "revenue"), n=4),
    )
    run_fact.setdefault(
        "operating_cash_flow_ttm",
        sum_latest_n_first_key(cf_rows, ("n_cashflow_act", "n_cash_flow_act"), n=4),
    )
    run_fact.setdefault("dv_ratio", provenance.get("dv_ratio"))
    run_fact.setdefault("dv_ttm", provenance.get("dv_ttm"))
    return run_fact, industry


def enrich_run_fact_from_provider_snapshot(ts_code: str, run_fact: dict[str, Any]) -> dict[str, Any]:
    """
    库内/跑批行仍缺关键字段时，用与一键拉数相同的数据源拉取单标的快照，仅填补 run_fact 中仍为空的键。
    拉取失败则原样返回，由上层继续按缺字段报错。
    """

    if not missing_required_iq_fields(run_fact):
        return run_fact
    settings = AShareIngestionSettings.from_env()
    try:
        provider = build_composite_provider(settings)
    except Exception as exc:  # noqa: BLE001
        logger.warning("investment_quality provider init failed ts_code=%s: %s", ts_code, exc)
        return run_fact
    try:
        fetched = provider.fetch_snapshots([ts_code])
    except Exception as exc:  # noqa: BLE001
        logger.warning("investment_quality fetch_snapshots failed ts_code=%s: %s", ts_code, exc)
        return run_fact
    if not fetched or isinstance(fetched[0], SymbolFetchFailure):
        if fetched and isinstance(fetched[0], SymbolFetchFailure):
            logger.info(
                "investment_quality snapshot unavailable ts_code=%s reason=%s",
                ts_code,
                fetched[0].reason,
            )
        return run_fact
    snap = fetched[0]
    merged = dict(run_fact)
    dump = snap.model_dump(mode="json")
    for key, val in dump.items():
        if key == "symbol":
            continue
        if merged.get(key) is not None:
            continue
        if val is None:
            continue
        merged[key] = val

    if missing_required_iq_fields(merged):
        merged = merge_tushare_balancesheet_into_run_fact(ts_code, merged, settings)
    return merged


def compute_investment_quality_for_run_symbol(
    engine: Engine,
    run_id: int,
    ts_code: str,
) -> dict[str, Any] | None:
    """
    基于该 run 的 screening 行计算投资质量；行业与关键字段不齐时返回 None（调用方不强行补全）。
    """

    code = to_ts_code(ts_code)
    repo = ScreeningRepository(engine)
    ranking = CombinedRankingParams.from_env()
    with engine.connect() as conn:
        row = repo.get_result_row_for_run_symbol(conn, run_id, code, ranking=ranking)
        if row is None:
            return None
        hydrated_run_fact, hydrated_industry = hydrate_run_fact_from_db(
            conn=conn,
            ts_code=code,
            row=row,
        )
        if not hydrated_industry:
            return None
        if missing_required_iq_fields(hydrated_run_fact):
            hydrated_run_fact = enrich_run_fact_from_provider_snapshot(code, hydrated_run_fact)
        if missing_required_iq_fields(hydrated_run_fact):
            return None
        analyzer = InvestmentQualityAnalyzer()
        row_for_iq = dict(row)
        row_for_iq["run_fact_json"] = hydrated_run_fact
        row_for_iq["industry"] = hydrated_industry
        decorated = attach_investment_quality_for_result_row(analyzer, row_for_iq)
        iq = decorated.get("investment_quality")
        return iq if isinstance(iq, dict) else None
