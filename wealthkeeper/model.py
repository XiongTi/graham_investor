from __future__ import annotations
import logging
import math
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date
from itertools import takewhile
import pandas as pd
import yfinance as yf
from yfinance.exceptions import YFRateLimitError
from .config import DEFAULT_MARKET, MARKET_PROFILES, MarketProfile
from .monitor import _current_market_date
from .screener import (
    _load_investable_universe,
    is_snapshot_stale,
    snapshot_age_days,
    explain_candidates,
    normalize_tickers,
)
warnings.filterwarnings("ignore", module=r"yfinance\..*")
warnings.filterwarnings("ignore", module=r"urllib3\..*")
warnings.filterwarnings("ignore", category=FutureWarning, module=r"pandas\..*")
logger = logging.getLogger(__name__)
ANALYSIS_MAX_WORKERS = 3
INFO_RETRY_ATTEMPTS = 2
RETRYABLE_ERROR_MARKERS = ("Too Many Requests", "Invalid Crumb", "Unauthorized")
GRADE_ORDER = {"A": 5, "B": 4, "C": 3, "D": 2, "F": 1}
@dataclass
class StockAnalysis:
    ticker: str; market: str
    company_name: str = ""; sector: str = ""; current_price: float | None = None; eps: float | None = None; book_value_per_share: float | None = None
    pe_ratio: float | None = None; pb_ratio: float | None = None; pe_times_pb: float | None = None; graham_number: float | None = None; margin_of_safety: float | None = None
    current_ratio: float | None = None; debt_to_equity: float | None = None; profitable_years: int | None = None; profitable_years_observed: int | None = None; dividend_years: int | None = None
    earnings_growth: float | None = None; revenue_growth: float | None = None; dividend_yield: float | None = None; roe: float | None = None; net_margin: float | None = None; fcf_yield: float | None = None
    scores: dict[str, float | None] = field(default_factory=dict); total_score: float = 0.0; coverage_ratio: float = 0.0; grade: str = ""; data_source: str = "live"; snapshot_age_days: int | None = None; snapshot_stale: bool = False; error: str = ""
def _score_lower_is_better(value: float | None, max_threshold: float) -> float | None:
    if value is None or value <= 0:
        return None
    if value <= max_threshold * 0.5:
        return 100
    if value <= max_threshold:
        return 100 - (value - max_threshold * 0.5) / (max_threshold * 0.5) * 40
    if value <= max_threshold * 2:
        return 60 - (value - max_threshold) / max_threshold * 60
    return 0
def _score_years_vs_target(years: int | None, target: int) -> float | None:
    if years is None:
        return None
    if years >= target:
        return min(100, 70 + (years - target) * 6)
    return max(0, years / target * 70)
def _effective_year_target(target: int, observed_window: int | None) -> int:
    if observed_window is None or observed_window <= 0:
        return target
    return max(1, min(target, observed_window))
def _score_growth(growth: float | None, target: float, *, full_marks_mul: float = 2, interp_divisor_mul: float = 1, negative_floor: float = 0) -> float | None:
    if growth is None:
        return None
    if growth >= target * full_marks_mul:
        return 100
    if growth >= target:
        return 70 + (growth - target) / (target * interp_divisor_mul) * 30
    if growth > 0:
        return growth / target * 70
    if negative_floor > 0 and growth > -0.1:
        return negative_floor
    return 0
def _score_quality_tier(value: float | None, excellent: float, good: float, floor_threshold: float, floor_base: float) -> float | None:
    if value is None or value <= 0:
        return None
    if value >= excellent * 2:
        return 100
    if value >= excellent:
        return 80 + (value - excellent) / excellent * 20
    if value >= good:
        return 60 + (value - good) / (excellent - good) * 20
    if value >= floor_threshold:
        return floor_base + (value - floor_threshold) / (good - floor_threshold) * (60 - floor_base)
    return value / floor_threshold * floor_base
def _score_pe(pe: float | None, profile: MarketProfile) -> float | None: return _score_lower_is_better(pe, profile.criteria["pe_ratio_max"])
def _score_pb(pb: float | None, profile: MarketProfile) -> float | None: return _score_lower_is_better(pb, profile.criteria["pb_ratio_max"])
def _score_graham_number(price: float | None, graham_num: float | None) -> float | None:
    if graham_num is None or price is None or graham_num <= 0 or price <= 0:
        return None
    margin = (graham_num - price) / graham_num
    if margin >= 0.5:
        return 100
    if margin >= 0.3:
        return 80
    if margin >= 0.1:
        return 60
    if margin >= 0:
        return 40
    if margin >= -0.3:
        return 20
    return 0
def _score_current_ratio(cr: float | None, profile: MarketProfile) -> float | None:
    if cr is None or cr <= 0:
        return None
    minimum = profile.criteria["current_ratio_min"]
    if cr >= minimum + 1.0:
        return 100
    if cr >= minimum:
        return 70 + (cr - minimum) * 30
    lower_floor = max(0.8, minimum - 0.5)
    if cr >= lower_floor:
        return 35 + (cr - lower_floor) / (minimum - lower_floor) * 35
    return max(0, cr / lower_floor * 35)
def _score_debt_equity(de: float | None, profile: MarketProfile) -> float | None:
    if de is None:
        return None
    if de <= 0:
        return 100
    max_de = profile.criteria["debt_equity_max"]
    if de <= max_de * 0.5:
        return 100
    if de <= max_de:
        return 70 + (max_de - de) / (max_de * 0.5) * 30
    if de <= max_de * 2:
        return 30 + (max_de * 2 - de) / max_de * 40
    if de <= max_de * 4:
        return (max_de * 4 - de) / (max_de * 2) * 30
    return 0
def _score_earnings_stability(
    years: int | None,
    profile: MarketProfile,
    observed_window: int | None = None,
) -> float | None:
    target = _effective_year_target(int(profile.criteria["min_profitable_years"]), observed_window)
    return _score_years_vs_target(years, target)
def _score_dividend(years: int | None, profile: MarketProfile) -> float | None:
    return _score_years_vs_target(years, int(profile.criteria["min_dividend_years"]))
def _score_earnings_growth(growth: float | None, profile: MarketProfile) -> float | None:
    return _score_growth(growth, profile.criteria["earnings_growth_min"])
def _score_revenue_growth(growth: float | None, profile: MarketProfile) -> float | None:
    return _score_growth(
        growth,
        profile.criteria["revenue_growth_min"],
        full_marks_mul=3,
        interp_divisor_mul=2,
        negative_floor=20,
    )
def _apply_earnings_decline_penalty(total: float, earnings_growth: float | None, profile: MarketProfile) -> float:
    if earnings_growth is None or earnings_growth >= 0:
        return total
    penalty = profile.earnings_decline_penalty
    if earnings_growth > penalty["mild"][0]:
        return total * penalty["mild"][1]
    if earnings_growth > penalty["moderate"][0]:
        return total * penalty["moderate"][1]
    return total * penalty["severe"][1]
def _score_roe(roe: float | None, profile: MarketProfile) -> float | None:
    return _score_quality_tier(
        roe,
        profile.criteria["roe_excellent"],
        profile.criteria["roe_good"],
        0.08,
        30,
    )
def _score_net_margin(margin: float | None, profile: MarketProfile) -> float | None:
    return _score_quality_tier(
        margin,
        profile.criteria["net_margin_excellent"],
        profile.criteria["net_margin_good"],
        0.03,
        20,
    )
def _score_fcf_yield(fcf_yield: float | None, profile: MarketProfile) -> float | None:
    if fcf_yield is None or fcf_yield <= 0:
        return None
    good = profile.criteria["fcf_yield_good"]
    if fcf_yield >= good * 2:
        return 100
    if fcf_yield >= good:
        return 70 + (fcf_yield - good) / good * 30
    if fcf_yield >= 0.02:
        return 30 + (fcf_yield - 0.02) / (good - 0.02) * 40
    return fcf_yield / 0.02 * 30
def _get_num(info: dict, key: str) -> float | None:
    value = info.get(key)
    return None if value in (None, "") else (float(value) if math.isfinite(float(value)) else None)
def _find_financial_row(financials: pd.DataFrame, labels: list[str]) -> pd.Series | None: return next((financials.loc[label] for label in labels if label in financials.index), None)
def _count_trailing_positive(values) -> int | None:
    valid = [value for value in values if pd.notna(value)]
    return None if not valid else sum(1 for _ in takewhile(lambda v: v > 0, reversed(valid)))
def _calc_growth_rate(series: pd.Series) -> float | None:
    clean = series.dropna().sort_index()
    if len(clean) < 2:
        return None
    oldest, newest = clean.iloc[0], clean.iloc[-1]
    if oldest > 0:
        return (newest - oldest) / abs(oldest)
    return None
def calc_graham_number(eps: float | None, bvps: float | None) -> float | None: return None if eps is None or bvps is None or eps <= 0 or bvps <= 0 else math.sqrt(22.5 * eps * bvps)
def _snapshot_num(snapshot: dict[str, object] | None, key: str) -> float | None:
    if not snapshot:
        return None
    value = snapshot.get(key)
    if value in (None, ""):
        return None
    numeric = float(value)
    return numeric if math.isfinite(numeric) else None
def _apply_snapshot_seed(result: StockAnalysis, snapshot: dict[str, object] | None) -> None:
    if not snapshot:
        return
    result.current_price = _snapshot_num(snapshot, "price")
    result.pe_ratio = _snapshot_num(snapshot, "pe")
    result.pb_ratio = _snapshot_num(snapshot, "pb")
    result.roe = _snapshot_num(snapshot, "roe")
    result.net_margin = _snapshot_num(snapshot, "net_margin")
    result.snapshot_age_days = snapshot_age_days(snapshot)
    result.snapshot_stale = is_snapshot_stale(snapshot)
    result.data_source = "snapshot_seeded"
    if result.pe_ratio is not None and result.pb_ratio is not None:
        result.pe_times_pb = result.pe_ratio * result.pb_ratio
def _is_retryable_quote_error(exc: Exception) -> bool: return isinstance(exc, YFRateLimitError) or any(marker in str(exc) for marker in RETRYABLE_ERROR_MARKERS)
def _get_info_with_retry(stock: yf.Ticker) -> dict | None:
    last_exc: Exception | None = None
    for attempt in range(INFO_RETRY_ATTEMPTS + 1):
        try:
            return stock.info
        except Exception as exc:
            last_exc = exc
            if attempt >= INFO_RETRY_ATTEMPTS or not _is_retryable_quote_error(exc):
                break
            time.sleep(1.2 * (attempt + 1))
    if last_exc:
        raise last_exc
    return None
def fetch_stock_data(ticker: str, profile: MarketProfile, snapshot: dict[str, object] | None = None) -> StockAnalysis:
    result = StockAnalysis(ticker=ticker, market=profile.code)
    _apply_snapshot_seed(result, snapshot)
    try:
        stock = yf.Ticker(ticker)
        info = _get_info_with_retry(stock)
        if not info:
            if result.current_price is None:
                result.error = "无法获取数据"
            return result
        result.data_source = "live"
        current_price = info.get("currentPrice", info.get("regularMarketPrice"))
        if current_price is None and result.current_price is None:
            result.error = "缺少当前价格"
            return result
        result.company_name = info.get("shortName", info.get("longName", ticker))
        result.sector = info.get("sector", "N/A")
        if current_price is not None:
            result.current_price = float(current_price)
        result.eps = _get_num(info, "trailingEps")
        result.book_value_per_share = _get_num(info, "bookValue")
        trailing_pe = _get_num(info, "trailingPE")
        price_to_book = _get_num(info, "priceToBook")
        return_on_equity = _get_num(info, "returnOnEquity")
        profit_margins = _get_num(info, "profitMargins")
        result.pe_ratio = trailing_pe if trailing_pe is not None else result.pe_ratio
        result.pb_ratio = price_to_book if price_to_book is not None else result.pb_ratio
        if result.pe_ratio is not None and result.pb_ratio is not None:
            result.pe_times_pb = result.pe_ratio * result.pb_ratio
        result.graham_number = calc_graham_number(result.eps, result.book_value_per_share)
        if result.graham_number and result.current_price:
            result.margin_of_safety = (result.graham_number - result.current_price) / result.graham_number
        result.current_ratio = _get_num(info, "currentRatio")
        debt_equity = _get_num(info, "debtToEquity")
        result.debt_to_equity = debt_equity / 100 if debt_equity is not None else None
        result.dividend_yield = _get_num(info, "dividendYield")
        result.roe = return_on_equity if return_on_equity is not None else result.roe
        result.net_margin = profit_margins if profit_margins is not None else result.net_margin
        fcf = _get_num(info, "freeCashflow")
        market_cap = _get_num(info, "marketCap")
        if fcf is not None and market_cap and market_cap > 0 and fcf > 0:
            result.fcf_yield = fcf / market_cap
        _analyze_earnings_history(stock, result)
    except Exception as exc:
        logger.warning("获取 %s 数据失败: %s", ticker, exc)
        if result.current_price is None and result.pe_ratio is None and result.pb_ratio is None and result.roe is None and result.net_margin is None:
            result.error = str(exc)
        elif result.data_source == "snapshot_seeded":
            result.data_source = "snapshot_fallback"
    return result
def _analyze_earnings_history(stock: yf.Ticker, result: StockAnalysis) -> None:
    try:
        financials = stock.financials
    except Exception as exc:
        logger.warning("%s: 获取年度财务数据失败: %s", result.ticker, exc)
        financials = None
    if financials is not None and not financials.empty:
        try:
            net_income = _find_financial_row(
                financials,
                ["Net Income", "NetIncome", "Net Income Common Stockholders"],
            )
            if net_income is not None:
                yearly = net_income.dropna().sort_index()
                result.profitable_years_observed = len(yearly)
                result.profitable_years = _count_trailing_positive(yearly.values)
                result.earnings_growth = _calc_growth_rate(yearly)
        except Exception as exc:
            logger.warning("%s: 盈利历史分析失败: %s", result.ticker, exc)
        try:
            revenue = _find_financial_row(financials, ["Total Revenue", "TotalRevenue", "Revenue"])
            if revenue is not None:
                yearly = revenue.dropna().sort_index()
                result.revenue_growth = _calc_growth_rate(yearly)
        except Exception as exc:
            logger.warning("%s: 营收增长分析失败: %s", result.ticker, exc)
    try:
        dividends = stock.dividends
        if dividends is not None and not dividends.empty:
            years_with_div = dividends.resample("YE").sum()
            result.dividend_years = _count_trailing_positive(years_with_div.values)
    except Exception as exc:
        logger.warning("%s: 分红历史分析失败: %s", result.ticker, exc)
def score_stock(analysis: StockAnalysis, profile: MarketProfile) -> StockAnalysis:
    if analysis.error:
        return analysis
    scores = {
        "pe_score": _score_pe(analysis.pe_ratio, profile),
        "pb_score": _score_pb(analysis.pb_ratio, profile),
        "graham_number_score": _score_graham_number(analysis.current_price, analysis.graham_number),
        "current_ratio_score": _score_current_ratio(analysis.current_ratio, profile),
        "debt_equity_score": _score_debt_equity(analysis.debt_to_equity, profile),
        "earnings_stability": _score_earnings_stability(
            analysis.profitable_years,
            profile,
            observed_window=analysis.profitable_years_observed,
        ),
        "dividend_score": _score_dividend(analysis.dividend_years, profile),
        "earnings_growth_score": _score_earnings_growth(analysis.earnings_growth, profile),
        "revenue_growth_score": _score_revenue_growth(analysis.revenue_growth, profile),
        "roe_score": _score_roe(analysis.roe, profile),
        "net_margin_score": _score_net_margin(analysis.net_margin, profile),
        "fcf_yield_score": _score_fcf_yield(analysis.fcf_yield, profile),
    }
    analysis.scores = scores
    weighted_total = 0.0
    available_weight = 0.0
    for key, score in scores.items():
        if score is None:
            continue
        weight = profile.score_weights[key]
        weighted_total += score * weight
        available_weight += weight
    analysis.coverage_ratio = round(available_weight, 4)
    total = weighted_total / available_weight if available_weight else 0.0
    total = _apply_earnings_decline_penalty(total, analysis.earnings_growth, profile)
    if analysis.snapshot_stale:
        total *= 0.85
    if analysis.data_source == "snapshot_fallback":
        total *= 0.92
    if not math.isfinite(total):
        total = 0.0
    analysis.total_score = round(total, 1)
    if total >= 80:
        analysis.grade = "A"
    elif total >= 65:
        analysis.grade = "B"
    elif total >= 50:
        analysis.grade = "C"
    elif total >= 35:
        analysis.grade = "D"
    else:
        analysis.grade = "F"
    if analysis.coverage_ratio < 0.6:
        analysis.grade = _cap_grade(analysis.grade, "C")
    elif analysis.coverage_ratio < 0.8:
        analysis.grade = _cap_grade(analysis.grade, "B")
    analysis.grade = _apply_graham_grade_cap(analysis.grade, analysis, profile)
    if analysis.grade == "A" and not _qualifies_for_a_grade(analysis, profile):
        analysis.grade = "B"
    if analysis.snapshot_stale and analysis.grade in {"A", "B"}:
        analysis.grade = "C"
    return analysis
def _cap_grade(current_grade: str, max_grade: str) -> str:
    return current_grade if GRADE_ORDER[current_grade] <= GRADE_ORDER[max_grade] else max_grade
def _is_financial_sector(sector: str | None) -> bool:
    return (sector or "").strip().lower() == "financial services"
def _graham_core_failure_categories(analysis: StockAnalysis, profile: MarketProfile) -> set[str]:
    failures: set[str] = set()
    criteria = profile.criteria
    if analysis.pe_ratio is not None and analysis.pe_ratio > criteria["pe_ratio_max"]:
        failures.add("valuation")
    if analysis.pb_ratio is not None and analysis.pb_ratio > criteria["pb_ratio_max"]:
        failures.add("valuation")
    if (
        analysis.pe_ratio is not None
        and analysis.pb_ratio is not None
        and analysis.pe_ratio * analysis.pb_ratio > criteria["pe_times_pb_max"]
    ):
        failures.add("valuation")
    if (
        not _is_financial_sector(analysis.sector)
        and analysis.current_ratio is not None
        and analysis.current_ratio < criteria["current_ratio_min"]
    ):
        failures.add("balance_sheet")
    if analysis.debt_to_equity is not None and analysis.debt_to_equity > criteria["debt_equity_max"]:
        failures.add("balance_sheet")
    profit_target = _effective_year_target(
        int(criteria["min_profitable_years"]),
        analysis.profitable_years_observed,
    )
    if analysis.profitable_years is not None and analysis.profitable_years < profit_target:
        failures.add("earnings_stability")
    if analysis.dividend_years is not None and analysis.dividend_years < int(criteria["min_dividend_years"]):
        failures.add("dividend_history")
    return failures
def _apply_graham_grade_cap(current_grade: str, analysis: StockAnalysis, profile: MarketProfile) -> str:
    failures = _graham_core_failure_categories(analysis, profile)
    if "valuation" in failures and "earnings_stability" in failures:
        return _cap_grade(current_grade, "C")
    if "valuation" in failures or "earnings_stability" in failures:
        return _cap_grade(current_grade, "B")
    if len(failures) >= 2:
        return _cap_grade(current_grade, "B")
    return current_grade
def _qualifies_for_a_grade(analysis: StockAnalysis, profile: MarketProfile) -> bool:
    if analysis.coverage_ratio < 0.95:
        return False
    failures = _graham_core_failure_categories(analysis, profile)
    if failures:
        return False
    min_dividend_years = int(profile.criteria["min_dividend_years"])
    if analysis.dividend_years is None or analysis.dividend_years < min_dividend_years:
        return False
    return True
def _safe_round(value: float | None, digits: int = 2) -> float | None:
    return round(value, digits) if value is not None else None
def _format_data_source(result: StockAnalysis) -> str:
    if result.data_source == "snapshot_fallback":
        return "旧快照回退" if result.snapshot_stale else "快照回退"
    if result.data_source == "snapshot_seeded":
        return "快照补全"
    return "实时抓取"
def _format_snapshot_status(result: StockAnalysis) -> str:
    if result.snapshot_stale and result.snapshot_age_days is not None:
        return f"旧快照（{result.snapshot_age_days}天）"
    if result.snapshot_stale:
        return "旧快照"
    if result.snapshot_age_days is not None:
        return "新快照"
    return "实时数据"
def _model_candidate_limit(profile: MarketProfile) -> int:
    return int(
        profile.screener_config.get(
            "model_top_n",
            profile.screener_config.get("top_n", profile.screener_config.get("max_candidates", 30)),
        )
    )
def _portfolio_run_date(market: str, run_date: date | None = None) -> str:
    return (run_date or _current_market_date(market)).isoformat()
def _portfolio_snapshot_rows(
    df: pd.DataFrame,
    profile: MarketProfile,
    *,
    top_n: int,
    run_date: date | None = None,
) -> list[dict[str, object]]:
    run_date_str = _portfolio_run_date(profile.code, run_date)
    portfolio_name = f"{profile.code}_model_top{top_n}_{run_date_str}"
    model_group = f"model_top{top_n}"
    watchlist_group = f"watchlist_top{top_n}"
    rows: list[dict[str, object]] = []
    selected = df.head(top_n).reset_index(drop=True) if not df.empty else pd.DataFrame()
    selected_count = len(selected)
    selected_weight = round(1 / selected_count, 6) if selected_count else 0.0
    for rank, row in enumerate(selected.to_dict("records"), 1):
        rows.append(
            {
                "run_date": run_date_str,
                "market": profile.code,
                "portfolio_name": portfolio_name,
                "group_name": model_group,
                "ticker": row["代码"],
                "rank": rank,
                "weight": selected_weight,
                "score": row.get("总分"),
                "grade": row.get("评级"),
                "data_source": row.get("数据来源"),
                "snapshot_status": row.get("快照状态"),
            }
        )
    watchlist_limit = selected_count if selected_count else top_n
    watchlist = normalize_tickers(profile.fallback_watchlist, profile.code)[:watchlist_limit]
    watchlist_count = len(watchlist)
    watchlist_weight = round(1 / watchlist_count, 6) if watchlist_count else 0.0
    for rank, ticker in enumerate(watchlist, 1):
        rows.append(
            {
                "run_date": run_date_str,
                "market": profile.code,
                "portfolio_name": portfolio_name,
                "group_name": watchlist_group,
                "ticker": ticker,
                "rank": rank,
                "weight": watchlist_weight,
                "score": None,
                "grade": "",
                "data_source": "市场内置股票池",
                "snapshot_status": "",
            }
        )
    return rows
def screen_stocks(
    tickers: list[str] | None = None,
    *,
    auto_discover: bool = True,
    market: str = DEFAULT_MARKET,
    show_progress: bool = True,
) -> pd.DataFrame:
    profile = MARKET_PROFILES[market]
    candidate_details: list[dict[str, object]] | None = None
    if tickers is None:
        if auto_discover:
            try:
                limit = _model_candidate_limit(profile)
                candidate_details = explain_candidates(profile, refresh_limit=limit)
                if len(candidate_details) > limit:
                    if show_progress:
                        print(f"  ✂ 深度评分仅保留预筛前 {limit} 只候选，避免请求过载")
                    candidate_details = candidate_details[:limit]
                tickers = [item["symbol"] for item in candidate_details]
                if not tickers:
                    if show_progress:
                        print("  ⚠ 未找到候选，回退到内置股票池")
                    tickers = normalize_tickers(profile.fallback_watchlist, market)
            except Exception as exc:
                if show_progress:
                    print(f"  ⚠ 候选发现失败 ({exc})，回退到内置股票池")
                tickers = normalize_tickers(profile.fallback_watchlist, market)
        else:
            tickers = normalize_tickers(profile.fallback_watchlist, market)
    else:
        tickers = normalize_tickers(tickers, market)
    results: list[StockAnalysis] = []
    total = len(tickers)
    snapshot_map = {
        str(item["symbol"]): item.get("snapshot", {})
        for item in candidate_details or []
        if item.get("snapshot")
    }
    def _analyze_one(ticker: str) -> StockAnalysis:
        return score_stock(fetch_stock_data(ticker, profile, snapshot=snapshot_map.get(ticker)), profile)
    with ThreadPoolExecutor(max_workers=min(ANALYSIS_MAX_WORKERS, max(1, total))) as executor:
        futures = {executor.submit(_analyze_one, ticker): ticker for ticker in tickers}
        for i, future in enumerate(as_completed(futures), 1):
            ticker = futures[future]
            try:
                analysis = future.result()
            except Exception as exc:
                analysis = StockAnalysis(ticker=ticker, market=market, error=str(exc))
            if show_progress:
                if analysis.error:
                    print(f"  [{i}/{total}] {ticker} ⚠ {analysis.error}")
                else:
                    coverage = round(analysis.coverage_ratio * 100, 1)
                    print(f"  [{i}/{total}] {ticker} ✓ 评分: {analysis.total_score} ({analysis.grade}), 数据完整度 {coverage}%")
            results.append(analysis)
    rows = []
    for result in results:
        if result.error:
            continue
        rows.append(
            {
                "市场": profile.label,
                "代码": result.ticker,
                "公司": result.company_name[:20],
                "行业": result.sector,
                "币种": profile.currency,
                "价格": _safe_round(result.current_price, 2),
                "P/E": _safe_round(result.pe_ratio, 1),
                "P/B": _safe_round(result.pb_ratio, 2),
                "P/E×P/B": _safe_round(result.pe_times_pb, 1),
                "Graham#": _safe_round(result.graham_number, 2),
                "安全边际%": _safe_round(result.margin_of_safety * 100 if result.margin_of_safety is not None else None, 1),
                "流动比率": _safe_round(result.current_ratio, 2),
                "D/E": _safe_round(result.debt_to_equity, 2),
                "盈利年数": result.profitable_years,
                "分红年数": result.dividend_years,
                "盈利增长%": _safe_round(result.earnings_growth * 100 if result.earnings_growth is not None else None, 1),
                "营收增长%": _safe_round(result.revenue_growth * 100 if result.revenue_growth is not None else None, 1),
                "ROE%": _safe_round(result.roe * 100 if result.roe is not None else None, 1),
                "净利率%": _safe_round(result.net_margin * 100 if result.net_margin is not None else None, 1),
                "FCF收益%": _safe_round(result.fcf_yield * 100 if result.fcf_yield is not None else None, 1),
                "数据来源": _format_data_source(result),
                "快照状态": _format_snapshot_status(result),
                "数据完整度%": round(result.coverage_ratio * 100, 1),
                "总分": result.total_score,
                "评级": result.grade,
            }
        )
    df = pd.DataFrame(rows)
    if not df.empty:
        df["_评级排序"] = df["评级"].map(GRADE_ORDER).fillna(0)
        df = (
            df.sort_values(["_评级排序", "总分", "数据完整度%"], ascending=[False, False, False])
            .drop(columns=["_评级排序"])
            .reset_index(drop=True)
        )
        df.index += 1
    return df
def print_report(df: pd.DataFrame, profile: MarketProfile, *, top_n: int | None = None) -> None:
    print("\n" + "=" * 90)
    print(f"  📊 WealthKeeper - {profile.label} 市场量化选股报告")
    print("=" * 90)
    if df.empty:
        print("  没有有效数据")
        return
    with pd.option_context(
        "display.max_columns", None,
        "display.width", 220,
        "display.float_format", "{:.2f}".format,
    ):
        summary_cols = ["代码", "公司", "价格", "P/E", "P/B", "Graham#", "安全边际%", "快照状态", "数据完整度%", "总分", "评级"]
        top_ab = df[df["评级"].isin(["A", "B"])]
        if not top_ab.empty:
            print(f"\n【{profile.label} 市场推荐 (A/B 级)】共 {len(top_ab)} 只\n")
            print(top_ab[summary_cols].to_string(index=False))
        else:
            print(f"\n【{profile.label} 市场推荐】\n")
            print("当前没有达到 A/B 门槛的推荐标的。\n")
        watchlist = df[df["评级"].eq("C")]
        if not watchlist.empty:
            print(f"【{profile.label} 市场观察名单 (C 级)】共 {len(watchlist)} 只\n")
            print(watchlist[summary_cols].to_string(index=False))
        ranking_title = f"{profile.label} 市场前 {top_n} 名" if top_n is not None else f"{profile.label} 市场完整排名"
        print(f"\n【{ranking_title}】\n")
        print(df.to_string(index=False))
    print("\n" + "=" * 90)
    print("  ⚠ 声明: 本模型仅供学习研究，不构成投资建议。投资有风险，入市需谨慎。")
    print("=" * 90 + "\n")
if __name__ == "__main__":
    raise SystemExit("请使用 `ws insight --market <market> --top <n>` 查看市场候选，或使用 `ws analyze --market <market> <ticker...>` 分析指定股票。")
