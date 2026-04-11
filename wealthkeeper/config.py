from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = PACKAGE_DIR.parent
DATA_DIR = PROJECT_ROOT / "data"
UNIVERSE_DIR = DATA_DIR / "universe"
UNIVERSE_SOURCE_DIR = UNIVERSE_DIR / "sources"
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
@dataclass(frozen=True)
class MarketProfile:
    code: str
    label: str
    currency: str
    criteria: dict[str, float]
    score_weights: dict[str, float]
    earnings_decline_penalty: dict[str, tuple[float | None, float]]
    screener_config: dict[str, object]
    universe_config: dict[str, object]
    fallback_watchlist: list[str]
    discovery_universe: list[str]
def _cfg(base: dict[str, object], /, **overrides: object) -> dict[str, object]:
    return {**base, **overrides}
EARNINGS_DECLINE_PENALTY = {"mild": (-0.10, 0.80), "moderate": (-0.30, 0.60), "severe": (None, 0.45)}
COMMON_CRITERIA = {"roe_excellent": 0.18, "roe_good": 0.12, "net_margin_excellent": 0.18, "net_margin_good": 0.08}
US_CRITERIA = _cfg(COMMON_CRITERIA, pe_ratio_max=15.0, pb_ratio_max=1.5, pe_times_pb_max=22.5, current_ratio_min=2.0, debt_equity_max=0.5, min_profitable_years=5, earnings_growth_min=0.33, min_dividend_years=5, revenue_growth_min=0.10, roe_excellent=0.20, roe_good=0.15, net_margin_excellent=0.20, net_margin_good=0.10, fcf_yield_good=0.05)
CN_CRITERIA = _cfg(COMMON_CRITERIA, pe_ratio_max=22.0, pb_ratio_max=2.8, pe_times_pb_max=45.0, current_ratio_min=1.5, debt_equity_max=0.8, min_profitable_years=4, earnings_growth_min=0.20, min_dividend_years=2, revenue_growth_min=0.12, fcf_yield_good=0.04)
HK_CRITERIA = _cfg(COMMON_CRITERIA, pe_ratio_max=18.0, pb_ratio_max=1.8, pe_times_pb_max=28.0, current_ratio_min=1.5, debt_equity_max=0.7, min_profitable_years=4, earnings_growth_min=0.25, min_dividend_years=3, revenue_growth_min=0.08, fcf_yield_good=0.05)
COMMON_WEIGHTS = {"pe_score": 0.10, "pb_score": 0.08, "graham_number_score": 0.10, "current_ratio_score": 0.08, "debt_equity_score": 0.08, "earnings_stability": 0.08, "dividend_score": 0.04, "earnings_growth_score": 0.08, "revenue_growth_score": 0.08, "roe_score": 0.12, "net_margin_score": 0.08, "fcf_yield_score": 0.08}
US_SCORE_WEIGHTS = _cfg(COMMON_WEIGHTS, graham_number_score=0.12, debt_equity_score=0.07, fcf_yield_score=0.07)
CN_SCORE_WEIGHTS = _cfg(COMMON_WEIGHTS, pe_score=0.09, graham_number_score=0.08, earnings_stability=0.09, dividend_score=0.02, earnings_growth_score=0.10, revenue_growth_score=0.10)
HK_SCORE_WEIGHTS = _cfg(COMMON_WEIGHTS, current_ratio_score=0.07, debt_equity_score=0.07, dividend_score=0.06, revenue_growth_score=0.07, roe_score=0.11, fcf_yield_score=0.10)
COMMON_SCREENER = {"top_n": 30, "pre_screen_roe_min": 0.05, "pre_screen_net_margin_min": 0.03, "max_candidates": 30, "model_top_n": 30}
US_SCREENER_CONFIG = _cfg(COMMON_SCREENER, exchanges=["NMS", "NYQ"], top_n=40, min_market_cap=1_000_000_000, min_avg_volume=300_000, pre_screen_pe_max=18, pre_screen_pb_max=2.0, pre_screen_roe_min=0.01, pre_screen_net_margin_min=0.01, max_candidates=200, model_top_n=40)
CN_SCREENER_CONFIG = _cfg(COMMON_SCREENER, min_market_cap=20_000_000_000, min_avg_volume=1_000_000, pre_screen_pe_max=35, pre_screen_pb_max=5.0)
HK_SCREENER_CONFIG = _cfg(COMMON_SCREENER, min_market_cap=15_000_000_000, min_avg_volume=500_000, pre_screen_pe_max=28, pre_screen_pb_max=3.0)
def _universe_config(code: str, exchanges: list[str], boards: list[str], keywords: list[str]) -> dict[str, object]:
    return {"source_path": UNIVERSE_SOURCE_DIR / f"{code}_seed.csv", "raw_path": UNIVERSE_DIR / f"{code}_raw.csv", "investable_path": UNIVERSE_DIR / f"{code}_investable.csv", "allowed_exchanges": exchanges, "allowed_boards": boards, "exclude_name_keywords": keywords}
US_UNIVERSE_CONFIG = _universe_config("us", ["NMS", "NYQ"], ["main_board"], ["ETF", "TRUST", "FUND", "PREFERRED", "WARRANT", "RIGHT"])
CN_UNIVERSE_CONFIG = _universe_config("cn", ["SH", "SZ"], ["main_board", "chinext"], ["ST", "*ST", "ETF", "LOF"])
HK_UNIVERSE_CONFIG = _universe_config("hk", ["HKEX"], ["main_board"], ["ETF", "TRUST", "FUND", "WARRANT", "RIGHTS"])
US_WATCHLIST = ["AAPL", "MSFT", "GOOGL", "JNJ", "PG", "KO", "PEP", "WMT", "JPM", "BAC", "XOM", "CVX", "UNH", "HD", "BRK-B", "INTC", "VZ", "T", "IBM", "MMM", "CAT", "GE", "F", "GM", "MRK", "PFE", "ABBV", "BMY", "MCD", "NKE", "DIS", "COST", "LOW", "TGT", "CL", "GIS", "K", "SJM", "HRL", "ADM", "DE", "EMR"]
CN_WATCHLIST = ["600519", "000858", "600036", "600900", "600276", "600887", "000333", "002415", "002594", "300750", "300760", "300308", "000651", "601318", "600309", "000063", "002311", "300274", "600809", "601166", "601088", "600438", "601899", "000725"]
HK_WATCHLIST = ["0700", "9988", "9618", "1810", "0005", "0388", "0941", "0883", "0001", "2318", "2388", "1109", "0688", "0762", "0823", "1928", "2015", "1299", "3988"]
CN_DISCOVERY_UNIVERSE = ["600519", "000858", "600036", "600900", "600276", "600887", "000333", "002415", "002594", "300750", "300760", "300308", "000651", "601318", "600309", "000063", "002311", "300274", "600809", "601166", "601088", "600438", "601899", "000725", "601012", "600031", "002352", "603259", "600426", "601668", "601398", "601288", "601939", "601988", "600030", "600585", "603288", "002714", "300124", "002142", "603501", "600690", "600048", "000001", "000568", "600104", "002179", "300015", "300059", "300033", "300498", "300408", "600196", "600406", "002050", "601601"]
HK_DISCOVERY_UNIVERSE = ["0700", "9988", "9618", "1810", "0005", "0388", "0941", "0883", "0001", "2318", "2388", "1109", "0688", "0762", "0823", "1928", "2015", "1299", "3988", "3690", "1211", "9888", "9999", "6618", "1024", "1093", "1177", "2269", "0960", "0002", "0016", "0011", "0267", "0291", "0686", "1038", "0175", "0285", "2319", "2007", "1113", "1209", "3328", "3968", "9633", "6862", "9618", "6690", "3888", "0144"]
MARKET_PROFILES = {
    "us": MarketProfile(code="us", label="US", currency="USD", criteria=US_CRITERIA, score_weights=US_SCORE_WEIGHTS, earnings_decline_penalty=EARNINGS_DECLINE_PENALTY, screener_config=US_SCREENER_CONFIG, universe_config=US_UNIVERSE_CONFIG, fallback_watchlist=US_WATCHLIST, discovery_universe=US_WATCHLIST),
    "cn": MarketProfile(code="cn", label="CN", currency="CNY", criteria=CN_CRITERIA, score_weights=CN_SCORE_WEIGHTS, earnings_decline_penalty=EARNINGS_DECLINE_PENALTY, screener_config=CN_SCREENER_CONFIG, universe_config=CN_UNIVERSE_CONFIG, fallback_watchlist=CN_WATCHLIST, discovery_universe=CN_DISCOVERY_UNIVERSE),
    "hk": MarketProfile(code="hk", label="HK", currency="HKD", criteria=HK_CRITERIA, score_weights=HK_SCORE_WEIGHTS, earnings_decline_penalty=EARNINGS_DECLINE_PENALTY, screener_config=HK_SCREENER_CONFIG, universe_config=HK_UNIVERSE_CONFIG, fallback_watchlist=HK_WATCHLIST, discovery_universe=HK_DISCOVERY_UNIVERSE),
}
DEFAULT_MARKET = "us"
