"""
Graham 量化选股模型 - 基于《聪明的投资者》

核心思想：
1. 安全边际 (Margin of Safety) - 以低于内在价值的价格买入
2. Graham Number = sqrt(22.5 × EPS × BVPS) 作为内在价值估算
3. 多维度评分：估值、财务稳健性、盈利稳定性、分红、成长性
"""

import logging
import math
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from itertools import takewhile
from typing import List, Optional

import pandas as pd
import yfinance as yf

from .config import GRAHAM_CRITERIA, SCORE_WEIGHTS, FALLBACK_WATCHLIST, EARNINGS_DECLINE_PENALTY
from .screener import discover_candidates

# 仅过滤 yfinance / urllib3 的噪音警告，不影响其他库
warnings.filterwarnings("ignore", module=r"yfinance\..*")
warnings.filterwarnings("ignore", module=r"urllib3\..*")
warnings.filterwarnings("ignore", category=FutureWarning, module=r"pandas\..*")

logger = logging.getLogger(__name__)


@dataclass
class StockAnalysis:
    """单只股票的 Graham 分析结果"""
    ticker: str
    company_name: str = ""
    sector: str = ""
    # 价格与估值
    current_price: float = 0.0
    eps: float = 0.0
    book_value_per_share: float = 0.0
    pe_ratio: float = 0.0
    pb_ratio: float = 0.0
    pe_times_pb: float = 0.0
    graham_number: float = 0.0
    margin_of_safety: float = 0.0  # (Graham Number - Price) / Graham Number
    # 财务指标
    current_ratio: float = 0.0
    debt_to_equity: float = 0.0
    # 盈利与分红
    profitable_years: int = 0
    dividend_years: int = 0
    earnings_growth: float = 0.0
    revenue_growth: float = 0.0
    dividend_yield: float = 0.0
    # 质量因子
    roe: float = 0.0
    net_margin: float = 0.0
    fcf_yield: float = 0.0
    # 评分
    scores: dict = field(default_factory=dict)
    total_score: float = 0.0
    grade: str = ""  # A/B/C/D/F
    error: str = ""


# ============================================================
# 通用评分工具函数 — 消除各 _score_xxx 之间的重复逻辑
# ============================================================

def _score_lower_is_better(value: float, max_threshold: float) -> float:
    """通用"越低越好"评分（用于 P/E、P/B 等）"""
    if value <= 0:
        return 0
    if value <= max_threshold * 0.5:
        return 100
    elif value <= max_threshold:
        return 100 - (value - max_threshold * 0.5) / (max_threshold * 0.5) * 40
    elif value <= max_threshold * 2:
        return 60 - (value - max_threshold) / max_threshold * 60
    return 0


def _score_years_vs_target(years: int, target: int) -> float:
    """通用"连续年数"评分（用于盈利稳定性、分红持续性）"""
    if years >= target:
        return min(100, 70 + (years - target) * 6)
    return max(0, years / target * 70)


def _score_growth(growth: float, target: float, full_marks_mul: float = 2,
                  interp_divisor_mul: float = 1, negative_floor: float = 0) -> float:
    """通用增长评分（用于盈利增长、营收增长）
    full_marks_mul: growth >= target * full_marks_mul 时满分
    interp_divisor_mul: 70→100 区间的插值分母倍数
    negative_floor: 轻微负增长时的保底分（0 表示不允许负增长得分）
    """
    if growth >= target * full_marks_mul:
        return 100
    elif growth >= target:
        return 70 + (growth - target) / (target * interp_divisor_mul) * 30
    elif growth > 0:
        return growth / target * 70
    elif negative_floor > 0 and growth > -0.1:
        return negative_floor
    return 0


def _score_quality_tier(value: float, excellent: float, good: float,
                        floor_threshold: float, floor_base: float) -> float:
    """通用"质量因子"评分（用于 ROE、净利率、FCF 收益率）"""
    if value <= 0:
        return 0
    if value >= excellent * 2:
        return 100
    elif value >= excellent:
        return 80 + (value - excellent) / excellent * 20
    elif value >= good:
        return 60 + (value - good) / (excellent - good) * 20
    elif value >= floor_threshold:
        return floor_base + (value - floor_threshold) / (good - floor_threshold) * (60 - floor_base)
    return value / floor_threshold * floor_base


def _score_pe(pe: float) -> float:
    return _score_lower_is_better(pe, GRAHAM_CRITERIA["pe_ratio_max"])


def _score_pb(pb: float) -> float:
    return _score_lower_is_better(pb, GRAHAM_CRITERIA["pb_ratio_max"])


def _score_graham_number(price: float, graham_num: float) -> float:
    """Graham Number 安全边际评分"""
    if graham_num <= 0 or price <= 0:
        return 0
    margin = (graham_num - price) / graham_num
    if margin >= 0.5:
        return 100
    elif margin >= 0.3:
        return 80
    elif margin >= 0.1:
        return 60
    elif margin >= 0:
        return 40
    elif margin >= -0.3:
        return 20
    return 0


def _score_current_ratio(cr: float) -> float:
    """流动比率评分"""
    if cr >= 3.0:
        return 100
    elif cr >= GRAHAM_CRITERIA["current_ratio_min"]:
        return 70 + (cr - 2.0) * 30
    elif cr >= 1.5:
        return 40 + (cr - 1.5) * 60
    elif cr >= 1.0:
        return (cr - 1.0) * 80
    return 0


def _score_debt_equity(de: float) -> float:
    """债务权益比评分: 越低越好"""
    if de <= 0:
        return 100
    max_de = GRAHAM_CRITERIA["debt_equity_max"]
    if de <= max_de * 0.5:
        return 100
    elif de <= max_de:
        return 70 + (max_de - de) / (max_de * 0.5) * 30
    elif de <= max_de * 2:
        return 30 + (max_de * 2 - de) / max_de * 40
    elif de <= max_de * 4:
        return (max_de * 4 - de) / (max_de * 2) * 30
    return 0


def _score_earnings_stability(years: int) -> float:
    return _score_years_vs_target(years, GRAHAM_CRITERIA["min_profitable_years"])


def _score_dividend(years: int) -> float:
    return _score_years_vs_target(years, GRAHAM_CRITERIA["min_dividend_years"])


def _score_earnings_growth(growth: float) -> float:
    return _score_growth(growth, GRAHAM_CRITERIA["earnings_growth_min"])


def _score_revenue_growth(growth: float) -> float:
    return _score_growth(growth, GRAHAM_CRITERIA["revenue_growth_min"],
                         full_marks_mul=3, interp_divisor_mul=2, negative_floor=20)


def _apply_earnings_decline_penalty(total: float, earnings_growth: float) -> float:
    """盈利负增长时对总分施加惩罚"""
    if earnings_growth >= 0:
        return total
    decline = earnings_growth  # 负数
    penalty = EARNINGS_DECLINE_PENALTY
    if decline > penalty["mild"][0]:
        return total * penalty["mild"][1]
    elif decline > penalty["moderate"][0]:
        return total * penalty["moderate"][1]
    else:
        return total * penalty["severe"][1]


def _score_roe(roe: float) -> float:
    return _score_quality_tier(roe, GRAHAM_CRITERIA["roe_excellent"],
                               GRAHAM_CRITERIA["roe_good"], 0.08, 30)


def _score_net_margin(margin: float) -> float:
    return _score_quality_tier(margin, GRAHAM_CRITERIA["net_margin_excellent"],
                               GRAHAM_CRITERIA["net_margin_good"], 0.03, 20)


def _score_fcf_yield(fcf_yield: float) -> float:
    """自由现金流收益率评分 — 原始逻辑: good*2 满分, good 得 70, 0.02 得 30"""
    if fcf_yield <= 0:
        return 0
    good = GRAHAM_CRITERIA["fcf_yield_good"]
    if fcf_yield >= good * 2:
        return 100
    elif fcf_yield >= good:
        return 70 + (fcf_yield - good) / good * 30
    elif fcf_yield >= 0.02:
        return 30 + (fcf_yield - 0.02) / (good - 0.02) * 40
    return fcf_yield / 0.02 * 30


def _get_num(info: dict, key: str) -> float:
    """从 yfinance info dict 安全取数值，处理 None 返回"""
    return info.get(key, 0) or 0


def _find_financial_row(financials: pd.DataFrame, labels: list) -> Optional[pd.Series]:
    """在 financials 中按优先级查找行（yfinance 不同版本可能用不同标签）"""
    for label in labels:
        if label in financials.index:
            return financials.loc[label]
    return None


def _count_trailing_positive(values) -> int:
    """从末尾开始计数连续 > 0 的个数"""
    return sum(1 for _ in takewhile(lambda v: v > 0, reversed(values)))


def _calc_growth_rate(series: pd.Series) -> Optional[float]:
    """计算首尾增长率，要求至少 2 个数据点且起点 > 0"""
    if len(series) < 2:
        return None
    oldest, newest = series.iloc[0], series.iloc[-1]
    if oldest > 0:
        return (newest - oldest) / abs(oldest)
    return None


def calc_graham_number(eps: float, bvps: float) -> float:
    """
    Graham Number = sqrt(22.5 × EPS × BVPS)

    这是 Graham 估算股票内在价值的简化公式。
    22.5 = 15 (合理P/E) × 1.5 (合理P/B)
    """
    if eps <= 0 or bvps <= 0:
        return 0.0
    return math.sqrt(22.5 * eps * bvps)


def fetch_stock_data(ticker: str) -> StockAnalysis:
    """从 yfinance 获取股票数据并计算 Graham 各项指标"""
    result = StockAnalysis(ticker=ticker)

    try:
        stock = yf.Ticker(ticker)
        info = stock.info

        if not info or info.get("regularMarketPrice") is None and info.get("currentPrice") is None:
            result.error = "无法获取数据"
            return result

        result.company_name = info.get("shortName", info.get("longName", ticker))
        result.sector = info.get("sector", "N/A")
        result.current_price = info.get("currentPrice", info.get("regularMarketPrice", 0))

        result.eps = _get_num(info, "trailingEps")
        result.book_value_per_share = _get_num(info, "bookValue")
        result.pe_ratio = _get_num(info, "trailingPE")
        result.pb_ratio = _get_num(info, "priceToBook")
        result.pe_times_pb = result.pe_ratio * result.pb_ratio

        result.graham_number = calc_graham_number(result.eps, result.book_value_per_share)
        if result.graham_number > 0 and result.current_price > 0:
            result.margin_of_safety = (result.graham_number - result.current_price) / result.graham_number

        result.current_ratio = _get_num(info, "currentRatio")
        # yfinance 返回百分比形式的 D/E，需要除以 100
        result.debt_to_equity = _get_num(info, "debtToEquity") / 100
        result.dividend_yield = _get_num(info, "dividendYield")

        result.roe = _get_num(info, "returnOnEquity")
        result.net_margin = _get_num(info, "profitMargins")
        fcf = _get_num(info, "freeCashflow")
        market_cap = _get_num(info, "marketCap")
        if market_cap > 0 and fcf > 0:
            result.fcf_yield = fcf / market_cap

        _analyze_earnings_history(stock, result)

    except Exception as e:
        logger.warning("获取 %s 数据失败: %s", ticker, e, exc_info=True)
        result.error = str(e)

    return result


def _analyze_earnings_history(stock: yf.Ticker, result: StockAnalysis):
    """分析历史盈利和分红数据"""
    # 一次性获取 financials，避免重复 HTTP 请求
    try:
        financials = stock.financials
    except Exception as e:
        logger.warning("%s: 获取年度财务数据失败: %s", result.ticker, e)
        financials = None

    if financials is not None and not financials.empty:
        # 盈利分析
        try:
            net_income = _find_financial_row(financials,
                                             ["Net Income", "NetIncome", "Net Income Common Stockholders"])
            if net_income is not None:
                yearly = net_income.dropna().sort_index()
                result.profitable_years = _count_trailing_positive(yearly.values)
                growth = _calc_growth_rate(yearly)
                if growth is not None:
                    result.earnings_growth = growth
        except Exception as e:
            logger.warning("%s: 盈利历史分析失败: %s", result.ticker, e)

        # 营收增长率
        try:
            revenue = _find_financial_row(financials, ["Total Revenue", "TotalRevenue", "Revenue"])
            if revenue is not None:
                yearly = revenue.dropna().sort_index()
                growth = _calc_growth_rate(yearly)
                if growth is not None:
                    result.revenue_growth = growth
        except Exception as e:
            logger.warning("%s: 营收增长分析失败: %s", result.ticker, e)

    # 分红历史
    try:
        dividends = stock.dividends
        if dividends is not None and not dividends.empty:
            years_with_div = dividends.resample("YE").sum()
            result.dividend_years = _count_trailing_positive(years_with_div.values)
    except Exception as e:
        logger.warning("%s: 分红历史分析失败: %s", result.ticker, e)


def score_stock(analysis: StockAnalysis) -> StockAnalysis:
    """对股票进行 Graham 多维度评分"""
    if analysis.error:
        return analysis

    scores = {
        "pe_score": _score_pe(analysis.pe_ratio),
        "pb_score": _score_pb(analysis.pb_ratio),
        "graham_number_score": _score_graham_number(analysis.current_price, analysis.graham_number),
        "current_ratio_score": _score_current_ratio(analysis.current_ratio),
        "debt_equity_score": _score_debt_equity(analysis.debt_to_equity),
        "earnings_stability": _score_earnings_stability(analysis.profitable_years),
        "dividend_score": _score_dividend(analysis.dividend_years),
        "earnings_growth_score": _score_earnings_growth(analysis.earnings_growth),
        "revenue_growth_score": _score_revenue_growth(analysis.revenue_growth),
        "roe_score": _score_roe(analysis.roe),
        "net_margin_score": _score_net_margin(analysis.net_margin),
        "fcf_yield_score": _score_fcf_yield(analysis.fcf_yield),
    }
    analysis.scores = scores

    total = sum(scores[k] * SCORE_WEIGHTS[k] for k in scores)
    total = _apply_earnings_decline_penalty(total, analysis.earnings_growth)
    analysis.total_score = round(total, 1)

    if total >= 80:
        analysis.grade = "A"   # 强烈推荐 - 高度符合 Graham 标准
    elif total >= 65:
        analysis.grade = "B"   # 推荐 - 大部分符合
    elif total >= 50:
        analysis.grade = "C"   # 中性 - 部分符合
    elif total >= 35:
        analysis.grade = "D"   # 谨慎 - 较少符合
    else:
        analysis.grade = "F"   # 不推荐 - 不符合 Graham 标准

    return analysis


def screen_stocks(tickers: Optional[List[str]] = None, auto_discover: bool = True) -> pd.DataFrame:
    """批量筛选股票并返回排名结果

    Args:
        tickers: 指定的股票代码列表。如果提供，则直接使用。
        auto_discover: 当 tickers 为 None 时，是否自动发现候选股票。
                       True  -> 调用 screener 自动预筛（默认）
                       False -> 使用 FALLBACK_WATCHLIST 备用列表
    """
    if tickers is None:
        if auto_discover:
            try:
                tickers = discover_candidates()
                if not tickers:
                    print("  ⚠ 自动发现未找到候选，回退到备用列表")
                    tickers = FALLBACK_WATCHLIST
            except Exception as e:
                print(f"  ⚠ 自动发现失败 ({e})，回退到备用列表")
                tickers = FALLBACK_WATCHLIST
        else:
            tickers = FALLBACK_WATCHLIST

    results = []
    total = len(tickers)

    def _analyze_one(ticker: str) -> StockAnalysis:
        analysis = fetch_stock_data(ticker)
        return score_stock(analysis)

    # 并发获取和评分，I/O 密集型任务使用线程池加速
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(_analyze_one, t): t for t in tickers}
        for i, future in enumerate(as_completed(futures), 1):
            ticker = futures[future]
            try:
                analysis = future.result()
            except Exception as e:
                analysis = StockAnalysis(ticker=ticker, error=str(e))

            if analysis.error:
                print(f"  [{i}/{total}] {ticker} ⚠ {analysis.error}")
            else:
                print(f"  [{i}/{total}] {ticker} ✓ 评分: {analysis.total_score} ({analysis.grade})")
            results.append(analysis)

    rows = []
    for r in results:
        if r.error:
            continue
        rows.append({
            "代码": r.ticker,
            "公司": r.company_name[:20],
            "行业": r.sector,
            "价格": round(r.current_price, 2),
            "P/E": round(r.pe_ratio, 1),
            "P/B": round(r.pb_ratio, 2),
            "P/E×P/B": round(r.pe_times_pb, 1),
            "Graham#": round(r.graham_number, 2),
            "安全边际%": round(r.margin_of_safety * 100, 1),
            "流动比率": round(r.current_ratio, 2),
            "D/E": round(r.debt_to_equity, 2),
            "盈利年数": r.profitable_years,
            "分红年数": r.dividend_years,
            "盈利增长%": round(r.earnings_growth * 100, 1),
            "营收增长%": round(r.revenue_growth * 100, 1),
            "ROE%": round(r.roe * 100, 1),
            "净利率%": round(r.net_margin * 100, 1),
            "FCF收益%": round(r.fcf_yield * 100, 1),
            "总分": r.total_score,
            "评级": r.grade,
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("总分", ascending=False).reset_index(drop=True)
        df.index += 1  # 排名从1开始

    return df



def print_report(df: pd.DataFrame):
    """打印分析报告"""
    print("\n" + "=" * 90)
    print("  📊 Graham 聪明投资者 - 量化选股报告")
    print("=" * 90)

    if df.empty:
        print("  没有有效数据")
        return

    # 使用 option_context 避免污染全局设置
    with pd.option_context(
        "display.max_columns", None,
        "display.width", 180,
        "display.float_format", "{:.2f}".format,
    ):
        summary_cols = ["代码", "公司", "价格", "P/E", "P/B", "Graham#", "安全边际%", "总分", "评级"]

        # A/B 级推荐（高分股一目了然）
        top_ab = df[df["评级"].isin(["A", "B"])]
        if not top_ab.empty:
            print(f"\n【Graham 推荐 (A/B 级)】共 {len(top_ab)} 只\n")
            print(top_ab[summary_cols].to_string(index=False))

        # 完整排名
        print("\n【完整排名】\n")
        print(df.to_string(index=False))

    print("\n" + "=" * 90)
    print("  ⚠ 声明: 本模型仅供学习研究，不构成投资建议。投资有风险，入市需谨慎。")
    print("=" * 90 + "\n")


if __name__ == "__main__":
    import sys
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--top10", action="store_true", help="仅显示值得投资的前 10 只股票")
    parser.add_argument("--watchlist", action="store_true", help="使用备用股票池而非自动发现")
    # 其余位置参数作为 ticker 列表
    parser.add_argument("tickers", nargs="*", help="可选的股票代码列表")
    args, unknown = parser.parse_known_args()

    # 如果用户提供了 ticker 参数则使用，否则根据模式选择
    tickers = args.tickers if args.tickers else None
    auto_discover = not args.watchlist

    print("\n🔍 Graham 聪明投资者 - 量化选股模型")
    print("   基于《The Intelligent Investor》核心原则\n")

    if tickers:
        print(f"  📋 分析指定的 {len(tickers)} 只股票\n")
    elif auto_discover:
        print("  🌐 模式: 自动发现（全美股预筛）\n")
    else:
        print("  📋 模式: 备用股票池\n")

    df = screen_stocks(tickers, auto_discover=auto_discover)
    if args.top10:
        # 仅打印前 10 名（已在 print_report 中包含该区块）
        print_report(df.head(10))
    else:
        print_report(df)
