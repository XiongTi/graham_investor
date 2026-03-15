"""
Graham 量化选股模型 - 基于《聪明的投资者》

核心思想：
1. 安全边际 (Margin of Safety) - 以低于内在价值的价格买入
2. Graham Number = sqrt(22.5 × EPS × BVPS) 作为内在价值估算
3. 多维度评分：估值、财务稳健性、盈利稳定性、分红、成长性
"""

import math
import warnings
from dataclasses import dataclass, field
from typing import List, Optional

import pandas as pd
import yfinance as yf

from .config import GRAHAM_CRITERIA, SCORE_WEIGHTS, DEFAULT_WATCHLIST

warnings.filterwarnings("ignore")


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
    dividend_yield: float = 0.0
    # 评分
    scores: dict = field(default_factory=dict)
    total_score: float = 0.0
    grade: str = ""  # A/B/C/D/F
    error: str = ""


def _score_pe(pe: float) -> float:
    """P/E 评分: 越低越好, 0为最差, 100为最好"""
    if pe <= 0:
        return 0  # 亏损
    max_pe = GRAHAM_CRITERIA["pe_ratio_max"]
    if pe <= max_pe * 0.5:
        return 100
    elif pe <= max_pe:
        return 100 - (pe - max_pe * 0.5) / (max_pe * 0.5) * 40
    elif pe <= max_pe * 2:
        return 60 - (pe - max_pe) / max_pe * 60
    return 0


def _score_pb(pb: float) -> float:
    """P/B 评分"""
    if pb <= 0:
        return 0
    max_pb = GRAHAM_CRITERIA["pb_ratio_max"]
    if pb <= max_pb * 0.5:
        return 100
    elif pb <= max_pb:
        return 100 - (pb - max_pb * 0.5) / (max_pb * 0.5) * 40
    elif pb <= max_pb * 2:
        return 60 - (pb - max_pb) / max_pb * 60
    return 0


def _score_graham_number(price: float, graham_num: float) -> float:
    """Graham Number 安全边际评分"""
    if graham_num <= 0 or price <= 0:
        return 0
    margin = (graham_num - price) / graham_num
    if margin >= 0.5:
        return 100  # 50%+ 安全边际，极佳
    elif margin >= 0.3:
        return 80
    elif margin >= 0.1:
        return 60
    elif margin >= 0:
        return 40  # 价格接近但未超过 Graham Number
    elif margin >= -0.3:
        return 20  # 轻度高估
    return 0  # 严重高估


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
        return 100  # 无债务
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
    """盈利稳定性评分"""
    target = GRAHAM_CRITERIA["min_profitable_years"]
    if years >= target:
        return min(100, 70 + (years - target) * 6)
    return max(0, years / target * 70)


def _score_dividend(years: int) -> float:
    """分红持续性评分"""
    target = GRAHAM_CRITERIA["min_dividend_years"]
    if years >= target:
        return min(100, 70 + (years - target) * 6)
    return max(0, years / target * 70)


def _score_earnings_growth(growth: float) -> float:
    """盈利增长评分"""
    target = GRAHAM_CRITERIA["earnings_growth_min"]
    if growth >= target * 2:
        return 100
    elif growth >= target:
        return 70 + (growth - target) / target * 30
    elif growth > 0:
        return growth / target * 70
    return 0  # 负增长


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

        # 基本信息
        result.company_name = info.get("shortName", info.get("longName", ticker))
        result.sector = info.get("sector", "N/A")
        result.current_price = info.get("currentPrice", info.get("regularMarketPrice", 0))

        # EPS 和 Book Value
        result.eps = info.get("trailingEps", 0) or 0
        result.book_value_per_share = info.get("bookValue", 0) or 0

        # P/E 和 P/B
        result.pe_ratio = info.get("trailingPE", 0) or 0
        result.pb_ratio = info.get("priceToBook", 0) or 0
        result.pe_times_pb = result.pe_ratio * result.pb_ratio

        # Graham Number
        result.graham_number = calc_graham_number(result.eps, result.book_value_per_share)
        if result.graham_number > 0 and result.current_price > 0:
            result.margin_of_safety = (result.graham_number - result.current_price) / result.graham_number

        # 财务指标
        result.current_ratio = info.get("currentRatio", 0) or 0
        result.debt_to_equity = (info.get("debtToEquity", 0) or 0) / 100  # yfinance 返回百分比

        # 股息
        result.dividend_yield = info.get("dividendYield", 0) or 0

        # 历史盈利分析
        _analyze_earnings_history(stock, result)

    except Exception as e:
        result.error = str(e)

    return result


def _analyze_earnings_history(stock: yf.Ticker, result: StockAnalysis):
    """分析历史盈利和分红数据"""
    try:
        # 年度财务数据
        financials = stock.financials
        if financials is not None and not financials.empty:
            net_income_row = None
            for label in ["Net Income", "NetIncome", "Net Income Common Stockholders"]:
                if label in financials.index:
                    net_income_row = financials.loc[label]
                    break

            if net_income_row is not None:
                yearly_earnings = net_income_row.dropna().sort_index()
                # 连续盈利年数
                consecutive = 0
                for val in reversed(yearly_earnings.values):
                    if val > 0:
                        consecutive += 1
                    else:
                        break
                result.profitable_years = consecutive

                # 盈利增长率
                if len(yearly_earnings) >= 2:
                    oldest = yearly_earnings.iloc[0]
                    newest = yearly_earnings.iloc[-1]
                    if oldest > 0:
                        result.earnings_growth = (newest - oldest) / abs(oldest)
    except Exception:
        pass

    try:
        # 分红历史
        dividends = stock.dividends
        if dividends is not None and not dividends.empty:
            years_with_div = dividends.resample("YE").sum()
            consecutive_div = 0
            for val in reversed(years_with_div.values):
                if val > 0:
                    consecutive_div += 1
                else:
                    break
            result.dividend_years = consecutive_div
    except Exception:
        pass


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
    }
    analysis.scores = scores

    # 加权总分
    total = sum(scores[k] * SCORE_WEIGHTS[k] for k in scores)
    analysis.total_score = round(total, 1)

    # 评级
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


def screen_stocks(tickers: Optional[List[str]] = None) -> pd.DataFrame:
    """批量筛选股票并返回排名结果"""
    if tickers is None:
        tickers = DEFAULT_WATCHLIST

    results = []
    total = len(tickers)

    for i, ticker in enumerate(tickers, 1):
        print(f"  [{i}/{total}] 分析 {ticker}...", end="", flush=True)
        analysis = fetch_stock_data(ticker)
        analysis = score_stock(analysis)

        if analysis.error:
            print(f" ⚠ {analysis.error}")
        else:
            print(f" ✓ 评分: {analysis.total_score} ({analysis.grade})")

        results.append(analysis)

    # 构建 DataFrame
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
            "总分": r.total_score,
            "评级": r.grade,
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("总分", ascending=False).reset_index(drop=True)
        df.index += 1  # 排名从1开始

    return df


def select_top10(tickers: Optional[List[str]] = None, top_n: int = 10) -> pd.DataFrame:
    """返回前 top_n 名的股票（按总分）
    该函数会复用 screen_stocks 完成完整筛选，然后取前 N 行。
    """
    df = screen_stocks(tickers)
    if df.empty:
        return df
    return df.head(top_n)


def print_report(df: pd.DataFrame):
    """打印分析报告"""
    print("\n" + "=" * 90)
    print("  📊 Graham 聪明投资者 - 量化选股报告")
    print("=" * 90)

    if df.empty:
        print("  没有有效数据")
        return

    # 设置 pandas 显示选项（局部）
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 180)
    pd.set_option("display.float_format", "{:.2f}".format)

    # 完整排名
    print("\n【完整排名】\n")
    print(df.to_string(index=False))

    # 价值最高的 10 只股票（按总分）
    top10 = df.head(10)
    if not top10.empty:
        print("\n【值得投资的 10 只股票】\n")
        cols = ["代码", "公司", "价格", "总分", "评级", "安全边际%"]
        # 确保安全边际列存在并保留一位小数
        top10 = top10.copy()
        top10["安全边际%"] = top10["安全边际%"].round(1)
        print(top10[cols].to_string(index=False))

    # A/B 级股票
    top_ab = df[df["评级"].isin(["A", "B"])]
    if not top_ab.empty:
        print(f"\n\n【Graham 推荐 (A/B 级)】共 {len(top_ab)} 只\n")
        print(top_ab[["代码", "公司", "价格", "P/E", "P/B", "Graham#", "安全边际%", "总分", "评级"]].to_string(index=False))

    # 安全边际最高
    positive_margin = df[df["安全边际%"] > 0].head(5)
    if not positive_margin.empty:
        print(f"\n\n【安全边际最高 Top 5】\n")
        print(positive_margin[["代码", "公司", "价格", "Graham#", "安全边际%", "总分"]].to_string(index=False))

    print("\n" + "=" * 90)
    print("  ⚠ 声明: 本模型仅供学习研究，不构成投资建议。投资有风险，入市需谨慎。")
    print("=" * 90 + "\n")


if __name__ == "__main__":
    import sys
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--top10", action="store_true", help="仅显示值得投资的前 10 只股票")
    # 其余位置参数作为 ticker 列表
    parser.add_argument("tickers", nargs="*", help="可选的股票代码列表")
    args, unknown = parser.parse_known_args()

    # 如果用户提供了 ticker 参数则使用，否则使用默认池
    tickers = args.tickers if args.tickers else None

    print("\n🔍 Graham 聪明投资者 - 量化选股模型")
    print("   基于《The Intelligent Investor》核心原则\n")

    df = screen_stocks(tickers)
    if args.top10:
        # 仅打印前 10 名（已在 print_report 中包含该区块）
        print_report(df.head(10))
    else:
        print_report(df)
