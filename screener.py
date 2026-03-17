"""
自动股票发现模块 - 使用 yfinance EquityQuery 批量预筛美股

策略：
1. 用 yf.screen() 按 Graham 宽松标准做粗筛（单次 API 调用，无需逐只查询）
2. 返回候选 ticker 列表，交给 model.py 做精细评分
"""

import yfinance as yf
from yfinance import EquityQuery

from .config import SCREENER_CONFIG


def _build_graham_query() -> EquityQuery:
    """构建 Graham 风格的预筛查询（宽松标准，避免漏掉潜在价值股）"""
    cfg = SCREENER_CONFIG

    filters = [
        # 仅美股主要交易所
        EquityQuery("is-in", ["exchange"] + cfg["exchanges"]),
        # 市值下限 — 过滤微型股
        EquityQuery("gt", ["intradaymarketcap", cfg["min_market_cap"]]),
        # 流动性 — 3个月日均成交量
        EquityQuery("gt", ["avgdailyvol3m", cfg["min_avg_volume"]]),
        # P/E > 0（排除亏损）且 < 宽松上限
        EquityQuery("gt", ["peratio.lasttwelvemonths", 0]),
        EquityQuery("lt", ["peratio.lasttwelvemonths", cfg["pre_screen_pe_max"]]),
        # P/B < 宽松上限
        EquityQuery("lt", ["pricebookratio.quarterly", cfg["pre_screen_pb_max"]]),
    ]

    return EquityQuery("and", filters)


def _paginate_screen(query: EquityQuery, max_results: int = 0) -> list:
    """分页获取 yf.screen() 全部结果"""
    cfg = SCREENER_CONFIG
    if max_results <= 0:
        max_results = cfg["max_candidates"]

    all_quotes = []
    offset = 0
    page_size = 250  # yfinance 单次上限

    while len(all_quotes) < max_results:
        try:
            result = yf.screen(query, offset=offset, size=page_size)
        except Exception as e:
            print(f"  ⚠ 筛选请求失败 (offset={offset}): {e}")
            break

        quotes = result.get("quotes", [])
        if not quotes:
            break

        all_quotes.extend(quotes)
        offset += page_size

        if len(quotes) < page_size:
            break

    return all_quotes[:max_results]


def discover_candidates() -> list[str]:
    """
    自动发现符合 Graham 宽松标准的美股候选列表。

    Returns:
        ticker 列表（已去重）
    """
    print("  🔍 正在自动筛选美股候选池...")
    query = _build_graham_query()
    quotes = _paginate_screen(query)

    tickers = []
    seen = set()
    for q in quotes:
        symbol = q.get("symbol", "")
        if symbol and symbol not in seen:
            tickers.append(symbol)
            seen.add(symbol)

    print(f"  ✓ 预筛完成，发现 {len(tickers)} 只候选股票")
    return tickers
