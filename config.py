"""Graham 选股模型配置参数 - 基于《聪明的投资者》"""

# ============================================================
# Graham 防御型投资者选股标准
# ============================================================

GRAHAM_CRITERIA = {
    # 估值指标
    "pe_ratio_max": 15,           # P/E 不超过 15 (过去3年平均盈利)
    "pb_ratio_max": 1.5,          # P/B 不超过 1.5
    "pe_times_pb_max": 22.5,      # P/E × P/B < 22.5 (Graham 综合估值上限)

    # 财务稳健性
    "current_ratio_min": 2.0,     # 流动比率 > 2 (流动资产至少为流动负债2倍)
    "debt_equity_max": 0.5,       # 长期债务不超过净流动资产

    # 盈利稳定性
    "min_profitable_years": 5,    # 至少连续5年盈利 (Graham原书要求10年)
    "earnings_growth_min": 0.33,  # 5年盈利增长至少33% (Graham原书要求10年增长1/3)

    # 分红记录
    "min_dividend_years": 5,      # 至少连续5年分红 (Graham原书要求20年)
}

# ============================================================
# 评分权重
# ============================================================

SCORE_WEIGHTS = {
    "pe_score": 0.20,             # P/E 评分权重
    "pb_score": 0.15,             # P/B 评分权重
    "graham_number_score": 0.20,  # Graham Number 安全边际权重
    "current_ratio_score": 0.10,  # 流动比率权重
    "debt_equity_score": 0.10,    # 债务权益比权重
    "earnings_stability": 0.10,   # 盈利稳定性权重
    "dividend_score": 0.05,       # 分红持续性权重
    "earnings_growth_score": 0.10,# 盈利增长权重
}

# ============================================================
# 默认股票池 - 可替换为自定义列表
# ============================================================

DEFAULT_WATCHLIST = [
    # 大盘蓝筹
    "AAPL", "MSFT", "GOOGL", "JNJ", "PG", "KO", "PEP",
    "WMT", "JPM", "BAC", "XOM", "CVX", "UNH", "HD",
    # 价值股
    "BRK-B", "INTC", "VZ", "T", "IBM", "MMM", "CAT",
    "GE", "F", "GM", "MRK", "PFE", "ABBV", "BMY",
    # 消费/工业
    "MCD", "NKE", "DIS", "COST", "LOW", "TGT", "CL",
    "GIS", "K", "SJM", "HRL", "ADM", "DE", "EMR",
]
