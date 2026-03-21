"""
长期观测脚本：跟踪两组美股组合的每日收益与累计收益。

用法：
    python -m graham_investor.monitor
    python -m graham_investor.monitor --date 2026-03-18
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf


PORTFOLIO_GROUPS = {
    "fallback_top10": ["BRK-B", "JPM", "BAC", "GOOGL", "MSFT", "MRK", "T", "DIS", "JNJ", "IBM"],
    "market_top10": ["INVA", "SLDE", "JHG", "TROW", "LUXE", "TSLX", "PAGS", "VICI", "RCI", "STNG"],
}

INCEPTION_DATE = "2026-03-18"
INITIAL_CAPITAL_USD = 30_000.0
DATA_DIR = Path(__file__).resolve().parent / "data"
POSITIONS_CSV = DATA_DIR / "daily_positions.csv"
PORTFOLIOS_CSV = DATA_DIR / "daily_portfolios.csv"


def _parse_date(value: str | None) -> date:
    if not value:
        return date.today()
    return datetime.strptime(value, "%Y-%m-%d").date()


def _is_us_market_holiday(check_date: date) -> bool:
    """检查是否为美股休市日（周末或美国法定节假日）"""
    # 周末
    if check_date.weekday() >= 5:  # Saturday=5, Sunday=6
        return True
    
    # 美国法定节假日（2026年）
    holidays_2026 = [
        (2026, 1, 1),   # 新年 New Year's Day
        (2026, 1, 19),  # 马丁·路德·金纪念日 MLK Day
        (2026, 2, 16),  # 总统日 Presidents' Day
        (2026, 5, 26),  # 阵亡将士纪念日 Memorial Day
        (2026, 7, 4),   # 独立日 Independence Day
        (2026, 9, 1),   # 劳动节 Labor Day
        (2026, 11, 26),  # 感恩节 Thanksgiving
        (2026, 12, 25), # 圣诞节 Christmas Day
    ]
    
    for y, m, d in holidays_2026:
        if check_date == date(y, m, d):
            return True
    
    return False


def _get_latest_trading_day(as_of_date: date) -> date:
    """获取指定日期之前的最后一个美股交易日"""
    check_date = as_of_date
    for _ in range(10):  # 最多回溯10天
        if not _is_us_market_holiday(check_date):
            return check_date
        check_date -= timedelta(days=1)
    return as_of_date


def _price_history(ticker: str, inception_date: str, as_of_date: date) -> pd.Series:
    start = (datetime.strptime(inception_date, "%Y-%m-%d").date() - timedelta(days=10)).isoformat()
    end = (as_of_date + timedelta(days=1)).isoformat()
    history = yf.Ticker(ticker).history(start=start, end=end, auto_adjust=False)
    if history is None or history.empty:
        return pd.Series(dtype=float)
    return history["Close"].dropna()


def _first_close_on_or_after(closes: pd.Series, target_date: str) -> tuple[pd.Timestamp | None, float | None]:
    if closes.empty:
        return None, None
    cutoff = pd.Timestamp(target_date)
    if getattr(closes.index, "tz", None) is not None:
        cutoff = cutoff.tz_localize(closes.index.tz)
    valid = closes[closes.index >= cutoff]
    if valid.empty:
        return None, None
    return valid.index[0], float(valid.iloc[0])


def _entry_close(closes: pd.Series, inception_date: str, as_of_date: date) -> tuple[pd.Timestamp | None, float | None]:
    buy_idx, buy_price = _first_close_on_or_after(closes, inception_date)
    if buy_price is not None:
        return buy_idx, buy_price

    # If the inception day has not closed yet, use the latest available close
    # up to the observation date as a provisional entry baseline.
    latest_idx, latest_price, _, _ = _latest_two_closes(closes, as_of_date)
    return latest_idx, latest_price


def _latest_two_closes(closes: pd.Series, as_of_date: date) -> tuple[pd.Timestamp | None, float | None, pd.Timestamp | None, float | None]:
    if closes.empty:
        return None, None, None, None
    cutoff = pd.Timestamp(as_of_date.isoformat())
    if getattr(closes.index, "tz", None) is not None:
        cutoff = cutoff.tz_localize(closes.index.tz)
    valid = closes[closes.index <= cutoff + pd.Timedelta(days=1)]
    if valid.empty:
        return None, None, None, None
    latest_idx = valid.index[-1]
    latest_price = float(valid.iloc[-1])
    prev_idx = valid.index[-2] if len(valid) >= 2 else None
    prev_price = float(valid.iloc[-2]) if len(valid) >= 2 else None
    return latest_idx, latest_price, prev_idx, prev_price


def _to_iso(value: pd.Timestamp | None) -> str:
    if value is None:
        return ""
    if getattr(value, "tzinfo", None) is not None:
        value = value.tz_convert(None)
    return value.date().isoformat()


def _build_group_snapshot(group_name: str, tickers: list[str], as_of_date: date, capital_usd: float, prev_portfolio: dict | None = None, prev_positions: dict | None = None) -> tuple[list[dict], dict]:
    allocation = capital_usd / len(tickers)
    position_rows: list[dict] = []
    portfolio_day_start = 0.0
    portfolio_day_end = 0.0
    portfolio_cost = 0.0

    for ticker in tickers:
        closes = _price_history(ticker, INCEPTION_DATE, as_of_date)
        buy_idx, buy_price = _entry_close(closes, INCEPTION_DATE, as_of_date)
        latest_idx, latest_price, prev_idx, prev_price = _latest_two_closes(closes, as_of_date)

        shares = allocation / buy_price if buy_price else 0.0
        cost_basis = shares * buy_price if buy_price else 0.0
        current_value = shares * latest_price if latest_price else 0.0
        previous_value = shares * prev_price if prev_price else current_value
        day_pnl = current_value - previous_value
        
        # 计算每只股票的累计盈亏
        position_key = (group_name, ticker)
        prev_position = prev_positions.get(position_key) if prev_positions else None
        if prev_position is not None:
            prev_total_pnl = prev_position.get("total_pnl_usd", 0.0) or 0.0
            cumulative_pnl = prev_total_pnl + day_pnl
        else:
            cumulative_pnl = day_pnl  # 第一天就是当天的盈亏
        
        total_pnl = cumulative_pnl
        day_return_pct = (day_pnl / previous_value * 100) if previous_value else 0.0
        total_return_pct = (total_pnl / cost_basis * 100) if cost_basis else 0.0

        portfolio_day_start += previous_value
        portfolio_day_end += current_value
        portfolio_cost += cost_basis

        position_rows.append(
            {
                "observation_date": as_of_date.isoformat(),
                "group_name": group_name,
                "ticker": ticker,
                "allocation_usd": round(allocation, 2),
                "shares": round(shares, 6),
                "buy_price_date": _to_iso(buy_idx),
                "buy_price": round(buy_price, 4) if buy_price else None,
                "prev_price_date": _to_iso(prev_idx),
                "prev_close": round(prev_price, 4) if prev_price else None,
                "latest_price_date": _to_iso(latest_idx),
                "latest_close": round(latest_price, 4) if latest_price else None,
                "day_pnl_usd": round(day_pnl, 2),
                "day_return_pct": round(day_return_pct, 2),
                "total_pnl_usd": round(total_pnl, 2),
                "total_return_pct": round(total_return_pct, 2),
                "current_value_usd": round(current_value, 2),
            }
        )

    portfolio_row = {
        "observation_date": as_of_date.isoformat(),
        "group_name": group_name,
        "initial_capital_usd": round(capital_usd, 2),
        "cost_basis_usd": round(portfolio_cost, 2),
        "prev_value_usd": round(portfolio_day_start, 2),
        "current_value_usd": round(portfolio_day_end, 2),
        "day_pnl_usd": round(portfolio_day_end - portfolio_day_start, 2),
        "day_return_pct": round(((portfolio_day_end - portfolio_day_start) / portfolio_day_start * 100) if portfolio_day_start else 0.0, 2),
    }

    # 计算累计盈亏
    day_pnl = portfolio_day_end - portfolio_day_start
    if prev_portfolio is not None:
        prev_total_pnl = prev_portfolio.get("total_pnl_usd", 0.0) or 0.0
        cumulative_pnl = prev_total_pnl + day_pnl
    else:
        cumulative_pnl = day_pnl  # 第一天就是当天的盈亏
    
    portfolio_row["total_pnl_usd"] = round(cumulative_pnl, 2)
    portfolio_row["total_return_pct"] = round((cumulative_pnl / capital_usd * 100) if capital_usd else 0.0, 2)

    return position_rows, portfolio_row


def _upsert_csv(path: Path, rows: list[dict], key_columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    new_df = pd.DataFrame(rows)
    if path.exists():
        current_df = pd.read_csv(path)
        combined = pd.concat([current_df, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=key_columns, keep="last")
    else:
        combined = new_df
    combined = combined.sort_values(key_columns).reset_index(drop=True)
    combined.to_csv(path, index=False)


def _print_report(portfolio_rows: list[dict], position_rows: list[dict]) -> None:
    portfolio_df = pd.DataFrame(portfolio_rows)
    positions_df = pd.DataFrame(position_rows)

    print("\n=== Daily Portfolio Summary ===\n")
    print(
        portfolio_df[
            ["observation_date", "group_name", "current_value_usd", "day_pnl_usd", "day_return_pct", "total_pnl_usd", "total_return_pct"]
        ].to_string(index=False)
    )

    for group_name in portfolio_df["group_name"]:
        print(f"\n=== {group_name} Positions ===\n")
        group_df = positions_df[positions_df["group_name"] == group_name].copy()
        print(
            group_df[
                [
                    "ticker",
                    "latest_price_date",
                    "latest_close",
                    "day_pnl_usd",
                    "day_return_pct",
                    "total_pnl_usd",
                    "total_return_pct",
                    "current_value_usd",
                ]
            ].to_string(index=False)
        )


def run_monitor(as_of_date: date, capital_usd: float) -> None:
    # 北京时间 9 AM = 美国时间前一天的 8 PM (夏令时为 9 PM)
    # 检查这个美国时间是否为周末或节假日
    from datetime import timezone
    
    # 将北京时间转换为美国东部时间
    beijing_tz = timezone(timedelta(hours=8))
    us_tz = timezone(timedelta(hours=-5))  # EST
    
    # 北京时间 9 点对应的时间
    beijing_now = datetime.now(beijing_tz)
    # 转为美国时间（前一天的晚上 8-9 点）
    us_time = beijing_now.astimezone(us_tz)
    us_date = us_time.date()
    
    # 如果美国时间对应的是周末或节假日，跳过
    if _is_us_market_holiday(us_date):
        print(f"\n⚠️ 当前美国时间 {us_date} 是休市日（周末/节假日），跳过运行\n")
        return
    
    # 确定目标日期（取美国时间对应的交易日）
    target_date = _get_latest_trading_day(us_date)
    if target_date != us_date:
        print(f"\n⚠️ 美国时间 {us_date} 非交易日，自动计算 {target_date} 的收益\n")
        as_of_date = target_date
    else:
        # 北京时间日期和美国日期可能不同（如周一早上对应周日美国时间）
        # 用美国日期作为数据日期
        as_of_date = us_date
    
    # 检查前一天是否休市
    prev_day = _get_latest_trading_day(as_of_date - timedelta(days=1))
    if prev_day != as_of_date - timedelta(days=1):
        print(f"\n⚠️ 提示：{prev_day} 是上一个交易日\n")
    
    all_positions: list[dict] = []
    all_portfolios: list[dict] = []

    # 读取前一天的数据用于计算累计盈亏
    # 如果前一天是休市日，需要找到上一个交易日
    prev_day = _get_latest_trading_day(as_of_date - timedelta(days=1))
    prev_portfolios = {}
    prev_positions = {}
    if PORTFOLIOS_CSV.exists():
        prev_df = pd.read_csv(PORTFOLIOS_CSV)
        prev_day_df = prev_df[prev_df["observation_date"] == prev_day.isoformat()]
        for _, row in prev_day_df.iterrows():
            prev_portfolios[row["group_name"]] = row
    
    if POSITIONS_CSV.exists():
        prev_pos_df = pd.read_csv(POSITIONS_CSV)
        prev_pos_day_df = prev_pos_df[prev_pos_df["observation_date"] == prev_day.isoformat()]
        for _, row in prev_pos_day_df.iterrows():
            key = (row["group_name"], row["ticker"])
            prev_positions[key] = row

    for group_name, tickers in PORTFOLIO_GROUPS.items():
        positions, portfolio = _build_group_snapshot(
            group_name, tickers, as_of_date, capital_usd, 
            prev_portfolios.get(group_name), prev_positions
        )
        all_positions.extend(positions)
        all_portfolios.append(portfolio)

    _upsert_csv(POSITIONS_CSV, all_positions, ["observation_date", "group_name", "ticker"])
    _upsert_csv(PORTFOLIOS_CSV, all_portfolios, ["observation_date", "group_name"])
    _print_report(all_portfolios, all_positions)

    print(f"\nCSV updated: {POSITIONS_CSV}")
    print(f"CSV updated: {PORTFOLIOS_CSV}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Track daily performance of two US stock portfolios.")
    parser.add_argument("--date", help="Observation date in YYYY-MM-DD. Defaults to today.")
    parser.add_argument("--capital", type=float, default=INITIAL_CAPITAL_USD, help="Initial capital per portfolio in USD.")
    args = parser.parse_args()
    run_monitor(_parse_date(args.date), args.capital)
