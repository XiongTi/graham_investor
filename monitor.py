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
from zoneinfo import ZoneInfo

import exchange_calendars as ecals
import pandas as pd
import yfinance as yf

# NYSE 交易日历（一次初始化，全局复用）
_NYSE_CAL = ecals.get_calendar("XNYS")

PORTFOLIO_GROUPS = {
    "fallback_top10": ["BRK-B", "JPM", "BAC", "GOOGL", "MSFT", "MRK", "T", "DIS", "JNJ", "IBM"],
    "market_top10": ["INVA", "SLDE", "JHG", "TROW", "LUXE", "TSLX", "PAGS", "VICI", "RCI", "STNG"],
}

INCEPTION_DATE = "2026-03-18"
# 预计算 inception 前 10 天的起始日期，避免每次调用 _price_history 时重复解析
_INCEPTION_START = (datetime.strptime(INCEPTION_DATE, "%Y-%m-%d").date() - timedelta(days=10)).isoformat()
INITIAL_CAPITAL_USD = 30_000.0
DATA_DIR = Path(__file__).resolve().parent / "data"
POSITIONS_CSV = DATA_DIR / "daily_positions.csv"
PORTFOLIOS_CSV = DATA_DIR / "daily_portfolios.csv"


def _parse_date(value: str | None) -> date:
    if not value:
        return date.today()
    return datetime.strptime(value, "%Y-%m-%d").date()


def _is_trading_day(check_date: date) -> bool:
    """检查是否为 NYSE 交易日"""
    return _NYSE_CAL.is_session(pd.Timestamp(check_date))


def _get_latest_trading_day(as_of_date: date) -> date:
    """获取指定日期当天或之前的最近一个 NYSE 交易日"""
    ts = pd.Timestamp(as_of_date)
    if _NYSE_CAL.is_session(ts):
        return as_of_date
    prev = _NYSE_CAL.previous_close(ts)
    return prev.date()


def _price_history(ticker: str, inception_date: str, as_of_date: date) -> pd.Series:
    end = (as_of_date + timedelta(days=1)).isoformat()
    history = yf.Ticker(ticker).history(start=_INCEPTION_START, end=end, auto_adjust=False)
    if history is None or history.empty:
        return pd.Series(dtype=float)
    return history["Close"].dropna()


def _make_tz_aware_cutoff(date_value, tz) -> pd.Timestamp:
    """构建与 Series index 时区一致的 cutoff Timestamp"""
    cutoff = pd.Timestamp(date_value if isinstance(date_value, str) else date_value.isoformat())
    if tz is not None:
        cutoff = cutoff.tz_localize(tz)
    return cutoff


def _first_close_on_or_after(closes: pd.Series, target_date: str) -> tuple[pd.Timestamp | None, float | None]:
    if closes.empty:
        return None, None
    cutoff = _make_tz_aware_cutoff(target_date, getattr(closes.index, "tz", None))
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
    """获取截至 as_of_date 当天（含）的最近两个收盘价"""
    if closes.empty:
        return None, None, None, None
    cutoff = _make_tz_aware_cutoff(as_of_date, getattr(closes.index, "tz", None))
    # 用 < 次日 00:00 来精确包含当天所有时间点的数据
    next_day = cutoff.normalize() + pd.Timedelta(days=1)
    valid = closes[closes.index < next_day]
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


def _build_group_snapshot(group_name: str, tickers: list[str], as_of_date: date, capital_usd: float) -> tuple[list[dict], dict]:
    """构建组合快照 — 累计盈亏直接用 current_value - cost_basis 计算，不依赖前日 CSV"""
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

        # 累计盈亏 = 当前市值 - 成本，无需依赖 CSV 连续性
        total_pnl = current_value - cost_basis
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

    portfolio_day_pnl = portfolio_day_end - portfolio_day_start
    portfolio_total_pnl = portfolio_day_end - portfolio_cost

    portfolio_row = {
        "observation_date": as_of_date.isoformat(),
        "group_name": group_name,
        "initial_capital_usd": round(capital_usd, 2),
        "cost_basis_usd": round(portfolio_cost, 2),
        "prev_value_usd": round(portfolio_day_start, 2),
        "current_value_usd": round(portfolio_day_end, 2),
        "day_pnl_usd": round(portfolio_day_pnl, 2),
        "day_return_pct": round((portfolio_day_pnl / portfolio_day_start * 100) if portfolio_day_start else 0.0, 2),
        "total_pnl_usd": round(portfolio_total_pnl, 2),
        "total_return_pct": round((portfolio_total_pnl / capital_usd * 100) if capital_usd else 0.0, 2),
    }

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


def run_monitor(as_of_date: date, capital_usd: float, *, date_explicit: bool = False) -> None:
    """
    运行每日监控。

    Args:
        as_of_date: 观测日期
        capital_usd: 每组初始资金
        date_explicit: 用户是否通过 --date 显式指定了日期。
                       True  → 直接使用该日期，不做时区自动推断。
                       False → 根据当前北京时间自动推算美东交易日。
    """
    if date_explicit:
        if not _is_trading_day(as_of_date):
            print(f"\n⚠️ {as_of_date} 是休市日（周末/节假日），跳过运行\n")
            return
        target_date = as_of_date
    else:
        us_tz = ZoneInfo("America/New_York")
        us_date = datetime.now(us_tz).date()

        if not _is_trading_day(us_date):
            print(f"\n⚠️ 当前美国时间 {us_date} 是休市日（周末/节假日），跳过运行\n")
            return

        target_date = _get_latest_trading_day(us_date)
        if target_date != us_date:
            print(f"\n⚠️ 美国时间 {us_date} 非交易日，自动计算 {target_date} 的收益\n")

    all_positions: list[dict] = []
    all_portfolios: list[dict] = []

    for group_name, tickers in PORTFOLIO_GROUPS.items():
        positions, portfolio = _build_group_snapshot(
            group_name, tickers, target_date, capital_usd,
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
    run_monitor(
        _parse_date(args.date),
        args.capital,
        date_explicit=args.date is not None,
    )
