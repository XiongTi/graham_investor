from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import exchange_calendars as ecals
import pandas as pd
import yfinance as yf
from exchange_calendars import errors as calendar_errors

from . import db
from .config import MARKET_PROFILES

CALENDARS = {"us": ecals.get_calendar("XNYS"), "cn": ecals.get_calendar("XSHG"), "hk": ecals.get_calendar("XHKG")}
MARKET_TIMEZONES = {"us": ZoneInfo("America/New_York"), "cn": ZoneInfo("Asia/Shanghai"), "hk": ZoneInfo("Asia/Hong_Kong")}


def _parse_date(value: str | None) -> date:
    if not value:
        return date.today()
    return datetime.strptime(value, "%Y-%m-%d").date()


def _is_trading_day(market: str, check_date: date) -> bool:
    calendar = CALENDARS[market]
    ts = pd.Timestamp(check_date)
    try:
        return calendar.is_session(ts)
    except calendar_errors.DateOutOfBounds:
        return False


def _calendar_last_supported(market: str) -> date:
    return CALENDARS[market].last_session.date()


def _calendar_supports_date(market: str, check_date: date) -> bool:
    calendar = CALENDARS[market]
    return calendar.first_session.date() <= check_date <= calendar.last_session.date()


def _roll_back_to_weekday(check_date: date) -> date:
    while check_date.weekday() >= 5:
        check_date -= timedelta(days=1)
    return check_date


def _get_latest_trading_day(market: str, as_of_date: date) -> date:
    calendar = CALENDARS[market]
    ts = pd.Timestamp(as_of_date)
    if calendar.is_session(ts):
        return as_of_date
    prev = calendar.previous_close(ts)
    return prev.date()


def _resolve_target_date(market: str, requested_date: date, *, date_explicit: bool) -> date | None:
    if _calendar_supports_date(market, requested_date):
        if date_explicit:
            if not _is_trading_day(market, requested_date):
                print(f"\n⚠ {market.upper()} 的 {requested_date} 不是交易日，跳过\n")
                return None
            return requested_date
        return _get_latest_trading_day(market, requested_date)
    last_supported = _calendar_last_supported(market)
    if date_explicit:
        print(f"\n⚠ {market.upper()} 交易日历当前只覆盖到 {last_supported}，无法精确校验 {requested_date} 是否为交易日，将直接按该日期请求行情\n")
        return requested_date
    fallback_date = _roll_back_to_weekday(requested_date)
    if fallback_date == requested_date:
        print(f"\n⚠ {market.upper()} 交易日历当前只覆盖到 {last_supported}，将直接按当前市场日期 {fallback_date} 请求行情\n")
    else:
        print(f"\n⚠ {market.upper()} 交易日历当前只覆盖到 {last_supported}，当前市场日期 {requested_date} 为周末，回退到最近工作日 {fallback_date} 请求行情\n")
    return fallback_date


def _current_market_date(market: str) -> date:
    return datetime.now(MARKET_TIMEZONES[market]).date()


def _price_history(ticker: str, inception_date: str, as_of_date: date) -> pd.Series:
    start = (datetime.strptime(inception_date, "%Y-%m-%d").date() - timedelta(days=10)).isoformat()
    end = (as_of_date + timedelta(days=1)).isoformat()
    try:
        history = yf.Ticker(ticker).history(start=start, end=end, auto_adjust=False)
    except Exception as exc:
        print(f"  ⚠ {ticker} 行情获取失败: {exc}")
        return pd.Series(dtype=float)
    if history is None or history.empty:
        return pd.Series(dtype=float)
    return history["Close"].dropna()


def _make_tz_aware_cutoff(date_value, tz) -> pd.Timestamp:
    cutoff = pd.Timestamp(date_value if isinstance(date_value, str) else date_value.isoformat())
    if tz is not None:
        cutoff = cutoff.tz_localize(tz)
    return cutoff


def _latest_two_closes(closes: pd.Series, as_of_date: date) -> tuple[pd.Timestamp | None, float | None, pd.Timestamp | None, float | None]:
    if closes.empty:
        return None, None, None, None
    cutoff = _make_tz_aware_cutoff(as_of_date, getattr(closes.index, "tz", None))
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


def _performance_metrics(
    *,
    shares: float,
    cost_basis: float,
    latest_price: float | None,
    prev_price: float | None,
) -> dict[str, float | None]:
    current_value = shares * latest_price if latest_price is not None else None
    previous_value = shares * prev_price if prev_price is not None else current_value
    day_pnl = None if current_value is None or previous_value is None else current_value - previous_value
    total_pnl = None if current_value is None else current_value - cost_basis
    day_return_pct = None if day_pnl is None or not previous_value else (day_pnl / previous_value * 100)
    total_return_pct = None if total_pnl is None or not cost_basis else (total_pnl / cost_basis * 100)
    return {
        "current_value": current_value,
        "previous_value": previous_value,
        "day_pnl": day_pnl,
        "total_pnl": total_pnl,
        "day_return_pct": day_return_pct,
        "total_return_pct": total_return_pct,
    }


def _build_wallet_snapshot(
    market: str,
    currency: str,
    as_of_date: date,
    *,
    ledger_through_date: date | None = None,
) -> tuple[list[dict], dict | None]:
    ledger_date = ledger_through_date or as_of_date
    ledger_date_str = ledger_date.isoformat()
    with db.get_connection() as conn:
        wallet = db.load_wallet_optional(conn, market)
        if wallet is None:
            return [], None
        wallet_id = int(wallet["id"])
        positions = db.compute_wallet_positions(conn, wallet_id, through_date=ledger_date_str)
        trades = db.load_trades(conn, wallet_id, through_date=ledger_date_str)
        net_deposit = db.load_net_deposit(conn, wallet_id, through_date=ledger_date_str)
    if not positions and net_deposit == 0:
        return [], None

    inception_map: dict[str, str] = {}
    for trade in trades:
        ticker = str(trade["ticker"])
        trade_date = str(trade["trade_date"])
        if ticker not in inception_map or trade_date < inception_map[ticker]:
            inception_map[ticker] = trade_date

    position_rows: list[dict] = []
    priced_count = 0
    missing_price_count = 0
    positions_day_start = 0.0
    positions_day_end = 0.0
    for position in positions:
        ticker = str(position["ticker"])
        shares = float(position["shares"])
        cost_basis = float(position["cost_basis"])
        inception_date = inception_map.get(ticker, as_of_date.isoformat())
        closes = _price_history(ticker, inception_date, as_of_date)
        latest_idx, latest_price, prev_idx, prev_price = _latest_two_closes(closes, as_of_date)
        metrics = _performance_metrics(shares=shares, cost_basis=cost_basis, latest_price=latest_price, prev_price=prev_price)
        current_value = metrics["current_value"]
        previous_value = metrics["previous_value"]
        if current_value is None:
            missing_price_count += 1
        else:
            priced_count += 1
            positions_day_end += current_value
            if previous_value is not None:
                positions_day_start += previous_value
        position_rows.append(
            {
                "observation_date": as_of_date.isoformat(),
                "market": market,
                "currency": currency,
                "group_name": "wallet_live",
                "ticker": ticker,
                "shares": round(shares, 6),
                "cost_basis": round(cost_basis, 2),
                "avg_cost": round(float(position["avg_cost"]), 4),
                "latest_price_date": _to_iso(latest_idx),
                "latest_close": round(latest_price, 4) if latest_price is not None else None,
                "price_available": current_value is not None,
                "current_value": round(current_value, 2) if current_value is not None else None,
                "day_pnl": round(metrics["day_pnl"], 2) if metrics["day_pnl"] is not None else None,
                "day_return_pct": round(metrics["day_return_pct"], 2) if metrics["day_return_pct"] is not None else None,
                "total_pnl": round(metrics["total_pnl"], 2) if metrics["total_pnl"] is not None else None,
                "total_return_pct": round(metrics["total_return_pct"], 2) if metrics["total_return_pct"] is not None else None,
            }
        )

    cash_balance = net_deposit
    for trade in trades:
        shares = float(trade["shares"])
        price = float(trade["price"])
        fees = float(trade["fees"] or 0.0)
        gross_amount = shares * price
        if str(trade["side"]) == "buy":
            cash_balance -= gross_amount + fees
        else:
            cash_balance += gross_amount - fees

    data_complete = priced_count == len(positions)
    current_total_equity = cash_balance + positions_day_end if data_complete else None
    previous_total_equity = cash_balance + positions_day_start if data_complete else None
    total_pnl = None if current_total_equity is None else current_total_equity - net_deposit
    day_pnl = None if current_total_equity is None or previous_total_equity is None else current_total_equity - previous_total_equity
    day_return_pct = None if day_pnl is None or not previous_total_equity else (day_pnl / previous_total_equity * 100)
    total_return_pct = None if total_pnl is None or not net_deposit else (total_pnl / net_deposit * 100)
    wallet_row = {
        "observation_date": as_of_date.isoformat(),
        "market": market,
        "currency": currency,
        "wallet_name": str(wallet["name"]),
        "initial_capital": round(net_deposit, 2),
        "cash_balance": round(cash_balance, 2),
        "position_market_value": round(positions_day_end, 2) if data_complete else None,
        "current_value": round(current_total_equity, 2) if current_total_equity is not None else None,
        "total_pnl": round(total_pnl, 2) if total_pnl is not None else None,
        "day_pnl": round(day_pnl, 2) if day_pnl is not None else None,
        "day_return_pct": round(day_return_pct, 2) if day_return_pct is not None else None,
        "total_return_pct": round(total_return_pct, 2) if total_return_pct is not None else None,
        "position_count": len(positions),
        "priced_count": priced_count,
        "missing_price_count": missing_price_count,
        "data_complete": data_complete,
    }
    return position_rows, wallet_row


def _format_display_table(df: pd.DataFrame, columns: list[str], rename_map: dict[str, str]) -> pd.DataFrame:
    display_df = df[columns].copy()
    display_df = display_df.rename(columns=rename_map)
    return display_df.fillna("--")


def _print_wallet_report(wallet_rows: list[dict], wallet_positions: list[dict]) -> None:
    wallet_df = pd.DataFrame(wallet_rows)
    positions_df = pd.DataFrame(wallet_positions)
    if wallet_df.empty:
        print("\n=== 持仓概览 ===\n")
        print("当前市场钱包为空，还没有任何真实交易记录。\n")
        return
    print("\n=== 持仓概览 ===\n")
    print(
        _format_display_table(
            wallet_df,
            ["observation_date", "market", "wallet_name", "initial_capital", "cash_balance", "position_market_value", "current_value", "total_pnl", "day_return_pct", "total_return_pct", "position_count", "priced_count", "missing_price_count", "data_complete"],
            {"observation_date": "观测日期", "market": "市场", "wallet_name": "钱包", "initial_capital": "累计入金", "cash_balance": "现金余额", "position_market_value": "持仓市值", "current_value": "总权益", "total_pnl": "累计收益额", "day_return_pct": "单日收益%", "total_return_pct": "累计收益%", "position_count": "持仓数", "priced_count": "已定价数量", "missing_price_count": "缺失价格数量", "data_complete": "数据完整"},
        ).to_string(index=False)
    )
    if positions_df.empty:
        print("\n=== 持仓明细 ===\n")
        print("当前钱包没有持仓。\n")
        return
    print("\n=== 持仓明细 ===\n")
    print(
        _format_display_table(
            positions_df,
            ["ticker", "shares", "avg_cost", "cost_basis", "latest_price_date", "latest_close", "current_value", "day_return_pct", "total_return_pct"],
            {"ticker": "代码", "shares": "持仓股数", "avg_cost": "平均成本", "cost_basis": "持仓成本", "latest_price_date": "最新价格日期", "latest_close": "最新收盘价", "current_value": "当前市值", "day_return_pct": "单日收益%", "total_return_pct": "累计收益%"},
        ).to_string(index=False)
    )


def run_monitor(market: str, as_of_date: date, *, run_date: str | None = None, date_explicit: bool = False) -> None:
    del run_date
    markets = sorted(MARKET_PROFILES) if market == "all" else [market]
    all_wallet_rows: list[dict] = []
    all_wallet_positions: list[dict] = []
    for market_code in markets:
        profile = MARKET_PROFILES[market_code]
        requested_date = as_of_date if date_explicit else _current_market_date(market_code)
        target_date = _resolve_target_date(market_code, requested_date, date_explicit=date_explicit)
        if target_date is None:
            continue
        wallet_positions, wallet_row = _build_wallet_snapshot(
            market_code,
            profile.currency,
            target_date,
            ledger_through_date=requested_date,
        )
        all_wallet_positions.extend(wallet_positions)
        if wallet_row is not None:
            all_wallet_rows.append(wallet_row)
    if not all_wallet_rows and not all_wallet_positions:
        print("\n本次没有生成任何监控记录。\n")
        return
    _print_wallet_report(all_wallet_rows, all_wallet_positions)


if __name__ == "__main__":
    raise SystemExit("请使用 `ws track --market <market>` 作为跟踪主入口。")
