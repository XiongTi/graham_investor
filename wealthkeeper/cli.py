from __future__ import annotations

import argparse
from datetime import timedelta

import pandas as pd
import yfinance as yf

from . import db
from .copilot import build_market_copilot, print_copilot_report
from .config import MARKET_PROFILES
from .model import fetch_stock_data, print_report, screen_stocks
from .monitor import _current_market_date, _parse_date, _resolve_target_date, run_monitor
from .refresh_universe import ensure_investable_universe, refresh_universe
from .screener import normalize_ticker, normalize_tickers


def _add_trade_parser(subparsers, command: str, help_text: str) -> None:
    parser = subparsers.add_parser(command, help=help_text)
    parser.add_argument("--market", choices=sorted(MARKET_PROFILES), required=True, help="交易市场")
    parser.add_argument("--ticker", required=True, help="股票代码")
    parser.add_argument("--shares", type=float, required=True, help=f"{'买入' if command == 'buy' else '卖出'}股数")
    parser.add_argument("--price", type=float, help="成交价格；不传则自动抓取")
    parser.add_argument("--fees", type=float, default=0.0, help="手续费，默认 0")
    parser.add_argument("--note", help="交易备注")


def _add_wallet_amount_parser(subparsers, command: str, help_text: str) -> None:
    parser = subparsers.add_parser(command, help=help_text)
    parser.add_argument("--market", choices=sorted(MARKET_PROFILES), required=True, help="钱包所属市场")
    parser.add_argument("--amount", type=float, required=True, help="金额")
    parser.add_argument("--note", help="备注")


def _trade_date_str(market: str) -> str:
    current_market_date = _current_market_date(market)
    trade_date = _resolve_target_date(market, current_market_date, date_explicit=False)
    return (trade_date or current_market_date).isoformat()


def _resolve_trade_price(market: str, ticker: str, raw_price: float | None, trade_date: str) -> float:
    if raw_price is not None:
        return float(raw_price)
    target_date, end_date = _parse_date(trade_date), _parse_date(trade_date) + timedelta(days=10)
    try:
        history = yf.Ticker(ticker).history(start=target_date.isoformat(), end=(end_date + timedelta(days=1)).isoformat(), auto_adjust=False)
    except Exception as exc:
        raise ValueError(f"{ticker} 无法获取 {trade_date} 附近价格，请手动传入 `--price`") from exc
    if history is not None and not history.empty:
        closes = history["Close"].dropna()
        if not closes.empty:
            return float(closes.iloc[0])
    analysis = fetch_stock_data(ticker, MARKET_PROFILES[market])
    if analysis.current_price is not None and trade_date == _current_market_date(market).isoformat():
        return float(analysis.current_price)
    raise ValueError(f"{ticker} 无法获取 {trade_date} 附近价格，请手动传入 `--price`")


def _print_wallet_summary(market: str) -> None:
    with db.get_connection() as conn:
        wallet = db.load_wallet(conn, market)
        wallet_id = int(wallet["id"])
        positions = db.compute_wallet_positions(conn, wallet_id)
        net_deposit = db.load_net_deposit(conn, wallet_id)
    position_cost = sum(float(position["cost_basis"]) for position in positions)
    total_assets = float(wallet["cash_balance"]) + position_cost
    print("\n💼 钱包概览")
    print(f"  🌍 市场: {market.upper()}")
    print(f"  💵 现金余额: {float(wallet['cash_balance']):.2f} {wallet['currency']}")
    print(f"  🏦 净入金: {net_deposit:.2f} {wallet['currency']}")
    print(f"  📦 持仓数: {len(positions)}")
    print(f"  📊 当前总资产(按成本口径): {total_assets:.2f} {wallet['currency']}")
    if positions:
        print("  📋 持仓摘要:")
        for position in positions:
            print(f"    {position['ticker']}: {float(position['shares']):.6f} 股 | 成本 {float(position['cost_basis']):.2f}")
    print()


def _print_wallet_state(market: str) -> None:
    _print_wallet_summary(market)


def _describe_insight_row(row: dict[str, object]) -> str:
    ticker = str(row.get("代码", ""))
    grade = str(row.get("评级", ""))
    score = float(row.get("总分") or 0.0)
    margin = row.get("安全边际%")
    pe = row.get("P/E")
    pb = row.get("P/B")
    roe = row.get("ROE%")
    net_margin = row.get("净利率%")
    if grade in {"A", "B"}:
        lead = f"{ticker}: 已达到 {grade} 级，属于当前市场里更值得优先看的标的。"
    elif grade == "C":
        lead = f"{ticker}: 目前是 C 级，更适合放进观察名单，还没到直接推荐买入的程度。"
    else:
        lead = f"{ticker}: 当前只有 {grade or '未评级'}，基本面或估值至少有一块偏弱。"
    details: list[str] = []
    if margin is not None and not pd.isna(margin):
        margin_value = float(margin)
        if margin_value >= 0:
            details.append(f"安全边际约 {margin_value:.1f}%")
        else:
            details.append(f"当前价格高于 Graham 估值约 {abs(margin_value):.1f}%")
    if pe is not None and not pd.isna(pe):
        details.append(f"P/E 约 {float(pe):.1f}")
    if pb is not None and not pd.isna(pb):
        details.append(f"P/B 约 {float(pb):.2f}")
    if roe is not None and not pd.isna(roe):
        details.append(f"ROE 约 {float(roe):.1f}%")
    if net_margin is not None and not pd.isna(net_margin):
        details.append(f"净利率约 {float(net_margin):.1f}%")
    metric_text = "，".join(details[:3])
    if metric_text:
        return f"{lead} 目前看点是：{metric_text}。总分 {score:.1f}。"
    return f"{lead} 当前总分 {score:.1f}。"


def _print_insight_explanations(df: pd.DataFrame) -> None:
    if df.empty:
        return
    explain_df = df.head(10)
    print("  📝 候选说明:")
    for idx, row in enumerate(explain_df.to_dict("records"), 1):
        print(f"    {idx}. {_describe_insight_row(row)}")
    print()


def _load_or_build_investable_tickers(market: str) -> list[str]:
    profile = MARKET_PROFILES[market]
    path = profile.universe_config["investable_path"]
    try:
        df = pd.read_csv(path, dtype=str)
        print(f"  📁 使用本地 {profile.label} 可投资股票池（{len(df)} 行）")
    except FileNotFoundError:
        print(f"  📁 未发现本地 {profile.label} 可投资股票池，先自动构建...")
        _, df = ensure_investable_universe(market, persist=True)
    except Exception as exc:
        print(f"  ⚠ 读取本地 {profile.label} 股票池失败 ({exc})，改为自动构建...")
        _, df = ensure_investable_universe(market, persist=True)
    if "is_investable" in df.columns:
        normalized = df["is_investable"].astype(str).str.strip().str.lower()
        df = df[normalized.isin(["true", "1", "yes", "y"])]
    if "ticker" not in df.columns:
        raise ValueError(f"{profile.label} 可投资股票池缺少 ticker 列")
    tickers = normalize_tickers(df["ticker"].astype(str).tolist(), market)
    print(f"  📦 本次将全量评分 {len(tickers)} 只 {profile.label} 股票")
    return tickers


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ws", description="WealthKeeper 命令行入口：市场洞察、跟踪、交易与钱包管理。")
    subparsers = parser.add_subparsers(dest="command", required=True)

    refresh_parser = subparsers.add_parser("refresh", help="刷新目标市场的本地股票池")
    refresh_parser.add_argument("--market", choices=[*sorted(MARKET_PROFILES), "all"], required=True, help="要刷新的市场")

    insight_parser = subparsers.add_parser("insight", help="输出市场候选排名和大白话说明")
    insight_parser.add_argument("--market", choices=sorted(MARKET_PROFILES), required=True, help="要查看的市场")
    insight_parser.add_argument("--top", type=int, help="仅展示前 N 名；不传则输出完整排名")

    analyze_parser = subparsers.add_parser("analyze", help="分析指定股票代码并输出评级结果")
    analyze_parser.add_argument("--market", choices=sorted(MARKET_PROFILES), required=True, help="要分析的市场")
    analyze_parser.add_argument("tickers", nargs="+", help="要分析的股票代码")

    _add_trade_parser(subparsers, "buy", "记录一笔买入交易并更新钱包")
    _add_trade_parser(subparsers, "sell", "记录一笔卖出交易并更新钱包")

    wallet_parser = subparsers.add_parser("wallet", help="查看或管理钱包余额")
    wallet_subparsers = wallet_parser.add_subparsers(dest="wallet_command", required=True)
    wallet_show = wallet_subparsers.add_parser("show", help="查看钱包概览")
    wallet_show.add_argument("--market", choices=sorted(MARKET_PROFILES), required=True, help="钱包所属市场")
    _add_wallet_amount_parser(wallet_subparsers, "deposit", "往钱包充值")
    _add_wallet_amount_parser(wallet_subparsers, "withdraw", "从钱包提现")

    copilot_parser = subparsers.add_parser("copilot", help="结合持仓和市场候选给出调仓建议")
    copilot_parser.add_argument("--market", choices=[*sorted(MARKET_PROFILES), "all"], required=True, help="要分析的市场")
    copilot_parser.add_argument("--top", type=int, default=10, help="参考候选股票数量，默认 10")

    track_parser = subparsers.add_parser("track", help="查看真实持仓的总收益和个股收益明细")
    track_parser.add_argument("--market", choices=[*sorted(MARKET_PROFILES), "all"], required=True, help="要跟踪的市场")
    track_parser.add_argument("--date", help="观测日期，格式 YYYY-MM-DD；不传则默认按市场当天")
    return parser


def run_refresh(args: argparse.Namespace) -> None:
    for market in (sorted(MARKET_PROFILES) if args.market == "all" else [args.market]):
        refresh_universe(market)


def run_insight(args: argparse.Namespace) -> None:
    profile = MARKET_PROFILES[args.market]
    top_n = max(1, int(args.top)) if args.top is not None else None
    print("\n🔎 WealthKeeper - 市场洞察")
    print(f"  🌍 市场: {profile.label} ({profile.currency})")
    print(f"  📌 输出范围: {'完整排名' if top_n is None else f'Top {top_n}'}\n")
    tickers = _load_or_build_investable_tickers(args.market)
    df = screen_stocks(tickers=tickers, auto_discover=False, market=args.market) if tickers else pd.DataFrame()
    display_df = df if top_n is None else df.head(top_n)
    print_report(display_df, profile, top_n=top_n)
    _print_insight_explanations(display_df)


def run_analyze(args: argparse.Namespace) -> None:
    profile = MARKET_PROFILES[args.market]
    tickers = normalize_tickers(args.tickers, args.market)
    print("\n🔎 WealthKeeper - 指定股票分析")
    print(f"  🌍 市场: {profile.label} ({profile.currency})")
    print(f"  📋 代码: {', '.join(tickers)}\n")
    print_report(screen_stocks(tickers=tickers, auto_discover=False, market=args.market), profile)


def _run_trade(args: argparse.Namespace, side: str) -> None:
    ticker = normalize_ticker(args.ticker, args.market)
    trade_date = _trade_date_str(args.market)
    price = _resolve_trade_price(args.market, ticker, args.price, trade_date)
    with db.get_connection() as conn:
        db.record_trade(conn, market=args.market, ticker=ticker, side=side, shares=float(args.shares), price=price, fees=float(args.fees), trade_date=trade_date, note=args.note)
    total = float(args.shares) * price + (float(args.fees) if side == "buy" else -float(args.fees))
    print(f"\n{'🟢 买入已记录' if side == 'buy' else '🔴 卖出已记录'}")
    print(f"  🌍 市场: {args.market.upper()}")
    print(f"  {'📈 股票' if side == 'buy' else '📉 股票'}: {ticker}")
    print(f"  🔢 数量: {float(args.shares):.6f} 股")
    print(f"  💵 成交价: {price:.4f}")
    print(f"  {'💸 交易总额' if side == 'buy' else '💰 到账金额'}: {total:.2f}")
    _print_wallet_state(args.market)


def run_buy(args: argparse.Namespace) -> None:
    _run_trade(args, "buy")


def run_sell(args: argparse.Namespace) -> None:
    _run_trade(args, "sell")


def run_wallet(args: argparse.Namespace) -> None:
    if args.wallet_command == "show":
        _print_wallet_summary(args.market)
        return
    trade_date = _trade_date_str(args.market)
    with db.get_connection() as conn:
        if args.wallet_command == "deposit":
            db.add_cash(conn, market=args.market, amount=float(args.amount), trade_date=trade_date, reason="manual_fund", note=args.note)
        elif args.wallet_command == "withdraw":
            db.withdraw_cash(conn, market=args.market, amount=float(args.amount), trade_date=trade_date, reason="manual_withdraw", note=args.note)
        else:
            raise ValueError(f"未知钱包命令: {args.wallet_command}")
    print(f"\n{'🟢 入金已记录' if args.wallet_command == 'deposit' else '🟠 提现已记录'}")
    print(f"  🌍 市场: {args.market.upper()}")
    print(f"  💵 金额: {float(args.amount):.2f}")
    if args.note:
        print(f"  📝 备注: {args.note}")
    _print_wallet_summary(args.market)


def run_copilot(args: argparse.Namespace) -> None:
    markets = sorted(MARKET_PROFILES) if args.market == "all" else [args.market]
    for market in markets:
        result = build_market_copilot(market, top_n=max(1, int(args.top)))
        print_copilot_report(result)


def run_track(args: argparse.Namespace) -> None:
    run_monitor(args.market, _parse_date(args.date), date_explicit=args.date is not None)


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    {
        "refresh": run_refresh,
        "insight": run_insight,
        "analyze": run_analyze,
        "buy": run_buy,
        "sell": run_sell,
        "wallet": run_wallet,
        "copilot": run_copilot,
        "track": run_track,
    }.get(args.command, lambda *_: build_parser().error(f"未知命令: {args.command}"))(args)
