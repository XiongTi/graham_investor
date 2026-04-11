from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

from .config import MARKET_PROFILES
from .monitor import _build_wallet_snapshot, _current_market_date, _resolve_target_date
from .model import screen_stocks


@dataclass
class CopilotResult:
    market: str
    currency: str
    as_of_date: str
    wallet: dict[str, object] | None
    hold_rows: list[dict[str, object]]
    sell_rows: list[dict[str, object]]
    buy_rows: list[dict[str, object]]
    rebalance_rows: list[dict[str, object]]


def _as_lookup(df: pd.DataFrame) -> dict[str, dict[str, object]]:
    if df.empty or "代码" not in df.columns:
        return {}
    return {str(row["代码"]): row for row in df.to_dict("records")}


def _sell_reason(row: dict[str, object]) -> str:
    grade = str(row.get("评级") or "")
    total_return = row.get("持仓累计收益%")
    score = row.get("总分")
    if grade in {"F", "D"}:
        return f"评级只有 {grade}，基本面质量已经偏弱。"
    if isinstance(total_return, (int, float)) and total_return <= -15 and grade not in {"A", "B"}:
        return f"当前浮亏约 {total_return:.1f}%，而且评级没有进入 A/B。"
    if isinstance(score, (int, float)) and score < 60:
        return f"总分只有 {score:.1f}，继续持有的胜率不高。"
    return "当前质量和回报不匹配，建议腾出仓位。"


def _hold_reason(row: dict[str, object]) -> str:
    grade = str(row.get("评级") or "")
    total_return = row.get("持仓累计收益%")
    if grade in {"A", "B"}:
        if isinstance(total_return, (int, float)) and total_return > 0:
            return f"评级仍是 {grade}，而且当前还有正收益。"
        return f"评级仍是 {grade}，基本面暂时没有明显坏掉。"
    return "暂时没有到明确卖出的程度，可以继续观察。"


def _buy_reason(row: dict[str, object]) -> str:
    grade = str(row.get("评级") or "")
    score = row.get("总分")
    safety = row.get("安全边际%")
    reasons: list[str] = []
    if grade in {"A", "B"}:
        reasons.append(f"评级是 {grade}")
    if isinstance(score, (int, float)):
        reasons.append(f"总分 {score:.1f}")
    if isinstance(safety, (int, float)):
        reasons.append(f"安全边际约 {safety:.1f}%")
    return "，".join(reasons) + "。"


def _classify_positions(position_df: pd.DataFrame) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    hold_rows: list[dict[str, object]] = []
    sell_rows: list[dict[str, object]] = []
    for row in position_df.to_dict("records"):
        grade = str(row.get("评级") or "")
        total_return = row.get("持仓累计收益%")
        score = row.get("总分")
        should_sell = grade in {"F", "D"} or (
            isinstance(total_return, (int, float)) and total_return <= -15 and grade not in {"A", "B"}
        ) or (
            isinstance(score, (int, float)) and score < 60
        )
        if should_sell:
            row["建议理由"] = _sell_reason(row)
            sell_rows.append(row)
        else:
            row["建议理由"] = _hold_reason(row)
            hold_rows.append(row)
    hold_rows.sort(key=lambda item: (str(item.get("评级") or ""), float(item.get("总分") or 0.0)), reverse=True)
    sell_rows.sort(key=lambda item: (float(item.get("总分") or 0.0), float(item.get("持仓累计收益%") or 0.0)))
    return hold_rows, sell_rows


def _pick_buy_candidates(candidate_df: pd.DataFrame, held_tickers: set[str], top_n: int) -> list[dict[str, object]]:
    buy_rows: list[dict[str, object]] = []
    for row in candidate_df.to_dict("records"):
        ticker = str(row.get("代码") or "")
        if not ticker or ticker in held_tickers:
            continue
        grade = str(row.get("评级") or "")
        score = row.get("总分")
        if grade not in {"A", "B"} and not (isinstance(score, (int, float)) and score >= 75):
            continue
        row["建议理由"] = _buy_reason(row)
        buy_rows.append(row)
        if len(buy_rows) >= top_n:
            break
    return buy_rows


def _build_rebalance_plan(
    buy_rows: list[dict[str, object]],
    sell_rows: list[dict[str, object]],
    wallet_row: dict[str, object] | None,
) -> list[dict[str, object]]:
    plan: list[dict[str, object]] = []
    for row in sell_rows[:3]:
        plan.append(
            {
                "动作": "卖出",
                "代码": str(row.get("代码") or ""),
                "建议投入金额": None,
                "参考价格": row.get("现价"),
                "建议股数": row.get("持仓股数"),
                "建议理由": str(row.get("建议理由") or ""),
            }
        )
    if wallet_row is None:
        return plan
    available_cash = float(wallet_row.get("cash_balance") or 0.0)
    sell_value = sum(float(item.get("当前市值") or 0.0) for item in sell_rows)
    deployable_cash = available_cash + sell_value
    if deployable_cash <= 0 or not buy_rows:
        return plan
    slots = min(len(buy_rows), 3)
    per_slot = deployable_cash / slots if slots else 0.0
    for row in buy_rows[:slots]:
        ticker = str(row.get("代码") or "")
        price = row.get("价格")
        suggested_shares = None
        if isinstance(price, (int, float)) and price > 0:
            suggested_shares = int(per_slot // float(price))
        plan.append(
            {
                "动作": "买入",
                "代码": ticker,
                "建议投入金额": round(per_slot, 2),
                "参考价格": round(float(price), 2) if isinstance(price, (int, float)) else None,
                "建议股数": suggested_shares if suggested_shares and suggested_shares > 0 else None,
                "建议理由": str(row.get("建议理由") or ""),
            }
        )
    return plan


def build_market_copilot(market: str, *, top_n: int = 10, as_of_date: date | None = None) -> CopilotResult:
    profile = MARKET_PROFILES[market]
    requested_date = as_of_date or _current_market_date(market)
    target_date = _resolve_target_date(market, requested_date, date_explicit=as_of_date is not None)
    if target_date is None:
        return CopilotResult(market=market, currency=profile.currency, as_of_date=requested_date.isoformat(), wallet=None, hold_rows=[], sell_rows=[], buy_rows=[], rebalance_rows=[])
    wallet_positions, wallet_row = _build_wallet_snapshot(market, profile.currency, target_date)
    held_tickers = {str(row["ticker"]) for row in wallet_positions}
    holding_df = screen_stocks(tickers=sorted(held_tickers), auto_discover=False, market=market, show_progress=False) if held_tickers else pd.DataFrame()
    holding_lookup = _as_lookup(holding_df)
    position_rows: list[dict[str, object]] = []
    for position in wallet_positions:
        ticker = str(position["ticker"])
        analysis = holding_lookup.get(ticker, {})
        position_rows.append(
            {
                "代码": ticker,
                "持仓股数": float(position["shares"]),
                "持仓成本": float(position["cost_basis"]),
                "现价": position.get("latest_close"),
                "当前市值": position.get("current_value"),
                "持仓累计收益%": position.get("total_return_pct"),
                "总分": analysis.get("总分"),
                "评级": analysis.get("评级"),
            }
        )
    position_df = pd.DataFrame(position_rows)
    hold_rows, sell_rows = _classify_positions(position_df) if not position_df.empty else ([], [])
    candidate_df = screen_stocks(auto_discover=True, market=market, show_progress=False)
    buy_rows = _pick_buy_candidates(candidate_df, held_tickers, top_n)
    rebalance_rows = _build_rebalance_plan(buy_rows, sell_rows, wallet_row)
    return CopilotResult(
        market=market,
        currency=profile.currency,
        as_of_date=target_date.isoformat(),
        wallet=wallet_row,
        hold_rows=hold_rows,
        sell_rows=sell_rows,
        buy_rows=buy_rows,
        rebalance_rows=rebalance_rows,
    )


def print_copilot_report(result: CopilotResult) -> None:
    print(f"\n🤖 WealthKeeper Copilot - {result.market.upper()} 市场")
    print(f"  🗓 观测日: {result.as_of_date}")
    if result.wallet is None:
        print("  当前没有真实持仓记录，暂时无法给出调仓建议。\n")
        return
    print(f"  💵 现金余额: {float(result.wallet.get('cash_balance') or 0.0):.2f} {result.currency}")
    print(f"  📦 当前持仓数: {int(result.wallet.get('position_count') or 0)}\n")

    print("  ✅ 建议继续持有:")
    if not result.hold_rows:
        print("    暂无。")
    for row in result.hold_rows:
        print(f"    {row['代码']}: {row['建议理由']}")

    print("\n  ⚠ 建议卖出:")
    if not result.sell_rows:
        print("    暂无。")
    for row in result.sell_rows:
        print(f"    {row['代码']}: {row['建议理由']}")

    print("\n  🛒 建议买入/关注:")
    if not result.buy_rows:
        print("    暂无。")
    for row in result.buy_rows:
        print(f"    {row['代码']}: {row['建议理由']}")

    print("\n  🔄 推荐调仓方案:")
    if not result.rebalance_rows:
        print("    当前没有明确的调仓动作，先继续观察。")
    for row in result.rebalance_rows:
        shares = f" | 建议股数 {row['建议股数']}" if row.get("建议股数") is not None else ""
        price = f"{float(row['参考价格']):.2f}" if row.get("参考价格") is not None else "--"
        amount_text = "" if row.get("建议投入金额") is None else f" | 建议投入 {float(row['建议投入金额']):.2f}"
        print(f"    {row['动作']} {row['代码']}{amount_text} | 参考价 {price}{shares}")
        print(f"      理由: {row['建议理由']}")
    print()
