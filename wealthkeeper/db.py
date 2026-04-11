from __future__ import annotations
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator
from .config import DATA_DIR, MARKET_PROFILES, utc_now_iso
LEGACY_DB_PATH = DATA_DIR / "graham_investor.db"
DB_PATH = DATA_DIR / "wealthkeeper.db"
STRATEGY_COLUMNS = "p.market, p.run_date, p.portfolio_name, p.group_name, p.ticker, p.rank, p.weight, p.score, p.grade, p.data_source, p.snapshot_status"
TRADE_COLUMNS = "id, wallet_id, market, ticker, side, shares, price, fees, trade_date, note, created_at"
RUN_COLUMNS = "id, market, run_date, top_n, skip_screener, initial_capital, created_at"
def _as_dicts(cursor: sqlite3.Cursor) -> list[dict[str, object]]:
    columns = [column[0] for column in cursor.description or []]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]
def _first(cursor: sqlite3.Cursor) -> dict[str, object] | None:
    rows = _as_dicts(cursor)
    return rows[0] if rows else None
def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
def _wallet_row(cursor: sqlite3.Cursor) -> dict[str, object] | None:
    row = cursor.fetchone()
    return None if row is None else dict(zip([column[0] for column in cursor.description or []], row))
def _compute_positions_from_trades(trades: list[dict[str, object]]) -> list[dict[str, object]]:
    positions: dict[str, dict[str, float | str]] = {}
    for trade in trades:
        ticker, side, shares, price, fees = str(trade["ticker"]), str(trade["side"]), float(trade["shares"]), float(trade["price"]), float(trade["fees"] or 0.0)
        position = positions.setdefault(ticker, {"ticker": ticker, "shares": 0.0, "cost_basis": 0.0, "avg_cost": 0.0, "realized_pnl": 0.0, "market": str(trade["market"])})
        held, cost, realized = float(position["shares"]), float(position["cost_basis"]), float(position["realized_pnl"])
        if side == "buy":
            held, cost = held + shares, cost + shares * price + fees
        else:
            if held + 1e-9 < shares:
                raise ValueError(f"{ticker} 持仓不足，无法卖出 {shares} 股")
            avg_cost = cost / held if held else 0.0
            held, cost, realized = held - shares, cost - avg_cost * shares, realized + shares * price - fees - avg_cost * shares
            if abs(held) < 1e-9:
                held = cost = 0.0
        position.update(shares=round(held, 6), cost_basis=round(cost, 6), avg_cost=round(cost / held, 6) if held else 0.0, realized_pnl=round(realized, 6))
    return [position for position in positions.values() if float(position["shares"]) > 0]
def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA foreign_keys = ON;
        CREATE TABLE IF NOT EXISTS strategy_runs (id INTEGER PRIMARY KEY AUTOINCREMENT, market TEXT NOT NULL, run_date TEXT NOT NULL, top_n INTEGER NOT NULL, skip_screener INTEGER NOT NULL DEFAULT 0, initial_capital REAL NOT NULL DEFAULT 30000, created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS strategy_positions (id INTEGER PRIMARY KEY AUTOINCREMENT, strategy_run_id INTEGER NOT NULL, market TEXT NOT NULL, run_date TEXT NOT NULL, portfolio_name TEXT NOT NULL, group_name TEXT NOT NULL, ticker TEXT NOT NULL, rank INTEGER NOT NULL, weight REAL NOT NULL, score REAL, grade TEXT, data_source TEXT, snapshot_status TEXT, FOREIGN KEY(strategy_run_id) REFERENCES strategy_runs(id) ON DELETE CASCADE);
        CREATE INDEX IF NOT EXISTS idx_strategy_runs_market_date ON strategy_runs(market, run_date DESC, id DESC);
        CREATE INDEX IF NOT EXISTS idx_strategy_positions_run_group ON strategy_positions(strategy_run_id, group_name, rank);
        CREATE TABLE IF NOT EXISTS wallets (id INTEGER PRIMARY KEY AUTOINCREMENT, market TEXT NOT NULL UNIQUE, name TEXT NOT NULL, currency TEXT NOT NULL, cash_balance REAL NOT NULL DEFAULT 0, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS cash_ledger (id INTEGER PRIMARY KEY AUTOINCREMENT, wallet_id INTEGER NOT NULL, trade_date TEXT NOT NULL, amount REAL NOT NULL, reason TEXT NOT NULL, note TEXT, created_at TEXT NOT NULL, FOREIGN KEY(wallet_id) REFERENCES wallets(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS trades (id INTEGER PRIMARY KEY AUTOINCREMENT, wallet_id INTEGER NOT NULL, market TEXT NOT NULL, ticker TEXT NOT NULL, side TEXT NOT NULL CHECK(side IN ('buy', 'sell')), shares REAL NOT NULL, price REAL NOT NULL, fees REAL NOT NULL DEFAULT 0, trade_date TEXT NOT NULL, note TEXT, created_at TEXT NOT NULL, FOREIGN KEY(wallet_id) REFERENCES wallets(id) ON DELETE CASCADE);
        CREATE INDEX IF NOT EXISTS idx_trades_wallet_date ON trades(wallet_id, trade_date, id);
        """
    )
    if "initial_capital" not in _table_columns(conn, "strategy_runs"):
        conn.execute("ALTER TABLE strategy_runs ADD COLUMN initial_capital REAL NOT NULL DEFAULT 30000")
    conn.commit()
def _resolve_db_path(db_path: Path) -> Path:
    if db_path == DB_PATH and not DB_PATH.exists() and LEGACY_DB_PATH.exists():
        LEGACY_DB_PATH.replace(DB_PATH)
    return db_path
def connect(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path = _resolve_db_path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    init_db(conn)
    return conn
@contextmanager
def get_connection(db_path: Path = DB_PATH) -> Iterator[sqlite3.Connection]:
    conn = connect(db_path)
    try:
        yield conn
    finally:
        conn.close()
def _latest_run_where(run_date: str | None) -> str:
    return "market = ? AND run_date = ?" if run_date else "market = ?"
def _latest_run_params(market: str, run_date: str | None) -> tuple[str, ...]:
    return (market, run_date) if run_date else (market,)
def save_strategy_snapshot(conn: sqlite3.Connection, *, market: str, run_date: str, top_n: int, skip_screener: bool, initial_capital: float = 30_000.0, rows: list[dict[str, object]]) -> int:
    created_at = utc_now_iso()
    strategy_run_id = int(conn.execute("INSERT INTO strategy_runs (market, run_date, top_n, skip_screener, initial_capital, created_at) VALUES (?, ?, ?, ?, ?, ?)", (market, run_date, int(top_n), 1 if skip_screener else 0, float(initial_capital), created_at)).lastrowid)
    conn.executemany("INSERT INTO strategy_positions (strategy_run_id, market, run_date, portfolio_name, group_name, ticker, rank, weight, score, grade, data_source, snapshot_status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [(strategy_run_id, row["market"], row["run_date"], row["portfolio_name"], row["group_name"], row["ticker"], int(row["rank"]), float(row["weight"]), row.get("score"), row.get("grade"), row.get("data_source"), row.get("snapshot_status")) for row in rows])
    conn.commit()
    return strategy_run_id
def load_strategy_snapshot(conn: sqlite3.Connection, *, market: str, run_date: str | None = None) -> list[dict[str, object]]:
    cursor = conn.execute(f"SELECT {STRATEGY_COLUMNS} FROM strategy_positions p JOIN strategy_runs r ON r.id = p.strategy_run_id WHERE r.id = (SELECT id FROM strategy_runs WHERE {_latest_run_where(run_date)} ORDER BY run_date DESC, id DESC LIMIT 1) ORDER BY p.group_name, p.rank", _latest_run_params(market, run_date))
    return _as_dicts(cursor)
def load_strategy_run_meta(conn: sqlite3.Connection, *, market: str, run_date: str | None = None) -> dict[str, object] | None:
    return _first(conn.execute(f"SELECT {RUN_COLUMNS} FROM strategy_runs WHERE id = (SELECT id FROM strategy_runs WHERE {_latest_run_where(run_date)} ORDER BY run_date DESC, id DESC LIMIT 1)", _latest_run_params(market, run_date)))
def get_or_create_wallet(conn: sqlite3.Connection, market: str) -> dict[str, object]:
    wallet = _wallet_row(conn.execute("SELECT * FROM wallets WHERE market = ?", (market,)))
    if wallet is not None:
        return wallet
    created_at, profile = utc_now_iso(), MARKET_PROFILES[market]
    wallet_id = int(conn.execute("INSERT INTO wallets (market, name, currency, cash_balance, created_at, updated_at) VALUES (?, ?, ?, 0, ?, ?)", (market, f"{market}_wallet", profile.currency, created_at, created_at)).lastrowid)
    conn.commit()
    return {"id": wallet_id, "market": market, "name": f"{market}_wallet", "currency": profile.currency, "cash_balance": 0.0, "created_at": created_at, "updated_at": created_at}
def add_cash(conn: sqlite3.Connection, *, market: str, amount: float, trade_date: str, reason: str = "manual_fund", note: str | None = None) -> dict[str, object]:
    if amount <= 0:
        raise ValueError("入金金额必须大于 0")
    wallet, created_at = get_or_create_wallet(conn, market), utc_now_iso()
    new_balance = float(wallet["cash_balance"]) + float(amount)
    conn.execute("UPDATE wallets SET cash_balance = ?, updated_at = ? WHERE id = ?", (new_balance, created_at, wallet["id"]))
    conn.execute("INSERT INTO cash_ledger (wallet_id, trade_date, amount, reason, note, created_at) VALUES (?, ?, ?, ?, ?, ?)", (wallet["id"], trade_date, float(amount), reason, note, created_at))
    conn.commit()
    wallet.update(cash_balance=new_balance, updated_at=created_at)
    return wallet
def withdraw_cash(conn: sqlite3.Connection, *, market: str, amount: float, trade_date: str, reason: str = "manual_withdraw", note: str | None = None) -> dict[str, object]:
    if amount <= 0:
        raise ValueError("提现金额必须大于 0")
    wallet, created_at = get_or_create_wallet(conn, market), utc_now_iso()
    current_balance = float(wallet["cash_balance"])
    if current_balance + 1e-9 < float(amount):
        raise ValueError("钱包现金不足，无法提现")
    new_balance = current_balance - float(amount)
    conn.execute("UPDATE wallets SET cash_balance = ?, updated_at = ? WHERE id = ?", (new_balance, created_at, wallet["id"]))
    conn.execute("INSERT INTO cash_ledger (wallet_id, trade_date, amount, reason, note, created_at) VALUES (?, ?, ?, ?, ?, ?)", (wallet["id"], trade_date, -float(amount), reason, note, created_at))
    conn.commit()
    wallet.update(cash_balance=new_balance, updated_at=created_at)
    return wallet
def load_wallet(conn: sqlite3.Connection, market: str) -> dict[str, object]:
    return get_or_create_wallet(conn, market)
def load_wallet_optional(conn: sqlite3.Connection, market: str) -> dict[str, object] | None:
    return _wallet_row(conn.execute("SELECT * FROM wallets WHERE market = ?", (market,)))
def load_net_deposit(conn: sqlite3.Connection, wallet_id: int, *, through_date: str | None = None) -> float:
    sql, params = ("SELECT COALESCE(SUM(amount), 0) FROM cash_ledger WHERE wallet_id = ?", (wallet_id,)) if through_date is None else ("SELECT COALESCE(SUM(amount), 0) FROM cash_ledger WHERE wallet_id = ? AND trade_date <= ?", (wallet_id, through_date))
    row = conn.execute(sql, params).fetchone()
    return float(row[0] or 0.0)
def load_trades(conn: sqlite3.Connection, wallet_id: int, *, through_date: str | None = None) -> list[dict[str, object]]:
    sql, params = (f"SELECT {TRADE_COLUMNS} FROM trades WHERE wallet_id = ? ORDER BY trade_date, id", (wallet_id,)) if through_date is None else (f"SELECT {TRADE_COLUMNS} FROM trades WHERE wallet_id = ? AND trade_date <= ? ORDER BY trade_date, id", (wallet_id, through_date))
    return _as_dicts(conn.execute(sql, params))
def compute_wallet_positions(conn: sqlite3.Connection, wallet_id: int, *, through_date: str | None = None) -> list[dict[str, object]]:
    return _compute_positions_from_trades(load_trades(conn, wallet_id, through_date=through_date))
def record_trade(conn: sqlite3.Connection, *, market: str, ticker: str, side: str, shares: float, price: float, fees: float, trade_date: str, note: str | None = None, cash_in: float = 0.0) -> dict[str, object]:
    if shares <= 0:
        raise ValueError("交易股数必须大于 0")
    if price <= 0:
        raise ValueError("交易价格必须大于 0")
    if fees < 0:
        raise ValueError("手续费不能为负数")
    if side not in {"buy", "sell"}:
        raise ValueError("side 必须是 buy 或 sell")
    if cash_in < 0:
        raise ValueError("cash_in 不能为负数")
    wallet = get_or_create_wallet(conn, market)
    existing_trades = load_trades(conn, int(wallet["id"]))
    if existing_trades:
        latest_trade_date = max(str(trade["trade_date"]) for trade in existing_trades)
        if trade_date < latest_trade_date:
            raise ValueError(f"交易日期不能早于已有交易记录的最新日期 {latest_trade_date}")
    position_map = {str(item["ticker"]): item for item in _compute_positions_from_trades(existing_trades)}
    gross_amount, cash_balance = shares * price, float(wallet["cash_balance"]) + float(cash_in)
    if side == "buy":
        cash_delta = -(gross_amount + fees)
        if cash_balance + 1e-9 < -cash_delta:
            raise ValueError("钱包现金不足，请先入金")
    else:
        held = float(position_map.get(ticker, {}).get("shares", 0.0))
        if held + 1e-9 < shares:
            raise ValueError(f"{ticker} 当前仅持有 {held:.6f} 股，无法卖出 {shares:.6f} 股")
        cash_delta = gross_amount - fees
    created_at = utc_now_iso()
    with conn:
        if cash_in > 0:
            conn.execute("INSERT INTO cash_ledger (wallet_id, trade_date, amount, reason, note, created_at) VALUES (?, ?, ?, ?, ?, ?)", (wallet["id"], trade_date, float(cash_in), "trade_fund", note, created_at))
        conn.execute("INSERT INTO trades (wallet_id, market, ticker, side, shares, price, fees, trade_date, note, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (wallet["id"], market, ticker, side, shares, price, fees, trade_date, note, created_at))
        conn.execute("UPDATE wallets SET cash_balance = ?, updated_at = ? WHERE id = ?", (cash_balance + cash_delta, created_at, wallet["id"]))
    wallet.update(cash_balance=cash_balance + cash_delta, updated_at=created_at)
    return wallet
