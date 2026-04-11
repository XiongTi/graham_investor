from __future__ import annotations
import argparse
import pandas as pd
import yfinance as yf
from .config import MARKET_PROFILES, MarketProfile, utc_now_iso
from .screener import normalize_ticker, normalize_tickers
UNIVERSE_COLUMNS = ["ticker", "name", "market", "exchange", "board", "status", "is_active", "is_common_stock", "updated_at", "source"]
BOOL_MAP = {"true": True, "1": True, "yes": True, "y": True, "false": False, "0": False, "no": False, "n": False}
def _to_bool_series(series: pd.Series, default: bool) -> pd.Series:
    return series.fillna(default).map(lambda v: BOOL_MAP.get(str(v).strip().lower(), default)).astype(bool)
def _base_record(symbol: str, profile: MarketProfile) -> dict[str, object]:
    exchange, board = ("SH" if symbol.endswith(".SS") else "SZ", "chinext" if symbol.split(".")[0].startswith("30") else "main_board") if profile.code == "cn" else (("HKEX", "main_board") if profile.code == "hk" else ("UNKNOWN", "main_board"))
    return {"ticker": symbol, "name": symbol, "market": profile.code, "exchange": exchange, "board": board, "status": "normal", "is_active": True, "is_common_stock": True, "updated_at": utc_now_iso(), "source": "seed_universe"}
def _fetch_symbol_metadata(symbol: str, profile: MarketProfile) -> dict[str, object]:
    record = _base_record(symbol, profile)
    try:
        info = yf.Ticker(symbol).info or {}
    except Exception:
        return record
    record.update(name=info.get("shortName") or info.get("longName") or symbol, is_active=info.get("regularMarketPrice") is not None or info.get("currentPrice") is not None, is_common_stock=str(info.get("quoteType") or "").upper() in ("EQUITY", "COMMON STOCK", ""), source="yfinance")
    if profile.code == "us":
        record.update(exchange=str(info.get("exchange") or record["exchange"]), board="main_board")
    return record
def load_source_universe(profile: MarketProfile) -> pd.DataFrame:
    for key in ("source_path", "raw_path"):
        path = profile.universe_config[key]
        if path.exists():
            return pd.read_csv(path, dtype=str)
    return pd.DataFrame([_fetch_symbol_metadata(symbol, profile) for symbol in normalize_tickers(profile.discovery_universe, profile.code)])
def normalize_universe(df: pd.DataFrame, profile: MarketProfile) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=UNIVERSE_COLUMNS)
    frame = df.copy()
    now_iso = utc_now_iso()
    defaults = {"name": None, "exchange": "", "board": "main_board", "status": "normal", "is_active": True, "is_common_stock": True, "updated_at": now_iso, "source": "seed_universe"}
    for column, default in defaults.items():
        if column not in frame.columns:
            frame[column] = default
    frame["ticker"] = frame["ticker"].astype(str).map(lambda value: normalize_ticker(value, profile.code))
    frame["market"] = profile.code
    for column, fallback in {"name": frame["ticker"], "exchange": "", "board": "main_board", "status": "normal", "updated_at": now_iso, "source": "seed_universe"}.items():
        frame[column] = frame[column].fillna(fallback)
    frame["is_active"] = _to_bool_series(frame["is_active"], True)
    frame["is_common_stock"] = _to_bool_series(frame["is_common_stock"], True)
    return frame.drop_duplicates(subset=["ticker"], keep="last")
def _exclude_reason(row: pd.Series, profile: MarketProfile) -> str:
    cfg, name, exchange, board = profile.universe_config, str(row.get("name") or "").upper().strip(), str(row.get("exchange") or ""), str(row.get("board") or "")
    if not bool(row.get("is_active")):
        return "inactive"
    if not bool(row.get("is_common_stock")):
        return "non_common_stock"
    if exchange not in cfg["allowed_exchanges"]:
        return "exchange_not_allowed"
    if board not in cfg["allowed_boards"]:
        return "board_not_allowed"
    for keyword in map(str.upper, cfg["exclude_name_keywords"]):
        if keyword in {"ST", "*ST"} and name.startswith(keyword):
            return f"name:{keyword.lower()}"
        if keyword not in {"ST", "*ST"} and keyword in name:
            return f"name:{keyword.lower()}"
    return ""
def build_investable_universe(df: pd.DataFrame, profile: MarketProfile) -> pd.DataFrame:
    frame = df.copy()
    if frame.empty:
        frame["is_investable"], frame["exclude_reason"] = pd.Series(dtype=bool), pd.Series(dtype=str)
        return frame
    frame["exclude_reason"] = frame.apply(lambda row: _exclude_reason(row, profile), axis=1)
    frame["is_investable"] = frame["exclude_reason"].eq("")
    return frame.sort_values(["is_investable", "ticker"], ascending=[False, True]).reset_index(drop=True)
def write_universe(df: pd.DataFrame, path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
def ensure_investable_universe(market: str, *, persist: bool = True) -> tuple[pd.DataFrame, pd.DataFrame]:
    profile = MARKET_PROFILES[market]
    raw_df = normalize_universe(load_source_universe(profile), profile)
    investable_df = build_investable_universe(raw_df, profile)
    if persist:
        write_universe(raw_df, profile.universe_config["raw_path"])
        write_universe(investable_df, profile.universe_config["investable_path"])
    return raw_df, investable_df
def refresh_universe(market: str) -> None:
    profile = MARKET_PROFILES[market]
    raw_df, investable_df = ensure_investable_universe(market, persist=True)
    print(f"[{profile.label}] source universe: {profile.universe_config['source_path']}")
    print(f"[{profile.label}] raw universe saved: {profile.universe_config['raw_path']}")
    print(f"[{profile.label}] investable universe saved: {profile.universe_config['investable_path']}")
    print(f"[{profile.label}] investable count: {int(investable_df['is_investable'].sum()) if not investable_df.empty else 0}")
def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh local investable universe files.")
    parser.add_argument("--market", choices=[*sorted(MARKET_PROFILES), "all"], required=True)
    args = parser.parse_args()
    for market in (sorted(MARKET_PROFILES) if args.market == "all" else [args.market]):
        refresh_universe(market)
if __name__ == "__main__":
    main()
