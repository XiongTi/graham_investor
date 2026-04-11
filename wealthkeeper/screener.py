from __future__ import annotations
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
import yfinance as yf
from yfinance import EquityQuery
from .config import MARKET_PROFILES, MarketProfile, UNIVERSE_DIR, utc_now_iso
SNAPSHOT_FRESH_DAYS = 90
def _default_top_n(profile: MarketProfile) -> int:
    return int(profile.screener_config.get("top_n", profile.screener_config.get("max_candidates", 20)))
def normalize_ticker(ticker: str, market: str) -> str:
    """Normalize user input to the yfinance symbol convention for each market."""
    raw = ticker.strip().upper()
    if not raw:
        return raw
    if market == "us":
        return raw
    if market == "cn":
        if raw.endswith(".SH"):
            return raw[:-3] + ".SS"
        if raw.endswith(".SZ") or raw.endswith(".SS"):
            return raw
        digits = re.sub(r"\D", "", raw)
        if len(digits) != 6:
            return raw
        if digits.startswith(("6", "9")):
            return f"{digits}.SS"
        return f"{digits}.SZ"
    if market == "hk":
        if raw.endswith(".HK"):
            prefix = raw[:-3]
            if prefix.isdigit():
                digits = prefix.lstrip("0") or "0"
                return f"{int(digits):04d}.HK"
            return raw
        digits = re.sub(r"\D", "", raw)
        if not digits:
            return raw
        digits = digits.lstrip("0") or "0"
        return f"{int(digits):04d}.HK"
    return raw
def normalize_tickers(tickers: list[str], market: str) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for ticker in tickers:
        symbol = normalize_ticker(ticker, market)
        if symbol and symbol not in seen:
            normalized.append(symbol)
            seen.add(symbol)
    return normalized
def _build_us_query(profile: MarketProfile) -> EquityQuery:
    cfg = profile.screener_config
    return EquityQuery("and", [EquityQuery("is-in", ["exchange"] + cfg["exchanges"]), EquityQuery("gt", ["intradaymarketcap", cfg["min_market_cap"]]), EquityQuery("gt", ["avgdailyvol3m", cfg["min_avg_volume"]]), EquityQuery("gt", ["peratio.lasttwelvemonths", 0]), EquityQuery("lt", ["peratio.lasttwelvemonths", cfg["pre_screen_pe_max"]]), EquityQuery("lt", ["pricebookratio.quarterly", cfg["pre_screen_pb_max"]]), EquityQuery("gt", ["returnonequity.lasttwelvemonths", cfg["pre_screen_roe_min"]]), EquityQuery("gt", ["netincomemargin.lasttwelvemonths", cfg["pre_screen_net_margin_min"]])])
def _paginate_screen(query: EquityQuery, max_results: int) -> list[dict]:
    all_quotes: list[dict] = []
    offset = 0
    page_size = 250
    while len(all_quotes) < max_results:
        try:
            result = yf.screen(query, offset=offset, size=page_size)
        except Exception as exc:
            print(f"  ⚠ 筛选请求失败 (offset={offset}): {exc}")
            break
        quotes = result.get("quotes", [])
        if not quotes:
            break
        all_quotes.extend(quotes)
        offset += page_size
        if len(quotes) < page_size:
            break
    return all_quotes[:max_results]
def _snapshot_cache_path(profile: MarketProfile) -> Path:
    return UNIVERSE_DIR / f"{profile.code}_snapshots.csv"
def _parse_snapshot_at(snapshot_at: str | None) -> datetime | None:
    if not snapshot_at:
        return None
    try:
        parsed = datetime.fromisoformat(snapshot_at.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
def snapshot_age_days(snapshot: dict[str, float | str | None]) -> int | None:
    snapshot_at = _parse_snapshot_at(snapshot.get("snapshot_at"))
    if snapshot_at is None:
        return None
    age = datetime.now(timezone.utc) - snapshot_at
    return max(0, age.days)
def is_snapshot_stale(snapshot: dict[str, float | str | None]) -> bool:
    age_days = snapshot_age_days(snapshot)
    return age_days is None or age_days > SNAPSHOT_FRESH_DAYS
def _get_snapshot(symbol: str) -> dict[str, float | str | None]:
    try:
        info = yf.Ticker(symbol).info
    except Exception as exc:
        return {"symbol": symbol, "error": str(exc)}
    return {"symbol": symbol, "price": _safe_float(info.get("currentPrice", info.get("regularMarketPrice"))), "market_cap": _safe_float(info.get("marketCap")), "avg_volume": _safe_float(info.get("averageVolume")), "pe": _safe_float(info.get("trailingPE")), "pb": _safe_float(info.get("priceToBook")), "roe": _safe_float(info.get("returnOnEquity")), "net_margin": _safe_float(info.get("profitMargins")), "snapshot_at": utc_now_iso(), "error": None}
def _load_snapshot_cache(profile: MarketProfile) -> dict[str, dict[str, float | str | None]]:
    path = _snapshot_cache_path(profile)
    try:
        df = pd.read_csv(path, dtype=str)
    except FileNotFoundError:
        return {}
    except Exception as exc:
        print(f"  ⚠ 读取 snapshot cache 失败: {exc}")
        return {}
    cache: dict[str, dict[str, float | str | None]] = {}
    for row in df.to_dict("records"):
        symbol = row.get("symbol")
        if not symbol:
            continue
        parsed: dict[str, float | str | None] = {"symbol": symbol, "error": None}
        for key in ("price", "market_cap", "avg_volume", "pe", "pb", "roe", "net_margin"):
            value = row.get(key)
            if value in (None, "", "nan", "NaN"):
                parsed[key] = None
            else:
                try:
                    parsed[key] = float(value)
                except ValueError:
                    parsed[key] = None
        parsed["snapshot_at"] = row.get("snapshot_at")
        cache[symbol] = parsed
    return cache
def _write_snapshot_cache(profile: MarketProfile, snapshots: dict[str, dict[str, float | str | None]]) -> None:
    if not snapshots:
        return
    rows = [{"symbol": symbol, "price": snapshot.get("price"), "market_cap": snapshot.get("market_cap"), "avg_volume": snapshot.get("avg_volume"), "pe": snapshot.get("pe"), "pb": snapshot.get("pb"), "roe": snapshot.get("roe"), "net_margin": snapshot.get("net_margin"), "snapshot_at": snapshot.get("snapshot_at")} for symbol, snapshot in sorted(snapshots.items())]
    path = _snapshot_cache_path(profile)
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)
def _cached_rank_order(universe: list[str], cached_snapshots: dict[str, dict[str, float | str | None]], cfg: dict[str, object]) -> list[str]:
    ranked: list[tuple[str, float, float]] = []
    remainder: list[str] = []
    for symbol in universe:
        snapshot = cached_snapshots.get(symbol)
        if not snapshot or not _passes_market_floor(snapshot, cfg):
            remainder.append(symbol)
            continue
        rank_score, coverage = _snapshot_rank(snapshot, cfg)
        ranked.append((symbol, rank_score, coverage))
    ranked.sort(key=lambda item: (item[1], item[2]), reverse=True)
    ordered = [symbol for symbol, _, _ in ranked]
    seen = set(ordered)
    ordered.extend(symbol for symbol in remainder if symbol not in seen)
    return ordered
def _merge_snapshot(live: dict[str, float | str | None], cached: dict[str, float | str | None] | None) -> dict[str, float | str | None]:
    if not cached:
        return live
    merged = dict(cached)
    merged["symbol"] = live.get("symbol", cached.get("symbol"))
    merged["error"] = None
    refreshed = False
    for key in ("price", "market_cap", "avg_volume", "pe", "pb", "roe", "net_margin"):
        if live.get(key) not in (None, "", "nan", "NaN"):
            merged[key] = live.get(key)
            refreshed = True
    if refreshed:
        merged["snapshot_at"] = live.get("snapshot_at")
    return merged
def _value_score(value: float | None, good: float, ceiling: float, *, lower_better: bool) -> float:
    if value is None:
        return 0.0
    if lower_better:
        if value <= 0:
            return 0.0
        if value <= good:
            return 1.0
        if value >= ceiling:
            return 0.0
        return max(0.0, 1 - (value - good) / (ceiling - good))
    if value >= ceiling:
        return 1.0
    if value <= good:
        return 0.0
    return max(0.0, (value - good) / (ceiling - good))
def _liquidity_score(snapshot: dict[str, float | str | None], cfg: dict[str, object]) -> float:
    market_cap = _safe_float(snapshot.get("market_cap"))
    avg_volume = _safe_float(snapshot.get("avg_volume"))
    cap_score = _value_score(market_cap, float(cfg["min_market_cap"]), float(cfg["min_market_cap"]) * 3, lower_better=False)
    vol_score = _value_score(avg_volume, float(cfg["min_avg_volume"]), float(cfg["min_avg_volume"]) * 3, lower_better=False)
    return (cap_score + vol_score) / 2
def _snapshot_components(snapshot: dict[str, float | str | None], cfg: dict[str, object]) -> dict[str, tuple[float, float, bool]]:
    pe = _safe_float(snapshot.get("pe"))
    pb = _safe_float(snapshot.get("pb"))
    roe = _safe_float(snapshot.get("roe"))
    net_margin = _safe_float(snapshot.get("net_margin"))
    market_cap = _safe_float(snapshot.get("market_cap"))
    avg_volume = _safe_float(snapshot.get("avg_volume"))
    if float(cfg.get("pre_screen_pb_max", 0)) <= 2.0 and float(cfg.get("pre_screen_pe_max", 0)) <= 18.0:
        weights = {"pe": 0.28, "pb": 0.12, "roe": 0.25, "net_margin": 0.20, "liquidity": 0.15}
    else:
        weights = {"pe": 0.28, "pb": 0.22, "roe": 0.20, "net_margin": 0.15, "liquidity": 0.15}
    return {"pe": (_value_score(pe, float(cfg["pre_screen_pe_max"]) * 0.5, float(cfg["pre_screen_pe_max"]), lower_better=True), weights["pe"], pe is not None), "pb": (_value_score(pb, float(cfg["pre_screen_pb_max"]) * 0.5, float(cfg["pre_screen_pb_max"]), lower_better=True), weights["pb"], pb is not None), "roe": (_value_score(roe, float(cfg["pre_screen_roe_min"]), max(0.20, float(cfg["pre_screen_roe_min"]) * 4), lower_better=False), weights["roe"], roe is not None), "net_margin": (_value_score(net_margin, float(cfg["pre_screen_net_margin_min"]), max(0.15, float(cfg["pre_screen_net_margin_min"]) * 4), lower_better=False), weights["net_margin"], net_margin is not None), "liquidity": (_liquidity_score(snapshot, cfg), weights["liquidity"], market_cap is not None or avg_volume is not None)}
def _snapshot_rank(snapshot: dict[str, float | str | None], cfg: dict[str, object]) -> tuple[float, float]:
    components = _snapshot_components(snapshot, cfg)
    weighted = 0.0
    available_weight = 0.0
    for score, weight, available in components.values():
        if not available:
            continue
        weighted += score * weight
        available_weight += weight
    normalized = weighted / available_weight if available_weight else 0.0
    if available_weight and is_snapshot_stale(snapshot):
        normalized *= 0.85
    return normalized, available_weight
def _passes_market_floor(snapshot: dict[str, float | str | None], cfg: dict[str, object]) -> bool:
    price = _safe_float(snapshot.get("price"))
    pe = _safe_float(snapshot.get("pe"))
    pb = _safe_float(snapshot.get("pb"))
    roe = _safe_float(snapshot.get("roe"))
    net_margin = _safe_float(snapshot.get("net_margin"))
    if price == 0:
        return False
    if pe is not None and (pe <= 0 or pe > float(cfg["pre_screen_pe_max"]) * 1.6):
        return False
    if pb is not None and pb <= 0:
        return False
    if price is None and pe is None and pb is None and roe is None and net_margin is None:
        return False
    return True
def _safe_float(value: float | str | None) -> float | None:
    if value in (None, "", "nan", "NaN"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
def _format_pct(value: float | None) -> str:
    if value is None:
        return "缺失"
    return f"{value * 100:.1f}%"
def _format_num(value: float | None, digits: int = 1) -> str:
    if value is None:
        return "缺失"
    return f"{value:.{digits}f}"
def _coverage_label(coverage: float) -> str:
    if coverage >= 0.9:
        return "数据比较完整"
    if coverage >= 0.65:
        return "核心字段大体够用"
    return "字段还不够完整"
def _explain_candidate(symbol: str, snapshot: dict[str, float | str | None], cfg: dict[str, object], rank_score: float, coverage: float) -> str:
    pe = _safe_float(snapshot.get("pe"))
    pb = _safe_float(snapshot.get("pb"))
    roe = _safe_float(snapshot.get("roe"))
    margin = _safe_float(snapshot.get("net_margin"))
    market_cap = _safe_float(snapshot.get("market_cap"))
    avg_volume = _safe_float(snapshot.get("avg_volume"))
    reasons: list[str] = []
    risks: list[str] = []
    data_status: list[str] = []
    age_days = snapshot_age_days(snapshot)
    stale = is_snapshot_stale(snapshot)
    if pe is not None:
        if pe <= float(cfg["pre_screen_pe_max"]) * 0.7:
            reasons.append(f"估值不贵，市盈率约 {_format_num(pe)}")
        elif pe <= float(cfg["pre_screen_pe_max"]):
            reasons.append(f"估值还在可接受范围，市盈率约 {_format_num(pe)}")
        else:
            risks.append(f"市盈率偏高，约 {_format_num(pe)}")
    else:
        risks.append("市盈率数据缺失")
    if pb is not None:
        if pb <= float(cfg["pre_screen_pb_max"]) * 0.7:
            reasons.append(f"市净率不高，约 {_format_num(pb, 2)}")
        elif pb > float(cfg["pre_screen_pb_max"]):
            risks.append(f"市净率偏高，约 {_format_num(pb, 2)}")
    if roe is not None:
        if roe >= max(0.15, float(cfg["pre_screen_roe_min"]) * 2):
            reasons.append(f"赚钱能力不错，ROE 约 {_format_pct(roe)}")
        elif roe < float(cfg["pre_screen_roe_min"]):
            risks.append(f"ROE 偏低，约 {_format_pct(roe)}")
    else:
        risks.append("ROE 数据缺失")
    if margin is not None:
        if margin >= max(0.10, float(cfg["pre_screen_net_margin_min"]) * 2):
            reasons.append(f"利润率还可以，净利率约 {_format_pct(margin)}")
        elif margin < float(cfg["pre_screen_net_margin_min"]):
            risks.append(f"净利率偏低，约 {_format_pct(margin)}")
    else:
        risks.append("净利率数据缺失")
    if market_cap is not None and avg_volume is not None:
        reasons.append("盘子和成交量都过了基本门槛")
    elif market_cap is not None:
        reasons.append("公司体量达到基本门槛")
    elif avg_volume is not None:
        reasons.append("成交活跃度达到基本门槛")
    else:
        risks.append("流动性数据缺失")
    if stale:
        if age_days is None:
            data_status.append("快照时间缺失，数据新鲜度不好判断")
        else:
            data_status.append(f"快照大约是 {age_days} 天前的，最新行情可能还没完全补齐")
    else:
        data_status.append("快照还比较新")
    if not reasons:
        if stale:
            reasons.append("最近实时字段拿得不完整，但旧快照里暂时没有明显硬伤")
        else:
            reasons.append("可用数据不多，但现有指标里没有明显硬伤")
    plain = "；".join(reasons[:3])
    status_text = _coverage_label(coverage)
    if data_status:
        status_text += f"；{'；'.join(data_status[:2])}"
    if risks:
        plain += f"。数据情况：{status_text}。要注意：{risks[0]}"
    else:
        plain += f"。数据情况：{status_text}"
    coverage_text = f"数据完整度约 {coverage * 100:.0f}%"
    return f"{symbol}: {plain}。预筛分 {rank_score * 100:.1f} 分，{coverage_text}。"
def _fallback_explanation(symbol: str, source_label: str, snapshot: dict[str, float | str | None] | None = None) -> str:
    if snapshot:
        age_days = snapshot_age_days(snapshot)
        if age_days is None:
            freshness = "快照时间缺失，数据新鲜度不好判断"
        else:
            freshness = f"最近可用快照大约是 {age_days} 天前的"
        return f"{symbol}: 当前实时字段不足，所以先按 {source_label} 保守保留。数据情况：{freshness}。适合继续观察，不适合现在下结论。"
    return f"{symbol}: 当前缺少足够实时字段，所以先按 {source_label} 保守保留。等后续新数据补齐后，再判断是否值得进入深度评分。"
def _build_candidate_details(profile: MarketProfile, refresh_limit: int | None = None) -> list[dict[str, object]]:
    cfg = profile.screener_config
    universe = normalize_tickers(profile.discovery_universe, profile.code)
    print(f"  🔍 正在预筛 {profile.label} 市场候选池（基础范围 {len(universe)} 只）...")
    cached_snapshots = _load_snapshot_cache(profile)
    merged_snapshots: dict[str, dict[str, float | str | None]] = {}
    refresh_universe = universe
    if refresh_limit is not None:
        refresh_universe = _cached_rank_order(universe, cached_snapshots, cfg)[:refresh_limit]
        print(f"  ♻ 先用本地快照做粗排，再仅刷新前 {len(refresh_universe)} 只候选")
    if refresh_universe:
        with ThreadPoolExecutor(max_workers=12) as executor:
            futures = {executor.submit(_get_snapshot, symbol): symbol for symbol in refresh_universe}
            for future in as_completed(futures):
                symbol = futures[future]
                live = future.result()
                if live.get("error"):
                    cached = cached_snapshots.get(symbol)
                    if cached:
                        merged_snapshots[symbol] = dict(cached)
                    continue
                merged_snapshots[symbol] = _merge_snapshot(live, cached_snapshots.get(symbol))
    for symbol, cached in cached_snapshots.items():
        if symbol not in merged_snapshots and symbol in universe:
            merged_snapshots[symbol] = dict(cached)
    _write_snapshot_cache(profile, merged_snapshots)
    ranked: list[dict[str, object]] = []
    for symbol in universe:
        snapshot = merged_snapshots.get(symbol)
        if not snapshot or not _passes_market_floor(snapshot, cfg):
            continue
        rank_score, coverage = _snapshot_rank(snapshot, cfg)
        ranked.append({"symbol": symbol, "snapshot": snapshot, "rank_score": rank_score, "coverage": coverage, "explanation": _explain_candidate(symbol, snapshot, cfg, rank_score, coverage)})
    ranked.sort(key=lambda item: (item["rank_score"], item["coverage"]), reverse=True)
    top_n = int(cfg["top_n"])
    if ranked:
        print(f"  ✓ {profile.label} 市场预筛完成，选出 {min(len(ranked), top_n)} 只头部候选")
        return ranked[:top_n]
    fallback_universe = universe
    fallback = []
    if fallback_universe:
        print(f"  ⚠ {profile.label} 市场预筛结果为空，回退到本地可投资股票池的前 {top_n} 只")
        for symbol in fallback_universe[:top_n]:
            cached = cached_snapshots.get(symbol)
            fallback.append({"symbol": symbol, "snapshot": cached or {}, "rank_score": 0.0, "coverage": 0.0, "explanation": _fallback_explanation(symbol, "本地可投资股票池", cached)})
        return fallback
    print(f"  ⚠ {profile.label} 市场预筛结果为空，回退到内置股票池")
    for symbol in normalize_tickers(profile.fallback_watchlist, profile.code)[:top_n]:
        cached = cached_snapshots.get(symbol)
        fallback.append({"symbol": symbol, "snapshot": cached or {}, "rank_score": 0.0, "coverage": 0.0, "explanation": _fallback_explanation(symbol, "内置股票池", cached)})
    return fallback
def _profile_with_discovery_universe(profile: MarketProfile, discovery_universe: list[str], *, top_n: int | None = None) -> MarketProfile:
    return replace(profile, screener_config={**profile.screener_config, "top_n": top_n if top_n is not None else _default_top_n(profile)}, discovery_universe=discovery_universe)
def _candidate_details_from_profile(profile: MarketProfile, *, top_n: int | None = None, refresh_limit: int | None = None) -> list[dict[str, object]]:
    local_universe = _load_investable_universe(profile)
    if local_universe:
        print(f"  📁 使用本地 {profile.label} 可投资股票池（{len(local_universe)} 只）")
        return _build_candidate_details(_profile_with_discovery_universe(profile, local_universe, top_n=top_n), refresh_limit=refresh_limit)
    if profile.code == "us":
        print("  🔍 正在自动筛选美股候选池...")
        query = _build_us_query(profile)
        quotes = _paginate_screen(query, int(profile.screener_config["max_candidates"]))
        tickers = list(dict.fromkeys(q.get("symbol", "") for q in quotes if q.get("symbol")))
        if tickers:
            print(f"  ✓ 预筛完成，发现 {len(tickers)} 只候选股票")
            effective_refresh_limit = refresh_limit if refresh_limit is not None else top_n
            return _build_candidate_details(_profile_with_discovery_universe(profile, tickers, top_n=top_n), refresh_limit=effective_refresh_limit)
        print("  ⚠ 美股在线预筛不可用，回退到内置股票池")
    return [{"symbol": symbol, "snapshot": {}, "rank_score": 0.0, "coverage": 0.0, "explanation": _fallback_explanation(symbol, "内置股票池")} for symbol in normalize_tickers(profile.fallback_watchlist, profile.code)[: int(top_n if top_n is not None else _default_top_n(profile))]]
def _load_investable_universe(profile: MarketProfile) -> list[str]:
    path = profile.universe_config["investable_path"]
    try:
        df = pd.read_csv(path, dtype=str)
    except FileNotFoundError:
        return []
    except Exception as exc:
        print(f"  ⚠ 读取本地 universe 失败: {exc}")
        return []
    if "ticker" not in df.columns:
        return []
    if "is_investable" in df.columns:
        normalized = df["is_investable"].astype(str).str.strip().str.lower()
        df = df[normalized.isin(["true", "1", "yes", "y"])]
    tickers = normalize_tickers(df["ticker"].astype(str).tolist(), profile.code)
    return tickers
def explain_candidates(profile: MarketProfile, *, refresh_limit: int | None = None) -> list[dict[str, object]]:
    return _candidate_details_from_profile(profile, top_n=_default_top_n(profile), refresh_limit=refresh_limit)
def print_candidate_report(profile: MarketProfile, details: list[dict[str, object]]) -> None:
    print("\n" + "=" * 90)
    print(f"  🔎 {profile.label} 市场预筛候选前 {len(details)} 名")
    print("=" * 90 + "\n")
    for idx, item in enumerate(details, 1):
        symbol = item["symbol"]
        snapshot = item["snapshot"]
        print(f"{idx:>2}. {symbol}")
        print(f"    {item['explanation']}")
        if snapshot:
            print(
                "    "
                f"P/E={_format_num(_safe_float(snapshot.get('pe')))} | "
                f"P/B={_format_num(_safe_float(snapshot.get('pb')), 2)} | "
                f"ROE={_format_pct(_safe_float(snapshot.get('roe')))} | "
                f"净利率={_format_pct(_safe_float(snapshot.get('net_margin')))}"
            )
        print()
if __name__ == "__main__":
    raise SystemExit("请使用 `ws insight --market <market> --top <n>`；预筛模块现在作为内部能力使用。")
