import math
import unittest
import warnings
from datetime import date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

warnings.filterwarnings("ignore", message=r"urllib3 v2 only supports OpenSSL 1\.1\.1\+.*")

import pandas as pd

from wealthkeeper import cli, copilot, db, model, monitor, screener
from wealthkeeper.config import MARKET_PROFILES
from wealthkeeper.model import StockAnalysis, score_stock


class MultiMarketDesignTests(unittest.TestCase):
    def test_package_cli_insight_and_track_parse(self) -> None:
        parser = cli.build_parser()

        insight_args = parser.parse_args(["insight", "--market", "us"])
        self.assertEqual(insight_args.command, "insight")
        self.assertEqual(insight_args.market, "us")
        self.assertIsNone(insight_args.top)

        top_insight_args = parser.parse_args(["insight", "--market", "us", "--top", "10"])
        self.assertEqual(top_insight_args.top, 10)

        track_args = parser.parse_args(["track", "--market", "cn"])
        self.assertEqual(track_args.command, "track")
        self.assertEqual(track_args.market, "cn")
        self.assertIsNone(track_args.date)

        analyze_args = parser.parse_args(["analyze", "--market", "us", "AAPL"])
        self.assertEqual(analyze_args.command, "analyze")
        self.assertEqual(analyze_args.tickers, ["AAPL"])

        buy_args = parser.parse_args(["buy", "--market", "us", "--ticker", "AAPL", "--shares", "10"])
        self.assertEqual(buy_args.command, "buy")
        self.assertEqual(buy_args.ticker, "AAPL")

        sell_args = parser.parse_args(["sell", "--market", "us", "--ticker", "AAPL", "--shares", "5"])
        self.assertEqual(sell_args.command, "sell")
        self.assertEqual(sell_args.shares, 5)

        wallet_args = parser.parse_args(["wallet", "deposit", "--market", "us", "--amount", "5000"])
        self.assertEqual(wallet_args.command, "wallet")
        self.assertEqual(wallet_args.wallet_command, "deposit")
        self.assertEqual(wallet_args.amount, 5000)

        copilot_args = parser.parse_args(["copilot", "--market", "us", "--top", "5"])
        self.assertEqual(copilot_args.command, "copilot")
        self.assertEqual(copilot_args.market, "us")
        self.assertEqual(copilot_args.top, 5)

    @patch("wealthkeeper.cli._resolve_target_date")
    @patch("wealthkeeper.cli._current_market_date")
    def test_trade_date_defaults_to_latest_trading_day(self, mock_current_market_date, mock_resolve_target_date) -> None:
        mock_current_market_date.return_value = date(2026, 4, 11)
        mock_resolve_target_date.return_value = date(2026, 4, 10)

        trade_date = cli._trade_date_str("us")

        self.assertEqual(trade_date, "2026-04-10")
        mock_current_market_date.assert_called_once_with("us")
        mock_resolve_target_date.assert_called_once_with("us", date(2026, 4, 11), date_explicit=False)

    @patch("wealthkeeper.cli._print_insight_explanations")
    @patch("wealthkeeper.cli._load_or_build_investable_tickers")
    @patch("wealthkeeper.cli.print_report")
    @patch("wealthkeeper.cli.screen_stocks")
    def test_package_cli_insight_reuses_model_pipeline(
        self,
        mock_screen_stocks,
        mock_print_report,
        mock_load_or_build_investable_tickers,
        mock_print_insight_explanations,
    ) -> None:
        mock_df = pd.DataFrame([{"代码": "AAPL", "总分": 90.0, "评级": "A"}])
        mock_screen_stocks.return_value = mock_df
        mock_load_or_build_investable_tickers.return_value = ["AAPL"]

        args = cli.build_parser().parse_args(["insight", "--market", "us", "--top", "10"])
        cli.run_insight(args)

        mock_load_or_build_investable_tickers.assert_called_once_with("us")
        mock_screen_stocks.assert_called_once_with(tickers=["AAPL"], auto_discover=False, market="us")
        self.assertEqual(mock_print_report.call_count, 1)
        report_args, report_kwargs = mock_print_report.call_args
        pd.testing.assert_frame_equal(report_args[0], mock_df.head(10))
        self.assertEqual(report_args[1], MARKET_PROFILES["us"])
        self.assertEqual(report_kwargs, {"top_n": 10})
        self.assertEqual(mock_print_insight_explanations.call_count, 1)
        explanation_args, _ = mock_print_insight_explanations.call_args
        pd.testing.assert_frame_equal(explanation_args[0], mock_df.head(10))

    @patch("wealthkeeper.cli.run_monitor")
    def test_package_cli_track_reuses_monitor_pipeline(self, mock_run_monitor) -> None:
        args = cli.build_parser().parse_args(["track", "--market", "hk"])

        cli.run_track(args)

        mock_run_monitor.assert_called_once()

    def test_wallet_trade_book_tracks_cash_and_remaining_position(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "graham.db"
            with db.get_connection(path) as conn:
                db.add_cash(conn, market="us", amount=10_000, trade_date="2026-03-27")
                db.record_trade(
                    conn,
                    market="us",
                    ticker="AAPL",
                    side="buy",
                    shares=10,
                    price=100,
                    fees=5,
                    trade_date="2026-03-27",
                )
                db.record_trade(
                    conn,
                    market="us",
                    ticker="AAPL",
                    side="sell",
                    shares=4,
                    price=110,
                    fees=2,
                    trade_date="2026-03-28",
                )

                wallet = db.load_wallet(conn, "us")
                positions = db.compute_wallet_positions(conn, int(wallet["id"]))

        self.assertEqual(round(float(wallet["cash_balance"]), 2), 9433.0)
        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0]["ticker"], "AAPL")
        self.assertEqual(positions[0]["shares"], 6.0)
        self.assertEqual(positions[0]["avg_cost"], 100.5)

    def test_wallet_withdraw_reduces_cash_balance(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "graham.db"
            with db.get_connection(path) as conn:
                db.add_cash(conn, market="us", amount=5_000, trade_date="2026-03-27")
                db.withdraw_cash(conn, market="us", amount=1_250, trade_date="2026-03-27")
                wallet = db.load_wallet(conn, "us")
                net_deposit = db.load_net_deposit(conn, int(wallet["id"]))

        self.assertEqual(round(float(wallet["cash_balance"]), 2), 3750.0)
        self.assertEqual(round(net_deposit, 2), 3750.0)

    @patch("wealthkeeper.copilot._build_wallet_snapshot")
    @patch("wealthkeeper.copilot.screen_stocks")
    @patch("wealthkeeper.copilot._resolve_target_date")
    @patch("wealthkeeper.copilot._current_market_date")
    def test_copilot_splits_hold_sell_and_buy_candidates(
        self,
        mock_current_market_date,
        mock_resolve_target_date,
        mock_screen_stocks,
        mock_build_wallet_snapshot,
    ) -> None:
        mock_current_market_date.return_value = date(2026, 4, 11)
        mock_resolve_target_date.return_value = date(2026, 4, 11)
        mock_build_wallet_snapshot.return_value = (
            [
                {"ticker": "AAPL", "shares": 10.0, "cost_basis": 1000.0, "latest_close": 120.0, "current_value": 1200.0, "total_return_pct": 20.0},
                {"ticker": "TSLA", "shares": 5.0, "cost_basis": 900.0, "latest_close": 120.0, "current_value": 600.0, "total_return_pct": -33.3},
            ],
            {"cash_balance": 500.0, "position_count": 2},
        )
        mock_screen_stocks.side_effect = [
            pd.DataFrame(
                [
                    {"代码": "AAPL", "总分": 88.0, "评级": "A"},
                    {"代码": "TSLA", "总分": 55.0, "评级": "D"},
                ]
            ),
            pd.DataFrame(
                [
                    {"代码": "AAPL", "总分": 88.0, "评级": "A", "价格": 120.0, "安全边际%": 15.0},
                    {"代码": "MSFT", "总分": 84.0, "评级": "A", "价格": 100.0, "安全边际%": 20.0},
                    {"代码": "NVDA", "总分": 78.0, "评级": "B", "价格": 80.0, "安全边际%": 10.0},
                ]
            ),
        ]

        result = copilot.build_market_copilot("us", top_n=3)

        self.assertEqual([row["代码"] for row in result.hold_rows], ["AAPL"])
        self.assertEqual([row["代码"] for row in result.sell_rows], ["TSLA"])
        self.assertEqual([row["代码"] for row in result.buy_rows], ["MSFT", "NVDA"])
        self.assertTrue(result.rebalance_rows)

    def test_failed_buy_with_cash_in_does_not_leave_partial_deposit(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "graham.db"
            with db.get_connection(path) as conn:
                with self.assertRaisesRegex(ValueError, "钱包现金不足"):
                    db.record_trade(
                        conn,
                        market="us",
                        ticker="AAPL",
                        side="buy",
                        shares=10,
                        price=100,
                        fees=0,
                        trade_date="2026-03-27",
                        cash_in=500,
                    )

                wallet = db.load_wallet(conn, "us")
                trades = db.load_trades(conn, int(wallet["id"]))
                net_deposit = db.load_net_deposit(conn, int(wallet["id"]))

        self.assertEqual(float(wallet["cash_balance"]), 0.0)
        self.assertEqual(net_deposit, 0.0)
        self.assertEqual(trades, [])

    def test_backdated_sell_is_rejected_when_it_breaks_trade_timeline(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "graham.db"
            with db.get_connection(path) as conn:
                db.add_cash(conn, market="us", amount=10_000, trade_date="2026-03-27")
                db.record_trade(
                    conn,
                    market="us",
                    ticker="AAPL",
                    side="buy",
                    shares=10,
                    price=100,
                    fees=0,
                    trade_date="2026-03-27",
                )

                with self.assertRaisesRegex(ValueError, "交易日期不能早于已有交易记录的最新日期"):
                    db.record_trade(
                        conn,
                        market="us",
                        ticker="AAPL",
                        side="sell",
                        shares=5,
                        price=110,
                        fees=0,
                        trade_date="2026-03-26",
                    )

                positions = db.compute_wallet_positions(conn, 1)

        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0]["shares"], 10.0)

    def test_ticker_normalization_for_cn_and_hk(self) -> None:
        self.assertEqual(screener.normalize_ticker("600519", "cn"), "600519.SS")
        self.assertEqual(screener.normalize_ticker("300750", "cn"), "300750.SZ")
        self.assertEqual(screener.normalize_ticker("600519.SH", "cn"), "600519.SS")
        self.assertEqual(screener.normalize_ticker("700", "hk"), "0700.HK")
        self.assertEqual(screener.normalize_ticker("5.HK", "hk"), "0005.HK")
        self.assertEqual(screener.normalize_ticker("09988", "hk"), "9988.HK")
        self.assertEqual(screener.normalize_ticker("09618.HK", "hk"), "9618.HK")

    def test_hk_normalization_deduplicates_leading_zero_aliases(self) -> None:
        self.assertEqual(
            screener.normalize_tickers(["9988", "09988", "0700"], "hk"),
            ["9988.HK", "0700.HK"],
        )

    def test_score_normalization_uses_available_weights_only(self) -> None:
        profile = MARKET_PROFILES["us"]
        analysis = StockAnalysis(
            ticker="TEST",
            market="us",
            current_price=10.0,
            pe_ratio=10.0,
            pb_ratio=1.0,
            roe=0.20,
        )

        scored = score_stock(analysis, profile)
        available_scores = {
            key: value for key, value in scored.scores.items() if value is not None
        }
        expected_weight = sum(profile.score_weights[key] for key in available_scores)
        expected_total = round(
            sum(available_scores[key] * profile.score_weights[key] for key in available_scores)
            / expected_weight,
            1,
        )

        self.assertAlmostEqual(scored.coverage_ratio, round(expected_weight, 4))
        self.assertEqual(scored.total_score, expected_total)

    def test_earnings_stability_uses_observed_window_cap(self) -> None:
        profile = MARKET_PROFILES["us"]

        score = model._score_earnings_stability(4, profile, observed_window=4)

        self.assertEqual(score, 70)

    @patch("wealthkeeper.screener._candidate_details_from_profile")
    def test_explain_candidates_returns_profile_details(self, mock_candidate_details_from_profile) -> None:
        profile = MARKET_PROFILES["us"]
        details = [
            {"symbol": "AAPL", "snapshot": {}, "rank_score": 0.8, "coverage": 1.0, "explanation": "a"},
            {"symbol": "MSFT", "snapshot": {}, "rank_score": 0.7, "coverage": 1.0, "explanation": "b"},
        ]
        mock_candidate_details_from_profile.return_value = details

        self.assertEqual(screener.explain_candidates(profile), details)

    def test_score_stock_total_is_finite_even_with_nan_inputs(self) -> None:
        profile = MARKET_PROFILES["us"]
        analysis = StockAnalysis(
            ticker="TEST",
            market="us",
            current_price=10.0,
            pe_ratio=float("nan"),
            pb_ratio=1.0,
            roe=0.18,
            net_margin=0.12,
        )

        scored = score_stock(analysis, profile)

        self.assertTrue(math.isfinite(scored.total_score))

    def test_fallback_explanation_mentions_observation_state(self) -> None:
        text = screener._fallback_explanation("AAPL", "本地可投资股票池")
        self.assertIn("后续新数据补齐", text)
        self.assertIn("不适合现在下结论", screener._fallback_explanation(
            "AAPL",
            "本地可投资股票池",
            {"snapshot_at": "2026-01-01T00:00:00+00:00"},
        ))

    def test_data_source_is_formatted_in_chinese(self) -> None:
        analysis = StockAnalysis(ticker="AAPL", market="us", data_source="snapshot_fallback", snapshot_stale=True)
        self.assertEqual(model._format_data_source(analysis), "旧快照回退")

    def test_snapshot_status_is_formatted_in_chinese(self) -> None:
        analysis = StockAnalysis(ticker="AAPL", market="us", snapshot_stale=True, snapshot_age_days=120)
        self.assertEqual(model._format_snapshot_status(analysis), "旧快照（120天）")

    @patch("wealthkeeper.model.explain_candidates")
    @patch("wealthkeeper.model.fetch_stock_data")
    def test_us_watchlist_path_skips_auto_discovery(self, mock_fetch_stock_data, mock_explain_candidates) -> None:
        mock_explain_candidates.side_effect = AssertionError("watchlist path should not discover candidates")

        def fake_fetch_stock_data(ticker: str, profile, snapshot=None) -> StockAnalysis:
            return StockAnalysis(
                ticker=ticker,
                market=profile.code,
                company_name=ticker,
                current_price=10.0,
                pe_ratio=10.0,
                pb_ratio=1.0,
                roe=0.18,
                net_margin=0.12,
            )

        mock_fetch_stock_data.side_effect = fake_fetch_stock_data

        df = model.screen_stocks(auto_discover=False, market="us", show_progress=False)
        expected = set(screener.normalize_tickers(MARKET_PROFILES["us"].fallback_watchlist, "us"))

        self.assertFalse(df.empty)
        self.assertEqual(set(df["代码"].tolist()), expected)
        mock_explain_candidates.assert_not_called()

    @patch("wealthkeeper.model.logger.warning")
    @patch("wealthkeeper.model._get_info_with_retry")
    @patch("wealthkeeper.model.yf.Ticker")
    def test_fetch_stock_data_uses_snapshot_when_info_is_rate_limited(self, mock_ticker, mock_get_info_with_retry, mock_warning) -> None:
        profile = MARKET_PROFILES["us"]
        mock_get_info_with_retry.side_effect = RuntimeError("Too Many Requests. Rate limited. Try after a while.")
        mock_ticker.return_value = SimpleNamespace()

        analysis = model.fetch_stock_data(
            "AAPL",
            profile,
            snapshot={"price": 100.0, "pe": 10.0, "pb": 1.2, "roe": 0.18, "net_margin": 0.12},
        )

        self.assertEqual(analysis.error, "")
        self.assertEqual(analysis.current_price, 100.0)
        self.assertEqual(analysis.pe_ratio, 10.0)
        self.assertEqual(analysis.pb_ratio, 1.2)
        self.assertEqual(analysis.roe, 0.18)
        self.assertEqual(analysis.net_margin, 0.12)
        mock_warning.assert_called_once()

    def test_stale_snapshot_fallback_caps_grade_and_marks_source(self) -> None:
        profile = MARKET_PROFILES["us"]
        analysis = StockAnalysis(
            ticker="TEST",
            market="us",
            current_price=10.0,
            pe_ratio=8.0,
            pb_ratio=1.0,
            roe=0.2,
            net_margin=0.15,
            data_source="snapshot_fallback",
            snapshot_stale=True,
            snapshot_age_days=120,
        )

        scored = score_stock(analysis, profile)

        self.assertEqual(scored.data_source, "snapshot_fallback")
        self.assertEqual(scored.snapshot_age_days, 120)
        self.assertIn(scored.grade, {"C", "D", "F"})

    def test_graham_core_failures_cap_grade(self) -> None:
        profile = MARKET_PROFILES["us"]
        analysis = StockAnalysis(
            ticker="TEST",
            market="us",
            sector="Consumer Cyclical",
            current_price=10.0,
            pe_ratio=8.0,
            pb_ratio=1.0,
            current_ratio=1.0,
            debt_to_equity=0.2,
            profitable_years=3,
            profitable_years_observed=4,
            dividend_years=2,
            earnings_growth=0.8,
            revenue_growth=0.3,
            roe=0.25,
            net_margin=0.2,
            fcf_yield=0.08,
        )

        scored = score_stock(analysis, profile)

        self.assertEqual(scored.grade, "B")

    def test_financial_sector_skips_current_ratio_grade_cap(self) -> None:
        profile = MARKET_PROFILES["us"]
        analysis = StockAnalysis(
            ticker="BANK",
            market="us",
            sector="Financial Services",
            current_price=10.0,
            pe_ratio=8.0,
            pb_ratio=1.0,
            current_ratio=0.2,
            debt_to_equity=0.2,
            profitable_years=4,
            profitable_years_observed=4,
            dividend_years=10,
            earnings_growth=0.6,
            revenue_growth=0.2,
            roe=0.25,
            net_margin=0.2,
            fcf_yield=0.08,
        )

        scored = score_stock(analysis, profile)

        self.assertEqual(scored.grade, "B")

    def test_low_coverage_a_grade_is_capped_to_b(self) -> None:
        profile = MARKET_PROFILES["us"]
        analysis = StockAnalysis(
            ticker="PARTIAL",
            market="us",
            current_price=10.0,
            pe_ratio=8.0,
            pb_ratio=1.0,
            roe=0.25,
            net_margin=0.2,
        )

        scored = score_stock(analysis, profile)

        self.assertEqual(scored.grade, "C")

    def test_very_low_coverage_caps_grade_to_c(self) -> None:
        profile = MARKET_PROFILES["us"]
        analysis = StockAnalysis(
            ticker="SPARSE",
            market="us",
            current_price=10.0,
            pe_ratio=8.0,
            pb_ratio=1.0,
        )

        scored = score_stock(analysis, profile)

        self.assertEqual(scored.grade, "C")

    def test_a_grade_requires_clean_graham_profile(self) -> None:
        profile = MARKET_PROFILES["us"]
        analysis = StockAnalysis(
            ticker="ALMOST_A",
            market="us",
            sector="Healthcare",
            current_price=10.0,
            pe_ratio=8.0,
            pb_ratio=1.0,
            current_ratio=2.5,
            debt_to_equity=0.2,
            profitable_years=4,
            profitable_years_observed=4,
            dividend_years=2,
            earnings_growth=0.5,
            revenue_growth=0.3,
            roe=0.25,
            net_margin=0.2,
            fcf_yield=0.08,
        )

        scored = score_stock(analysis, profile)

        self.assertEqual(scored.grade, "B")

    @patch("wealthkeeper.model.explain_candidates")
    @patch("wealthkeeper.model.fetch_stock_data")
    def test_model_limits_deep_analysis_to_top_screener_candidates(self, mock_fetch_stock_data, mock_explain_candidates) -> None:
        profile = MARKET_PROFILES["us"]
        mock_explain_candidates.return_value = [
            {"symbol": f"T{i}", "snapshot": {"price": 10.0, "pe": 10.0, "pb": 1.0, "roe": 0.2, "net_margin": 0.1}}
            for i in range(50)
        ]

        def fake_fetch_stock_data(ticker: str, profile, snapshot=None) -> StockAnalysis:
            return StockAnalysis(
                ticker=ticker,
                market=profile.code,
                company_name=ticker,
                current_price=10.0,
                pe_ratio=10.0,
                pb_ratio=1.0,
                roe=0.18,
                net_margin=0.12,
            )

        mock_fetch_stock_data.side_effect = fake_fetch_stock_data

        df = model.screen_stocks(auto_discover=True, market="us", show_progress=False)

        self.assertEqual(len(df), int(profile.screener_config["model_top_n"]))
        self.assertEqual(mock_fetch_stock_data.call_count, int(profile.screener_config["model_top_n"]))

    @patch("wealthkeeper.screener._build_candidate_details")
    def test_explain_candidates_passes_refresh_limit(self, mock_build_candidate_details) -> None:
        profile = MARKET_PROFILES["us"]
        mock_build_candidate_details.return_value = []

        screener.explain_candidates(profile, refresh_limit=7)

        self.assertEqual(mock_build_candidate_details.call_args.kwargs["refresh_limit"], 7)

    def test_portfolio_snapshot_rows_include_model_and_watchlist_groups(self) -> None:
        profile = MARKET_PROFILES["us"]
        df = pd.DataFrame(
            [
                {"代码": "AAA", "总分": 91.2, "评级": "A", "数据来源": "实时抓取", "快照状态": "实时数据"},
                {"代码": "BBB", "总分": 88.6, "评级": "A", "数据来源": "实时抓取", "快照状态": "新快照"},
            ]
        )

        rows = model._portfolio_snapshot_rows(df, profile, top_n=2)

        self.assertEqual(len(rows), 4)
        self.assertEqual(rows[0]["group_name"], "model_top2")
        self.assertEqual(rows[2]["group_name"], "watchlist_top2")
        self.assertEqual(rows[0]["ticker"], "AAA")
        self.assertEqual(rows[2]["ticker"], screener.normalize_tickers(profile.fallback_watchlist, "us")[0])

    def test_portfolio_snapshot_rows_reweight_when_selected_count_is_below_top_n(self) -> None:
        profile = MARKET_PROFILES["us"]
        df = pd.DataFrame(
            [
                {"代码": "AAA", "总分": 91.2, "评级": "A", "数据来源": "实时抓取", "快照状态": "实时数据"},
                {"代码": "BBB", "总分": 88.6, "评级": "A", "数据来源": "实时抓取", "快照状态": "新快照"},
            ]
        )

        rows = model._portfolio_snapshot_rows(df, profile, top_n=4)
        model_rows = [row for row in rows if row["group_name"] == "model_top4"]
        watchlist_rows = [row for row in rows if row["group_name"] == "watchlist_top4"]

        self.assertEqual(len(model_rows), 2)
        self.assertEqual(len(watchlist_rows), 2)
        self.assertEqual(sum(row["weight"] for row in model_rows), 1.0)
        self.assertEqual(sum(row["weight"] for row in watchlist_rows), 1.0)

    @patch("wealthkeeper.model._current_market_date")
    def test_portfolio_run_date_uses_market_local_date(self, mock_current_market_date) -> None:
        mock_current_market_date.return_value = date(2026, 3, 26)

        self.assertEqual(model._portfolio_run_date("us"), "2026-03-26")
        mock_current_market_date.assert_called_once_with("us")

    @patch(
        "wealthkeeper.monitor.CALENDARS",
        {"us": SimpleNamespace(first_session=pd.Timestamp("2006-01-01"), last_session=pd.Timestamp("2025-12-31"))},
    )
    def test_resolve_target_date_uses_requested_date_when_calendar_range_is_outdated(self) -> None:
        self.assertEqual(
            monitor._resolve_target_date("us", date(2026, 3, 26), date_explicit=False),
            date(2026, 3, 26),
        )

    @patch("wealthkeeper.monitor.datetime")
    def test_monitor_current_market_date_uses_market_timezone(self, mock_datetime) -> None:
        mock_datetime.now.return_value = datetime(2026, 3, 26, 10, 0, 0)
        self.assertEqual(monitor._current_market_date("us"), date(2026, 3, 26))

    @patch("wealthkeeper.cli._current_market_date")
    def test_trade_date_defaults_to_market_local_date(self, mock_current_market_date) -> None:
        mock_current_market_date.return_value = date(2026, 3, 26)

        self.assertEqual(cli._trade_date_str("us"), "2026-03-26")
        mock_current_market_date.assert_called_once_with("us")

    @patch("wealthkeeper.cli.yf.Ticker")
    def test_trade_price_prefers_trade_date_history(self, mock_ticker) -> None:
        mock_ticker.return_value.history.return_value = pd.DataFrame(
            {"Close": [123.45]},
            index=pd.to_datetime(["2026-03-20"]),
        )

        price = cli._resolve_trade_price("us", "AAPL", None, "2026-03-20")

        self.assertEqual(price, 123.45)

    @patch("wealthkeeper.cli.fetch_stock_data")
    @patch("wealthkeeper.cli._current_market_date")
    @patch("wealthkeeper.cli.yf.Ticker")
    def test_trade_price_allows_live_fallback_only_for_market_today(
        self,
        mock_ticker,
        mock_current_market_date,
        mock_fetch_stock_data,
    ) -> None:
        mock_ticker.return_value.history.return_value = pd.DataFrame({"Close": []})
        mock_current_market_date.return_value = date(2026, 3, 26)
        mock_fetch_stock_data.return_value = StockAnalysis(ticker="AAPL", market="us", current_price=130.0)

        price = cli._resolve_trade_price("us", "AAPL", None, "2026-03-26")

        self.assertEqual(price, 130.0)

    @patch("wealthkeeper.cli.fetch_stock_data")
    @patch("wealthkeeper.cli._current_market_date")
    @patch("wealthkeeper.cli.yf.Ticker")
    def test_trade_price_rejects_live_fallback_for_past_dates(
        self,
        mock_ticker,
        mock_current_market_date,
        mock_fetch_stock_data,
    ) -> None:
        mock_ticker.return_value.history.return_value = pd.DataFrame({"Close": []})
        mock_current_market_date.return_value = date(2026, 3, 26)
        mock_fetch_stock_data.return_value = StockAnalysis(ticker="AAPL", market="us", current_price=130.0)

        with self.assertRaisesRegex(ValueError, "附近价格"):
            cli._resolve_trade_price("us", "AAPL", None, "2026-03-20")

    @patch("wealthkeeper.monitor._price_history")
    def test_wallet_snapshot_respects_as_of_date_for_trades_and_cash(self, mock_price_history) -> None:
        mock_price_history.return_value = pd.Series(
            [100.0],
            index=pd.to_datetime(["2026-03-20"]),
        )

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "graham.db"
            original_get_connection = db.get_connection
            with patch("wealthkeeper.monitor.db.get_connection", side_effect=lambda *args, **kwargs: original_get_connection(path)):
                with db.get_connection(path) as conn:
                    db.add_cash(conn, market="us", amount=10_000, trade_date="2026-03-20")
                    db.record_trade(
                        conn,
                        market="us",
                        ticker="AAPL",
                        side="buy",
                        shares=10,
                        price=100,
                        fees=0,
                        trade_date="2026-03-20",
                    )
                    db.add_cash(conn, market="us", amount=5_000, trade_date="2026-03-21")
                    db.record_trade(
                        conn,
                        market="us",
                        ticker="AAPL",
                        side="sell",
                        shares=10,
                        price=110,
                        fees=0,
                        trade_date="2026-03-21",
                    )

                positions, wallet = monitor._build_wallet_snapshot("us", "USD", date(2026, 3, 20))

        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0]["ticker"], "AAPL")
        self.assertEqual(wallet["initial_capital"], 10000.0)
        self.assertEqual(wallet["cash_balance"], 9000.0)
        self.assertEqual(wallet["position_count"], 1)

    def test_build_wallet_snapshot_does_not_create_empty_wallet(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "graham.db"
            original_get_connection = db.get_connection
            with patch("wealthkeeper.monitor.db.get_connection", side_effect=lambda *args, **kwargs: original_get_connection(path)):
                positions, wallet = monitor._build_wallet_snapshot("us", "USD", date(2026, 3, 20))

            with db.get_connection(path) as conn:
                self.assertIsNone(db.load_wallet_optional(conn, "us"))

        self.assertEqual(positions, [])
        self.assertIsNone(wallet)

    @patch("wealthkeeper.monitor._print_wallet_report")
    @patch("wealthkeeper.monitor._price_history")
    def test_run_monitor_only_reports_wallet_positions(
        self,
        mock_price_history,
        mock_print_wallet_report,
    ) -> None:
        mock_price_history.return_value = pd.Series(
            [100.0, 100.0],
            index=pd.to_datetime(["2026-03-20", "2026-03-25"]),
        )

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "graham.db"
            with db.get_connection(path) as conn:
                db.add_cash(conn, market="us", amount=1_000, trade_date="2026-03-20")
                db.record_trade(
                    conn,
                    market="us",
                    ticker="AAPL",
                    side="buy",
                    shares=10,
                    price=100,
                    fees=0,
                    trade_date="2026-03-20",
                )

            original_get_connection = db.get_connection
            with patch("wealthkeeper.monitor.db.get_connection", side_effect=lambda *args, **kwargs: original_get_connection(path)):
                monitor.run_monitor("us", date(2026, 3, 25), date_explicit=True)

        wallet_rows, wallet_positions = mock_print_wallet_report.call_args.args
        self.assertEqual(len(wallet_rows), 1)
        self.assertEqual(wallet_rows[0]["initial_capital"], 1000.0)
        self.assertEqual(len(wallet_positions), 1)
        self.assertEqual(wallet_positions[0]["ticker"], "AAPL")


if __name__ == "__main__":
    unittest.main()
