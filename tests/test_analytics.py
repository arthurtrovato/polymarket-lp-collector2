from __future__ import annotations

import gzip
import json
import tempfile
import unittest
from pathlib import Path

try:
    import pyarrow.parquet as pq

    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

from polymarket_collector.backtest import (
    BacktestConfig,
    reward_order_score,
    reward_two_sided_proxy,
    run_backtest,
)
from polymarket_collector.etl import convert
from polymarket_collector.orderbook import OrderBook, reconstruct


def ws(received_at_ns: int, payload: dict[str, object]) -> dict[str, object]:
    return {
        "record_type": "market_ws",
        "received_at_ns": received_at_ns,
        "connection_id": "test-connection",
        "payload": payload,
    }


@unittest.skipUnless(HAS_PYARROW, "pyarrow analytics extra is not installed")
class AnalyticsPipelineTests(unittest.TestCase):
    def _write_fixture(self, path: Path) -> None:
        records = [
            {
                "record_type": "rewarded_market_discovery",
                "received_at_ns": 900_000_000,
                "market_count": 1,
                "asset_count": 2,
                "markets": [
                    {
                        "condition_id": "market-1",
                        "market_id": "1",
                        "question": "Test market?",
                        "collector_daily_reward": 100,
                        "rewards_max_spread": 3,
                        "rewards_min_size": 5,
                        "collector_volume_24hr": 1_000,
                        "tokens": [
                            {"token_id": "A", "outcome": "Yes", "price": 0.5},
                            {"token_id": "B", "outcome": "No", "price": 0.5},
                        ],
                    }
                ],
            },
            ws(
                1_000_000_000,
                {
                    "event_type": "book",
                    "timestamp": "1000",
                    "market": "market-1",
                    "asset_id": "A",
                    "tick_size": "0.01",
                    "last_trade_price": "0.50",
                    "hash": "book-1",
                    "bids": [{"price": "0.49", "size": "20"}],
                    "asks": [{"price": "0.51", "size": "20"}],
                },
            ),
            ws(
                1_200_000_000,
                {
                    "event_type": "price_change",
                    "timestamp": "1200",
                    "market": "market-1",
                    "price_changes": [
                        {
                            "asset_id": "A",
                            "side": "SELL",
                            "price": "0.51",
                            "size": "15",
                            "best_bid": "0.49",
                            "best_ask": "0.51",
                        },
                        {
                            "asset_id": "A",
                            "side": "BUY",
                            "price": "0.48",
                            "size": "5",
                            "best_bid": "0.49",
                            "best_ask": "0.51",
                        }
                    ],
                },
            ),
            ws(
                1_500_000_000,
                {
                    "event_type": "last_trade_price",
                    "timestamp": "1500",
                    "market": "market-1",
                    "asset_id": "A",
                    "side": "BUY",
                    "price": "0.51",
                    "size": "25",
                    "fee_rate_bps": "0",
                    "transaction_hash": "tx-1",
                },
            ),
            ws(
                2_000_000_000,
                {
                    "event_type": "last_trade_price",
                    "timestamp": "2000",
                    "market": "market-1",
                    "asset_id": "A",
                    "side": "SELL",
                    "price": "0.49",
                    "size": "25",
                    "fee_rate_bps": "0",
                    "transaction_hash": "tx-2",
                },
            ),
        ]
        with gzip.open(path, "wt", encoding="utf-8") as target:
            for record in records:
                target.write(json.dumps(record) + "\n")
            target.write("{invalid json\n")

    def test_end_to_end_pipeline_and_conservative_fills(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "sample.jsonl.gz"
            normalized = root / "normalized"
            self._write_fixture(source)
            report = convert([source], normalized, batch_size=2)

            self.assertEqual(report["raw_rows"], 6)
            self.assertEqual(report["invalid_json_rows"], 1)
            self.assertEqual(report["normalized_events"], 6)
            self.assertEqual(report["book_level_rows"], 2)
            self.assertEqual(report["market_token_rows"], 2)
            self.assertFalse(
                (normalized / ".quality-fingerprints.sqlite3.tmp").exists()
            )
            events = pq.read_table(normalized / "events.parquet")
            self.assertEqual(events.num_rows, 6)

            books_path = root / "books.parquet"
            reconstruction = reconstruct(
                normalized / "events.parquet",
                normalized / "book_levels.parquet",
                books_path,
                snapshot_interval_ms=500,
            )
            self.assertEqual(reconstruction["assets_initialized"], 1)
            self.assertEqual(reconstruction["counters"]["price_changes"], 2)
            self.assertGreaterEqual(reconstruction["output_rows"], 2)

            summary = run_backtest(
                normalized / "events.parquet",
                normalized / "book_levels.parquet",
                root / "backtest",
                asset_id="A",
                config=BacktestConfig(
                    order_size=5,
                    initial_cash=100,
                    initial_inventory=10,
                    min_inventory=0,
                    max_inventory=20,
                    refresh_ms=10_000,
                    latency_ms=0,
                    queue_ahead_fraction=0,
                    markout_ms=100,
                    equity_interval_ms=100,
                    reward_max_spread_cents=3,
                    reward_min_size=5,
                ),
            )
            self.assertEqual(summary["fills"], 2)
            self.assertAlmostEqual(summary["gross_trading_pnl"], 0.10, places=8)
            self.assertAlmostEqual(
                summary["lp_excess_pnl_vs_hold"],
                0.10,
                places=8,
            )
            self.assertEqual(summary["maker_fee_paid"], 0)
            self.assertEqual(summary["resolved_markouts"], 1)
            self.assertEqual(summary["unresolved_markouts"], 1)
            self.assertTrue((root / "backtest" / "fills.parquet").exists())
            self.assertTrue((root / "backtest" / "summary.json").exists())

    def test_reward_scoring_is_bounded_and_rewards_two_sided_quotes(self) -> None:
        self.assertAlmostEqual(
            reward_order_score(
                max_spread_cents=3,
                distance_cents=1,
                size=100,
            ),
            100 * (2 / 3) ** 2,
        )
        self.assertEqual(
            reward_order_score(
                max_spread_cents=3,
                distance_cents=4,
                size=100,
            ),
            0,
        )
        two_sided = reward_two_sided_proxy(
            mid=0.5,
            bid_price=0.49,
            ask_price=0.51,
            size=100,
            max_spread_cents=3,
            min_size=50,
        )
        single_sided = reward_two_sided_proxy(
            mid=0.5,
            bid_price=0.49,
            ask_price=None,
            size=100,
            max_spread_cents=3,
            min_size=50,
        )
        self.assertGreater(two_sided, single_sided)


class OrderBookTests(unittest.TestCase):
    def test_snapshot_change_delete_and_metrics(self) -> None:
        book = OrderBook("A")
        book.load_snapshot(
            [
                {"side": "BUY", "price": 0.48, "size": 10},
                {"side": "BUY", "price": 0.49, "size": 20},
                {"side": "SELL", "price": 0.51, "size": 30},
                {"side": "SELL", "price": 0.52, "size": 40},
            ]
        )
        self.assertEqual(book.best_bid, 0.49)
        self.assertEqual(book.best_ask, 0.51)
        self.assertAlmostEqual(book.mid or 0, 0.5)
        self.assertEqual(book.depth("BUY", 1), 20)
        book.apply_change("BUY", 0.49, 0)
        self.assertEqual(book.best_bid, 0.48)
        book.apply_change("SELL", 0.50, 5)
        self.assertEqual(book.best_ask, 0.50)
        self.assertFalse(book.crossed)

    def test_authoritative_top_prunes_stale_levels(self) -> None:
        book = OrderBook("A")
        book.load_snapshot(
            [
                {"side": "BUY", "price": 0.48, "size": 10},
                {"side": "BUY", "price": 0.49, "size": 20},
                {"side": "SELL", "price": 0.51, "size": 30},
                {"side": "SELL", "price": 0.52, "size": 40},
            ]
        )
        removed = book.reconcile_top(best_bid=0.48, best_ask=0.52)
        self.assertEqual(removed, 2)
        self.assertEqual(book.best_bid, 0.48)
        self.assertEqual(book.best_ask, 0.52)


if __name__ == "__main__":
    unittest.main()
