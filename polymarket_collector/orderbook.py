from __future__ import annotations

import argparse
import json
import logging
import math
import time
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .analytics_common import ParquetSink, iter_parquet_rows, require_pyarrow


LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class OrderBook:
    asset_id: str
    market: str = ""
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)
    tick_size: float | None = None
    last_trade_price: float | None = None
    initialized: bool = False

    def load_snapshot(self, levels: list[dict[str, Any]]) -> None:
        self.bids.clear()
        self.asks.clear()
        for level in levels:
            price = float(level["price"])
            size = float(level["size"])
            if not math.isfinite(price) or not math.isfinite(size) or size <= 0:
                continue
            target = self.bids if level["side"] == "BUY" else self.asks
            target[price] = size
        self.initialized = True

    def apply_change(self, side: str, price: float, size: float) -> None:
        if side not in {"BUY", "SELL"}:
            raise ValueError(f"Unknown side {side!r}")
        if not math.isfinite(price) or not math.isfinite(size) or size < 0:
            raise ValueError("Invalid price change")
        target = self.bids if side == "BUY" else self.asks
        if size == 0:
            target.pop(price, None)
        else:
            target[price] = size

    def reconcile_top(
        self,
        *,
        best_bid: float | None,
        best_ask: float | None,
        tolerance: float = 1e-12,
    ) -> int:
        """Remove levels contradicted by an authoritative top-of-book update."""
        removed = 0
        if best_bid is not None and math.isfinite(best_bid):
            stale_bids = [
                price for price in self.bids if price > best_bid + tolerance
            ]
            for price in stale_bids:
                del self.bids[price]
            removed += len(stale_bids)
        if best_ask is not None and math.isfinite(best_ask):
            stale_asks = [
                price for price in self.asks if price < best_ask - tolerance
            ]
            for price in stale_asks:
                del self.asks[price]
            removed += len(stale_asks)
        return removed

    @property
    def best_bid(self) -> float | None:
        return max(self.bids, default=None)

    @property
    def best_ask(self) -> float | None:
        return min(self.asks, default=None)

    @property
    def mid(self) -> float | None:
        bid, ask = self.best_bid, self.best_ask
        if bid is None or ask is None:
            return None
        return (bid + ask) / 2

    @property
    def spread(self) -> float | None:
        bid, ask = self.best_bid, self.best_ask
        if bid is None or ask is None:
            return None
        return ask - bid

    @property
    def crossed(self) -> bool:
        bid, ask = self.best_bid, self.best_ask
        return bid is not None and ask is not None and bid >= ask

    def top_levels(self, side: str, depth: int) -> list[tuple[float, float]]:
        source = self.bids if side == "BUY" else self.asks
        return sorted(
            source.items(),
            key=lambda item: item[0],
            reverse=side == "BUY",
        )[:depth]

    def depth(self, side: str, levels: int) -> float:
        return sum(size for _, size in self.top_levels(side, levels))

    def level_size(self, side: str, price: float) -> float:
        source = self.bids if side == "BUY" else self.asks
        return source.get(price, 0.0)


class LevelGroups:
    def __init__(self, path: str | Path) -> None:
        self.rows: Iterator[dict[str, Any]] = iter_parquet_rows(
            path,
            columns=[
                "sequence",
                "side",
                "price",
                "size",
            ],
        )
        self.current: dict[str, Any] | None = next(self.rows, None)

    def for_sequence(self, sequence: int) -> list[dict[str, Any]]:
        while self.current is not None and self.current["sequence"] < sequence:
            self._discard_group(self.current["sequence"])
        if self.current is None or self.current["sequence"] != sequence:
            return []
        result: list[dict[str, Any]] = []
        while self.current is not None and self.current["sequence"] == sequence:
            result.append(self.current)
            self.current = next(self.rows, None)
        return result

    def _discard_group(self, sequence: int) -> None:
        while self.current is not None and self.current["sequence"] == sequence:
            self.current = next(self.rows, None)


def _snapshot_schema() -> Any:
    pa, _ = require_pyarrow()
    return pa.schema(
        [
            ("sequence", pa.int64()),
            ("received_at_ns", pa.int64()),
            ("exchange_timestamp_ms", pa.int64()),
            ("connection_id", pa.string()),
            ("market", pa.string()),
            ("asset_id", pa.string()),
            ("reason", pa.string()),
            ("tick_size", pa.float64()),
            ("last_trade_price", pa.float64()),
            ("best_bid", pa.float64()),
            ("best_ask", pa.float64()),
            ("mid", pa.float64()),
            ("spread", pa.float64()),
            ("bid_depth_1", pa.float64()),
            ("bid_depth_5", pa.float64()),
            ("bid_depth_10", pa.float64()),
            ("ask_depth_1", pa.float64()),
            ("ask_depth_5", pa.float64()),
            ("ask_depth_10", pa.float64()),
            ("imbalance_5", pa.float64()),
            ("bids_json", pa.string()),
            ("asks_json", pa.string()),
            ("crossed", pa.bool_()),
        ]
    )


def snapshot_row(
    book: OrderBook,
    event: dict[str, Any],
    *,
    reason: str,
    depth: int,
) -> dict[str, Any]:
    bid_depth_5 = book.depth("BUY", 5)
    ask_depth_5 = book.depth("SELL", 5)
    denominator = bid_depth_5 + ask_depth_5
    imbalance = (
        (bid_depth_5 - ask_depth_5) / denominator if denominator > 0 else None
    )
    return {
        "sequence": event["sequence"],
        "received_at_ns": event.get("received_at_ns"),
        "exchange_timestamp_ms": event.get("exchange_timestamp_ms"),
        "connection_id": str(event.get("connection_id") or ""),
        "market": book.market,
        "asset_id": book.asset_id,
        "reason": reason,
        "tick_size": book.tick_size,
        "last_trade_price": book.last_trade_price,
        "best_bid": book.best_bid,
        "best_ask": book.best_ask,
        "mid": book.mid,
        "spread": book.spread,
        "bid_depth_1": book.depth("BUY", 1),
        "bid_depth_5": bid_depth_5,
        "bid_depth_10": book.depth("BUY", 10),
        "ask_depth_1": book.depth("SELL", 1),
        "ask_depth_5": ask_depth_5,
        "ask_depth_10": book.depth("SELL", 10),
        "imbalance_5": imbalance,
        "bids_json": json.dumps(book.top_levels("BUY", depth), separators=(",", ":")),
        "asks_json": json.dumps(book.top_levels("SELL", depth), separators=(",", ":")),
        "crossed": book.crossed,
    }


def reconstruct(
    events_path: str | Path,
    levels_path: str | Path,
    output_path: str | Path,
    *,
    snapshot_interval_ms: int = 1_000,
    depth: int = 20,
    gap_threshold_ms: int = 120_000,
    batch_size: int = 50_000,
) -> dict[str, Any]:
    if snapshot_interval_ms < 0 or depth < 1 or gap_threshold_ms < 1:
        raise ValueError("Invalid reconstruction settings")
    started_at = time.time()
    groups = LevelGroups(levels_path)
    books: dict[str, OrderBook] = {}
    last_emit: dict[str, int] = {}
    last_event: dict[str, int] = {}
    gaps_by_asset: dict[str, int] = defaultdict(int)
    max_gap_by_asset: dict[str, int] = defaultdict(int)
    counters: dict[str, int] = defaultdict(int)
    output = Path(output_path).expanduser().resolve()
    sink = ParquetSink(output, _snapshot_schema(), batch_size=batch_size)
    event_columns = [
        "sequence",
        "received_at_ns",
        "connection_id",
        "event_type",
        "exchange_timestamp_ms",
        "market",
        "asset_id",
        "change_index",
        "change_count",
        "side",
        "price",
        "size",
        "best_bid",
        "best_ask",
        "tick_size",
        "last_trade_price",
    ]
    try:
        for event in iter_parquet_rows(events_path, columns=event_columns):
            counters["events_seen"] += 1
            asset_id = str(event.get("asset_id") or "")
            event_type = str(event.get("event_type") or "")
            if not asset_id:
                counters["events_without_asset"] += 1
                continue
            book = books.setdefault(asset_id, OrderBook(asset_id=asset_id))
            if event.get("market"):
                book.market = str(event["market"])
            timestamp = event.get("exchange_timestamp_ms")
            if timestamp is not None:
                previous = last_event.get(asset_id)
                if previous is not None:
                    gap = int(timestamp) - previous
                    if gap > gap_threshold_ms:
                        gaps_by_asset[asset_id] += 1
                        max_gap_by_asset[asset_id] = max(
                            max_gap_by_asset[asset_id],
                            gap,
                        )
                last_event[asset_id] = max(int(timestamp), previous or int(timestamp))

            reason: str | None = None
            if event_type == "book":
                book.load_snapshot(groups.for_sequence(int(event["sequence"])))
                book.tick_size = event.get("tick_size") or book.tick_size
                book.last_trade_price = (
                    event.get("last_trade_price") or book.last_trade_price
                )
                counters["full_snapshots"] += 1
                reason = "book"
            elif event_type == "price_change":
                if not book.initialized:
                    counters["changes_before_snapshot"] += 1
                    continue
                try:
                    book.apply_change(
                        str(event.get("side") or ""),
                        float(event["price"]),
                        float(event["size"]),
                    )
                    counters["price_changes"] += 1
                except (KeyError, TypeError, ValueError):
                    counters["invalid_price_changes"] += 1
                    continue
                change_index = event.get("change_index")
                change_count = event.get("change_count")
                if (
                    change_index is not None
                    and change_count is not None
                    and int(change_index) + 1 < int(change_count)
                ):
                    continue
            elif event_type == "tick_size_change":
                if event.get("tick_size") is not None:
                    book.tick_size = float(event["tick_size"])
                    counters["tick_size_changes"] += 1
            elif event_type == "last_trade_price":
                if event.get("price") is not None:
                    book.last_trade_price = float(event["price"])
                    counters["trades"] += 1

            if not book.initialized:
                continue
            observed_bid, observed_ask = event.get("best_bid"), event.get("best_ask")
            tolerance = (book.tick_size or 0.001) / 2 + 1e-12
            if observed_bid is not None or observed_ask is not None:
                removed = book.reconcile_top(
                    best_bid=(
                        float(observed_bid) if observed_bid is not None else None
                    ),
                    best_ask=(
                        float(observed_ask) if observed_ask is not None else None
                    ),
                    tolerance=tolerance,
                )
                if removed:
                    counters["levels_pruned_by_authoritative_top"] += removed
                    counters["authoritative_top_reconciliations"] += 1
            if event_type == "best_bid_ask":
                counters["top_notifications"] += 1
                if (
                    observed_bid is not None
                    and (
                        book.best_bid is None
                        or abs(float(observed_bid) - book.best_bid) > tolerance
                    )
                ):
                    counters["top_notifications_awaiting_bid_level"] += 1
                if (
                    observed_ask is not None
                    and (
                        book.best_ask is None
                        or abs(float(observed_ask) - book.best_ask) > tolerance
                    )
                ):
                    counters["top_notifications_awaiting_ask_level"] += 1
                # This notification has no level size. Polymarket normally
                # follows it with the matching price_change at the same
                # exchange timestamp; wait for that depth-bearing event before
                # validating or emitting a reconstructed snapshot.
                continue
            if (
                observed_bid is not None
                and book.best_bid is not None
                and abs(float(observed_bid) - book.best_bid) > tolerance
            ):
                counters["best_bid_mismatches"] += 1
            if (
                observed_ask is not None
                and book.best_ask is not None
                and abs(float(observed_ask) - book.best_ask) > tolerance
            ):
                counters["best_ask_mismatches"] += 1

            if reason is None and timestamp is not None:
                previous_emit = last_emit.get(asset_id)
                if (
                    previous_emit is None
                    or int(timestamp) - previous_emit >= snapshot_interval_ms
                ):
                    reason = "interval"
            if reason is not None:
                row = snapshot_row(book, event, reason=reason, depth=depth)
                sink.add(row)
                counters["snapshots_written"] += 1
                if row["crossed"]:
                    counters["crossed_snapshots"] += 1
                if timestamp is not None:
                    last_emit[asset_id] = int(timestamp)
        rows = sink.close()
    except BaseException:
        sink.abort()
        raise
    report = {
        "schema_version": 1,
        "started_at_unix": started_at,
        "finished_at_unix": time.time(),
        "events_path": str(Path(events_path).resolve()),
        "levels_path": str(Path(levels_path).resolve()),
        "output_path": str(output),
        "output_rows": rows,
        "assets_initialized": sum(book.initialized for book in books.values()),
        "counters": dict(counters),
        "gaps_over_threshold_by_asset": dict(gaps_by_asset),
        "max_gap_ms_by_asset": dict(max_gap_by_asset),
        "settings": {
            "snapshot_interval_ms": snapshot_interval_ms,
            "depth": depth,
            "gap_threshold_ms": gap_threshold_ms,
        },
    }
    report_path = output.with_suffix(".quality.json")
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return report


def cli() -> None:
    parser = argparse.ArgumentParser(
        description="Reconstruct historical Polymarket order books from Parquet events."
    )
    parser.add_argument("--events", required=True)
    parser.add_argument("--book-levels", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--snapshot-interval-ms", type=int, default=1_000)
    parser.add_argument("--depth", type=int, default=20)
    parser.add_argument("--gap-threshold-ms", type=int, default=120_000)
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    report = reconstruct(
        args.events,
        args.book_levels,
        args.output,
        snapshot_interval_ms=args.snapshot_interval_ms,
        depth=args.depth,
        gap_threshold_ms=args.gap_threshold_ms,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    cli()
