from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import time
from collections import Counter
from pathlib import Path
from typing import Any

from .analytics_common import (
    ParquetSink,
    canonical_json,
    open_jsonl,
    require_pyarrow,
    resolve_jsonl_inputs,
)


LOGGER = logging.getLogger(__name__)


def _float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _event_schema() -> Any:
    pa, _ = require_pyarrow()
    return pa.schema(
        [
            ("sequence", pa.int64()),
            ("source_file", pa.string()),
            ("source_line", pa.int64()),
            ("record_type", pa.string()),
            ("received_at_ns", pa.int64()),
            ("connection_id", pa.string()),
            ("event_type", pa.string()),
            ("exchange_timestamp_ms", pa.int64()),
            ("capture_latency_ms", pa.float64()),
            ("market", pa.string()),
            ("asset_id", pa.string()),
            ("change_index", pa.int32()),
            ("change_count", pa.int32()),
            ("side", pa.string()),
            ("price", pa.float64()),
            ("size", pa.float64()),
            ("best_bid", pa.float64()),
            ("best_ask", pa.float64()),
            ("spread", pa.float64()),
            ("tick_size", pa.float64()),
            ("last_trade_price", pa.float64()),
            ("fee_rate_bps", pa.int32()),
            ("transaction_hash", pa.string()),
            ("book_hash", pa.string()),
            ("payload_json", pa.string()),
        ]
    )


def _level_schema() -> Any:
    pa, _ = require_pyarrow()
    return pa.schema(
        [
            ("sequence", pa.int64()),
            ("received_at_ns", pa.int64()),
            ("exchange_timestamp_ms", pa.int64()),
            ("market", pa.string()),
            ("asset_id", pa.string()),
            ("side", pa.string()),
            ("price", pa.float64()),
            ("size", pa.float64()),
            ("source_rank", pa.int32()),
        ]
    )


def _market_schema() -> Any:
    pa, _ = require_pyarrow()
    return pa.schema(
        [
            ("discovery_sequence", pa.int64()),
            ("received_at_ns", pa.int64()),
            ("condition_id", pa.string()),
            ("market_id", pa.string()),
            ("market_slug", pa.string()),
            ("question", pa.string()),
            ("end_date", pa.string()),
            ("token_id", pa.string()),
            ("outcome", pa.string()),
            ("token_price", pa.float64()),
            ("daily_reward", pa.float64()),
            ("rewards_max_spread_cents", pa.float64()),
            ("rewards_min_size", pa.float64()),
            ("volume_24hr", pa.float64()),
            ("market_json", pa.string()),
        ]
    )


class Quality:
    def __init__(self, inputs: list[Path]) -> None:
        self.started_at = time.time()
        self.inputs = inputs
        self.raw_rows = 0
        self.invalid_json_rows = 0
        self.invalid_record_rows = 0
        self.normalized_events = 0
        self.level_rows = 0
        self.market_rows = 0
        self.duplicate_records = 0
        self.timestamp_regressions = 0
        self.negative_capture_latency = 0
        self.missing_required = Counter[str]()
        self.record_types = Counter[str]()
        self.event_types = Counter[str]()
        self._fingerprints: set[bytes] = set()
        self._last_exchange_by_connection: dict[str, int] = {}

    def observe_record(self, record: dict[str, Any]) -> None:
        record_type = str(record.get("record_type") or "<missing>")
        self.record_types[record_type] += 1
        encoded = canonical_json(record).encode("utf-8")
        fingerprint = hashlib.blake2b(encoded, digest_size=12).digest()
        if fingerprint in self._fingerprints:
            self.duplicate_records += 1
        else:
            self._fingerprints.add(fingerprint)

    def observe_event(self, row: dict[str, Any]) -> None:
        self.normalized_events += 1
        self.event_types[str(row.get("event_type") or "<missing>")] += 1
        connection = row.get("connection_id")
        timestamp = row.get("exchange_timestamp_ms")
        if connection and timestamp is not None:
            previous = self._last_exchange_by_connection.get(connection)
            if previous is not None and timestamp < previous:
                self.timestamp_regressions += 1
            self._last_exchange_by_connection[connection] = max(
                timestamp,
                previous if previous is not None else timestamp,
            )
        latency = row.get("capture_latency_ms")
        if latency is not None and latency < -1:
            self.negative_capture_latency += 1

    def require(self, event_type: str, field: str, value: Any) -> None:
        if value in (None, ""):
            self.missing_required[f"{event_type}.{field}"] += 1

    def report(self, outputs: dict[str, Any]) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "started_at_unix": self.started_at,
            "finished_at_unix": time.time(),
            "input_files": [str(path) for path in self.inputs],
            "input_file_count": len(self.inputs),
            "input_bytes": sum(path.stat().st_size for path in self.inputs),
            "raw_rows": self.raw_rows,
            "invalid_json_rows": self.invalid_json_rows,
            "invalid_record_rows": self.invalid_record_rows,
            "duplicate_records": self.duplicate_records,
            "normalized_events": self.normalized_events,
            "book_level_rows": self.level_rows,
            "market_token_rows": self.market_rows,
            "timestamp_regressions": self.timestamp_regressions,
            "negative_capture_latency": self.negative_capture_latency,
            "missing_required_fields": dict(self.missing_required),
            "record_types": dict(self.record_types),
            "event_types": dict(self.event_types),
            "outputs": outputs,
        }


def _base_event(
    *,
    sequence: int,
    source_file: str,
    source_line: int,
    record: dict[str, Any],
    payload: dict[str, Any],
    event_type: str,
    change_index: int | None = None,
    change_count: int | None = None,
) -> dict[str, Any]:
    received_at_ns = _int(record.get("received_at_ns"))
    timestamp = _int(payload.get("timestamp"))
    latency = None
    if received_at_ns is not None and timestamp is not None:
        latency = received_at_ns / 1_000_000 - timestamp
    return {
        "sequence": sequence,
        "source_file": source_file,
        "source_line": source_line,
        "record_type": str(record.get("record_type") or ""),
        "received_at_ns": received_at_ns,
        "connection_id": str(record.get("connection_id") or ""),
        "event_type": event_type,
        "exchange_timestamp_ms": timestamp,
        "capture_latency_ms": latency,
        "market": str(payload.get("market") or payload.get("condition_id") or ""),
        "asset_id": str(payload.get("asset_id") or ""),
        "change_index": change_index,
        "change_count": change_count,
        "side": str(payload.get("side") or "").upper(),
        "price": _float(payload.get("price")),
        "size": _float(payload.get("size")),
        "best_bid": _float(payload.get("best_bid")),
        "best_ask": _float(payload.get("best_ask")),
        "spread": _float(payload.get("spread")),
        "tick_size": _float(
            payload.get("tick_size")
            or payload.get("new_tick_size")
            or payload.get("order_price_min_tick_size")
        ),
        "last_trade_price": _float(payload.get("last_trade_price")),
        "fee_rate_bps": _int(payload.get("fee_rate_bps")),
        "transaction_hash": str(payload.get("transaction_hash") or ""),
        "book_hash": str(payload.get("hash") or ""),
        "payload_json": canonical_json(payload),
    }


def _write_market_rows(
    record: dict[str, Any],
    *,
    discovery_sequence: int,
    sink: ParquetSink,
    quality: Quality,
) -> None:
    received_at_ns = _int(record.get("received_at_ns"))
    for market in record.get("markets") or []:
        if not isinstance(market, dict):
            quality.invalid_record_rows += 1
            continue
        tokens = market.get("tokens") or []
        if not tokens:
            tokens = [{}]
        market_json = canonical_json(market)
        for token in tokens:
            if not isinstance(token, dict):
                continue
            sink.add(
                {
                    "discovery_sequence": discovery_sequence,
                    "received_at_ns": received_at_ns,
                    "condition_id": str(market.get("condition_id") or ""),
                    "market_id": str(market.get("market_id") or ""),
                    "market_slug": str(market.get("market_slug") or ""),
                    "question": str(market.get("question") or ""),
                    "end_date": str(market.get("end_date") or ""),
                    "token_id": str(token.get("token_id") or ""),
                    "outcome": str(token.get("outcome") or ""),
                    "token_price": _float(token.get("price")),
                    "daily_reward": _float(
                        market.get("collector_daily_reward")
                        or market.get("total_daily_rate")
                    ),
                    "rewards_max_spread_cents": _float(
                        market.get("rewards_max_spread")
                    ),
                    "rewards_min_size": _float(market.get("rewards_min_size")),
                    "volume_24hr": _float(
                        market.get("collector_volume_24hr")
                        or market.get("volume_24hr")
                    ),
                    "market_json": market_json,
                }
            )
            quality.market_rows += 1


def convert(
    inputs: list[str | Path],
    output_dir: str | Path,
    *,
    batch_size: int = 50_000,
) -> dict[str, Any]:
    paths = resolve_jsonl_inputs(inputs)
    if not paths:
        raise ValueError("No input files")
    output = Path(output_dir).expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    events_path = output / "events.parquet"
    levels_path = output / "book_levels.parquet"
    markets_path = output / "markets.parquet"
    quality_path = output / "quality-report.json"
    quality = Quality(paths)
    events = ParquetSink(events_path, _event_schema(), batch_size=batch_size)
    levels = ParquetSink(levels_path, _level_schema(), batch_size=batch_size)
    markets = ParquetSink(markets_path, _market_schema(), batch_size=batch_size)
    sequence = 0
    try:
        for path in paths:
            LOGGER.info("Converting %s", path)
            with open_jsonl(path) as source:
                for line_number, line in enumerate(source, start=1):
                    quality.raw_rows += 1
                    try:
                        record = json.loads(line)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        quality.invalid_json_rows += 1
                        continue
                    if not isinstance(record, dict):
                        quality.invalid_record_rows += 1
                        continue
                    quality.observe_record(record)
                    record_type = str(record.get("record_type") or "")
                    if record_type == "rewarded_market_discovery":
                        sequence += 1
                        _write_market_rows(
                            record,
                            discovery_sequence=sequence,
                            sink=markets,
                            quality=quality,
                        )
                        row = _base_event(
                            sequence=sequence,
                            source_file=str(path),
                            source_line=line_number,
                            record=record,
                            payload=record,
                            event_type=record_type,
                        )
                        events.add(row)
                        quality.observe_event(row)
                        continue

                    payload = record.get("payload")
                    if record_type == "market_ws" and isinstance(payload, dict):
                        event_type = str(payload.get("event_type") or "unknown")
                        if event_type == "price_change":
                            changes = payload.get("price_changes") or []
                            if not isinstance(changes, list):
                                quality.invalid_record_rows += 1
                                continue
                            asset_counts = Counter(
                                str(change.get("asset_id") or "")
                                for change in changes
                                if isinstance(change, dict)
                            )
                            asset_indexes = Counter[str]()
                            for change in changes:
                                if not isinstance(change, dict):
                                    quality.invalid_record_rows += 1
                                    continue
                                asset_key = str(change.get("asset_id") or "")
                                index = asset_indexes[asset_key]
                                asset_indexes[asset_key] += 1
                                sequence += 1
                                normalized = dict(change)
                                normalized["market"] = payload.get("market")
                                normalized["timestamp"] = payload.get("timestamp")
                                normalized["event_type"] = event_type
                                row = _base_event(
                                    sequence=sequence,
                                    source_file=str(path),
                                    source_line=line_number,
                                    record=record,
                                    payload=normalized,
                                    event_type=event_type,
                                    change_index=index,
                                    change_count=asset_counts[asset_key],
                                )
                                quality.require(event_type, "asset_id", row["asset_id"])
                                quality.require(event_type, "price", row["price"])
                                quality.require(event_type, "size", row["size"])
                                quality.require(event_type, "side", row["side"])
                                events.add(row)
                                quality.observe_event(row)
                            continue

                        sequence += 1
                        row = _base_event(
                            sequence=sequence,
                            source_file=str(path),
                            source_line=line_number,
                            record=record,
                            payload=payload,
                            event_type=event_type,
                        )
                        if event_type in {
                            "book",
                            "last_trade_price",
                            "best_bid_ask",
                            "tick_size_change",
                        }:
                            quality.require(event_type, "asset_id", row["asset_id"])
                        if event_type == "last_trade_price":
                            quality.require(event_type, "price", row["price"])
                            quality.require(event_type, "size", row["size"])
                            quality.require(event_type, "side", row["side"])
                        events.add(row)
                        quality.observe_event(row)
                        if event_type == "book":
                            for side, field in (("BUY", "bids"), ("SELL", "asks")):
                                entries = payload.get(field) or []
                                if not isinstance(entries, list):
                                    quality.invalid_record_rows += 1
                                    continue
                                for rank, level in enumerate(entries):
                                    if not isinstance(level, dict):
                                        quality.invalid_record_rows += 1
                                        continue
                                    price = _float(level.get("price"))
                                    size = _float(level.get("size"))
                                    if price is None or size is None or size < 0:
                                        quality.invalid_record_rows += 1
                                        continue
                                    levels.add(
                                        {
                                            "sequence": sequence,
                                            "received_at_ns": row["received_at_ns"],
                                            "exchange_timestamp_ms": row[
                                                "exchange_timestamp_ms"
                                            ],
                                            "market": row["market"],
                                            "asset_id": row["asset_id"],
                                            "side": side,
                                            "price": price,
                                            "size": size,
                                            "source_rank": rank,
                                        }
                                    )
                                    quality.level_rows += 1
                        continue

                    sequence += 1
                    payload = record
                    row = _base_event(
                        sequence=sequence,
                        source_file=str(path),
                        source_line=line_number,
                        record=record,
                        payload=payload,
                        event_type=record_type or "unknown_record",
                    )
                    events.add(row)
                    quality.observe_event(row)

        outputs = {
            "events": {
                "path": str(events_path),
                "rows": events.close(),
            },
            "book_levels": {
                "path": str(levels_path),
                "rows": levels.close(),
            },
            "markets": {
                "path": str(markets_path),
                "rows": markets.close(),
            },
        }
    except BaseException:
        events.abort()
        levels.abort()
        markets.abort()
        raise
    report = quality.report(outputs)
    quality_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return report


def cli() -> None:
    parser = argparse.ArgumentParser(
        description="Convert raw Polymarket JSONL archives to normalized Parquet."
    )
    parser.add_argument("inputs", nargs="+", help="JSONL files or directories")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--batch-size", type=int, default=50_000)
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    report = convert(
        args.inputs,
        args.output_dir,
        batch_size=args.batch_size,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    cli()
