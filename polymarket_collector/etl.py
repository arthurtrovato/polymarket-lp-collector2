from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import sqlite3
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


def _bool(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "yes", "on"}:
        return True
    if normalized in {"false", "0", "no", "off"}:
        return False
    return None


def _first(*values: Any) -> Any:
    for value in values:
        if value is not None and value != "":
            return value
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
            ("received_monotonic_ns", pa.int64()),
            ("connection_id", pa.string()),
            ("subscription_revision", pa.int64()),
            ("frame_index", pa.int32()),
            ("frame_message_count", pa.int32()),
            ("control_event", pa.string()),
            ("checkpoint_id", pa.string()),
            ("data_source", pa.string()),
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
            ("symbol", pa.string()),
            ("reference_value", pa.float64()),
            ("game_id", pa.string()),
            ("slug", pa.string()),
            ("status", pa.string()),
            ("score", pa.string()),
            ("period", pa.string()),
            ("elapsed", pa.string()),
            ("live", pa.bool_()),
            ("ended", pa.bool_()),
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
            ("discovery_record_type", pa.string()),
            ("is_selected", pa.bool_()),
            ("received_at_ns", pa.int64()),
            ("condition_id", pa.string()),
            ("market_id", pa.string()),
            ("market_slug", pa.string()),
            ("event_id", pa.string()),
            ("event_slug", pa.string()),
            ("question", pa.string()),
            ("description", pa.string()),
            ("category", pa.string()),
            ("subcategory", pa.string()),
            ("tags_json", pa.string()),
            ("resolution_source", pa.string()),
            ("created_at", pa.string()),
            ("start_date", pa.string()),
            ("end_date", pa.string()),
            ("game_start_time", pa.string()),
            ("group_item_title", pa.string()),
            ("token_id", pa.string()),
            ("outcome", pa.string()),
            ("token_price", pa.float64()),
            ("winner", pa.bool_()),
            ("daily_reward", pa.float64()),
            ("native_daily_rate", pa.float64()),
            ("sponsored_daily_rate", pa.float64()),
            ("sponsors_count", pa.int32()),
            ("rewards_max_spread_cents", pa.float64()),
            ("rewards_min_size", pa.float64()),
            ("market_competitiveness", pa.float64()),
            ("spread", pa.float64()),
            ("volume_24hr", pa.float64()),
            ("one_day_price_change", pa.float64()),
            ("active", pa.bool_()),
            ("closed", pa.bool_()),
            ("archived", pa.bool_()),
            ("accepting_orders", pa.bool_()),
            ("enable_order_book", pa.bool_()),
            ("minimum_order_size", pa.float64()),
            ("minimum_tick_size", pa.float64()),
            ("seconds_delay", pa.int64()),
            ("neg_risk", pa.bool_()),
            ("fees_enabled", pa.bool_()),
            ("maker_base_fee_bps", pa.int64()),
            ("taker_base_fee_bps", pa.int64()),
            ("fee_rate", pa.float64()),
            ("fee_exponent", pa.float64()),
            ("fee_taker_only", pa.bool_()),
            ("rfq_enabled", pa.bool_()),
            ("taker_order_delay_enabled", pa.bool_()),
            ("blockaid_check_enabled", pa.bool_()),
            ("minimum_order_age_seconds", pa.int64()),
            ("clob_rewards_json", pa.string()),
            ("fee_details_json", pa.string()),
            ("market_json", pa.string()),
        ]
    )


class Quality:
    def __init__(self, inputs: list[Path], fingerprint_path: Path) -> None:
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
        self._last_exchange_by_connection: dict[str, int] = {}
        self._fingerprint_path = fingerprint_path
        self._fingerprint_path.unlink(missing_ok=True)
        self._fingerprints = sqlite3.connect(self._fingerprint_path)
        self._fingerprints.execute("PRAGMA journal_mode=OFF")
        self._fingerprints.execute("PRAGMA synchronous=OFF")
        self._fingerprints.execute(
            "CREATE TABLE fingerprints (digest BLOB PRIMARY KEY) WITHOUT ROWID"
        )
        self._fingerprints_since_commit = 0

    def observe_record(self, record: dict[str, Any]) -> None:
        record_type = str(record.get("record_type") or "<missing>")
        self.record_types[record_type] += 1
        encoded = canonical_json(record).encode("utf-8")
        fingerprint = hashlib.blake2b(encoded, digest_size=12).digest()
        cursor = self._fingerprints.execute(
            "INSERT OR IGNORE INTO fingerprints (digest) VALUES (?)",
            (fingerprint,),
        )
        if cursor.rowcount == 0:
            self.duplicate_records += 1
        self._fingerprints_since_commit += 1
        if self._fingerprints_since_commit >= 100_000:
            self._fingerprints.commit()
            self._fingerprints_since_commit = 0

    def close(self) -> None:
        if self._fingerprints is not None:
            self._fingerprints.commit()
            self._fingerprints.close()
            self._fingerprints = None
        self._fingerprint_path.unlink(missing_ok=True)

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
    nested_payload = (
        payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
    )
    timestamp = _int(
        _first(payload.get("timestamp"), nested_payload.get("timestamp"))
    )
    latency = None
    if received_at_ns is not None and timestamp is not None:
        latency = received_at_ns / 1_000_000 - timestamp
    return {
        "sequence": sequence,
        "source_file": source_file,
        "source_line": source_line,
        "record_type": str(record.get("record_type") or ""),
        "received_at_ns": received_at_ns,
        "received_monotonic_ns": _int(record.get("received_monotonic_ns")),
        "connection_id": str(record.get("connection_id") or ""),
        "subscription_revision": _int(record.get("subscription_revision")),
        "frame_index": _int(record.get("frame_index")),
        "frame_message_count": _int(record.get("frame_message_count")),
        "control_event": str(record.get("control_event") or ""),
        "checkpoint_id": str(record.get("checkpoint_id") or ""),
        "data_source": str(record.get("record_type") or ""),
        "event_type": event_type,
        "exchange_timestamp_ms": timestamp,
        "capture_latency_ms": latency,
        "market": str(
            payload.get("market")
            or payload.get("condition_id")
            or payload.get("slug")
            or ""
        ),
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
        "symbol": str(
            _first(payload.get("symbol"), nested_payload.get("symbol")) or ""
        ),
        "reference_value": _float(
            _first(payload.get("value"), nested_payload.get("value"))
        ),
        "game_id": str(
            _first(payload.get("gameId"), payload.get("game_id")) or ""
        ),
        "slug": str(payload.get("slug") or ""),
        "status": str(payload.get("status") or ""),
        "score": str(payload.get("score") or ""),
        "period": str(payload.get("period") or ""),
        "elapsed": str(payload.get("elapsed") or ""),
        "live": _bool(payload.get("live")),
        "ended": _bool(payload.get("ended")),
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
    record_type = str(record.get("record_type") or "")
    for market in record.get("markets") or []:
        if not isinstance(market, dict):
            quality.invalid_record_rows += 1
            continue
        current = (
            market.get("current_rewards")
            if isinstance(market.get("current_rewards"), dict)
            else {}
        )
        clob = (
            market.get("clob_market_info")
            if isinstance(market.get("clob_market_info"), dict)
            else {}
        )
        gamma = (
            market.get("gamma_market")
            if isinstance(market.get("gamma_market"), dict)
            else {}
        )
        fee_details = clob.get("fd") if isinstance(clob.get("fd"), dict) else {}
        gamma_event = (
            market.get("gamma_event")
            if isinstance(market.get("gamma_event"), dict)
            else {}
        )
        if not gamma_event:
            gamma_events = gamma.get("events") or []
            gamma_event = (
                gamma_events[0]
                if isinstance(gamma_events, list)
                and gamma_events
                and isinstance(gamma_events[0], dict)
                else {}
            )
        tags = _first(gamma.get("tags"), gamma_event.get("tags"), market.get("tags"))
        fee_rate = _float(fee_details.get("r"))
        fees_enabled = _bool(
            _first(gamma.get("feesEnabled"), gamma.get("fees_enabled"))
        )
        if fees_enabled is None and fee_rate is not None:
            fees_enabled = fee_rate > 0
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
                    "discovery_record_type": record_type,
                    "is_selected": record_type == "rewarded_market_discovery",
                    "received_at_ns": received_at_ns,
                    "condition_id": str(market.get("condition_id") or ""),
                    "market_id": str(market.get("market_id") or ""),
                    "market_slug": str(market.get("market_slug") or ""),
                    "event_id": str(market.get("event_id") or ""),
                    "event_slug": str(market.get("event_slug") or ""),
                    "question": str(market.get("question") or ""),
                    "description": str(
                        _first(
                            gamma.get("description"),
                            market.get("description"),
                        )
                        or ""
                    ),
                    "category": str(
                        _first(
                            gamma.get("category"),
                            gamma_event.get("category"),
                        )
                        or ""
                    ),
                    "subcategory": str(
                        _first(
                            gamma.get("subcategory"),
                            gamma_event.get("subcategory"),
                        )
                        or ""
                    ),
                    "tags_json": canonical_json(tags) if tags is not None else "",
                    "resolution_source": str(
                        _first(
                            gamma.get("resolutionSource"),
                            gamma.get("resolution_source"),
                            gamma_event.get("resolutionSource"),
                        )
                        or ""
                    ),
                    "created_at": str(
                        _first(
                            market.get("created_at"),
                            gamma.get("createdAt"),
                            gamma.get("created_at"),
                        )
                        or ""
                    ),
                    "start_date": str(
                        _first(
                            gamma.get("startDate"),
                            gamma.get("start_date"),
                            gamma_event.get("startDate"),
                        )
                        or ""
                    ),
                    "end_date": str(
                        _first(
                            market.get("end_date"),
                            gamma.get("endDate"),
                            gamma.get("end_date"),
                            gamma_event.get("endDate"),
                        )
                        or ""
                    ),
                    "game_start_time": str(
                        _first(
                            clob.get("gst"),
                            gamma.get("gameStartTime"),
                            gamma.get("game_start_time"),
                            market.get("game_start_time"),
                        )
                        or ""
                    ),
                    "group_item_title": str(
                        market.get("group_item_title") or ""
                    ),
                    "token_id": str(token.get("token_id") or ""),
                    "outcome": str(token.get("outcome") or ""),
                    "token_price": _float(token.get("price")),
                    "winner": _bool(token.get("winner")),
                    "daily_reward": _float(
                        _first(
                            market.get("collector_daily_reward"),
                            market.get("total_daily_rate"),
                            current.get("total_daily_rate"),
                        )
                    ),
                    "native_daily_rate": _float(
                        current.get("native_daily_rate")
                    ),
                    "sponsored_daily_rate": _float(
                        current.get("sponsored_daily_rate")
                    ),
                    "sponsors_count": _int(current.get("sponsors_count")),
                    "rewards_max_spread_cents": _float(
                        _first(
                            market.get("rewards_max_spread"),
                            current.get("rewards_max_spread"),
                        )
                    ),
                    "rewards_min_size": _float(
                        _first(
                            market.get("rewards_min_size"),
                            current.get("rewards_min_size"),
                        )
                    ),
                    "market_competitiveness": _float(
                        market.get("market_competitiveness")
                    ),
                    "spread": _float(market.get("spread")),
                    "volume_24hr": _float(
                        _first(
                            market.get("collector_volume_24hr"),
                            market.get("volume_24hr"),
                        )
                    ),
                    "one_day_price_change": _float(
                        market.get("one_day_price_change")
                    ),
                    "active": _bool(
                        _first(gamma.get("active"), market.get("active"))
                    ),
                    "closed": _bool(
                        _first(gamma.get("closed"), market.get("closed"))
                    ),
                    "archived": _bool(
                        _first(gamma.get("archived"), market.get("archived"))
                    ),
                    "accepting_orders": _bool(
                        _first(
                            gamma.get("acceptingOrders"),
                            gamma.get("accepting_orders"),
                            market.get("accepting_orders"),
                        )
                    ),
                    "enable_order_book": _bool(
                        _first(
                            gamma.get("enableOrderBook"),
                            gamma.get("enable_order_book"),
                            market.get("enable_order_book"),
                        )
                    ),
                    "minimum_order_size": _float(
                        _first(
                            clob.get("mos"),
                            gamma.get("orderMinSize"),
                            gamma.get("minimum_order_size"),
                        )
                    ),
                    "minimum_tick_size": _float(
                        _first(
                            clob.get("mts"),
                            gamma.get("orderPriceMinTickSize"),
                            gamma.get("minimum_tick_size"),
                        )
                    ),
                    "seconds_delay": _int(
                        _first(
                            gamma.get("secondsDelay"),
                            gamma.get("seconds_delay"),
                        )
                    ),
                    "neg_risk": _bool(
                        _first(
                            gamma.get("negRisk"),
                            gamma.get("neg_risk"),
                            market.get("neg_risk"),
                        )
                    ),
                    "fees_enabled": fees_enabled,
                    "maker_base_fee_bps": _int(
                        _first(
                            clob.get("mbf"),
                            gamma.get("makerBaseFee"),
                            gamma.get("maker_base_fee"),
                        )
                    ),
                    "taker_base_fee_bps": _int(
                        _first(
                            clob.get("tbf"),
                            gamma.get("takerBaseFee"),
                            gamma.get("taker_base_fee"),
                        )
                    ),
                    "fee_rate": fee_rate,
                    "fee_exponent": _float(fee_details.get("e")),
                    "fee_taker_only": _bool(fee_details.get("to")),
                    "rfq_enabled": _bool(
                        _first(clob.get("rfqe"), gamma.get("rfqEnabled"))
                    ),
                    "taker_order_delay_enabled": _bool(clob.get("itode")),
                    "blockaid_check_enabled": _bool(clob.get("ibce")),
                    "minimum_order_age_seconds": _int(clob.get("oas")),
                    "clob_rewards_json": (
                        canonical_json(clob["r"])
                        if isinstance(clob.get("r"), dict)
                        else ""
                    ),
                    "fee_details_json": (
                        canonical_json(fee_details) if fee_details else ""
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
    quality = Quality(paths, output / ".quality-fingerprints.sqlite3.tmp")
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
                    if record_type in {
                        "rewarded_market_discovery",
                        "rewarded_market_universe",
                    }:
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
                    is_order_book_record = record_type in {
                        "market_ws",
                        "rest_book_checkpoint",
                    }
                    if is_order_book_record and isinstance(payload, dict):
                        event_type = (
                            "book"
                            if record_type == "rest_book_checkpoint"
                            else str(payload.get("event_type") or "unknown")
                        )
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
                    if record_type in {"sports_ws", "rtds"} and isinstance(
                        payload, dict
                    ):
                        normalized_payload = payload
                        event_type = str(
                            _first(
                                payload.get("event_type"),
                                payload.get("type"),
                                payload.get("topic"),
                                record_type,
                            )
                        )
                    else:
                        normalized_payload = record
                        event_type = str(
                            _first(
                                record.get("control_event"),
                                record_type,
                                "unknown_record",
                            )
                        )
                    row = _base_event(
                        sequence=sequence,
                        source_file=str(path),
                        source_line=line_number,
                        record=record,
                        payload=normalized_payload,
                        event_type=event_type,
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
        quality.close()
        raise
    quality.close()
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
