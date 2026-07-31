from __future__ import annotations

import gzip
import json
import tempfile
import unittest
from pathlib import Path

try:
    import pyarrow  # noqa: F401

    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

from polymarket_collector.analytics_common import iter_parquet_rows
from polymarket_collector.incremental import incremental_convert


@unittest.skipUnless(HAS_PYARROW, "pyarrow analytics extra is not installed")
class IncrementalETLTests(unittest.TestCase):
    def _write_archive(self, path: Path, received_at_ns: int, record_type: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        if record_type == "market_ws":
            payload = {
                "event_type": "book",
                "timestamp": str(received_at_ns // 1_000_000),
                "market": "market-1",
                "asset_id": "asset-1",
                "bids": [{"price": "0.49", "size": "10"}],
                "asks": [{"price": "0.51", "size": "10"}],
            }
        else:
            payload = {
                "event_type": "crypto_prices",
                "symbol": "BTC",
                "value": "65000",
                "timestamp": str(received_at_ns // 1_000_000),
            }
        record = {
            "record_type": record_type,
            "received_at_ns": received_at_ns,
            "connection_id": "test-connection",
            "payload": payload,
        }
        with gzip.open(path, "wt", encoding="utf-8") as target:
            target.write(json.dumps(record) + "\n")

    def test_incremental_manifest_is_idempotent_and_partitioned(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = root / "raw"
            first = raw / "market_ws/2026/07/31/market_ws-20260731T010000Z.jsonl.gz"
            second = raw / "rtds/2026/07/31/rtds-20260731T010100Z.jsonl.gz"
            self._write_archive(first, 1_000_000_000, "market_ws")
            self._write_archive(second, 2_000_000_000, "rtds")
            analytics = root / "analytics"

            result = incremental_convert([raw], analytics, input_root=raw, batch_size=1)
            self.assertEqual(result["processed"], 2)
            self.assertEqual(result["skipped"], 0)
            self.assertEqual(result["summary"]["input_files"], 2)
            self.assertGreater(result["next_sequence"], 0)
            manifest = json.loads((analytics / "manifest.json").read_text())
            self.assertEqual(len(manifest["processed_files"]), 2)
            self.assertTrue(
                all(
                    entry["status"] == "completed"
                    for entry in manifest["processed_files"].values()
                )
            )
            self.assertEqual(len(list((analytics / "events").rglob("*.parquet"))), 2)
            self.assertEqual(
                sum(1 for _ in iter_parquet_rows(analytics / "events")),
                2,
            )
            sequences = [
                row["sequence"] for row in iter_parquet_rows(analytics / "events")
            ]
            self.assertEqual(sequences, sorted(sequences))

            retry = incremental_convert([raw], analytics, input_root=raw, batch_size=1)
            self.assertEqual(retry["processed"], 0)
            self.assertEqual(retry["skipped"], 2)
            self.assertEqual(retry["summary"]["input_files"], 2)

            with gzip.open(first, "at", encoding="utf-8") as target:
                target.write("\n")
            with self.assertRaisesRegex(ValueError, "changed after processing"):
                incremental_convert([first], analytics, input_root=raw)


if __name__ == "__main__":
    unittest.main()
