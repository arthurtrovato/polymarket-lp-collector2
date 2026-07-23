from __future__ import annotations

import asyncio
import gzip
import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from polymarket_collector.book_checkpoints import run_book_checkpoints
from polymarket_collector.state import CollectorState
from polymarket_collector.storage import RotatingJsonlWriter


class BookCheckpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_records_public_rest_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            writer = RotatingJsonlWriter(
                Path(temporary),
                "market_ws",
                rotation_seconds=3600,
                max_file_bytes=1024 * 1024,
                flush_interval_seconds=1,
            )
            await writer.start()
            state = CollectorState()
            stop = asyncio.Event()
            book = {
                "market": "condition-1",
                "asset_id": "asset-1",
                "timestamp": "1000",
                "hash": "checkpoint-hash",
                "bids": [{"price": "0.49", "size": "10"}],
                "asks": [{"price": "0.51", "size": "11"}],
            }
            with patch(
                "polymarket_collector.book_checkpoints._post_books",
                return_value=[book],
            ) as post:
                task = asyncio.create_task(
                    run_book_checkpoints(
                        clob_base_url="https://example.invalid",
                        asset_source=lambda: ("asset-1",),
                        interval_seconds=60,
                        writer=writer,
                        state=state,
                        stop_event=stop,
                    )
                )
                deadline = time.monotonic() + 2
                while (
                    state.book_checkpoints_total < 1
                    and time.monotonic() < deadline
                ):
                    await asyncio.sleep(0.01)
                stop.set()
                await asyncio.wait_for(task, 2)
            await writer.close()

            self.assertEqual(state.book_checkpoints_total, 1)
            post.assert_called_once_with(
                "https://example.invalid/books",
                ("asset-1",),
            )
            archive = next(Path(temporary).rglob("*.jsonl.gz"))
            with gzip.open(archive, "rt", encoding="utf-8") as handle:
                record = json.loads(handle.readline())
            self.assertEqual(record["record_type"], "rest_book_checkpoint")
            self.assertEqual(record["payload"], book)
            self.assertEqual(record["requested_asset_count"], 1)
            self.assertIn("checkpoint_id", record)


if __name__ == "__main__":
    unittest.main()
