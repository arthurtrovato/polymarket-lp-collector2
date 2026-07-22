from __future__ import annotations

import asyncio
import gzip
import json
import tempfile
import time
import unittest
from pathlib import Path

from websockets.asyncio.server import serve

from polymarket_collector.state import CollectorState
from polymarket_collector.storage import RotatingJsonlWriter
from polymarket_collector.stream import MarketStream


class StreamTests(unittest.IsolatedAsyncioTestCase):
    async def test_records_public_market_message(self) -> None:
        subscription_received = asyncio.Event()

        async def handler(websocket) -> None:
            subscription = json.loads(await websocket.recv())
            self.assertEqual(subscription["assets_ids"], ["asset-1", "asset-2"])
            subscription_received.set()
            await websocket.send(
                json.dumps(
                    {
                        "event_type": "book",
                        "asset_id": "asset-1",
                        "market": "condition-1",
                        "bids": [],
                        "asks": [],
                        "timestamp": "1",
                    }
                )
            )
            while True:
                try:
                    message = await websocket.recv()
                except Exception:
                    return
                if message == "PING":
                    await websocket.send("PONG")

        with tempfile.TemporaryDirectory() as temporary:
            writer = RotatingJsonlWriter(
                Path(temporary),
                "stream",
                rotation_seconds=3600,
                max_file_bytes=1024 * 1024,
                flush_interval_seconds=1,
            )
            await writer.start()
            state = CollectorState()
            stop = asyncio.Event()
            async with serve(handler, "127.0.0.1", 0) as websocket_server:
                port = websocket_server.sockets[0].getsockname()[1]
                stream = MarketStream(
                    f"ws://127.0.0.1:{port}", writer, state
                )
                stream.set_assets(["asset-1", "asset-2"])
                task = asyncio.create_task(stream.run(stop))
                await asyncio.wait_for(subscription_received.wait(), 2)
                deadline = time.monotonic() + 2
                while state.messages_total < 1 and time.monotonic() < deadline:
                    await asyncio.sleep(0.01)
                stop.set()
                await asyncio.wait_for(task, 3)
            await writer.close()

            self.assertEqual(state.messages_total, 1)
            archive = next(Path(temporary).rglob("*.jsonl.gz"))
            with gzip.open(archive, "rt", encoding="utf-8") as handle:
                record = json.loads(handle.readline())
            self.assertEqual(record["payload"]["event_type"], "book")
            self.assertIn("received_at_ns", record)


if __name__ == "__main__":
    unittest.main()

