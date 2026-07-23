from __future__ import annotations

import asyncio
import gzip
import json
import tempfile
import time
import unittest
from pathlib import Path

from websockets.asyncio.server import serve

from polymarket_collector.auxiliary_streams import (
    run_rtds_crypto_stream,
    run_sports_stream,
)
from polymarket_collector.state import CollectorState
from polymarket_collector.storage import RotatingJsonlWriter


class AuxiliaryStreamTests(unittest.IsolatedAsyncioTestCase):
    async def _wait_for(self, predicate, timeout: float = 2) -> None:
        deadline = time.monotonic() + timeout
        while not predicate() and time.monotonic() < deadline:
            await asyncio.sleep(0.01)
        self.assertTrue(predicate())

    async def test_records_sports_updates(self) -> None:
        async def handler(websocket) -> None:
            await websocket.send(
                json.dumps(
                    {
                        "gameId": "game-1",
                        "slug": "a-b",
                        "score": "1-0",
                        "live": True,
                    }
                )
            )
            await websocket.wait_closed()

        with tempfile.TemporaryDirectory() as temporary:
            writer = RotatingJsonlWriter(
                Path(temporary),
                "sports_ws",
                rotation_seconds=3600,
                max_file_bytes=1024 * 1024,
                flush_interval_seconds=1,
            )
            await writer.start()
            state = CollectorState()
            stop = asyncio.Event()
            async with serve(handler, "127.0.0.1", 0) as server:
                port = server.sockets[0].getsockname()[1]
                task = asyncio.create_task(
                    run_sports_stream(
                        f"ws://127.0.0.1:{port}",
                        writer,
                        state,
                        stop,
                    )
                )
                await self._wait_for(lambda: state.sports_messages_total == 1)
                stop.set()
                await asyncio.wait_for(task, 3)
            await writer.close()

            archive = next(Path(temporary).rglob("*.jsonl.gz"))
            with gzip.open(archive, "rt", encoding="utf-8") as handle:
                records = [json.loads(line) for line in handle]
            message = next(
                item for item in records if item["record_type"] == "sports_ws"
            )
            self.assertEqual(message["payload"]["gameId"], "game-1")

    async def test_subscribes_and_records_rtds_prices(self) -> None:
        subscription_received = asyncio.Event()

        async def handler(websocket) -> None:
            subscription = json.loads(await websocket.recv())
            topics = {
                item["topic"] for item in subscription["subscriptions"]
            }
            self.assertEqual(
                topics,
                {"crypto_prices", "crypto_prices_chainlink"},
            )
            self.assertTrue(
                all(
                    "filters" not in item
                    for item in subscription["subscriptions"]
                )
            )
            subscription_received.set()
            await websocket.send(
                json.dumps(
                    {
                        "topic": "crypto_prices",
                        "type": "update",
                        "timestamp": 1000,
                        "payload": {
                            "symbol": "btcusdt",
                            "timestamp": 1000,
                            "value": 100_000,
                        },
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
                "rtds",
                rotation_seconds=3600,
                max_file_bytes=1024 * 1024,
                flush_interval_seconds=1,
            )
            await writer.start()
            state = CollectorState()
            stop = asyncio.Event()
            async with serve(handler, "127.0.0.1", 0) as server:
                port = server.sockets[0].getsockname()[1]
                task = asyncio.create_task(
                    run_rtds_crypto_stream(
                        f"ws://127.0.0.1:{port}",
                        writer,
                        state,
                        stop,
                    )
                )
                await asyncio.wait_for(subscription_received.wait(), 2)
                await self._wait_for(lambda: state.rtds_messages_total == 1)
                stop.set()
                await asyncio.wait_for(task, 3)
            await writer.close()

            archive = next(Path(temporary).rglob("*.jsonl.gz"))
            with gzip.open(archive, "rt", encoding="utf-8") as handle:
                records = [json.loads(line) for line in handle]
            message = next(
                item for item in records if item["record_type"] == "rtds"
            )
            self.assertEqual(message["payload"]["payload"]["symbol"], "btcusdt")
            controls = [
                item
                for item in records
                if item["record_type"] == "rtds_control"
            ]
            self.assertTrue(
                any(item["control_event"] == "subscribed" for item in controls)
            )

    async def test_reconnects_when_rtds_produces_no_prices(self) -> None:
        connections = 0

        async def handler(websocket) -> None:
            nonlocal connections
            connections += 1
            await websocket.recv()
            if connections == 1:
                while True:
                    try:
                        message = await websocket.recv()
                    except Exception:
                        return
                    if message == "PING":
                        await websocket.send("PONG")
            await websocket.send(
                json.dumps(
                    {
                        "topic": "crypto_prices",
                        "type": "update",
                        "timestamp": 1000,
                        "payload": {
                            "symbol": "btcusdt",
                            "timestamp": 1000,
                            "value": 100_000,
                        },
                    }
                )
            )
            await websocket.wait_closed()

        with tempfile.TemporaryDirectory() as temporary:
            writer = RotatingJsonlWriter(
                Path(temporary),
                "rtds",
                rotation_seconds=3600,
                max_file_bytes=1024 * 1024,
                flush_interval_seconds=1,
            )
            await writer.start()
            state = CollectorState()
            stop = asyncio.Event()
            async with serve(handler, "127.0.0.1", 0) as server:
                port = server.sockets[0].getsockname()[1]
                task = asyncio.create_task(
                    run_rtds_crypto_stream(
                        f"ws://127.0.0.1:{port}",
                        writer,
                        state,
                        stop,
                        stale_after_seconds=0.05,
                    )
                )
                await asyncio.wait_for(
                    self._wait_for(
                        lambda: state.rtds_messages_total == 1,
                        timeout=5,
                    ),
                    6,
                )
                stop.set()
                await asyncio.wait_for(task, 3)
            await writer.close()

            self.assertGreaterEqual(connections, 2)
            self.assertGreaterEqual(state.auxiliary_reconnects_total, 1)
            archive = next(Path(temporary).rglob("*.jsonl.gz"))
            with gzip.open(archive, "rt", encoding="utf-8") as handle:
                controls = [
                    json.loads(line)
                    for line in handle
                    if '"record_type":"rtds_control"' in line
                ]
            self.assertTrue(
                any(item["control_event"] == "stale" for item in controls)
            )


if __name__ == "__main__":
    unittest.main()
