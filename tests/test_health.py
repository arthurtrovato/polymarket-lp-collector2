from __future__ import annotations

import asyncio
import json
import time
import unittest

from polymarket_collector.health import HealthServer
from polymarket_collector.state import CollectorState


class HealthTests(unittest.IsolatedAsyncioTestCase):
    async def test_health_endpoint(self) -> None:
        state = CollectorState(
            discovered_at=time.time(),
            connected_at=time.time(),
            last_message_at=time.time(),
            connected=True,
            markets=4,
            assets=8,
        )
        server = HealthServer(state, "127.0.0.1", 0, stale_after_seconds=60)
        await server.start()
        assert server._server is not None
        port = server._server.sockets[0].getsockname()[1]

        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n")
        await writer.drain()
        response = await reader.read()
        writer.close()
        await writer.wait_closed()
        await server.close()

        _, body = response.split(b"\r\n\r\n", 1)
        payload = json.loads(body)
        self.assertTrue(payload["healthy"])
        self.assertEqual(payload["markets"], 4)


if __name__ == "__main__":
    unittest.main()

