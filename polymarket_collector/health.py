from __future__ import annotations

import asyncio
import json
from typing import Final

from .state import CollectorState


MAX_REQUEST_BYTES: Final = 8192


class HealthServer:
    def __init__(
        self,
        state: CollectorState,
        host: str,
        port: int,
        stale_after_seconds: int,
    ) -> None:
        self.state = state
        self.host = host
        self.port = port
        self.stale_after_seconds = stale_after_seconds
        self._server: asyncio.Server | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle, host=self.host, port=self.port
        )

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    async def _handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            raw = await asyncio.wait_for(
                reader.readuntil(b"\r\n\r\n"), timeout=2
            )
            if len(raw) > MAX_REQUEST_BYTES:
                raise ValueError("request too large")
            first_line = raw.split(b"\r\n", 1)[0].decode("ascii", "replace")
            _, path, _ = first_line.split(" ", 2)
            if path == "/healthz":
                payload = self.state.as_dict(self.stale_after_seconds)
                status = 200 if payload["healthy"] else 503
                await self._respond_json(writer, status, payload)
            elif path == "/metrics":
                await self._respond_metrics(writer)
            else:
                await self._respond_json(writer, 404, {"error": "not_found"})
        except Exception:
            await self._respond_json(writer, 400, {"error": "bad_request"})
        finally:
            writer.close()
            await writer.wait_closed()

    async def _respond_json(self, writer: asyncio.StreamWriter, status: int, payload: object) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode()
        await self._respond(writer, status, "application/json", body)

    async def _respond_metrics(self, writer: asyncio.StreamWriter) -> None:
        healthy, _ = self.state.health(self.stale_after_seconds)
        lines = [
            "# TYPE polymarket_collector_up gauge",
            f"polymarket_collector_up {1 if healthy else 0}",
            "# TYPE polymarket_collector_markets gauge",
            f"polymarket_collector_markets {self.state.markets}",
            "# TYPE polymarket_collector_assets gauge",
            f"polymarket_collector_assets {self.state.assets}",
            "# TYPE polymarket_collector_messages_total counter",
            f"polymarket_collector_messages_total {self.state.messages_total}",
            "# TYPE polymarket_collector_reconnects_total counter",
            f"polymarket_collector_reconnects_total {self.state.reconnects_total}",
            "",
        ]
        await self._respond(writer, 200, "text/plain; version=0.0.4", "\n".join(lines).encode())

    async def _respond(
        self,
        writer: asyncio.StreamWriter,
        status: int,
        content_type: str,
        body: bytes,
    ) -> None:
        reason = {200: "OK", 400: "Bad Request", 404: "Not Found", 503: "Service Unavailable"}.get(status, "Error")
        headers = (
            f"HTTP/1.1 {status} {reason}\r\n"
            f"Content-Type: {content_type}\r\n"
            f"Content-Length: {len(body)}\r\n"
            "Connection: close\r\n\r\n"
        ).encode("ascii")
        writer.write(headers + body)
        await writer.drain()

