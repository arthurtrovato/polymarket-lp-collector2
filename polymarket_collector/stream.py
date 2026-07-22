from __future__ import annotations

import asyncio
import json
import logging
import random
import time
import uuid
from collections.abc import Iterable
from typing import Any

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from .state import CollectorState
from .storage import RotatingJsonlWriter


LOGGER = logging.getLogger(__name__)


class MarketStream:
    def __init__(
        self,
        url: str,
        writer: RotatingJsonlWriter,
        state: CollectorState,
    ) -> None:
        self.url = url
        self.writer = writer
        self.state = state
        self._desired_assets: set[str] = set()
        self._assets_changed = asyncio.Event()

    def set_assets(self, asset_ids: Iterable[str]) -> None:
        desired = {str(asset_id) for asset_id in asset_ids if asset_id}
        if desired != self._desired_assets:
            self._desired_assets = desired
            self._assets_changed.set()

    async def run(self, stop_event: asyncio.Event) -> None:
        delay = 1.0
        while not stop_event.is_set():
            if not self._desired_assets:
                await self._wait_for_assets_or_stop(stop_event)
                continue
            try:
                connected_for = await self._connected_loop(stop_event)
                delay = 1.0 if connected_for >= 30 else min(delay * 2, 60)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.state.last_error = f"websocket: {type(exc).__name__}: {exc}"
                LOGGER.warning("WebSocket disconnected: %s", exc)
                delay = min(delay * 2, 60)

            self.state.connected = False
            self.state.disconnected_at = time.time()
            if not stop_event.is_set():
                self.state.reconnects_total += 1
                try:
                    await asyncio.wait_for(stop_event.wait(), delay + random.random())
                except TimeoutError:
                    pass

    async def _wait_for_assets_or_stop(self, stop_event: asyncio.Event) -> None:
        assets_task = asyncio.create_task(self._assets_changed.wait())
        stop_task = asyncio.create_task(stop_event.wait())
        done, pending = await asyncio.wait(
            {assets_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        if assets_task in done:
            self._assets_changed.clear()

    async def _connected_loop(self, stop_event: asyncio.Event) -> float:
        connection_id = uuid.uuid4().hex
        connected_monotonic = time.monotonic()
        async with connect(
            self.url,
            open_timeout=20,
            close_timeout=5,
            ping_interval=None,
            max_size=None,
        ) as websocket:
            subscribed = set(self._desired_assets)
            await websocket.send(
                json.dumps(
                    {
                        "assets_ids": sorted(subscribed),
                        "type": "market",
                        "custom_feature_enabled": True,
                    },
                    separators=(",", ":"),
                )
            )
            if subscribed == self._desired_assets:
                self._assets_changed.clear()
            else:
                self._assets_changed.set()
            self.state.connected = True
            self.state.connected_at = time.time()
            self.state.last_error = None
            LOGGER.info("WebSocket connected for %d assets", len(subscribed))
            last_ping = 0.0

            while not stop_event.is_set():
                if self._assets_changed.is_set():
                    self._assets_changed.clear()
                    subscribed = await self._sync_subscriptions(websocket, subscribed)
                    if subscribed != self._desired_assets:
                        self._assets_changed.set()
                    if not subscribed:
                        break

                now = time.monotonic()
                if now - last_ping >= 10:
                    await websocket.send("PING")
                    last_ping = now

                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=1)
                except TimeoutError:
                    continue
                except ConnectionClosed:
                    break

                if isinstance(message, bytes):
                    message = message.decode("utf-8", "replace")
                if message == "PONG":
                    self.state.last_pong_at = time.time()
                    continue
                await self._record_message(message, connection_id)

        return time.monotonic() - connected_monotonic

    async def _sync_subscriptions(self, websocket: Any, subscribed: set[str]) -> set[str]:
        desired = set(self._desired_assets)
        removed = sorted(subscribed - desired)
        added = sorted(desired - subscribed)
        if removed:
            await websocket.send(
                json.dumps(
                    {"assets_ids": removed, "operation": "unsubscribe"},
                    separators=(",", ":"),
                )
            )
        if added:
            await websocket.send(
                json.dumps(
                    {
                        "assets_ids": added,
                        "operation": "subscribe",
                        "custom_feature_enabled": True,
                    },
                    separators=(",", ":"),
                )
            )
        if added or removed:
            LOGGER.info(
                "WebSocket subscriptions updated: +%d -%d (%d total)",
                len(added),
                len(removed),
                len(desired),
            )
        return desired

    async def _record_message(self, raw_message: str, connection_id: str) -> None:
        received_at_ns = time.time_ns()
        try:
            payload = json.loads(raw_message)
        except json.JSONDecodeError:
            self.state.invalid_messages_total += 1
            await self.writer.write(
                {
                    "record_type": "invalid_market_ws",
                    "received_at_ns": received_at_ns,
                    "connection_id": connection_id,
                    "raw": raw_message,
                }
            )
            return

        messages = payload if isinstance(payload, list) else [payload]
        for item in messages:
            if not isinstance(item, dict):
                self.state.invalid_messages_total += 1
                continue
            await self.writer.write(
                {
                    "record_type": "market_ws",
                    "received_at_ns": received_at_ns,
                    "connection_id": connection_id,
                    "payload": item,
                }
            )
            self.state.messages_total += 1
        self.state.last_message_at = time.time()
