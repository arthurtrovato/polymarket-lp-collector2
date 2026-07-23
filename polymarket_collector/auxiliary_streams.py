from __future__ import annotations

import asyncio
import json
import logging
import random
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from .state import CollectorState
from .storage import RotatingJsonlWriter


LOGGER = logging.getLogger(__name__)
RTDS_SUBSCRIPTION = {
    "action": "subscribe",
    "subscriptions": [
        {
            "topic": "crypto_prices",
            "type": "update",
        },
        {
            "topic": "crypto_prices_chainlink",
            "type": "*",
        },
    ],
}
RTDS_STALE_AFTER_SECONDS = 30.0


async def _write_control(
    writer: RotatingJsonlWriter,
    source: str,
    control_event: str,
    connection_id: str,
    **details: Any,
) -> None:
    await writer.write(
        {
            "record_type": f"{source}_control",
            "control_event": control_event,
            "received_at_ns": time.time_ns(),
            "received_monotonic_ns": time.monotonic_ns(),
            "connection_id": connection_id,
            **details,
        }
    )


async def _write_json_message(
    writer: RotatingJsonlWriter,
    source: str,
    raw_message: str,
    connection_id: str,
    state: CollectorState,
) -> None:
    received_at_ns = time.time_ns()
    received_monotonic_ns = time.monotonic_ns()
    try:
        payload = json.loads(raw_message)
    except json.JSONDecodeError:
        await writer.write(
            {
                "record_type": f"invalid_{source}",
                "received_at_ns": received_at_ns,
                "received_monotonic_ns": received_monotonic_ns,
                "connection_id": connection_id,
                "raw": raw_message,
            }
        )
        return
    messages = payload if isinstance(payload, list) else [payload]
    for frame_index, item in enumerate(messages):
        await writer.write(
            {
                "record_type": source,
                "received_at_ns": received_at_ns,
                "received_monotonic_ns": received_monotonic_ns,
                "connection_id": connection_id,
                "frame_index": frame_index,
                "frame_message_count": len(messages),
                "payload": item,
            }
        )
        if source == "sports_ws":
            state.sports_messages_total += 1
        else:
            state.rtds_messages_total += 1


async def _run_reconnecting(
    *,
    source: str,
    url: str,
    writer: RotatingJsonlWriter,
    state: CollectorState,
    stop_event: asyncio.Event,
    connected_loop: Callable[[Any, str], Awaitable[None]],
) -> None:
    delay = 1.0
    while not stop_event.is_set():
        connection_id = uuid.uuid4().hex
        try:
            async with connect(
                url,
                open_timeout=20,
                close_timeout=5,
                ping_interval=None,
                max_size=None,
            ) as websocket:
                await _write_control(
                    writer,
                    source,
                    "connected",
                    connection_id,
                )
                delay = 1.0
                await connected_loop(websocket, connection_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await _write_control(
                writer,
                source,
                "connection_error",
                connection_id,
                error_type=type(exc).__name__,
                error=str(exc),
            )
            LOGGER.warning("%s disconnected: %s", source, exc)
            delay = min(delay * 2, 60)
        finally:
            await _write_control(
                writer,
                source,
                "disconnected",
                connection_id,
                stop_requested=stop_event.is_set(),
            )
        if not stop_event.is_set():
            state.auxiliary_reconnects_total += 1
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=delay + random.random(),
                )
            except TimeoutError:
                pass


async def run_sports_stream(
    url: str,
    writer: RotatingJsonlWriter,
    state: CollectorState,
    stop_event: asyncio.Event,
) -> None:
    async def connected(websocket: Any, connection_id: str) -> None:
        while not stop_event.is_set():
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=1)
            except TimeoutError:
                continue
            except ConnectionClosed:
                return
            if isinstance(message, bytes):
                message = message.decode("utf-8", "replace")
            if message == "ping":
                await websocket.send("pong")
                continue
            await _write_json_message(
                writer,
                "sports_ws",
                message,
                connection_id,
                state,
            )

    await _run_reconnecting(
        source="sports_ws",
        url=url,
        writer=writer,
        state=state,
        stop_event=stop_event,
        connected_loop=connected,
    )


async def run_rtds_crypto_stream(
    url: str,
    writer: RotatingJsonlWriter,
    state: CollectorState,
    stop_event: asyncio.Event,
    *,
    stale_after_seconds: float = RTDS_STALE_AFTER_SECONDS,
) -> None:
    async def connected(websocket: Any, connection_id: str) -> None:
        await websocket.send(json.dumps(RTDS_SUBSCRIPTION, separators=(",", ":")))
        await _write_control(
            writer,
            "rtds",
            "subscribed",
            connection_id,
            subscriptions=RTDS_SUBSCRIPTION["subscriptions"],
        )
        last_ping = 0.0
        last_data = time.monotonic()
        while not stop_event.is_set():
            now = time.monotonic()
            if now - last_ping >= 5:
                await websocket.send("PING")
                last_ping = now
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=1)
            except TimeoutError:
                if time.monotonic() - last_data > stale_after_seconds:
                    await _write_control(
                        writer,
                        "rtds",
                        "stale",
                        connection_id,
                        stale_after_seconds=stale_after_seconds,
                    )
                    raise TimeoutError("RTDS stream produced no price data")
                continue
            except ConnectionClosed:
                return
            if isinstance(message, bytes):
                message = message.decode("utf-8", "replace")
            if message.upper() == "PONG":
                continue
            await _write_json_message(
                writer,
                "rtds",
                message,
                connection_id,
                state,
            )
            last_data = time.monotonic()

    await _run_reconnecting(
        source="rtds",
        url=url,
        writer=writer,
        state=state,
        stop_event=stop_event,
        connected_loop=connected,
    )
