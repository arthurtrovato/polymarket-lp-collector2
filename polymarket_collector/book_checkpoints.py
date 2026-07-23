from __future__ import annotations

import asyncio
import json
import logging
import time
import urllib.request
import uuid
from collections.abc import Callable, Sequence
from typing import Any

from .state import CollectorState
from .storage import RotatingJsonlWriter


LOGGER = logging.getLogger(__name__)
USER_AGENT = "polymarket-lp-collector/0.1 (+public-market-data-only)"
MAX_BATCH_SIZE = 500


def _post_books(url: str, asset_ids: Sequence[str], timeout: int = 30) -> list[dict[str, Any]]:
    request = urllib.request.Request(
        url,
        data=json.dumps(
            [{"token_id": asset_id} for asset_id in asset_ids],
            separators=(",", ":"),
        ).encode("utf-8"),
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status} from {url}")
        payload = json.loads(response.read())
    if not isinstance(payload, list):
        raise RuntimeError(f"Unexpected response type from {url}")
    return [item for item in payload if isinstance(item, dict)]


async def _wait_or_stop(stop_event: asyncio.Event, timeout: float) -> None:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=max(timeout, 0.0))
    except TimeoutError:
        pass


async def run_book_checkpoints(
    *,
    clob_base_url: str,
    asset_source: Callable[[], Sequence[str]],
    interval_seconds: int,
    writer: RotatingJsonlWriter,
    state: CollectorState,
    stop_event: asyncio.Event,
) -> None:
    if interval_seconds <= 0:
        return
    endpoint = f"{clob_base_url}/books"
    while not stop_event.is_set():
        cycle_started = time.monotonic()
        asset_ids = tuple(dict.fromkeys(str(item) for item in asset_source() if item))
        if not asset_ids:
            await _wait_or_stop(stop_event, 1)
            continue
        checkpoint_id = uuid.uuid4().hex
        try:
            books: list[dict[str, Any]] = []
            for start in range(0, len(asset_ids), MAX_BATCH_SIZE):
                batch = asset_ids[start : start + MAX_BATCH_SIZE]
                books.extend(await asyncio.to_thread(_post_books, endpoint, batch))
            received_at_ns = time.time_ns()
            received_monotonic_ns = time.monotonic_ns()
            for index, book in enumerate(books):
                await writer.write(
                    {
                        "record_type": "rest_book_checkpoint",
                        "received_at_ns": received_at_ns,
                        "received_monotonic_ns": received_monotonic_ns,
                        "checkpoint_id": checkpoint_id,
                        "checkpoint_index": index,
                        "checkpoint_book_count": len(books),
                        "requested_asset_count": len(asset_ids),
                        "payload": book,
                    }
                )
            state.book_checkpoints_total += len(books)
            state.last_book_checkpoint_at = received_at_ns / 1_000_000_000
            LOGGER.info(
                "Recorded REST checkpoint %s for %d/%d assets",
                checkpoint_id,
                len(books),
                len(asset_ids),
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            state.book_checkpoint_failures_total += 1
            await writer.write(
                {
                    "record_type": "rest_book_checkpoint_error",
                    "received_at_ns": time.time_ns(),
                    "received_monotonic_ns": time.monotonic_ns(),
                    "checkpoint_id": checkpoint_id,
                    "requested_asset_count": len(asset_ids),
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
            )
            LOGGER.warning("REST order-book checkpoint failed: %s", exc)
        await _wait_or_stop(
            stop_event,
            interval_seconds - (time.monotonic() - cycle_started),
        )
