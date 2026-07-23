from __future__ import annotations

import asyncio
import argparse
import json
import logging
import signal
import sys
import time
from typing import Any

from .auxiliary_streams import run_rtds_crypto_stream, run_sports_stream
from .book_checkpoints import run_book_checkpoints
from .config import Config
from .discovery import (
    MarketSelection,
    discover_rewarded_markets,
    discover_rewarded_universe,
    enrich_selected_markets,
)
from .health import HealthServer
from .state import CollectorState
from .storage import RotatingJsonlWriter
from .stream import MarketStream


LOGGER = logging.getLogger(__name__)


async def _wait_or_stop(stop_event: asyncio.Event, timeout: float) -> None:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout)
    except TimeoutError:
        pass


async def _discover(config: Config) -> MarketSelection:
    return await discover_rewarded_markets(
        config.clob_base_url,
        max_markets=config.max_markets,
        min_daily_reward=config.min_daily_reward,
        min_volume_24h=config.min_volume_24h,
    )


async def run(config: Config) -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    state = CollectorState()
    market_writer = RotatingJsonlWriter(
        config.data_dir / "market_ws",
        "market_ws",
        rotation_seconds=config.rotation_seconds,
        max_file_bytes=config.max_file_bytes,
        flush_interval_seconds=config.flush_interval_seconds,
    )
    discovery_writer = RotatingJsonlWriter(
        config.data_dir / "discovery",
        "discovery",
        rotation_seconds=config.rotation_seconds,
        max_file_bytes=config.max_file_bytes,
        flush_interval_seconds=config.flush_interval_seconds,
    )
    sports_writer = (
        RotatingJsonlWriter(
            config.data_dir / "sports_ws",
            "sports_ws",
            rotation_seconds=config.rotation_seconds,
            max_file_bytes=config.max_file_bytes,
            flush_interval_seconds=config.flush_interval_seconds,
        )
        if config.collect_sports
        else None
    )
    rtds_writer = (
        RotatingJsonlWriter(
            config.data_dir / "rtds",
            "rtds",
            rotation_seconds=config.rotation_seconds,
            max_file_bytes=config.max_file_bytes,
            flush_interval_seconds=config.flush_interval_seconds,
        )
        if config.collect_rtds_crypto
        else None
    )
    writers = [
        writer
        for writer in (
            market_writer,
            discovery_writer,
            sports_writer,
            rtds_writer,
        )
        if writer is not None
    ]
    await asyncio.gather(*(writer.start() for writer in writers))

    health = HealthServer(
        state,
        config.health_host,
        config.health_port,
        config.stale_after_seconds,
    )
    await health.start()
    LOGGER.info(
        "Health endpoint listening on http://%s:%d/healthz",
        config.health_host,
        config.health_port,
    )

    stream = MarketStream(config.websocket_url, market_writer, state)
    stream_task = asyncio.create_task(stream.run(stop_event), name="market-stream")
    background_tasks = [stream_task]
    if sports_writer is not None:
        background_tasks.append(
            asyncio.create_task(
                run_sports_stream(
                    config.sports_websocket_url,
                    sports_writer,
                    state,
                    stop_event,
                ),
                name="sports-stream",
            )
        )
    if rtds_writer is not None:
        background_tasks.append(
            asyncio.create_task(
                run_rtds_crypto_stream(
                    config.rtds_websocket_url,
                    rtds_writer,
                    state,
                    stop_event,
                ),
                name="rtds-stream",
            )
        )
    if config.book_checkpoint_interval_seconds > 0:
        background_tasks.append(
            asyncio.create_task(
                run_book_checkpoints(
                    clob_base_url=config.clob_base_url,
                    asset_source=lambda: stream.desired_assets,
                    interval_seconds=config.book_checkpoint_interval_seconds,
                    writer=market_writer,
                    state=state,
                    stop_event=stop_event,
                ),
                name="book-checkpoints",
            )
        )

    try:
        failure_delay = 10.0
        while not stop_event.is_set():
            cycle_started = time.monotonic()
            try:
                selection = await _discover(config)
                if stop_event.is_set():
                    break
                discovered_at_ns = time.time_ns()
                state.discovered_at = discovered_at_ns / 1_000_000_000
                state.markets = len(selection.markets)
                state.assets = len(selection.asset_ids)
                state.last_error = None
                # Start or update the irreversible L2 capture before running
                # slower metadata enrichment and useful-universe snapshot.
                stream.set_assets(selection.asset_ids)
                try:
                    selection = await enrich_selected_markets(
                        selection,
                        clob_base_url=config.clob_base_url,
                        gamma_base_url=config.gamma_base_url,
                    )
                except Exception as exc:
                    LOGGER.warning("Selected-market enrichment failed: %s", exc)
                    await discovery_writer.write(
                        {
                            "record_type": "market_enrichment_error",
                            "received_at_ns": time.time_ns(),
                            "error_type": type(exc).__name__,
                            "error": str(exc),
                        }
                    )
                if stop_event.is_set():
                    break
                await discovery_writer.write(
                    {
                        "record_type": "rewarded_market_discovery",
                        "received_at_ns": discovered_at_ns,
                        "market_count": len(selection.markets),
                        "asset_count": len(selection.asset_ids),
                        "candidate_count": selection.candidate_count,
                        "markets": list(selection.markets),
                    }
                )
                LOGGER.info(
                    "Selected %d rewarded markets (%d assets)",
                    len(selection.markets),
                    len(selection.asset_ids),
                )
                try:
                    universe = await discover_rewarded_universe(
                        config.clob_base_url
                    )
                    if stop_event.is_set():
                        break
                    await discovery_writer.write(
                        {
                            "record_type": "rewarded_market_universe",
                            "received_at_ns": time.time_ns(),
                            "market_count": len(universe.markets),
                            "coverage": universe.coverage,
                            "markets": list(universe.markets),
                        }
                    )
                    LOGGER.info(
                        "Archived useful rewarded universe (%d markets)",
                        len(universe.markets),
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    state.universe_failures_total += 1
                    LOGGER.warning("Rewarded-universe scan failed: %s", exc)
                    await discovery_writer.write(
                        {
                            "record_type": "rewarded_market_universe_error",
                            "received_at_ns": time.time_ns(),
                            "error_type": type(exc).__name__,
                            "error": str(exc),
                        }
                    )
                failure_delay = 10.0
                await _wait_or_stop(
                    stop_event,
                    max(
                        0,
                        config.discovery_interval_seconds
                        - (time.monotonic() - cycle_started),
                    ),
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                state.discovery_failures_total += 1
                state.last_error = f"discovery: {type(exc).__name__}: {exc}"
                LOGGER.exception("Market discovery failed")
                await discovery_writer.write(
                    {
                        "record_type": "discovery_error",
                        "received_at_ns": time.time_ns(),
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    }
                )
                await _wait_or_stop(stop_event, failure_delay)
                failure_delay = min(failure_delay * 2, 300)
    finally:
        stop_event.set()
        try:
            await asyncio.wait_for(
                asyncio.gather(*background_tasks),
                timeout=10,
            )
        except TimeoutError:
            for task in background_tasks:
                task.cancel()
            await asyncio.gather(*background_tasks, return_exceptions=True)
        await health.close()
        await asyncio.gather(*(writer.close() for writer in writers))
        LOGGER.info("Collector stopped cleanly")


def cli() -> None:
    try:
        parser = argparse.ArgumentParser(
            description="Collect public L2 data for rewarded Polymarket markets."
        )
        parser.add_argument(
            "--check",
            action="store_true",
            help="test public API discovery and exit without opening the collector",
        )
        args = parser.parse_args()
        config = Config.from_env()
        logging.basicConfig(
            level=getattr(logging, config.log_level, logging.INFO),
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )
        if args.check:
            selection = asyncio.run(_discover(config))
            print(
                json.dumps(
                    {
                        "ok": True,
                        "markets": len(selection.markets),
                        "assets": len(selection.asset_ids),
                        "top_markets": [
                            {
                                "question": market.get("question"),
                                "daily_reward": market.get("collector_daily_reward"),
                                "volume_24hr": market.get("collector_volume_24hr"),
                            }
                            for market in selection.markets[:5]
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
        else:
            asyncio.run(run(config))
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        print(f"fatal: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    cli()
