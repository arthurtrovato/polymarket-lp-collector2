from __future__ import annotations

import asyncio
import argparse
import json
import logging
import signal
import sys
import time
from typing import Any

from .config import Config
from .discovery import MarketSelection, discover_rewarded_markets
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
    await market_writer.start()
    await discovery_writer.start()

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

    try:
        failure_delay = 10.0
        while not stop_event.is_set():
            try:
                selection = await _discover(config)
                discovered_at_ns = time.time_ns()
                state.discovered_at = discovered_at_ns / 1_000_000_000
                state.markets = len(selection.markets)
                state.assets = len(selection.asset_ids)
                state.last_error = None
                await discovery_writer.write(
                    {
                        "record_type": "rewarded_market_discovery",
                        "received_at_ns": discovered_at_ns,
                        "market_count": len(selection.markets),
                        "asset_count": len(selection.asset_ids),
                        "markets": list(selection.markets),
                    }
                )
                stream.set_assets(selection.asset_ids)
                LOGGER.info(
                    "Selected %d rewarded markets (%d assets)",
                    len(selection.markets),
                    len(selection.asset_ids),
                )
                failure_delay = 10.0
                await _wait_or_stop(
                    stop_event, config.discovery_interval_seconds
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
            await asyncio.wait_for(stream_task, timeout=10)
        except TimeoutError:
            stream_task.cancel()
            await asyncio.gather(stream_task, return_exceptions=True)
        await health.close()
        await asyncio.gather(
            market_writer.close(), discovery_writer.close()
        )
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
