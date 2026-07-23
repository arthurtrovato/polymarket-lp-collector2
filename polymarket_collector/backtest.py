from __future__ import annotations

import argparse
import json
import logging
import math
import time
from collections import Counter, deque
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .analytics_common import ParquetSink, iter_parquet_rows, require_pyarrow
from .orderbook import LevelGroups, OrderBook


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class BacktestConfig:
    order_size: float = 25.0
    initial_cash: float = 1_000.0
    initial_inventory: float = 100.0
    min_inventory: float = 0.0
    max_inventory: float = 200.0
    refresh_ms: int = 5_000
    latency_ms: int = 250
    quote_offset_ticks: int = 0
    inventory_skew_ticks: int = 2
    queue_ahead_fraction: float = 1.0
    markout_ms: int = 60_000
    equity_interval_ms: int = 60_000
    fee_rate: float = 0.0
    rebate_capture_rate: float = 0.0
    reward_daily_pool: float = 0.0
    assumed_reward_share: float = 0.0
    reward_max_spread_cents: float = 0.0
    reward_min_size: float = 0.0

    def validate(self) -> None:
        if self.order_size <= 0:
            raise ValueError("order_size must be positive")
        if self.initial_cash < 0:
            raise ValueError("initial_cash must be non-negative")
        if not self.min_inventory <= self.initial_inventory <= self.max_inventory:
            raise ValueError("initial inventory is outside its limits")
        if self.refresh_ms < 1 or self.latency_ms < 0:
            raise ValueError("invalid timing setting")
        if self.quote_offset_ticks < 0 or self.inventory_skew_ticks < 0:
            raise ValueError("tick offsets must be non-negative")
        if not 0 <= self.queue_ahead_fraction:
            raise ValueError("queue_ahead_fraction must be non-negative")
        if not 0 <= self.rebate_capture_rate <= 1:
            raise ValueError("rebate_capture_rate must be between zero and one")
        if not 0 <= self.assumed_reward_share <= 1:
            raise ValueError("assumed_reward_share must be between zero and one")


@dataclass(slots=True)
class Quote:
    side: str
    price: float
    remaining: float
    queue_ahead: float
    activated_ms: int


@dataclass(slots=True)
class PendingQuotes:
    activate_at_ms: int
    bid_price: float | None
    ask_price: float | None


@dataclass(slots=True)
class Fill:
    timestamp_ms: int
    side: str
    price: float
    size: float
    cash_after: float
    inventory_after: float
    fee_equivalent: float
    markout_due_ms: int
    markout_mid: float | None = None
    markout_pnl: float | None = None


def reward_order_score(
    *,
    max_spread_cents: float,
    distance_cents: float,
    size: float,
) -> float:
    if max_spread_cents <= 0 or size <= 0:
        return 0.0
    if distance_cents < 0 or distance_cents > max_spread_cents:
        return 0.0
    position_score = ((max_spread_cents - distance_cents) / max_spread_cents) ** 2
    return position_score * size


def reward_two_sided_proxy(
    *,
    mid: float,
    bid_price: float | None,
    ask_price: float | None,
    size: float,
    max_spread_cents: float,
    min_size: float,
    scaling: float = 3.0,
) -> float:
    if size < min_size:
        return 0.0
    bid_score = (
        reward_order_score(
            max_spread_cents=max_spread_cents,
            distance_cents=(mid - bid_price) * 100,
            size=size,
        )
        if bid_price is not None
        else 0.0
    )
    ask_score = (
        reward_order_score(
            max_spread_cents=max_spread_cents,
            distance_cents=(ask_price - mid) * 100,
            size=size,
        )
        if ask_price is not None
        else 0.0
    )
    if 0.10 <= mid <= 0.90:
        return max(
            min(bid_score, ask_score),
            max(bid_score / scaling, ask_score / scaling),
        )
    return min(bid_score, ask_score)


def _round_bid(price: float, tick: float) -> float:
    return round(math.floor((price + 1e-12) / tick) * tick, 10)


def _round_ask(price: float, tick: float) -> float:
    return round(math.ceil((price - 1e-12) / tick) * tick, 10)


class LPBacktester:
    def __init__(self, asset_id: str, config: BacktestConfig) -> None:
        config.validate()
        self.asset_id = asset_id
        self.config = config
        self.book = OrderBook(asset_id=asset_id)
        self.cash = config.initial_cash
        self.inventory = config.initial_inventory
        self.active_bid: Quote | None = None
        self.active_ask: Quote | None = None
        self.pending: PendingQuotes | None = None
        self.next_refresh_ms: int | None = None
        self.next_equity_ms: int | None = None
        self.first_timestamp_ms: int | None = None
        self.last_timestamp_ms: int | None = None
        self.initial_mid: float | None = None
        self.fills: list[Fill] = []
        self.pending_markouts: deque[Fill] = deque()
        self.equity: list[dict[str, Any]] = []
        self.counters = Counter[str]()
        self.fee_equivalent = 0.0
        self.score_minutes = 0.0
        self.last_score_ms: int | None = None

    def _quote_score(self) -> float:
        mid = self.book.mid
        if mid is None:
            return 0.0
        return reward_two_sided_proxy(
            mid=mid,
            bid_price=self.active_bid.price if self.active_bid else None,
            ask_price=self.active_ask.price if self.active_ask else None,
            size=self.config.order_size,
            max_spread_cents=self.config.reward_max_spread_cents,
            min_size=self.config.reward_min_size,
        )

    def _accrue_score(self, timestamp_ms: int) -> None:
        if self.last_score_ms is not None and timestamp_ms > self.last_score_ms:
            self.score_minutes += (
                self._quote_score() * (timestamp_ms - self.last_score_ms) / 60_000
            )
        self.last_score_ms = timestamp_ms

    def _cancel_quotes(self) -> None:
        self.counters["cancellations"] += sum(
            quote is not None for quote in (self.active_bid, self.active_ask)
        )
        self.active_bid = None
        self.active_ask = None
        self.pending = None

    def _schedule_quotes(self, timestamp_ms: int) -> None:
        bid, ask, mid = self.book.best_bid, self.book.best_ask, self.book.mid
        if bid is None or ask is None or mid is None or bid >= ask:
            self.counters["refresh_without_valid_book"] += 1
            return
        tick = self.book.tick_size or min(max(ask - bid, 0.001), 0.01)
        inventory_range = max(
            self.config.max_inventory - self.config.min_inventory,
            self.config.order_size,
        )
        centered_inventory = (
            self.inventory
            - (self.config.max_inventory + self.config.min_inventory) / 2
        )
        skew = round(
            centered_inventory
            / inventory_range
            * 2
            * self.config.inventory_skew_ticks
        )
        bid_price = _round_bid(
            bid - (self.config.quote_offset_ticks + skew) * tick,
            tick,
        )
        ask_price = _round_ask(
            ask + (self.config.quote_offset_ticks - skew) * tick,
            tick,
        )
        bid_price = min(max(bid_price, tick), 1 - tick)
        ask_price = min(max(ask_price, tick), 1 - tick)
        if (
            self.inventory + self.config.order_size > self.config.max_inventory
            or self.cash < bid_price * self.config.order_size
        ):
            bid_price = None
            self.counters["bid_inventory_or_cash_limited"] += 1
        if self.inventory - self.config.order_size < self.config.min_inventory:
            ask_price = None
            self.counters["ask_inventory_limited"] += 1
        self.pending = PendingQuotes(
            activate_at_ms=timestamp_ms + self.config.latency_ms,
            bid_price=bid_price,
            ask_price=ask_price,
        )
        self.counters["quote_refreshes"] += 1

    def _activate_pending(self, timestamp_ms: int) -> None:
        pending = self.pending
        if pending is None or timestamp_ms < pending.activate_at_ms:
            return
        self.pending = None
        best_bid, best_ask = self.book.best_bid, self.book.best_ask
        if pending.bid_price is not None:
            if best_ask is not None and pending.bid_price >= best_ask:
                self.counters["post_only_rejections"] += 1
            else:
                self.active_bid = Quote(
                    side="BUY",
                    price=pending.bid_price,
                    remaining=self.config.order_size,
                    queue_ahead=self.book.level_size(
                        "BUY", pending.bid_price
                    )
                    * self.config.queue_ahead_fraction,
                    activated_ms=timestamp_ms,
                )
        if pending.ask_price is not None:
            if best_bid is not None and pending.ask_price <= best_bid:
                self.counters["post_only_rejections"] += 1
            else:
                self.active_ask = Quote(
                    side="SELL",
                    price=pending.ask_price,
                    remaining=self.config.order_size,
                    queue_ahead=self.book.level_size(
                        "SELL", pending.ask_price
                    )
                    * self.config.queue_ahead_fraction,
                    activated_ms=timestamp_ms,
                )

    def _maybe_refresh(self, timestamp_ms: int) -> None:
        if self.next_refresh_ms is None:
            self.next_refresh_ms = timestamp_ms
        if timestamp_ms < self.next_refresh_ms:
            self._activate_pending(timestamp_ms)
            return
        self._cancel_quotes()
        self._schedule_quotes(timestamp_ms)
        self.next_refresh_ms = timestamp_ms + self.config.refresh_ms
        self._activate_pending(timestamp_ms)

    def _record_fill(
        self,
        quote: Quote,
        *,
        size: float,
        timestamp_ms: int,
    ) -> None:
        if size <= 0:
            return
        notional = quote.price * size
        if quote.side == "BUY":
            affordable = self.cash / quote.price if quote.price > 0 else 0
            size = min(
                size,
                affordable,
                self.config.max_inventory - self.inventory,
            )
            if size <= 0:
                return
            self.cash -= quote.price * size
            self.inventory += size
        else:
            size = min(size, self.inventory - self.config.min_inventory)
            if size <= 0:
                return
            self.cash += quote.price * size
            self.inventory -= size
        quote.remaining -= size
        fee_equivalent = (
            size * self.config.fee_rate * quote.price * (1 - quote.price)
        )
        self.fee_equivalent += fee_equivalent
        fill = Fill(
            timestamp_ms=timestamp_ms,
            side=quote.side,
            price=quote.price,
            size=size,
            cash_after=self.cash,
            inventory_after=self.inventory,
            fee_equivalent=fee_equivalent,
            markout_due_ms=timestamp_ms + self.config.markout_ms,
        )
        self.fills.append(fill)
        self.pending_markouts.append(fill)
        self.counters["fills"] += 1
        self.counters[f"{quote.side.lower()}_fills"] += 1
        if quote.remaining <= 1e-12:
            if quote.side == "BUY":
                self.active_bid = None
            else:
                self.active_ask = None

    def _process_trade(
        self,
        *,
        trade_side: str,
        trade_price: float,
        trade_size: float,
        timestamp_ms: int,
    ) -> None:
        if trade_side == "BUY":
            quote = self.active_ask
            if quote is None or trade_price < quote.price:
                return
        elif trade_side == "SELL":
            quote = self.active_bid
            if quote is None or trade_price > quote.price:
                return
        else:
            self.counters["trades_without_side"] += 1
            return
        if abs(trade_price - quote.price) <= 1e-10:
            volume_after_queue = max(0.0, trade_size - quote.queue_ahead)
            quote.queue_ahead = max(0.0, quote.queue_ahead - trade_size)
            fill_size = min(quote.remaining, volume_after_queue)
        else:
            quote.queue_ahead = 0.0
            fill_size = quote.remaining
        self._record_fill(quote, size=fill_size, timestamp_ms=timestamp_ms)

    def _resolve_markouts(self, timestamp_ms: int) -> None:
        mid = self.book.mid
        if mid is None:
            return
        while (
            self.pending_markouts
            and self.pending_markouts[0].markout_due_ms <= timestamp_ms
        ):
            fill = self.pending_markouts.popleft()
            fill.markout_mid = mid
            if fill.side == "BUY":
                fill.markout_pnl = (mid - fill.price) * fill.size
            else:
                fill.markout_pnl = (fill.price - mid) * fill.size

    def _record_equity(self, timestamp_ms: int, *, force: bool = False) -> None:
        mid = self.book.mid
        if mid is None:
            return
        if self.initial_mid is None:
            self.initial_mid = mid
        if self.next_equity_ms is None:
            self.next_equity_ms = timestamp_ms
        if not force and timestamp_ms < self.next_equity_ms:
            return
        initial_value = (
            self.config.initial_cash + self.config.initial_inventory * self.initial_mid
        )
        value = self.cash + self.inventory * mid
        self.equity.append(
            {
                "timestamp_ms": timestamp_ms,
                "mid": mid,
                "cash": self.cash,
                "inventory": self.inventory,
                "equity": value,
                "gross_pnl": value - initial_value,
                "hold_equity": (
                    self.config.initial_cash
                    + self.config.initial_inventory * mid
                ),
                "lp_excess_pnl": value
                - (
                    self.config.initial_cash
                    + self.config.initial_inventory * mid
                ),
                "active_bid": self.active_bid.price if self.active_bid else None,
                "active_ask": self.active_ask.price if self.active_ask else None,
            }
        )
        self.next_equity_ms = timestamp_ms + self.config.equity_interval_ms

    def process(
        self,
        event: dict[str, Any],
        *,
        snapshot_levels: list[dict[str, Any]] | None = None,
    ) -> None:
        timestamp = event.get("exchange_timestamp_ms")
        if timestamp is None:
            return
        timestamp_ms = int(timestamp)
        if self.first_timestamp_ms is None:
            self.first_timestamp_ms = timestamp_ms
        self._accrue_score(timestamp_ms)
        self.last_timestamp_ms = max(timestamp_ms, self.last_timestamp_ms or timestamp_ms)
        event_type = str(event.get("event_type") or "")
        if event.get("market"):
            self.book.market = str(event["market"])
        if event_type == "book":
            self.book.load_snapshot(snapshot_levels or [])
            self.book.tick_size = event.get("tick_size") or self.book.tick_size
            self.book.last_trade_price = (
                event.get("last_trade_price") or self.book.last_trade_price
            )
            self.counters["book_resets"] += 1
        elif event_type == "price_change" and self.book.initialized:
            try:
                self.book.apply_change(
                    str(event.get("side") or ""),
                    float(event["price"]),
                    float(event["size"]),
                )
            except (KeyError, TypeError, ValueError):
                self.counters["invalid_price_changes"] += 1
        elif event_type == "tick_size_change":
            if event.get("tick_size") is not None:
                self.book.tick_size = float(event["tick_size"])

        if not self.book.initialized:
            return
        observed_bid, observed_ask = event.get("best_bid"), event.get("best_ask")
        if observed_bid is not None or observed_ask is not None:
            removed = self.book.reconcile_top(
                best_bid=(
                    float(observed_bid) if observed_bid is not None else None
                ),
                best_ask=(
                    float(observed_ask) if observed_ask is not None else None
                ),
                tolerance=(self.book.tick_size or 0.001) / 2 + 1e-12,
            )
            if removed:
                self.counters["levels_pruned_by_authoritative_top"] += removed
        if event_type == "best_bid_ask":
            self.counters["top_notifications_deferred"] += 1
            return
        if event_type == "price_change":
            change_index = event.get("change_index")
            change_count = event.get("change_count")
            if (
                change_index is not None
                and change_count is not None
                and int(change_index) + 1 < int(change_count)
            ):
                return
        if self.initial_mid is None and self.book.mid is not None:
            self.initial_mid = self.book.mid
        self._maybe_refresh(timestamp_ms)
        if event_type == "last_trade_price":
            self.counters["trades_seen"] += 1
            try:
                self._process_trade(
                    trade_side=str(event.get("side") or "").upper(),
                    trade_price=float(event["price"]),
                    trade_size=float(event["size"]),
                    timestamp_ms=timestamp_ms,
                )
            except (KeyError, TypeError, ValueError):
                self.counters["invalid_trades"] += 1
            if event.get("price") is not None:
                self.book.last_trade_price = float(event["price"])
        self._resolve_markouts(timestamp_ms)
        self._record_equity(timestamp_ms)

    def finish(self) -> dict[str, Any]:
        if self.last_timestamp_ms is None or self.initial_mid is None:
            raise RuntimeError("No initialized order book was available")
        self._accrue_score(self.last_timestamp_ms)
        self._record_equity(self.last_timestamp_ms, force=True)
        duration_ms = max(
            0,
            self.last_timestamp_ms - (self.first_timestamp_ms or self.last_timestamp_ms),
        )
        final_mid = self.book.mid or self.initial_mid
        initial_value = (
            self.config.initial_cash + self.config.initial_inventory * self.initial_mid
        )
        final_value = self.cash + self.inventory * final_mid
        gross_pnl = final_value - initial_value
        buy_and_hold_value = (
            self.config.initial_cash
            + self.config.initial_inventory * final_mid
        )
        buy_and_hold_pnl = buy_and_hold_value - initial_value
        lp_excess_pnl = final_value - buy_and_hold_value
        maker_rebate = self.fee_equivalent * self.config.rebate_capture_rate
        liquidity_reward = (
            self.config.reward_daily_pool
            * duration_ms
            / 86_400_000
            * self.config.assumed_reward_share
        )
        peak = -math.inf
        max_drawdown = 0.0
        excess_peak = -math.inf
        max_excess_drawdown = 0.0
        for point in self.equity:
            peak = max(peak, point["equity"])
            max_drawdown = max(max_drawdown, peak - point["equity"])
            excess_peak = max(excess_peak, point["lp_excess_pnl"])
            max_excess_drawdown = max(
                max_excess_drawdown,
                excess_peak - point["lp_excess_pnl"],
            )
        markouts = [
            fill.markout_pnl
            for fill in self.fills
            if fill.markout_pnl is not None
        ]
        return {
            "asset_id": self.asset_id,
            "market": self.book.market,
            "start_timestamp_ms": self.first_timestamp_ms,
            "end_timestamp_ms": self.last_timestamp_ms,
            "duration_hours": duration_ms / 3_600_000,
            "initial_mid": self.initial_mid,
            "final_mid": final_mid,
            "initial_value": initial_value,
            "final_mark_to_market_value": final_value,
            "final_buy_and_hold_value": buy_and_hold_value,
            "gross_trading_pnl": gross_pnl,
            "gross_mark_to_market_pnl": gross_pnl,
            "buy_and_hold_pnl": buy_and_hold_pnl,
            "lp_excess_pnl_vs_hold": lp_excess_pnl,
            "maker_fee_paid": 0.0,
            "fee_equivalent_generated": self.fee_equivalent,
            "maker_rebate_estimate": maker_rebate,
            "liquidity_reward_estimate": liquidity_reward,
            "net_pnl_with_enabled_estimates": gross_pnl
            + maker_rebate
            + liquidity_reward,
            "net_excess_pnl_vs_hold_with_enabled_estimates": lp_excess_pnl
            + maker_rebate
            + liquidity_reward,
            "final_cash": self.cash,
            "final_inventory": self.inventory,
            "fills": len(self.fills),
            "filled_shares": sum(fill.size for fill in self.fills),
            "max_drawdown": max_drawdown,
            "max_excess_drawdown": max_excess_drawdown,
            "resolved_markouts": len(markouts),
            "unresolved_markouts": len(self.pending_markouts),
            "mean_60s_markout_pnl": (
                sum(markouts) / len(markouts) if markouts else None
            ),
            "mean_markout_pnl": (
                sum(markouts) / len(markouts) if markouts else None
            ),
            "single_token_liquidity_score_minutes": self.score_minutes,
            "counters": dict(self.counters),
            "config": asdict(self.config),
            "caveats": [
                "Queue position is estimated from aggregate level size; order-level FIFO data is unavailable.",
                "Maker rebates are zero unless a rebate_capture_rate assumption is supplied.",
                "Liquidity rewards are zero unless an assumed_reward_share is supplied.",
                "The liquidity score is a single-token proxy; official rewards combine complementary outcome books and normalize across makers.",
                "Resolution PnL, gas, funding transfers, API failures, and exchange maintenance are not simulated.",
            ],
        }


def _fill_schema() -> Any:
    pa, _ = require_pyarrow()
    return pa.schema(
        [
            ("timestamp_ms", pa.int64()),
            ("side", pa.string()),
            ("price", pa.float64()),
            ("size", pa.float64()),
            ("cash_after", pa.float64()),
            ("inventory_after", pa.float64()),
            ("fee_equivalent", pa.float64()),
            ("markout_due_ms", pa.int64()),
            ("markout_mid", pa.float64()),
            ("markout_pnl", pa.float64()),
        ]
    )


def _equity_schema() -> Any:
    pa, _ = require_pyarrow()
    return pa.schema(
        [
            ("timestamp_ms", pa.int64()),
            ("mid", pa.float64()),
            ("cash", pa.float64()),
            ("inventory", pa.float64()),
            ("equity", pa.float64()),
            ("gross_pnl", pa.float64()),
            ("hold_equity", pa.float64()),
            ("lp_excess_pnl", pa.float64()),
            ("active_bid", pa.float64()),
            ("active_ask", pa.float64()),
        ]
    )


def choose_asset(events_path: str | Path) -> str:
    books = Counter[str]()
    trades = Counter[str]()
    for row in iter_parquet_rows(
        events_path,
        columns=["event_type", "asset_id"],
    ):
        asset_id = str(row.get("asset_id") or "")
        if not asset_id:
            continue
        if row["event_type"] == "book":
            books[asset_id] += 1
        elif row["event_type"] == "last_trade_price":
            trades[asset_id] += 1
    candidates = [asset for asset in books if trades[asset] > 0]
    if not candidates:
        raise RuntimeError("No asset has both book snapshots and trades")
    return max(candidates, key=lambda asset: (trades[asset], books[asset], asset))


def market_settings(
    markets_path: str | Path | None,
    asset_id: str,
) -> dict[str, float]:
    if markets_path is None:
        return {}
    selected: dict[str, Any] | None = None
    for row in iter_parquet_rows(
        markets_path,
        columns=[
            "received_at_ns",
            "token_id",
            "daily_reward",
            "rewards_max_spread_cents",
            "rewards_min_size",
        ],
    ):
        if str(row.get("token_id") or "") != asset_id:
            continue
        if selected is None or (row.get("received_at_ns") or 0) > (
            selected.get("received_at_ns") or 0
        ):
            selected = row
    if selected is None:
        return {}
    return {
        "reward_daily_pool": float(selected.get("daily_reward") or 0),
        "reward_max_spread_cents": float(
            selected.get("rewards_max_spread_cents") or 0
        ),
        "reward_min_size": float(selected.get("rewards_min_size") or 0),
    }


def run_backtest(
    events_path: str | Path,
    levels_path: str | Path,
    output_dir: str | Path,
    *,
    asset_id: str | None = None,
    config: BacktestConfig | None = None,
) -> dict[str, Any]:
    selected_asset = asset_id or choose_asset(events_path)
    engine = LPBacktester(selected_asset, config or BacktestConfig())
    groups = LevelGroups(levels_path)
    columns = [
        "sequence",
        "event_type",
        "exchange_timestamp_ms",
        "market",
        "asset_id",
        "change_index",
        "change_count",
        "side",
        "price",
        "size",
        "best_bid",
        "best_ask",
        "tick_size",
        "last_trade_price",
    ]
    for event in iter_parquet_rows(events_path, columns=columns):
        if str(event.get("asset_id") or "") != selected_asset:
            continue
        levels = (
            groups.for_sequence(int(event["sequence"]))
            if event["event_type"] == "book"
            else None
        )
        engine.process(event, snapshot_levels=levels)
    summary = engine.finish()
    output = Path(output_dir).expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    fill_sink = ParquetSink(output / "fills.parquet", _fill_schema(), batch_size=20_000)
    for fill in engine.fills:
        fill_sink.add(asdict(fill))
    fill_sink.close()
    equity_sink = ParquetSink(
        output / "equity.parquet",
        _equity_schema(),
        batch_size=20_000,
    )
    for point in engine.equity:
        equity_sink.add(point)
    equity_sink.close()
    summary_path = output / "summary.json"
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return summary


def cli() -> None:
    parser = argparse.ArgumentParser(
        description="Backtest a conservative passive LP strategy on reconstructed data."
    )
    parser.add_argument("--events", required=True)
    parser.add_argument("--book-levels", required=True)
    parser.add_argument("--markets")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--asset-id")
    parser.add_argument("--order-size", type=float, default=25)
    parser.add_argument("--initial-cash", type=float, default=1_000)
    parser.add_argument("--initial-inventory", type=float, default=100)
    parser.add_argument("--min-inventory", type=float, default=0)
    parser.add_argument("--max-inventory", type=float, default=200)
    parser.add_argument("--refresh-ms", type=int, default=5_000)
    parser.add_argument("--latency-ms", type=int, default=250)
    parser.add_argument("--quote-offset-ticks", type=int, default=0)
    parser.add_argument("--inventory-skew-ticks", type=int, default=2)
    parser.add_argument("--queue-ahead-fraction", type=float, default=1)
    parser.add_argument("--markout-ms", type=int, default=60_000)
    parser.add_argument("--fee-rate", type=float, default=0)
    parser.add_argument("--rebate-capture-rate", type=float, default=0)
    parser.add_argument("--assumed-reward-share", type=float, default=0)
    args = parser.parse_args()
    asset_id = args.asset_id or choose_asset(args.events)
    rewards = market_settings(args.markets, asset_id)
    config = BacktestConfig(
        order_size=args.order_size,
        initial_cash=args.initial_cash,
        initial_inventory=args.initial_inventory,
        min_inventory=args.min_inventory,
        max_inventory=args.max_inventory,
        refresh_ms=args.refresh_ms,
        latency_ms=args.latency_ms,
        quote_offset_ticks=args.quote_offset_ticks,
        inventory_skew_ticks=args.inventory_skew_ticks,
        queue_ahead_fraction=args.queue_ahead_fraction,
        markout_ms=args.markout_ms,
        fee_rate=args.fee_rate,
        rebate_capture_rate=args.rebate_capture_rate,
        assumed_reward_share=args.assumed_reward_share,
        **rewards,
    )
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    summary = run_backtest(
        args.events,
        args.book_levels,
        args.output_dir,
        asset_id=asset_id,
        config=config,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    cli()
