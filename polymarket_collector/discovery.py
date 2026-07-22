from __future__ import annotations

import asyncio
import json
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


USER_AGENT = "polymarket-lp-collector/0.1 (+public-market-data-only)"
PAGE_SIZE = 500
CURRENT_REWARDS_PAGE_LIMIT = 4
CANDIDATE_MULTIPLIER = 4


@dataclass(frozen=True, slots=True)
class MarketSelection:
    markets: tuple[dict[str, Any], ...]
    asset_ids: tuple[str, ...]


def daily_reward(market: dict[str, Any]) -> float:
    explicit = market.get("total_daily_rate")
    if explicit is not None:
        try:
            return float(explicit)
        except (TypeError, ValueError):
            pass
    total = 0.0
    for reward in market.get("rewards_config") or []:
        try:
            total += float(reward.get("rate_per_day") or 0)
        except (TypeError, ValueError):
            continue
    return total


def select_markets(
    markets: list[dict[str, Any]],
    *,
    max_markets: int,
    min_daily_reward: float,
    min_volume_24h: float,
) -> MarketSelection:
    eligible: list[dict[str, Any]] = []
    for market in markets:
        tokens = market.get("tokens") or []
        token_ids = [str(t.get("token_id", "")) for t in tokens if t.get("token_id")]
        if len(token_ids) < 2:
            continue
        try:
            volume = float(market.get("volume_24hr") or 0)
        except (TypeError, ValueError):
            volume = 0.0
        reward = daily_reward(market)
        if reward < min_daily_reward or volume < min_volume_24h:
            continue
        normalized = dict(market)
        normalized["collector_daily_reward"] = reward
        normalized["collector_volume_24hr"] = volume
        eligible.append(normalized)

    eligible.sort(
        key=lambda m: (
            float(m.get("collector_daily_reward") or 0),
            float(m.get("collector_volume_24hr") or 0),
        ),
        reverse=True,
    )
    selected = eligible[:max_markets]

    seen: set[str] = set()
    asset_ids: list[str] = []
    for market in selected:
        for token in market.get("tokens") or []:
            token_id = str(token.get("token_id", ""))
            if token_id and token_id not in seen:
                seen.add(token_id)
                asset_ids.append(token_id)
    return MarketSelection(tuple(selected), tuple(asset_ids))


def _get_json(url: str, timeout: int = 30) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status} from {url}")
        payload = json.loads(response.read())
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unexpected response type from {url}")
    return payload


async def discover_rewarded_markets(
    base_url: str,
    *,
    max_markets: int,
    min_daily_reward: float,
    min_volume_24h: float,
) -> MarketSelection:
    endpoint = f"{base_url}/rewards/markets/multi"
    cursor: str | None = None
    all_markets: list[dict[str, Any]] = []
    # The endpoint can contain many thousands of short-lived sports markets.
    # It is already sorted by reward rate, so fetching a bounded candidate set
    # avoids delaying the initial websocket subscription by several minutes.
    candidate_limit = max(PAGE_SIZE, max_markets * CANDIDATE_MULTIPLIER)

    for _ in range(100):
        params: list[tuple[str, str]] = [
            ("page_size", str(PAGE_SIZE)),
            ("order_by", "rate_per_day"),
            ("position", "DESC"),
        ]
        if min_volume_24h > 0:
            params.append(("min_volume_24hr", str(min_volume_24h)))
        if cursor:
            params.append(("next_cursor", cursor))
        url = f"{endpoint}?{urllib.parse.urlencode(params)}"
        payload = await asyncio.to_thread(_get_json, url)
        page = payload.get("data") or []
        if not isinstance(page, list):
            raise RuntimeError("Rewards API returned a non-list data field")
        all_markets.extend(item for item in page if isinstance(item, dict))
        cursor = str(payload.get("next_cursor") or "LTE=")
        if cursor == "LTE=" or not page or len(all_markets) >= candidate_limit:
            break
    else:
        raise RuntimeError("Rewards pagination exceeded 100 pages")

    # The current endpoint includes native + sponsored daily totals. Preserve
    # both raw responses so historic selection decisions remain reproducible.
    current_by_condition: dict[str, dict[str, Any]] = {}
    cursor = None
    current_endpoint = f"{base_url}/rewards/markets/current"
    # Current rewards are not sorted and can also span many thousands of
    # entries. A bounded enrichment pass keeps discovery timely; the raw
    # candidate reward configuration remains available as the fallback.
    for _ in range(CURRENT_REWARDS_PAGE_LIMIT):
        params = []
        if cursor:
            params.append(("next_cursor", cursor))
        suffix = f"?{urllib.parse.urlencode(params)}" if params else ""
        payload = await asyncio.to_thread(_get_json, current_endpoint + suffix)
        page = payload.get("data") or []
        if not isinstance(page, list):
            raise RuntimeError("Current rewards API returned a non-list data field")
        for item in page:
            if isinstance(item, dict) and item.get("condition_id"):
                current_by_condition[str(item["condition_id"])] = item
        cursor = str(payload.get("next_cursor") or "LTE=")
        if cursor == "LTE=" or not page:
            break

    for market in all_markets:
        current = current_by_condition.get(str(market.get("condition_id", "")))
        if current:
            market["current_rewards"] = current
            if current.get("total_daily_rate") is not None:
                market["total_daily_rate"] = current["total_daily_rate"]

    return select_markets(
        all_markets,
        max_markets=max_markets,
        min_daily_reward=min_daily_reward,
        min_volume_24h=min_volume_24h,
    )
