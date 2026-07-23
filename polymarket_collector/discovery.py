from __future__ import annotations

import asyncio
import json
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


USER_AGENT = "polymarket-lp-collector/0.1 (+public-market-data-only)"
PAGE_SIZE = 500
MAX_PAGES = 100
ENRICHMENT_CONCURRENCY = 12


@dataclass(frozen=True, slots=True)
class MarketSelection:
    markets: tuple[dict[str, Any], ...]
    asset_ids: tuple[str, ...]
    candidate_count: int = 0


@dataclass(frozen=True, slots=True)
class RewardedUniverseSnapshot:
    markets: tuple[dict[str, Any], ...]
    coverage: dict[str, Any]


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
    return MarketSelection(
        tuple(selected),
        tuple(asset_ids),
        candidate_count=len(markets),
    )


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
    """Quickly select the most valuable markets so streaming can start."""
    endpoint = f"{base_url}/rewards/markets/multi"
    params: list[tuple[str, str]] = [
        ("page_size", str(PAGE_SIZE)),
        ("order_by", "rate_per_day"),
        ("position", "DESC"),
    ]
    if min_volume_24h > 0:
        params.append(("min_volume_24hr", str(min_volume_24h)))
    url = f"{endpoint}?{urllib.parse.urlencode(params)}"
    payload = await asyncio.to_thread(_get_json, url)
    page = payload.get("data") or []
    if not isinstance(page, list):
        raise RuntimeError("Rewards API returned a non-list data field")
    candidates = [item for item in page if isinstance(item, dict)]

    return select_markets(
        candidates,
        max_markets=max_markets,
        min_daily_reward=min_daily_reward,
        min_volume_24h=min_volume_24h,
    )


async def _fetch_pages(
    endpoint: str,
    *,
    base_params: list[tuple[str, str]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    cursor: str | None = None
    for _ in range(MAX_PAGES):
        params = list(base_params)
        if cursor:
            params.append(("next_cursor", cursor))
        suffix = f"?{urllib.parse.urlencode(params)}" if params else ""
        payload = await asyncio.to_thread(_get_json, endpoint + suffix)
        page = payload.get("data") or []
        if not isinstance(page, list):
            raise RuntimeError(f"Paginated endpoint returned non-list data: {endpoint}")
        rows.extend(item for item in page if isinstance(item, dict))
        cursor = str(payload.get("next_cursor") or "LTE=")
        if cursor == "LTE=" or not page:
            return rows
    raise RuntimeError(f"Pagination exceeded {MAX_PAGES} pages: {endpoint}")


async def discover_rewarded_universe(
    base_url: str,
) -> RewardedUniverseSnapshot:
    """Archive the useful LP universe without scanning every low-rate market.

    The standard current-rewards endpoint can expose tens of thousands of
    native-reward markets and takes longer than a collection cycle to exhaust.
    The first rich page is explicitly sorted by reward rate, while the much
    smaller sponsored set is fully paginated. Coverage metadata makes this
    deliberate boundary machine-readable.
    """
    rich_params = [
        ("page_size", str(PAGE_SIZE)),
        ("order_by", "rate_per_day"),
        ("position", "DESC"),
    ]
    rich_url = (
        f"{base_url}/rewards/markets/multi?"
        f"{urllib.parse.urlencode(rich_params)}"
    )
    rich_payload, current = await asyncio.gather(
        asyncio.to_thread(_get_json, rich_url),
        _fetch_pages(
            f"{base_url}/rewards/markets/current",
            base_params=[
                ("page_size", str(PAGE_SIZE)),
                ("sponsored", "true"),
            ],
        ),
    )
    rich_page = rich_payload.get("data") or []
    if not isinstance(rich_page, list):
        raise RuntimeError("Rich rewards endpoint returned non-list data")
    markets = [item for item in rich_page if isinstance(item, dict)]
    current_by_condition = {
        str(item["condition_id"]): item
        for item in current
        if item.get("condition_id")
    }
    merged: dict[str, dict[str, Any]] = {}
    for market in markets:
        condition_id = str(market.get("condition_id") or "")
        if not condition_id:
            continue
        normalized = dict(market)
        current_rewards = current_by_condition.get(condition_id)
        if current_rewards:
            normalized["current_rewards"] = current_rewards
            if current_rewards.get("total_daily_rate") is not None:
                normalized["total_daily_rate"] = current_rewards["total_daily_rate"]
        normalized["collector_daily_reward"] = daily_reward(normalized)
        try:
            normalized["collector_volume_24hr"] = float(
                normalized.get("volume_24hr") or 0
            )
        except (TypeError, ValueError):
            normalized["collector_volume_24hr"] = 0.0
        merged[condition_id] = normalized

    # Preserve a compact row even if the current endpoint briefly exposes a
    # condition that is absent from the rich market listing.
    for condition_id, current_rewards in current_by_condition.items():
        if condition_id in merged:
            continue
        merged[condition_id] = {
            "condition_id": condition_id,
            "current_rewards": current_rewards,
            "total_daily_rate": current_rewards.get("total_daily_rate"),
            "collector_daily_reward": daily_reward(current_rewards),
            "collector_volume_24hr": 0.0,
            "collector_metadata_incomplete": True,
        }

    sorted_markets = tuple(
        sorted(
            merged.values(),
            key=lambda market: (
                float(market.get("collector_daily_reward") or 0),
                float(market.get("collector_volume_24hr") or 0),
            ),
            reverse=True,
        )
    )
    rich_next_cursor = str(rich_payload.get("next_cursor") or "LTE=")
    return RewardedUniverseSnapshot(
        markets=sorted_markets,
        coverage={
            "scope": "top_rewarded_plus_all_sponsored",
            "rich_market_count": len(markets),
            "rich_market_limit": PAGE_SIZE,
            "rich_market_order": "rate_per_day DESC",
            "rich_market_complete": rich_next_cursor == "LTE=",
            "rich_market_next_cursor": rich_next_cursor,
            "sponsored_current_count": len(current),
            "sponsored_current_complete": True,
        },
    )


async def enrich_selected_markets(
    selection: MarketSelection,
    *,
    clob_base_url: str,
    gamma_base_url: str,
) -> MarketSelection:
    """Best-effort enrichment; one failed endpoint never drops a market."""
    semaphore = asyncio.Semaphore(ENRICHMENT_CONCURRENCY)

    async def fetch(label: str, url: str) -> tuple[str, dict[str, Any] | None, str | None]:
        try:
            async with semaphore:
                payload = await asyncio.to_thread(_get_json, url)
            return label, payload, None
        except Exception as exc:
            return label, None, f"{type(exc).__name__}: {exc}"

    async def enrich(market: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(market)
        condition_id = str(market.get("condition_id") or "")
        market_id = str(market.get("market_id") or "")
        event_id = str(market.get("event_id") or "")
        requests = [
            fetch(
                "raw_rewards",
                f"{clob_base_url}/rewards/markets/"
                f"{urllib.parse.quote(condition_id, safe='')}?sponsored=true",
            ),
            fetch(
                "clob_market_info",
                f"{clob_base_url}/clob-markets/"
                f"{urllib.parse.quote(condition_id, safe='')}",
            ),
        ]
        if market_id:
            requests.append(
                fetch(
                    "gamma_market",
                    f"{gamma_base_url}/markets/"
                    f"{urllib.parse.quote(market_id, safe='')}",
                )
            )
        if event_id:
            requests.append(
                fetch(
                    "gamma_event",
                    f"{gamma_base_url}/events/"
                    f"{urllib.parse.quote(event_id, safe='')}",
                )
            )
        errors: dict[str, str] = {}
        for label, payload, error in await asyncio.gather(*requests):
            if payload is not None:
                normalized[label] = payload
            elif error is not None:
                errors[label] = error
        if errors:
            normalized["collector_enrichment_errors"] = errors
        return normalized

    enriched = tuple(await asyncio.gather(*(enrich(m) for m in selection.markets)))
    return MarketSelection(
        enriched,
        selection.asset_ids,
        candidate_count=selection.candidate_count,
    )
