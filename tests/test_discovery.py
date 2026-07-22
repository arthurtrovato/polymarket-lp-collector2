from __future__ import annotations

import unittest

from polymarket_collector.discovery import daily_reward, select_markets


def market(
    identifier: str,
    reward: float,
    volume: float,
    token_suffix: str,
) -> dict:
    return {
        "condition_id": identifier,
        "volume_24hr": volume,
        "rewards_config": [{"rate_per_day": reward}],
        "tokens": [
            {"token_id": f"yes-{token_suffix}", "outcome": "YES"},
            {"token_id": f"no-{token_suffix}", "outcome": "NO"},
        ],
    }


class DiscoveryTests(unittest.TestCase):
    def test_daily_reward_sums_configs(self) -> None:
        self.assertEqual(
            daily_reward(
                {"rewards_config": [{"rate_per_day": 2}, {"rate_per_day": "1.5"}]}
            ),
            3.5,
        )

    def test_selects_by_reward_then_volume(self) -> None:
        candidates = [
            market("a", 2, 1_000, "a"),
            market("b", 5, 10, "b"),
            market("c", 2, 2_000, "c"),
            market("d", 0.5, 10_000, "d"),
        ]
        selection = select_markets(
            candidates,
            max_markets=2,
            min_daily_reward=1,
            min_volume_24h=0,
        )
        self.assertEqual(
            [item["condition_id"] for item in selection.markets], ["b", "c"]
        )
        self.assertEqual(
            selection.asset_ids, ("yes-b", "no-b", "yes-c", "no-c")
        )

    def test_skips_market_without_two_tokens(self) -> None:
        invalid = market("a", 10, 1_000, "a")
        invalid["tokens"] = invalid["tokens"][:1]
        selection = select_markets(
            [invalid],
            max_markets=10,
            min_daily_reward=0,
            min_volume_24h=0,
        )
        self.assertEqual(selection.markets, ())


if __name__ == "__main__":
    unittest.main()

