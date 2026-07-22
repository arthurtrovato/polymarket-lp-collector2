from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from polymarket_collector.config import Config


class ConfigTests(unittest.TestCase):
    def test_loads_dotenv_and_respects_process_environment(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            env_file = Path(temporary) / ".env"
            env_file.write_text(
                "MAX_MARKETS=12\nHEALTH_PORT=9090\nMIN_DAILY_REWARD=2.5\n",
                encoding="utf-8",
            )
            clean = {
                key: value
                for key, value in os.environ.items()
                if key
                not in {"MAX_MARKETS", "HEALTH_PORT", "MIN_DAILY_REWARD"}
            }
            clean["ENV_FILE"] = str(env_file)
            clean["MAX_MARKETS"] = "15"
            with patch.dict(os.environ, clean, clear=True):
                config = Config.from_env()
            self.assertEqual(config.max_markets, 15)
            self.assertEqual(config.health_port, 9090)
            self.assertEqual(config.min_daily_reward, 2.5)

    def test_port_is_used_when_health_port_is_absent(self) -> None:
        clean = {
            key: value
            for key, value in os.environ.items()
            if key not in {"HEALTH_PORT", "PORT", "ENV_FILE"}
        }
        clean["ENV_FILE"] = "/does/not/exist"
        clean["PORT"] = "8765"
        with patch.dict(os.environ, clean, clear=True):
            config = Config.from_env()
        self.assertEqual(config.health_port, 8765)

    def test_rejects_invalid_limits(self) -> None:
        with patch.dict(
            os.environ,
            {"ENV_FILE": "/does/not/exist", "MAX_MARKETS": "0"},
            clear=True,
        ):
            with self.assertRaises(ValueError):
                Config.from_env()


if __name__ == "__main__":
    unittest.main()
