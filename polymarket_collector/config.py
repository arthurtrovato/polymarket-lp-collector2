from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _load_env_file(path: Path) -> None:
    """Load a small dotenv-compatible file without adding a dependency."""
    if not path.is_file():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("\"").strip("'")
        if key:
            os.environ.setdefault(key, value)


def _int(name: str, default: int, minimum: int = 0) -> int:
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    if value < minimum:
        raise ValueError(f"{name} must be >= {minimum}, got {value}")
    return value


def _float(name: str, default: float, minimum: float = 0) -> float:
    raw = os.getenv(name, str(default))
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got {raw!r}") from exc
    if value < minimum:
        raise ValueError(f"{name} must be >= {minimum}, got {value}")
    return value


def _bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean, got {raw!r}")


@dataclass(frozen=True, slots=True)
class Config:
    data_dir: Path
    max_markets: int
    min_daily_reward: float
    min_volume_24h: float
    discovery_interval_seconds: int
    rotation_seconds: int
    max_file_bytes: int
    flush_interval_seconds: int
    health_host: str
    health_port: int
    stale_after_seconds: int
    log_level: str
    book_checkpoint_interval_seconds: int
    collect_sports: bool
    collect_rtds_crypto: bool
    clob_base_url: str = "https://clob.polymarket.com"
    gamma_base_url: str = "https://gamma-api.polymarket.com"
    websocket_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    sports_websocket_url: str = "wss://sports-api.polymarket.com/ws"
    rtds_websocket_url: str = "wss://ws-live-data.polymarket.com"

    @classmethod
    def from_env(cls) -> "Config":
        env_file = Path(os.getenv("ENV_FILE", ".env"))
        _load_env_file(env_file)
        return cls(
            data_dir=Path(os.getenv("DATA_DIR", "data")).expanduser().resolve(),
            max_markets=_int("MAX_MARKETS", 75, 1),
            min_daily_reward=_float("MIN_DAILY_REWARD", 0),
            min_volume_24h=_float("MIN_VOLUME_24H", 0),
            discovery_interval_seconds=_int(
                "DISCOVERY_INTERVAL_SECONDS", 900, 30
            ),
            rotation_seconds=_int("ROTATION_SECONDS", 900, 60),
            max_file_bytes=_int("MAX_FILE_MIB", 64, 1) * 1024 * 1024,
            flush_interval_seconds=_int("FLUSH_INTERVAL_SECONDS", 2, 1),
            health_host=os.getenv("HEALTH_HOST", "127.0.0.1"),
            # PaaS providers such as Koyeb inject PORT. HEALTH_PORT keeps
            # precedence so local/systemd installations remain configurable.
            health_port=_int(
                "HEALTH_PORT", int(os.getenv("PORT", "8080")), 1
            ),
            stale_after_seconds=_int("STALE_AFTER_SECONDS", 180, 30),
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
            book_checkpoint_interval_seconds=_int(
                "BOOK_CHECKPOINT_INTERVAL_SECONDS", 300, 0
            ),
            collect_sports=_bool("COLLECT_SPORTS", True),
            collect_rtds_crypto=_bool("COLLECT_RTDS_CRYPTO", True),
            clob_base_url=os.getenv(
                "CLOB_BASE_URL", "https://clob.polymarket.com"
            ).rstrip("/"),
            gamma_base_url=os.getenv(
                "GAMMA_BASE_URL", "https://gamma-api.polymarket.com"
            ).rstrip("/"),
            websocket_url=os.getenv(
                "WEBSOCKET_URL",
                "wss://ws-subscriptions-clob.polymarket.com/ws/market",
            ),
            sports_websocket_url=os.getenv(
                "SPORTS_WEBSOCKET_URL",
                "wss://sports-api.polymarket.com/ws",
            ),
            rtds_websocket_url=os.getenv(
                "RTDS_WEBSOCKET_URL",
                "wss://ws-live-data.polymarket.com",
            ),
        )
