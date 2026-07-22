from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def _iso(timestamp: float | None) -> str | None:
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


@dataclass(slots=True)
class CollectorState:
    started_at: float = field(default_factory=time.time)
    discovered_at: float | None = None
    connected_at: float | None = None
    disconnected_at: float | None = None
    last_message_at: float | None = None
    last_pong_at: float | None = None
    connected: bool = False
    markets: int = 0
    assets: int = 0
    messages_total: int = 0
    invalid_messages_total: int = 0
    reconnects_total: int = 0
    discovery_failures_total: int = 0
    last_error: str | None = None

    def health(self, stale_after_seconds: int) -> tuple[bool, str]:
        if self.discovered_at is None:
            return False, "waiting_for_discovery"
        if self.assets == 0:
            return False, "no_rewarded_markets"
        if not self.connected:
            return False, "websocket_disconnected"
        if self.last_message_at is None:
            return False, "waiting_for_first_message"
        if time.time() - self.last_message_at > stale_after_seconds:
            return False, "market_data_stale"
        return True, "ok"

    def as_dict(self, stale_after_seconds: int) -> dict[str, Any]:
        healthy, reason = self.health(stale_after_seconds)
        return {
            "healthy": healthy,
            "reason": reason,
            "started_at": _iso(self.started_at),
            "uptime_seconds": round(time.time() - self.started_at, 3),
            "discovered_at": _iso(self.discovered_at),
            "connected_at": _iso(self.connected_at),
            "disconnected_at": _iso(self.disconnected_at),
            "last_message_at": _iso(self.last_message_at),
            "last_pong_at": _iso(self.last_pong_at),
            "connected": self.connected,
            "markets": self.markets,
            "assets": self.assets,
            "messages_total": self.messages_total,
            "invalid_messages_total": self.invalid_messages_total,
            "reconnects_total": self.reconnects_total,
            "discovery_failures_total": self.discovery_failures_total,
            "last_error": self.last_error,
        }

