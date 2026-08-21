"""TTL policy for KvSet entries."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta


@dataclass(frozen=True, slots=True)
class TtlPolicy:
    """Retention policy; ``None`` duration means no expiry."""

    ttl_duration: timedelta | None = None

    @classmethod
    def none(cls) -> TtlPolicy:
        return cls(ttl_duration=None)

    @classmethod
    def hourly(cls) -> TtlPolicy:
        return cls(ttl_duration=timedelta(hours=1))

    @classmethod
    def daily(cls) -> TtlPolicy:
        return cls(ttl_duration=timedelta(days=1))

    @classmethod
    def weekly(cls) -> TtlPolicy:
        return cls(ttl_duration=timedelta(weeks=1))

    @classmethod
    def monthly(cls) -> TtlPolicy:
        """Approximate calendar month (30 days)."""
        return cls(ttl_duration=timedelta(days=30))

    def ttl_seconds(self) -> int | None:
        if self.ttl_duration is None:
            return None
        return max(1, int(self.ttl_duration.total_seconds()))
