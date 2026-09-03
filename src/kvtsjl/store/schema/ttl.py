"""TTL policy for KvSet entries."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum

from kvtsjl.exceptions import KvStoreTtlUnsupported

# Far-future marker for ``TtlPolicy.none()`` on opted-in blob stores.
# Year 9999 is not encodable as an S3 HTTP ``Expires`` timestamp in botocore.
TTL_NONE_EXPIRES_AT = datetime(2099, 12, 31, 23, 59, 59, tzinfo=UTC)


class ExpiryGc(str, Enum):
    """What to do when a leaf observes an expired entry on get/scan."""

    LAZY_DELETE = "lazy_delete"
    """Delete the expired object/file from the medium (default)."""

    HIDE = "hide"
    """Treat as absent without mutating the medium (leave GC to lifecycle/ops)."""


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


def require_explicit_ttl_supported(
    ttl: TtlPolicy | None,
    *,
    allowed: bool,
    backend: str,
) -> None:
    """Raise if the caller passed a per-write override the backend cannot store."""
    if ttl is not None and not allowed:
        raise KvStoreTtlUnsupported(
            f"{backend} does not support per-write ttl with the current provision"
        )
