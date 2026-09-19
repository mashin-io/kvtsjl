"""TTL interpretation for object-store leaves."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import UTC, datetime
import time
from typing import Any

from kvtsjl.backends.object.driver import BlobStat
from kvtsjl.store.schema.stat import RecordStat
from kvtsjl.store.schema.ttl import TTL_NONE_EXPIRES_AT


class TtlSource(ABC):
    """Where expiry is read and written — two independent capabilities."""

    @property
    @abstractmethod
    def supports_write_clock(self) -> bool:
        """Whether ``created_at=`` can set the expiry clock base."""

    @property
    @abstractmethod
    def supports_per_write_ttl(self) -> bool:
        """Whether explicit per-write ``ttl=`` is accepted."""

    @abstractmethod
    def is_expired(
        self, stat: BlobStat, *, collection_ttl_seconds: int | None
    ) -> bool: ...

    @abstractmethod
    def to_record_stat(
        self, stat: BlobStat, *, collection_ttl_seconds: int | None
    ) -> RecordStat: ...

    @abstractmethod
    def write_headers(
        self,
        *,
        ttl_seconds: int | None,
        clock_base: float,
        explicit_ttl: bool,
    ) -> dict[str, Any]:
        """Medium write options (e.g. S3 ``Expires``). Empty if none."""

    def needs_head_for_list_expiry(self) -> bool:
        """True when list() metadata is insufficient to decide expiry."""
        return False


class ObjectTimeTtl(TtlSource):
    """Expiry = ``modified_at + collection_ttl``; no per-write ``ttl=``."""

    @property
    def supports_write_clock(self) -> bool:
        return False

    @property
    def supports_per_write_ttl(self) -> bool:
        return False

    def is_expired(
        self, stat: BlobStat, *, collection_ttl_seconds: int | None
    ) -> bool:
        if collection_ttl_seconds is None or stat.modified_at is None:
            return False
        return time.time() >= stat.modified_at.timestamp() + collection_ttl_seconds

    def to_record_stat(
        self, stat: BlobStat, *, collection_ttl_seconds: int | None
    ) -> RecordStat:
        modified = stat.modified_at
        if collection_ttl_seconds is None or modified is None:
            return RecordStat(
                created_at=modified,
                modified_at=modified,
                expires_at=None,
                size=stat.size,
                etag=stat.etag,
                persistent=True,
            )
        expires = datetime.fromtimestamp(
            modified.timestamp() + collection_ttl_seconds, tz=UTC
        )
        return RecordStat(
            created_at=modified,
            modified_at=modified,
            expires_at=expires,
            size=stat.size,
            etag=stat.etag,
            persistent=False,
        )

    def write_headers(
        self,
        *,
        ttl_seconds: int | None,
        clock_base: float,
        explicit_ttl: bool,
    ) -> dict[str, Any]:
        return {}


class S3ExpiresTtl(TtlSource):
    """HTTP ``Expires`` on explicit ``ttl=``; write-clock capable."""

    @property
    def supports_write_clock(self) -> bool:
        return True

    @property
    def supports_per_write_ttl(self) -> bool:
        return True

    def needs_head_for_list_expiry(self) -> bool:
        return True

    def is_expired(
        self, stat: BlobStat, *, collection_ttl_seconds: int | None
    ) -> bool:
        if stat.native_expires_at is not None:
            return time.time() >= stat.native_expires_at.timestamp()
        if collection_ttl_seconds is None or stat.modified_at is None:
            return False
        return time.time() >= stat.modified_at.timestamp() + collection_ttl_seconds

    def to_record_stat(
        self, stat: BlobStat, *, collection_ttl_seconds: int | None
    ) -> RecordStat:
        modified = stat.modified_at
        expires = stat.native_expires_at
        persistent = False
        expires_at = expires
        if expires is None:
            if collection_ttl_seconds is None or modified is None:
                persistent = True
            else:
                expires_at = datetime.fromtimestamp(
                    modified.timestamp() + collection_ttl_seconds, tz=UTC
                )
        elif expires == TTL_NONE_EXPIRES_AT:
            persistent = True
            expires_at = None
        return RecordStat(
            created_at=modified,
            modified_at=modified,
            expires_at=expires_at,
            size=stat.size,
            etag=stat.etag,
            persistent=persistent,
        )

    def write_headers(
        self,
        *,
        ttl_seconds: int | None,
        clock_base: float,
        explicit_ttl: bool,
    ) -> dict[str, Any]:
        if not explicit_ttl:
            return {}
        expires = (
            TTL_NONE_EXPIRES_AT
            if ttl_seconds is None
            else datetime.fromtimestamp(clock_base + ttl_seconds, tz=UTC)
        )
        return {"Expires": expires}
