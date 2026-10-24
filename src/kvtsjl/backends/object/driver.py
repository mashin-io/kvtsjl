"""Blob medium driver ABC for object-store leaves."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, BinaryIO

from kvtsjl.exceptions import KvStoreUnsupported


@dataclass(frozen=True, slots=True)
class BlobStat:
    """Medium-level object metadata (before TTL policy interpretation)."""

    key: str
    modified_at: datetime | None = None
    native_expires_at: datetime | None = None
    size: int | None = None
    etag: str | None = None


class BlobDriver(ABC):
    """Thin I/O adapter for one object/blob medium."""

    @property
    @abstractmethod
    def max_bulk_delete(self) -> int:
        """Max keys per ``bulk_delete`` call (FS: 1 — no native bulk)."""

    @abstractmethod
    def head(self, key: str) -> BlobStat | None:
        """Metadata if present (no expiry policy applied)."""

    @abstractmethod
    def read(self, key: str) -> tuple[bytes, BlobStat] | None:
        """Body + metadata if present."""

    @abstractmethod
    def write(self, key: str, data: bytes, *, headers: dict[str, Any] | None = None) -> None:
        """Put object; ``headers`` carries medium-specific write options."""

    def write_if_absent(
        self, key: str, data: bytes, *, headers: dict[str, Any] | None = None
    ) -> bool:
        """Put ``key`` only if no object is there; ``True`` if this call did.

        One request the medium itself makes conditional — ``If-None-Match: *``,
        ``ifGenerationMatch=0``, a link that fails if the name is taken. There
        is no default: a head-then-put is the race this exists to remove, so a
        driver without the primitive refuses.
        """
        raise KvStoreUnsupported(f"{type(self).__name__} has no conditional put")

    def write_stream(
        self,
        key: str,
        src: BinaryIO,
        *,
        size: int | None = None,
        headers: dict[str, Any] | None = None,
    ) -> None:
        """Write ``src`` to ``key`` without holding the whole body in memory.

        The default buffers and calls ``write``, so third-party drivers keep
        working — they just do not get the memory benefit. Drivers that have a
        native streaming upload override this.

        ``size`` is the **exact** encoded length, or ``None`` if unknown. Media
        that take it (GCS ``size=``, Azure ``length=``) write exactly that many
        bytes, so a wrong value truncates or fails the upload. Drivers must not
        guess one.
        """
        self.write(key, src.read(), headers=headers)

    def read_chunks(self, key: str) -> tuple[Iterator[bytes], BlobStat] | None:
        """Body as a chunk stream plus its stat, or ``None`` if absent.

        Pull-shaped on purpose: the value serde decodes a chunk *iterator*, so
        a pull-side read lets the driver's chunks flow straight through it with
        no buffer and no push/pull bridge in between.

        The default yields one chunk via ``read``, so third-party drivers keep
        working — they just do not get the memory benefit. Drivers with a
        native streaming download override this.

        The returned iterator may hold the underlying response open; callers
        drain or close it.
        """
        found = self.read(key)
        if found is None:
            return None
        data, stat = found
        return iter((data,)), stat

    @abstractmethod
    def delete(self, key: str) -> bool:
        """Delete if present; return whether it existed."""

    @abstractmethod
    def list(self, *, prefix: str) -> Iterator[BlobStat]:
        """List objects under ``prefix`` (may omit native expiry until head)."""

    @abstractmethod
    def bulk_delete(self, keys: Sequence[str]) -> int:
        """Delete up to ``max_bulk_delete`` keys; return number deleted."""
