"""Blob medium driver ABC for object-store leaves."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any


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

    @abstractmethod
    def delete(self, key: str) -> bool:
        """Delete if present; return whether it existed."""

    @abstractmethod
    def list(self, *, prefix: str) -> Iterator[BlobStat]:
        """List objects under ``prefix`` (may omit native expiry until head)."""

    @abstractmethod
    def bulk_delete(self, keys: Sequence[str]) -> int:
        """Delete up to ``max_bulk_delete`` keys; return number deleted."""
