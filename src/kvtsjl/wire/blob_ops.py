"""KBLOB composition algebra for key build and scan prefix ops."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass


class BlobOps[BLOB](ABC):
    """Monoid-like ops on homogeneous key blobs (write + scan)."""

    separator: BLOB

    @abstractmethod
    def concat(self, *parts: BLOB) -> BLOB: ...

    @abstractmethod
    def startswith(self, full: BLOB, prefix: BLOB) -> bool: ...

    @abstractmethod
    def strip_prefix(self, full: BLOB, prefix: BLOB) -> BLOB | None: ...

    @abstractmethod
    def len(self, blob: BLOB) -> int: ...

    def join(self, parts: Sequence[BLOB]) -> BLOB:
        if not parts:
            return self.concat()
        if len(parts) == 1:
            return parts[0]
        out = parts[0]
        for part in parts[1:]:
            out = self.concat(out, self.separator, part)
        return out


@dataclass(frozen=True, slots=True)
class BytesBlobOps(BlobOps[bytes]):
    separator: bytes = b":"

    def concat(self, *parts: bytes) -> bytes:
        return b"".join(parts)

    def startswith(self, full: bytes, prefix: bytes) -> bool:
        return full.startswith(prefix)

    def strip_prefix(self, full: bytes, prefix: bytes) -> bytes | None:
        if not full.startswith(prefix):
            return None
        return full[len(prefix) :]

    def len(self, blob: bytes) -> int:
        return len(blob)


@dataclass(frozen=True, slots=True)
class StrBlobOps(BlobOps[str]):
    separator: str = ":"

    def concat(self, *parts: str) -> str:
        return "".join(parts)

    def startswith(self, full: str, prefix: str) -> bool:
        return full.startswith(prefix)

    def strip_prefix(self, full: str, prefix: str) -> str | None:
        if not full.startswith(prefix):
            return None
        return full[len(prefix) :]

    def len(self, blob: str) -> int:
        return len(blob)
