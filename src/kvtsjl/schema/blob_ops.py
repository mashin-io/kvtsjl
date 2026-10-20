"""KBLOB composition algebra for key build and scan prefix ops."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass


class BlobOps[BLOB](ABC):
    """Monoid-like ops on homogeneous key blobs (write + scan)."""

    separator: BLOB

    @property
    @abstractmethod
    def separator_str(self) -> str:
        """The separator as text, for escaping inside a segment value."""

    @abstractmethod
    def escape(self, blob: BLOB) -> BLOB:
        """Percent-escape the separator and ``%`` in a whole blob."""

    @abstractmethod
    def unescape(self, blob: BLOB) -> BLOB:
        """Inverse of :meth:`escape`."""

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

    @property
    def separator_str(self) -> str:
        return self.separator.decode("utf-8")

    def escape(self, blob: bytes) -> bytes:
        esc = "".join(f"%{b:02X}" for b in self.separator).encode("ascii")
        return blob.replace(b"%", b"%25").replace(self.separator, esc)

    def unescape(self, blob: bytes) -> bytes:
        esc = "".join(f"%{b:02X}" for b in self.separator).encode("ascii")
        return blob.replace(esc, self.separator).replace(b"%25", b"%")

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

    @property
    def separator_str(self) -> str:
        return self.separator

    def escape(self, blob: str) -> str:
        return escape_segment(blob, self.separator)

    def unescape(self, blob: str) -> str:
        return unescape_segment(blob, self.separator)

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


def escape_segment(text: str, separator: str) -> str:
    """Percent-escape the separator and ``%`` inside a scope segment.

    Scope kinds and ids are escaped so a separator inside a *value* cannot be
    read as a boundary. The leaf is never escaped: it is the last part, and
    with the scope arity fixed the rest of the key is the leaf whatever it
    contains — so every key whose leaf holds a ``:`` keeps the bytes it has.

    A value containing neither ``%`` nor the separator encodes to itself.
    """
    escaped_sep = "".join(f"%{b:02X}" for b in separator.encode("utf-8"))
    return text.replace("%", "%25").replace(separator, escaped_sep)


def unescape_segment(text: str, separator: str) -> str:
    """Inverse of :func:`escape_segment`."""
    escaped_sep = "".join(f"%{b:02X}" for b in separator.encode("utf-8"))
    return text.replace(escaped_sep, separator).replace("%25", "%")
