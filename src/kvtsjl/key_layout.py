"""Physical key encoding strategies and scan query."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import Enum
from urllib.parse import quote, unquote


class KeyLayout(str, Enum):
    """How a leaf KBLOB is mapped to a physical storage key segment."""

    LITERAL = "literal"
    HASHED = "hashed"
    HIERARCHICAL = "hierarchical"


@dataclass(frozen=True, slots=True)
class ScanQuery[K]:
    prefix: K | None = None
    include_values: bool = False
    page_size: int = 100


def safe_segment(text: str) -> str:
    """Path/redis-safe single segment (no slashes / ``..`` / NUL)."""
    value = (text or "").strip()
    if not value:
        return "_"
    return (
        value.replace("/", "_").replace("\\", "_").replace("..", "_").replace("\0", "")
    )


def encode_literal_str(blob: str) -> str:
    return quote(blob, safe="")


def decode_literal_str(encoded: str) -> str:
    return unquote(encoded)


def encode_literal_bytes(blob: bytes) -> str:
    return quote(blob.decode("latin-1"), safe="")


def decode_literal_bytes(encoded: str) -> bytes:
    return unquote(encoded).encode("latin-1")


def hash_blob(blob: bytes | str) -> str:
    raw = blob if isinstance(blob, bytes) else blob.encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def hierarchical_parts(blob: str | bytes, *, delimiter: str = "/") -> list[str]:
    text = blob if isinstance(blob, str) else blob.decode("utf-8")
    parts = [p for p in text.split(delimiter) if p]
    return [safe_segment(p) for p in parts] or ["_"]


def supports_prefix_scan(layout: KeyLayout) -> bool:
    return layout in (KeyLayout.LITERAL, KeyLayout.HIERARCHICAL)


def layout_encode_for_fs(layout: KeyLayout, key_blob: str | bytes) -> str:
    """Filename or relative path under a collection dir for a leaf/in-key blob."""
    if layout is KeyLayout.HASHED:
        return hash_blob(key_blob)
    if layout is KeyLayout.HIERARCHICAL:
        parts = hierarchical_parts(key_blob)
        return "/".join(parts)
    # LITERAL
    if isinstance(key_blob, bytes):
        return encode_literal_bytes(key_blob)
    return encode_literal_str(key_blob)


def layout_decode_for_fs(
    layout: KeyLayout, rel: str, *, blob_type: type[str] | type[bytes]
) -> str | bytes:
    """Invert ``layout_encode_for_fs`` for reversible layouts (``LITERAL`` only)."""
    if layout is not KeyLayout.LITERAL:
        raise ValueError(f"layout {layout!r} is not reversible from a filesystem path")
    encoded = rel.replace("\\", "/")
    if blob_type is bytes:
        return decode_literal_bytes(encoded)
    return decode_literal_str(encoded)
