"""Document store wire schema (``KvSet`` and key layout)."""

from __future__ import annotations

from kvtsjl.store.schema.kvset import KvSet, join_blobs
from kvtsjl.store.schema.layout import KeyLayout, ScanQuery, supports_prefix_scan
from kvtsjl.store.schema.ttl import TtlPolicy

__all__ = [
    "KeyLayout",
    "KvSet",
    "ScanQuery",
    "TtlPolicy",
    "join_blobs",
    "supports_prefix_scan",
]
