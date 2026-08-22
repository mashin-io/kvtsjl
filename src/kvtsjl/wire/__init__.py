"""Physical wire descriptors and encoding primitives."""

from __future__ import annotations

from kvtsjl.wire.blob_ops import BlobOps, BytesBlobOps, StrBlobOps
from kvtsjl.wire.index_set import IndexSet
from kvtsjl.wire.kvset import KvSet, join_blobs
from kvtsjl.wire.layout import KeyLayout, ScanQuery, supports_prefix_scan
from kvtsjl.wire.ref import WireRef
from kvtsjl.wire.schema import PhysicalSchema
from kvtsjl.wire.ttl import TtlPolicy

__all__ = [
    "BlobOps",
    "BytesBlobOps",
    "IndexSet",
    "KeyLayout",
    "KvSet",
    "WireRef",
    "PhysicalSchema",
    "ScanQuery",
    "StrBlobOps",
    "TtlPolicy",
    "join_blobs",
    "supports_prefix_scan",
]
