"""Logical index API: search and sync over document keys."""

from __future__ import annotations

from kvtsjl.index.logical.abc import Index, IndexHit
from kvtsjl.index.logical.bundle import indexes_from_bundle
from kvtsjl.index.logical.envelope import EmptyEnvelope, VectorEnvelope
from kvtsjl.index.logical.vector import FlatMeta, VectorIndex, VectorQuery, VectorRecord

__all__ = [
    "EmptyEnvelope",
    "FlatMeta",
    "Index",
    "IndexHit",
    "VectorEnvelope",
    "VectorIndex",
    "VectorQuery",
    "VectorRecord",
    "indexes_from_bundle",
]
