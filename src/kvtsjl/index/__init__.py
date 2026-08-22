"""Logical search indexes over document stores."""

from __future__ import annotations

from kvtsjl.index.abc import Index, IndexHit
from kvtsjl.index.bundle import indexes_from_bundle
from kvtsjl.index.envelope import EmptyEnvelope, VectorEnvelope
from kvtsjl.index.vector import FlatMeta, VectorIndex, VectorQuery, VectorRecord

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
