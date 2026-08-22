"""Search indexes: logical API, wire schema, and leaf backends."""

from __future__ import annotations

from kvtsjl.index.backend import IndexBackend
from kvtsjl.index.logical import (
    EmptyEnvelope,
    FlatMeta,
    Index,
    IndexHit,
    VectorEnvelope,
    VectorIndex,
    VectorQuery,
    VectorRecord,
    indexes_from_bundle,
)
from kvtsjl.index.schema import IndexSet
from kvtsjl.index.vector_backend import VectorIndexBackend

__all__ = [
    "EmptyEnvelope",
    "FlatMeta",
    "Index",
    "IndexBackend",
    "IndexHit",
    "IndexSet",
    "VectorEnvelope",
    "VectorIndex",
    "VectorIndexBackend",
    "VectorQuery",
    "VectorRecord",
    "indexes_from_bundle",
]
