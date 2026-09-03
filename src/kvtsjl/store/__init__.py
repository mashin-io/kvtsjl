"""Document stores: logical API, wire schema, leaf backend, and wrappers."""

from __future__ import annotations

from kvtsjl.store.backend import KvBackend
from kvtsjl.store.compose import (
    FallbackReadKvStore,
    IndexedKvStore,
    MirrorKvStore,
    ReadonlyKvStore,
)
from kvtsjl.store.logical import KvStore
from kvtsjl.store.schema import KeyLayout, KvSet, ScanQuery, TtlPolicy
from kvtsjl.store.schema.ttl import ExpiryGc

__all__ = [
    "ExpiryGc",
    "FallbackReadKvStore",
    "IndexedKvStore",
    "KeyLayout",
    "KvBackend",
    "KvSet",
    "KvStore",
    "MirrorKvStore",
    "ReadonlyKvStore",
    "ScanQuery",
    "TtlPolicy",
]
