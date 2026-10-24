"""Document stores: logical API, wire schema, leaf backend, and wrappers."""

from __future__ import annotations

from kvtsjl.store.backend import KvBackend
from kvtsjl.store.compose import (
    FallbackReadKvStore,
    IndexedKvStore,
    MirrorKvStore,
    ReadonlyKvStore,
)
from kvtsjl.store.logical import FillStats, KvStore
from kvtsjl.store.readable import ReadableKvStore
from kvtsjl.store.schema import (
    KeyLayout,
    KvSet,
    RecordStat,
    ScanQuery,
    SyncPolicy,
    TtlPolicy,
)
from kvtsjl.store.schema.scan import (
    Expired,
    Predicate,
    Projection,
    Pushdown,
    ScanCursor,
    ScanOrder,
    ScanPlan,
    ScanRow,
    ScanSpec,
)
from kvtsjl.store.schema.ttl import ExpiryGc

__all__ = [
    "ExpiryGc",
    "Expired",
    "FallbackReadKvStore",
    "IndexedKvStore",
    "KeyLayout",
    "KvBackend",
    "KvSet",
    "FillStats",
    "KvStore",
    "MirrorKvStore",
    "Predicate",
    "Projection",
    "Pushdown",
    "ReadableKvStore",
    "ReadonlyKvStore",
    "RecordStat",
    "ScanCursor",
    "ScanOrder",
    "ScanPlan",
    "ScanQuery",
    "ScanRow",
    "ScanSpec",
    "SyncPolicy",
    "TtlPolicy",
]
