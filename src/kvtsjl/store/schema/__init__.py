"""Document store wire schema (``KvSet`` and key layout)."""

from __future__ import annotations

from kvtsjl.store.schema.kvset import KvSet
from kvtsjl.store.schema.layout import KeyLayout, ScanQuery, supports_prefix_scan
from kvtsjl.store.schema.policy import SyncPolicy
from kvtsjl.store.schema.scan import (
    AlwaysTrue,
    And,
    CompiledScan,
    Expired,
    KeyIn,
    KeyMatches,
    KeyPrefix,
    KeyRange,
    ModifiedAfter,
    ModifiedBefore,
    Needs,
    Not,
    Or,
    Predicate,
    Projection,
    Pushdown,
    ScanCapabilities,
    ScanCursor,
    ScanOrder,
    ScanPlan,
    ScanRow,
    ScanSpec,
    ScanStat,
    SizeBetween,
    ValueWhere,
    split_key_predicates,
)
from kvtsjl.store.schema.stat import RecordStat
from kvtsjl.store.schema.ttl import ExpiryGc, TtlPolicy

__all__ = [
    "AlwaysTrue",
    "And",
    "CompiledScan",
    "Expired",
    "ExpiryGc",
    "KeyIn",
    "KeyLayout",
    "KeyMatches",
    "KeyPrefix",
    "KeyRange",
    "KvSet",
    "ModifiedAfter",
    "ModifiedBefore",
    "Needs",
    "Not",
    "Or",
    "Predicate",
    "Projection",
    "Pushdown",
    "RecordStat",
    "ScanCapabilities",
    "ScanCursor",
    "ScanOrder",
    "ScanPlan",
    "ScanQuery",
    "ScanRow",
    "ScanSpec",
    "ScanStat",
    "SizeBetween",
    "SyncPolicy",
    "TtlPolicy",
    "ValueWhere",
    "split_key_predicates",
    "supports_prefix_scan",
]
