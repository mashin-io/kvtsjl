"""Logical store composition wrappers."""

from __future__ import annotations

from kvtsjl.store.compose.expand import ExpandKvStore, ExpandMapKvStore
from kvtsjl.store.compose.fallback import CoalescedKvStore, FallbackReadKvStore
from kvtsjl.store.compose.grouped import GroupedKvStore
from kvtsjl.store.compose.indexed import IndexedKvStore
from kvtsjl.store.compose.mapped import (
    IMappedKeysKvStore,
    IMappedKvStore,
    MappedKvStore,
)
from kvtsjl.store.compose.mirror import MirrorKvStore
from kvtsjl.store.compose.readonly import ReadonlyKvStore
from kvtsjl.store.compose.then import ThenKvStore, ThenWithKvStore
from kvtsjl.store.compose.zipped import ZippedKvStore, ZipWithKvStore

__all__ = [
    "CoalescedKvStore",
    "ExpandKvStore",
    "ExpandMapKvStore",
    "FallbackReadKvStore",
    "GroupedKvStore",
    "IMappedKeysKvStore",
    "IMappedKvStore",
    "IndexedKvStore",
    "MappedKvStore",
    "MirrorKvStore",
    "ReadonlyKvStore",
    "ThenKvStore",
    "ThenWithKvStore",
    "ZipWithKvStore",
    "ZippedKvStore",
]
