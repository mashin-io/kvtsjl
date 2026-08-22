"""Logical store composition wrappers."""

from __future__ import annotations

from kvtsjl.store.compose.algebra_expand import ExpandKvStore, ExpandMapKvStore
from kvtsjl.store.compose.algebra_map import (
    IMappedKeysKvStore,
    IMappedKvStore,
    MappedKvStore,
)
from kvtsjl.store.compose.algebra_then import ThenKvStore, ThenWithKvStore
from kvtsjl.store.compose.algebra_zip import ZippedKvStore, ZipWithKvStore
from kvtsjl.store.compose.fallback import FallbackReadKvStore
from kvtsjl.store.compose.indexed import IndexedKvStore
from kvtsjl.store.compose.mirror import MirrorKvStore
from kvtsjl.store.compose.readonly import ReadonlyKvStore

__all__ = [
    "ExpandKvStore",
    "ExpandMapKvStore",
    "FallbackReadKvStore",
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
