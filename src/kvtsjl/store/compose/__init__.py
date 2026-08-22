"""Logical store composition wrappers."""

from __future__ import annotations

from kvtsjl.store.compose.fallback import FallbackReadKvStore
from kvtsjl.store.compose.indexed import IndexedKvStore
from kvtsjl.store.compose.mirror import MirrorKvStore
from kvtsjl.store.compose.readonly import ReadonlyKvStore

__all__ = [
    "FallbackReadKvStore",
    "IndexedKvStore",
    "MirrorKvStore",
    "ReadonlyKvStore",
]
