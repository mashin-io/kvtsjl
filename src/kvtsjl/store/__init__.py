"""Document stores: logical surface, physical backend, and wrappers."""

from __future__ import annotations

from kvtsjl.physical.document import KvBackend
from kvtsjl.store.compose import (
    FallbackReadKvStore,
    IndexedKvStore,
    MirrorKvStore,
    ReadonlyKvStore,
)
from kvtsjl.store.logical import KvStore

__all__ = [
    "FallbackReadKvStore",
    "IndexedKvStore",
    "KvBackend",
    "KvStore",
    "MirrorKvStore",
    "ReadonlyKvStore",
]
