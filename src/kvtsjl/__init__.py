"""kvtsjl (kv-tasjil): a typed, composable facade for key-value storage."""

from __future__ import annotations

from kvtsjl.backends import (
    FilesystemKvStore,
    MemoryKvStore,
)
from kvtsjl.blob_ops import BlobOps, BytesBlobOps, StrBlobOps
from kvtsjl.compose import (
    FallbackReadKvStore,
    IndexedKvStore,
    MirrorKvStore,
    ReadonlyKvStore,
)
from kvtsjl.exceptions import (
    KvStoreComposeError,
    KvStoreError,
    KvStoreIndexError,
    KvStoreReadOnlyError,
    KvStoreScanUnsupported,
    KvStoreScopeError,
    KvStoreSerDeError,
)
from kvtsjl.index import (
    Index,
    IndexHit,
    MemoryKeyIndex,
    MemoryTermIndex,
)
from kvtsjl.key_layout import KeyLayout, ScanQuery
from kvtsjl.keymap import KeyMap
from kvtsjl.kvset import KvSet
from kvtsjl.kvset_ref import KvSetRef
from kvtsjl.namespace import (
    CollectionBinding,
    KeyPrefixBinder,
    NamespaceBinder,
    NativeCollectionBinder,
    NativeStrCollectionBinder,
    PhysicalRef,
)
from kvtsjl.scope import KeyPrefix, Scope, ScopeSegment
from kvtsjl.serde import SerDe
from kvtsjl.store import KvStore
from kvtsjl.ttl import TtlPolicy

__version__ = "0.1.0"

__all__ = [
    "BlobOps",
    "BytesBlobOps",
    "CollectionBinding",
    "FallbackReadKvStore",
    "FilesystemKvStore",
    "Index",
    "IndexHit",
    "IndexedKvStore",
    "KeyLayout",
    "KeyMap",
    "KeyPrefix",
    "KeyPrefixBinder",
    "KvSet",
    "KvSetRef",
    "KvStore",
    "KvStoreComposeError",
    "KvStoreError",
    "KvStoreIndexError",
    "KvStoreReadOnlyError",
    "KvStoreScanUnsupported",
    "KvStoreScopeError",
    "KvStoreSerDeError",
    "MemoryKeyIndex",
    "MemoryKvStore",
    "MemoryTermIndex",
    "MirrorKvStore",
    "NamespaceBinder",
    "NativeCollectionBinder",
    "NativeStrCollectionBinder",
    "PhysicalRef",
    "ReadonlyKvStore",
    "ScanQuery",
    "Scope",
    "ScopeSegment",
    "SerDe",
    "StrBlobOps",
    "TtlPolicy",
    "__version__",
]
