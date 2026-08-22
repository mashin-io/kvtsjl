"""kvtsjl (kv-tasjil): a typed, composable facade for key-value storage."""

from __future__ import annotations

from kvtsjl.backends import (
    FilesystemKvStore,
    MemoryKvStore,
)
from kvtsjl.backends.index import MemoryKeyIndex, MemoryTermIndex, MemoryVectorIndex
from kvtsjl.bind import (
    CollectionBinding,
    KeyPrefixBinder,
    NamespaceBinder,
    NativeCollectionBinder,
    NativeStrCollectionBinder,
    PhysicalRef,
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
    EmptyEnvelope,
    Index,
    IndexBackend,
    IndexHit,
    IndexSet,
    VectorEnvelope,
    VectorIndex,
    VectorIndexBackend,
    VectorQuery,
    VectorRecord,
)
from kvtsjl.keymap import KeyMap
from kvtsjl.keymap_algebra import DictKeyMap
from kvtsjl.leaf import PhysicalBackend
from kvtsjl.schema import BlobOps, BytesBlobOps, PhysicalSchema, StrBlobOps, WireRef
from kvtsjl.scope import KeyPrefix, Scope, ScopeSegment
from kvtsjl.serde import SerDe
from kvtsjl.store import (
    FallbackReadKvStore,
    IndexedKvStore,
    KeyLayout,
    KvBackend,
    KvSet,
    KvStore,
    MirrorKvStore,
    ReadonlyKvStore,
    ScanQuery,
    TtlPolicy,
)

__version__ = "0.1.0"

__all__ = [
    "BlobOps",
    "BytesBlobOps",
    "CollectionBinding",
    "DictKeyMap",
    "EmptyEnvelope",
    "FallbackReadKvStore",
    "FilesystemKvStore",
    "Index",
    "IndexBackend",
    "IndexHit",
    "IndexSet",
    "IndexedKvStore",
    "KeyLayout",
    "KeyMap",
    "KeyPrefix",
    "KeyPrefixBinder",
    "KvBackend",
    "KvSet",
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
    "MemoryVectorIndex",
    "MirrorKvStore",
    "NamespaceBinder",
    "NativeCollectionBinder",
    "NativeStrCollectionBinder",
    "PhysicalBackend",
    "PhysicalRef",
    "PhysicalSchema",
    "ReadonlyKvStore",
    "ScanQuery",
    "Scope",
    "ScopeSegment",
    "SerDe",
    "StrBlobOps",
    "TtlPolicy",
    "VectorEnvelope",
    "VectorIndex",
    "VectorIndexBackend",
    "VectorQuery",
    "VectorRecord",
    "WireRef",
    "__version__",
]
