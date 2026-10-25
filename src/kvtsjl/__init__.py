"""kvtsjl (kv-tasjil): a typed, composable facade for key-value storage."""

from __future__ import annotations

from kvtsjl.backends import (
    FilesystemKvStore,
    FilesystemTtlMode,
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
    KvStoreIndexUnsupported,
    KvStoreReadOnlyError,
    KvStoreScanUnsupported,
    KvStoreScopeError,
    KvStoreSerDeError,
    KvStoreTtlUnsupported,
    KvStoreUnsupported,
    PartialSyncError,
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
from kvtsjl.keymap import KeyMap, ReadableKeyMap
from kvtsjl.keymap_algebra import DictKeyMap
from kvtsjl.leaf import PhysicalBackend
from kvtsjl.schema import BlobOps, BytesBlobOps, PhysicalSchema, StrBlobOps, WireRef
from kvtsjl.scope import KeyPrefix, Scope, ScopeSegment
from kvtsjl.serde import CompressionCodec, SerDe
from kvtsjl.store import (
    ExpiryGc,
    FallbackReadKvStore,
    FillStats,
    IndexedKvStore,
    KeyLayout,
    KvBackend,
    KvSet,
    KvStore,
    MirrorKvStore,
    ReadableKvStore,
    ReadonlyKvStore,
    RecordStat,
    ScanQuery,
    SyncPolicy,
    TtlPolicy,
)

__version__ = "0.1.0"

__all__ = [
    "BlobOps",
    "BytesBlobOps",
    "CollectionBinding",
    "CompressionCodec",
    "DictKeyMap",
    "FillStats",
    "EmptyEnvelope",
    "ExpiryGc",
    "FallbackReadKvStore",
    "FilesystemKvStore",
    "FilesystemTtlMode",
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
    "KvStoreIndexUnsupported",
    "KvStoreReadOnlyError",
    "KvStoreScanUnsupported",
    "KvStoreScopeError",
    "KvStoreSerDeError",
    "KvStoreTtlUnsupported",
    "KvStoreUnsupported",
    "MemoryKeyIndex",
    "MemoryKvStore",
    "MemoryTermIndex",
    "MemoryVectorIndex",
    "MirrorKvStore",
    "NamespaceBinder",
    "NativeCollectionBinder",
    "NativeStrCollectionBinder",
    "PartialSyncError",
    "PhysicalBackend",
    "PhysicalRef",
    "PhysicalSchema",
    "ReadableKeyMap",
    "ReadableKvStore",
    "ReadonlyKvStore",
    "RecordStat",
    "ScanQuery",
    "Scope",
    "ScopeSegment",
    "SerDe",
    "StrBlobOps",
    "SyncPolicy",
    "TtlPolicy",
    "VectorEnvelope",
    "VectorIndex",
    "VectorIndexBackend",
    "VectorQuery",
    "VectorRecord",
    "WireRef",
    "__version__",
]
