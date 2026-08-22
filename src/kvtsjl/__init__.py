"""kvtsjl (kv-tasjil): a typed, composable facade for key-value storage."""

from __future__ import annotations

from kvtsjl.backends import (
    FilesystemKvStore,
    MemoryKvStore,
)
from kvtsjl.backends.index import MemoryKeyIndex, MemoryTermIndex
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
    IndexHit,
)
from kvtsjl.keymap import KeyMap
from kvtsjl.physical import IndexBackend, KvBackend, PhysicalBackend
from kvtsjl.scope import KeyPrefix, Scope, ScopeSegment
from kvtsjl.serde import SerDe
from kvtsjl.store import (
    FallbackReadKvStore,
    IndexedKvStore,
    KvStore,
    MirrorKvStore,
    ReadonlyKvStore,
)
from kvtsjl.wire import (
    BlobOps,
    BytesBlobOps,
    IndexSet,
    KeyLayout,
    KvSet,
    PhysicalSchema,
    ScanQuery,
    StrBlobOps,
    TtlPolicy,
    WireRef,
)

__version__ = "0.1.0"

__all__ = [
    "BlobOps",
    "BytesBlobOps",
    "CollectionBinding",
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
    "WireRef",
    "__version__",
]
