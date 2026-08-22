"""In-memory KvStore leaf backend."""

from __future__ import annotations

import time
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field

from kvtsjl.batching import chunk_sequence
from kvtsjl.bind import (
    CollectionBinding,
    NamespaceBinder,
    NativeStrCollectionBinder,
)
from kvtsjl.store.schema.kvset import KvSet
from kvtsjl.store.schema.layout import ScanQuery
from kvtsjl.scope import Scope
from kvtsjl.store import KvBackend


@dataclass
class _Entry[VBLOB]:
    value_blob: VBLOB
    expires_at: float | None = None


@dataclass
class _MemoryRoot[KBLOB, VBLOB]:
    collections: dict[str, dict[KBLOB, _Entry[VBLOB]]] = field(default_factory=dict)

    def bucket(self, collection: str | None) -> dict[KBLOB, _Entry[VBLOB]]:
        key = "" if collection is None else collection
        if key not in self.collections:
            self.collections[key] = {}
        return self.collections[key]


class MemoryKvStore[K, V, KBLOB, VBLOB](KvBackend[K, V, KBLOB, VBLOB, str]):
    """In-process store with string-named native collections."""

    def __init__(
        self,
        kvset: KvSet[K, V, KBLOB, VBLOB],
        *,
        scope: Scope | None = None,
        binder: NamespaceBinder[KBLOB, str] | None = None,
        binding: CollectionBinding[KBLOB, str] | None = None,
        batch_size: int = 500,
        root: _MemoryRoot[KBLOB, VBLOB] | None = None,
    ) -> None:
        super().__init__(
            kvset,
            scope=scope,
            binder=binder or NativeStrCollectionBinder(),
            binding=binding,
            batch_size=batch_size,
        )
        self._root: _MemoryRoot[KBLOB, VBLOB] = root or _MemoryRoot()

    def _clone_with_scope(self, scope: Scope) -> MemoryKvStore[K, V, KBLOB, VBLOB]:
        return MemoryKvStore(
            self.kvset,
            scope=scope,
            binding=self._binding,
            batch_size=self.batch_size,
            root=self._root,
        )

    def _bucket(self) -> dict[KBLOB, _Entry[VBLOB]]:
        return self._root.bucket(self._binding.collection)

    def _expired(self, entry: _Entry[VBLOB]) -> bool:
        return entry.expires_at is not None and time.time() >= entry.expires_at

    def _expires_at(self) -> float | None:
        secs = self.ttl_seconds()
        if secs is None:
            return None
        return time.time() + secs

    def get(self, key: K) -> V | None:
        pk = self._physical_key_blob(key)
        bucket = self._bucket()
        entry = bucket.get(pk)
        if entry is None:
            return None
        if self._expired(entry):
            bucket.pop(pk, None)
            return None
        return self.kvset.value_serde.deserialize(entry.value_blob)

    def set(self, key: K, value: V) -> None:
        pk = self._physical_key_blob(key)
        blob = self.kvset.value_serde.serialize(value)
        self._bucket()[pk] = _Entry(value_blob=blob, expires_at=self._expires_at())

    def delete(self, key: K) -> bool:
        pk = self._physical_key_blob(key)
        return self._bucket().pop(pk, None) is not None

    def batch_get(self, keys: Sequence[K]) -> dict[K, V]:
        out: dict[K, V] = {}
        for chunk in chunk_sequence(keys, self.batch_size):
            for key in chunk:
                value = self.get(key)
                if value is not None:
                    out[key] = value
        return out

    def batch_set(self, items: Mapping[K, V]) -> None:
        pairs = list(items.items())
        for chunk in chunk_sequence(pairs, self.batch_size):
            for key, value in chunk:
                self.set(key, value)

    def batch_delete(self, keys: Sequence[K]) -> int:
        deleted = 0
        for chunk in chunk_sequence(keys, self.batch_size):
            for key in chunk:
                if self.delete(key):
                    deleted += 1
        return deleted

    def _scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        prefix = self._scan_prefix_blob(query.prefix)
        ops = self.kvset.blob_ops
        bucket = self._bucket()
        for pk in list(bucket.keys()):
            entry = bucket.get(pk)
            if entry is None:
                continue
            if self._expired(entry):
                bucket.pop(pk, None)
                continue
            if not ops.startswith(pk, prefix):
                continue
            decoded = self._decode_key_from_physical(pk)
            if decoded is None:
                continue
            if query.include_values:
                yield decoded, self.kvset.value_serde.deserialize(entry.value_blob)
            else:
                yield decoded, None
