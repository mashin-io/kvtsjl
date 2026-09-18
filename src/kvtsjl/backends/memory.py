"""In-memory KvStore leaf backend."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
import time

from kvtsjl.batching import chunk_sequence
from kvtsjl.bind import (
    CollectionBinding,
    NamespaceBinder,
    NativeStrCollectionBinder,
)
from kvtsjl.scope import Scope
from kvtsjl.store import KvBackend
from kvtsjl.store.schema.kvset import KvSet
from kvtsjl.store.schema.layout import ScanQuery
from kvtsjl.store.schema.stat import RecordStat
from kvtsjl.store.schema.ttl import (
    ExpiryGc,
    TtlPolicy,
    require_write_clock_supported,
)


@dataclass
class _Entry[VBLOB]:
    value_blob: VBLOB
    expires_at: float | None = None
    created_at: float | None = None  # expiry clock base (may be caller-supplied)
    written_at: float | None = None  # wall time of this write


@dataclass
class _MemoryRoot[KBLOB, VBLOB]:
    collections: dict[str, dict[KBLOB, _Entry[VBLOB]]] = field(default_factory=dict)

    def bucket(self, collection: str | None) -> dict[KBLOB, _Entry[VBLOB]]:
        key = "" if collection is None else collection
        if key not in self.collections:
            self.collections[key] = {}
        return self.collections[key]


def _utc_from_epoch(epoch: float | None) -> datetime | None:
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=UTC)


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
        expiry_gc: ExpiryGc = ExpiryGc.LAZY_DELETE,
    ) -> None:
        super().__init__(
            kvset,
            scope=scope,
            binder=binder or NativeStrCollectionBinder(),
            binding=binding,
            batch_size=batch_size,
        )
        self._root: _MemoryRoot[KBLOB, VBLOB] = root or _MemoryRoot()
        self._expiry_gc = expiry_gc

    @property
    def supports_write_clock(self) -> bool:
        return True

    def _clone_with_scope(self, scope: Scope) -> MemoryKvStore[K, V, KBLOB, VBLOB]:
        return MemoryKvStore(
            self.kvset,
            scope=scope,
            binding=self._binding,
            batch_size=self.batch_size,
            root=self._root,
            expiry_gc=self._expiry_gc,
        )

    def _bucket(self) -> dict[KBLOB, _Entry[VBLOB]]:
        return self._root.bucket(self._binding.collection)

    def _expired(self, entry: _Entry[VBLOB]) -> bool:
        return entry.expires_at is not None and time.time() >= entry.expires_at

    def _clock_base(self, created_at: datetime | None) -> float:
        if created_at is None:
            return time.time()
        if created_at.tzinfo is None:
            return created_at.replace(tzinfo=UTC).timestamp()
        return created_at.timestamp()

    def _expires_at(
        self, ttl: TtlPolicy | None = None, *, created_at: datetime | None = None
    ) -> float | None:
        secs = self.resolve_ttl_seconds(ttl)
        if secs is None:
            return None
        return self._clock_base(created_at) + secs

    def _live_entry(self, key: K) -> _Entry[VBLOB] | None:
        pk = self._physical_key_blob(key)
        bucket = self._bucket()
        entry = bucket.get(pk)
        if entry is None:
            return None
        if self._expired(entry):
            if self._expiry_gc is ExpiryGc.LAZY_DELETE:
                bucket.pop(pk, None)
            return None
        return entry

    def get(self, key: K) -> V | None:
        entry = self._live_entry(key)
        if entry is None:
            return None
        return self.kvset.value_serde.deserialize(entry.value_blob)

    def exists(self, key: K) -> bool:
        return self._live_entry(key) is not None

    def batch_exists(self, keys: Sequence[K]) -> set[K]:
        return {key for key in keys if self.exists(key)}

    def stat(self, key: K) -> RecordStat | None:
        entry = self._live_entry(key)
        if entry is None:
            return None
        return RecordStat(
            created_at=_utc_from_epoch(entry.created_at),
            modified_at=_utc_from_epoch(entry.written_at),
            expires_at=_utc_from_epoch(entry.expires_at),
            persistent=entry.expires_at is None,
        )

    def batch_stat(self, keys: Sequence[K]) -> dict[K, RecordStat]:
        out: dict[K, RecordStat] = {}
        for key in keys:
            meta = self.stat(key)
            if meta is not None:
                out[key] = meta
        return out

    def set(
        self,
        key: K,
        value: V,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        require_write_clock_supported(
            created_at, allowed=True, backend="MemoryKvStore"
        )
        pk = self._physical_key_blob(key)
        blob = self.kvset.value_serde.serialize(value)
        base = self._clock_base(created_at)
        written = time.time()
        self._bucket()[pk] = _Entry(
            value_blob=blob,
            expires_at=self._expires_at(ttl, created_at=created_at),
            created_at=base,
            written_at=written,
        )

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

    def batch_set(
        self,
        items: Mapping[K, V],
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        pairs = list(items.items())
        for chunk in chunk_sequence(pairs, self.batch_size):
            for key, value in chunk:
                self.set(key, value, ttl=ttl, created_at=created_at)

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
                if self._expiry_gc is ExpiryGc.LAZY_DELETE:
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

    def _gc_expired_keys(self, *, max_entries: int) -> list[K]:
        if max_entries < 1:
            raise ValueError(f"max_entries must be >= 1, got {max_entries}")
        prefix = self._scan_prefix_blob(None)
        ops = self.kvset.blob_ops
        bucket = self._bucket()
        deleted: list[K] = []
        for pk in list(bucket.keys()):
            if len(deleted) >= max_entries:
                break
            entry = bucket.get(pk)
            if entry is None or not self._expired(entry):
                continue
            if not ops.startswith(pk, prefix):
                continue
            decoded = self._decode_key_from_physical(pk)
            if decoded is None:
                continue
            bucket.pop(pk, None)
            deleted.append(decoded)
        return deleted
