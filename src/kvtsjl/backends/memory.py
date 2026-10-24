"""In-memory KvStore leaf backend."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
import time
from typing import Any, cast

from kvtsjl.batching import chunk_sequence
from kvtsjl.bind import (
    CollectionBinding,
    NamespaceBinder,
    NativeStrCollectionBinder,
)
from kvtsjl.scope import Scope
from kvtsjl.serde import StreamingSerDe
from kvtsjl.store import KvBackend
from kvtsjl.store.leaf import LeafKvStore
from kvtsjl.store.schema.kvset import KvSet
from kvtsjl.store.schema.layout import ScanQuery
from kvtsjl.store.schema.stat import RecordStat
from kvtsjl.store.schema.ttl import (
    ExpiryGc,
    TtlPolicy,
    require_write_clock_supported,
)
from kvtsjl.store.streaming import (
    StreamSource,
    iter_source,
    require_streaming_serde,
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


class MemoryKvBackend[K, V, KBLOB, VBLOB](KvBackend[K, V, KBLOB, VBLOB, str]):
    """In-process KvBackend with string-named native collections."""

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

    def clone_with_scope(self, scope: Scope) -> MemoryKvBackend[K, V, KBLOB, VBLOB]:
        return MemoryKvBackend(
            self.kvset,
            scope=scope,
            binding=self._binding,
            batch_size=self.batch_size,
            root=self._root,
            expiry_gc=self._expiry_gc,
        )

    def _bucket(self) -> dict[KBLOB, _Entry[VBLOB]]:
        return self._root.bucket(self._binding.collection)

    def _expired(self, entry: _Entry[VBLOB], *, now: datetime | None = None) -> bool:
        at = time.time() if now is None else self._clock_base(now)
        return entry.expires_at is not None and at >= entry.expires_at

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
            created_at, allowed=True, backend="MemoryKvBackend"
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

    def set_stream(
        self,
        key: K,
        source: StreamSource,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
        size: int | None = None,
    ) -> None:
        """Buffer, then ``set`` — this store holds values in memory anyway.

        Buffering here is honest rather than a silent cost: the medium *is*
        memory, so there is no streaming benefit to be had. Declared so callers
        can see it, unlike a wrapper that would quietly defeat their intent.
        """
        # Streaming is defined over byte wires, so a streamable serde of this
        # collection encodes to bytes however ``VBLOB`` is declared.
        serde = cast(
            "StreamingSerDe[V, Any, bytes]",
            require_streaming_serde(
                self.kvset.value_serde, op="set_stream", backend=type(self).__name__
            ),
        )
        with iter_source(source) as chunks:
            blob = b"".join(serde.encode_stream(chunks))
        self.set(key, serde.deserialize(blob), ttl=ttl, created_at=created_at)

    def read_stream(self, key: K) -> Iterator[V] | None:
        """Buffer, then yield one chunk — see ``set_stream``."""
        require_streaming_serde(
            self.kvset.value_serde, op="read_stream", backend=type(self).__name__
        )
        value = self.get(key)
        if value is None:
            return None
        return iter((value,))

    def set_if_absent(
        self,
        key: K,
        value: V,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> bool:
        # A dict read and write with no await or I/O between them, so no other
        # caller can interleave: this is the whole operation.
        pk = self._physical_key_blob(key)
        if pk in self._bucket():
            return False
        self.set(key, value, ttl=ttl, created_at=created_at)
        return True

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

    def scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
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

    def expired_keys(self, *, now: datetime | None = None, limit: int) -> list[K]:
        if limit < 1:
            raise ValueError(f"limit must be >= 1, got {limit}")
        prefix = self._scan_prefix_blob(None)
        ops = self.kvset.blob_ops
        bucket = self._bucket()
        found: list[K] = []
        for pk in list(bucket.keys()):
            if len(found) >= limit:
                break
            entry = bucket.get(pk)
            if entry is None or not self._expired(entry, now=now):
                continue
            if not ops.startswith(pk, prefix):
                continue
            decoded = self._decode_key_from_physical(pk)
            if decoded is None:
                continue
            found.append(decoded)
        return found


class MemoryKvStore[K, V, KBLOB, VBLOB](LeafKvStore[K, V, KBLOB, VBLOB, str]):
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
            MemoryKvBackend(
                kvset,
                scope=scope,
                binder=binder,
                binding=binding,
                batch_size=batch_size,
                root=root,
                expiry_gc=expiry_gc,
            )
        )
