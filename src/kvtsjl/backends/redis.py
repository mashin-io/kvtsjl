"""Redis-backed KvStore (string keys or HASH collections).

Install with::

    pip install 'kvtsjl[redis]'

Flat keys use Redis ``EX`` (server-side delete). ``ExpiryGc`` does not apply —
expired keys are already gone before get/scan. ``gc_expired`` is a no-op (returns
``0``); the server removes expired flat keys.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, datetime
import time
from typing import TYPE_CHECKING, cast

from kvtsjl.batching import chunk_sequence
from kvtsjl.bind import (
    CollectionBinding,
    KeyPrefixBinder,
    NamespaceBinder,
    NativeStrCollectionBinder,
)
from kvtsjl.exceptions import KvStoreScanUnsupported, KvStoreTtlUnsupported
from kvtsjl.keymap import ConditionalWrite, Put, Versioned
from kvtsjl.scope import Scope
from kvtsjl.store import KvBackend
from kvtsjl.store.leaf import LeafKvStore
from kvtsjl.store.schema.kvset import KvSet
from kvtsjl.store.schema.layout import ScanQuery, supports_prefix_scan
from kvtsjl.store.schema.stat import RecordStat
from kvtsjl.store.schema.ttl import (
    TtlPolicy,
    require_explicit_ttl_supported,
    require_write_clock_supported,
)

if TYPE_CHECKING:
    # Prefer redis.client: under backends' reportUnknownMemberType=none,
    # ``from redis import Redis`` resolves to the module, not the client class.
    from redis.client import Pipeline, Redis

RedisWire = str | bytes
# Matches redis-py ``EncodableT`` for ``hset(..., mapping=)`` (Mapping is invariant).
_RedisHSetMapping = Mapping[
    str | bytes | bytearray | memoryview | int | float,
    str | bytes | bytearray | memoryview | int | float,
]


def _as_redis_key(blob: object, *, what: str) -> RedisWire:
    if isinstance(blob, (str, bytes)):
        return blob
    raise TypeError(f"{what} requires str|bytes KBLOB, got {type(blob).__name__}")


def _as_redis_value(blob: object, *, what: str) -> RedisWire:
    if isinstance(blob, (str, bytes)):
        return blob
    raise TypeError(f"{what} requires str|bytes VBLOB, got {type(blob).__name__}")


def _clock_base(created_at: datetime | None) -> float:
    if created_at is None:
        return time.time()
    if created_at.tzinfo is None:
        return created_at.replace(tzinfo=UTC).timestamp()
    return created_at.timestamp()


class RedisKvBackend[K, V, KBLOB, VBLOB, COLL](KvBackend[K, V, KBLOB, VBLOB, COLL]):
    """Sync redis-py client backend.

    Prefer the factories:

    - ``RedisKvStore.flat(...)`` — ``KeyPrefixBinder`` (``COLL=None``)
    - ``RedisKvStore.hash_collection(...)`` — one HASH per set (``COLL=str``)
    """

    def __init__(
        self,
        kvset: KvSet[K, V, KBLOB, VBLOB],
        client: Redis,
        *,
        scope: Scope | None = None,
        binder: NamespaceBinder[KBLOB, COLL] | None = None,
        binding: CollectionBinding[KBLOB, COLL] | None = None,
        batch_size: int = 500,
    ) -> None:
        super().__init__(
            kvset,
            scope=scope,
            binder=binder,
            binding=binding,
            batch_size=batch_size,
        )
        self._client = client
        self._use_hash = self._binding.collection is not None
        if self._use_hash and self.kvset.ttl_policy.ttl_seconds() is not None:
            raise KvStoreTtlUnsupported(
                "RedisKvStore hash collections do not support KvSet.ttl_policy; "
                "use a KvSet with TtlPolicy.none() (or omit ttl_policy)"
            )

    @property
    def supports_write_clock(self) -> bool:
        return not self._use_hash

    def clone_with_scope(self, scope: Scope) -> RedisKvBackend[K, V, KBLOB, VBLOB, COLL]:
        return RedisKvBackend(
            self.kvset,
            self._client,
            scope=scope,
            binding=self._binding,
            batch_size=self.batch_size,
        )

    def _collection_key(self) -> RedisWire:
        coll = self._binding.collection
        return _as_redis_key(coll, what="Redis collection")

    def _serialize_value(self, value: V) -> RedisWire:
        return _as_redis_value(
            self.kvset.value_serde.serialize(value), what="Redis value"
        )

    def _deserialize_value(self, raw: RedisWire) -> V:
        return self.kvset.value_serde.deserialize(cast(VBLOB, raw))

    def _pk(self, key: K) -> RedisWire:
        return _as_redis_key(self._physical_key_blob(key), what="Redis key")

    def get(self, key: K) -> V | None:
        pk = self._pk(key)
        if self._use_hash:
            raw = self._client.hget(self._collection_key(), pk)
        else:
            raw = self._client.get(pk)
        if raw is None:
            return None
        return self._deserialize_value(raw)

    def exists(self, key: K) -> bool:
        pk = self._pk(key)
        if self._use_hash:
            return bool(self._client.hexists(self._collection_key(), pk))
        return bool(self._client.exists(pk))

    def batch_exists(self, keys: Sequence[K]) -> set[K]:
        keys = list(dict.fromkeys(keys))
        held = self.batch_exists_across([(self, k) for k in keys])
        return {k for k, h in zip(keys, held, strict=True) if h}

    def batch_exists_across(self, pairs: Sequence[tuple[KvBackend[K, V, KBLOB, VBLOB, COLL], K]]) -> list[bool]:
        """One pipeline of ``EXISTS`` (``HEXISTS``) per batch, across scopes."""
        out: list[bool] = []
        for chunk in chunk_sequence(self._pks(pairs), self.batch_size):
            pipe = self._client.pipeline(transaction=False)
            for pk in chunk:
                if self._use_hash:
                    pipe.hexists(self._collection_key(), pk)
                else:
                    pipe.exists(pk)
            out.extend(bool(flag) for flag in pipe.execute())
        return out

    def _pks(self, pairs: Sequence[tuple[KvBackend[K, V, KBLOB, VBLOB, COLL], K]]) -> list[RedisWire]:
        """Each key addressed by its scope's clone; every scope shares the keyspace."""
        return [self._own(b)._pk(k) for b, k in pairs]

    def stat(self, key: K) -> RecordStat | None:
        if not self.exists(key):
            return None
        if self._use_hash:
            return RecordStat(persistent=True)
        pttl = int(self._client.pttl(self._pk(key)))
        if pttl < 0:
            return RecordStat(persistent=True)
        expires_at = datetime.fromtimestamp(time.time() + pttl / 1000.0, tz=UTC)
        return RecordStat(expires_at=expires_at, persistent=False)

    def batch_stat(self, keys: Sequence[K]) -> dict[K, RecordStat]:
        keys = list(dict.fromkeys(keys))
        got = self.batch_stat_across([(self, k) for k in keys])
        return {k: m for k, m in zip(keys, got, strict=True) if m is not None}

    def batch_stat_across(self, pairs: Sequence[tuple[KvBackend[K, V, KBLOB, VBLOB, COLL], K]]) -> list[RecordStat | None]:
        """One pipeline of ``EXISTS`` and ``PTTL`` per batch, across scopes."""
        if self._use_hash:
            return [
                RecordStat(persistent=True) if held else None
                for held in self.batch_exists_across(pairs)
            ]
        out: list[RecordStat | None] = []
        for chunk in chunk_sequence(self._pks(pairs), self.batch_size):
            pipe = self._client.pipeline(transaction=False)
            for pk in chunk:
                pipe.exists(pk)
                pipe.pttl(pk)
            replies = pipe.execute()
            now = time.time()
            for held, pttl in zip(replies[0::2], replies[1::2], strict=True):
                if not held:
                    out.append(None)
                elif int(pttl) < 0:
                    out.append(RecordStat(persistent=True))
                else:
                    expires_at = datetime.fromtimestamp(now + int(pttl) / 1000.0, tz=UTC)
                    out.append(RecordStat(expires_at=expires_at, persistent=False))
        return out

    def set(
        self,
        key: K,
        value: V,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        pk = self._pk(key)
        raw = self._serialize_value(value)
        require_explicit_ttl_supported(
            ttl, allowed=not self._use_hash, backend="RedisKvStore.hash_collection"
        )
        require_write_clock_supported(
            created_at,
            allowed=self.supports_write_clock,
            backend="RedisKvStore",
        )
        ttl_secs = self.resolve_ttl_seconds(ttl)
        if self._use_hash:
            self._client.hset(self._collection_key(), pk, raw)
            if ttl_secs is not None:
                self._client.expire(self._collection_key(), ttl_secs)
            return
        if created_at is not None and ttl_secs is not None:
            self._client.set(pk, raw)
            expire_at = int(_clock_base(created_at) + ttl_secs)
            self._client.expireat(pk, expire_at)
        else:
            self._client.set(pk, raw, ex=ttl_secs)

    def batch_read_versioned(self, keys: Sequence[K]) -> dict[K, Versioned[V]]:
        keys = list(dict.fromkeys(keys))
        return dict(zip(keys, self.batch_read_versioned_across([(self, k) for k in keys]), strict=True))

    def batch_read_versioned_across(self, pairs: Sequence[tuple[KvBackend[K, V, KBLOB, VBLOB, COLL], K]]) -> list[Versioned[V]]:
        """One ``MGET`` (``HMGET``) per batch; the stored bytes are the version.

        Redis removes expired keys itself, so there is never an expired entry
        to read.
        """
        out: list[Versioned[V]] = []
        for chunk in chunk_sequence(self._pks(pairs), self.batch_size):
            out.extend(
                Versioned.absent() if raw is None else Versioned(self._deserialize_value(raw), raw)
                for raw in self._read_raw(self._client, chunk)
            )
        return out

    def _read_raw(self, client: Redis | Pipeline, pks: Sequence[RedisWire]) -> list[RedisWire | None]:
        if self._use_hash:
            return client.hmget(self._collection_key(), list(pks))
        return client.mget(list(pks))

    def batch_write_if_unchanged(
        self, writes: Mapping[K, ConditionalWrite[V]], *, ttl: TtlPolicy | None = None
    ) -> set[K]:
        keys = list(writes)
        won = self.batch_write_if_unchanged_across([(self, k, writes[k]) for k in keys], ttl=ttl)
        return {k for k, w in zip(keys, won, strict=True) if w}

    def batch_write_if_unchanged_across(
        self,
        writes: Sequence[tuple[KvBackend[K, V, KBLOB, VBLOB, COLL], K, ConditionalWrite[V]]],
        *,
        ttl: TtlPolicy | None = None,
    ) -> list[bool]:
        """Per batch, inserts as one pipeline of ``SET NX``; the rest as one ``WATCH`` … ``EXEC``.

        Against nothing stored, ``SET NX`` — or ``HSETNX`` for a hash
        collection — the expiry riding on the same ``SET``. Against a version,
        the keys (or the whole hash) are watched, compared with the bytes
        seen, and those unchanged replaced or removed in one ``MULTI`` that
        Redis discards if anything touched them since the watch — so then
        none of them was written.
        """
        require_explicit_ttl_supported(
            ttl, allowed=not self._use_hash, backend="RedisKvStore.hash_collection"
        )
        for _, _, w in writes:
            require_write_clock_supported(
                w.created_at, allowed=self.supports_write_clock, backend="RedisKvStore"
            )
        ttl_secs = self.resolve_ttl_seconds(ttl)
        pks = self._pks([(b, k) for b, k, _ in writes])
        conditions = [w for _, _, w in writes]
        won = [False] * len(writes)
        inserts = [
            (i, pks[i], w.change.value, w.created_at)
            for i, w in enumerate(conditions)
            if w.seen.version is None and isinstance(w.change, Put)
        ]
        against = [i for i, w in enumerate(conditions) if w.seen.version is not None]
        for chunk in chunk_sequence(inserts, self.batch_size):
            for i in self._insert_if_absent(chunk, ttl_secs):
                won[i] = True
        for chunk in chunk_sequence(against, self.batch_size):
            for i in self._swap_if_unchanged([(i, pks[i], conditions[i]) for i in chunk], ttl_secs):
                won[i] = True
        return won

    def _insert_if_absent(
        self,
        inserts: Sequence[tuple[int, RedisWire, V, datetime | None]],
        ttl_secs: int | None,
    ) -> list[int]:
        pipe = self._client.pipeline(transaction=False)
        for _, pk, value, created_at in inserts:
            raw = self._serialize_value(value)
            if self._use_hash:
                pipe.hsetnx(self._collection_key(), pk, raw)
            elif created_at is not None and ttl_secs is not None:
                pipe.set(pk, raw, nx=True, exat=int(_clock_base(created_at) + ttl_secs))
            else:
                pipe.set(pk, raw, nx=True, ex=ttl_secs)
        written = [i for (i, *_), wrote in zip(inserts, pipe.execute(), strict=True) if wrote]
        if self._use_hash and written and ttl_secs is not None:
            self._client.expire(self._collection_key(), ttl_secs)
        return written

    def _swap_if_unchanged(
        self, against: Sequence[tuple[int, RedisWire, ConditionalWrite[V]]], ttl_secs: int | None
    ) -> list[int]:
        from redis.exceptions import WatchError

        pks = [pk for _, pk, _ in against]
        watched = [self._collection_key()] if self._use_hash else pks
        with self._client.pipeline() as pipe:
            try:
                pipe.watch(*watched)
                current = self._read_raw(pipe, pks)
                unchanged = [
                    (i, pk, w)
                    for (i, pk, w), raw in zip(against, current, strict=True)
                    if raw == w.seen.version
                ]
                if not unchanged:
                    return []
                pipe.multi()
                for _, pk, w in unchanged:
                    raw = (
                        self._serialize_value(w.change.value)
                        if isinstance(w.change, Put)
                        else None
                    )
                    self._queue_write(pipe, pk, raw, ttl_secs, w.created_at)
                pipe.execute()
            except WatchError:
                return []
        return [i for i, _, _ in unchanged]

    def _queue_write(
        self,
        pipe: Pipeline,
        pk: RedisWire,
        raw: RedisWire | None,
        ttl_secs: int | None,
        created_at: datetime | None,
    ) -> None:
        """The replace or removal, queued inside the ``MULTI``."""
        if self._use_hash:
            if raw is None:
                pipe.hdel(self._collection_key(), pk)
            else:
                pipe.hset(self._collection_key(), pk, raw)
                if ttl_secs is not None:
                    pipe.expire(self._collection_key(), ttl_secs)
            return
        if raw is None:
            pipe.delete(pk)
        elif created_at is not None and ttl_secs is not None:
            pipe.set(pk, raw, exat=int(_clock_base(created_at) + ttl_secs))
        else:
            pipe.set(pk, raw, ex=ttl_secs)

    def delete(self, key: K) -> bool:
        pk = self._pk(key)
        if self._use_hash:
            return bool(self._client.hdel(self._collection_key(), pk))
        return bool(self._client.delete(pk))

    def batch_get(self, keys: Sequence[K]) -> dict[K, V]:
        keys = list(dict.fromkeys(keys))
        got = self.batch_get_across([(self, k) for k in keys])
        return {k: v for k, v in zip(keys, got, strict=True) if v is not None}

    def batch_get_across(self, pairs: Sequence[tuple[KvBackend[K, V, KBLOB, VBLOB, COLL], K]]) -> list[V | None]:
        """One ``MGET`` (``HMGET``) per batch, across scopes."""
        out: list[V | None] = []
        for chunk in chunk_sequence(self._pks(pairs), self.batch_size):
            out.extend(
                None if raw is None else self._deserialize_value(raw)
                for raw in self._read_raw(self._client, chunk)
            )
        return out

    def batch_set(
        self,
        items: Mapping[K, V],
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        self.batch_set_across(
            [(self, k, v) for k, v in items.items()], ttl=ttl, created_at=created_at
        )

    def batch_set_across(
        self,
        items: Sequence[tuple[KvBackend[K, V, KBLOB, VBLOB, COLL], K, V]],
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        """One ``HSET`` or one pipeline of ``SET`` per batch, across scopes."""
        require_explicit_ttl_supported(
            ttl, allowed=not self._use_hash, backend="RedisKvStore.hash_collection"
        )
        require_write_clock_supported(
            created_at,
            allowed=self.supports_write_clock,
            backend="RedisKvStore",
        )
        pks = self._pks([(b, k) for b, k, _ in items])
        entries = [(pk, self._serialize_value(v)) for pk, (_, _, v) in zip(pks, items, strict=True)]
        ttl_secs = self.resolve_ttl_seconds(ttl)
        for chunk in chunk_sequence(entries, self.batch_size):
            if self._use_hash:
                self._client.hset(
                    self._collection_key(), mapping=cast(_RedisHSetMapping, dict(chunk))
                )
                if ttl_secs is not None:
                    self._client.expire(self._collection_key(), ttl_secs)
                continue
            pipe = self._client.pipeline(transaction=False)
            if created_at is not None and ttl_secs is not None:
                expire_at = int(_clock_base(created_at) + ttl_secs)
                for pk, raw in chunk:
                    pipe.set(pk, raw)
                    pipe.expireat(pk, expire_at)
            else:
                for pk, raw in chunk:
                    pipe.set(pk, raw, ex=ttl_secs)
            pipe.execute()

    def batch_delete(self, keys: Sequence[K]) -> int:
        return self.batch_delete_across([(self, k) for k in keys])

    def batch_delete_across(self, pairs: Sequence[tuple[KvBackend[K, V, KBLOB, VBLOB, COLL], K]]) -> int:
        """One ``UNLINK`` (``HDEL``) per batch, across scopes."""
        deleted = 0
        for chunk in chunk_sequence(list(dict.fromkeys(self._pks(pairs))), self.batch_size):
            if self._use_hash:
                deleted += int(self._client.hdel(self._collection_key(), *chunk) or 0)
            else:
                deleted += int(self._client.unlink(*chunk) or 0)
        return deleted

    def batch_unset_across(self, pairs: Sequence[tuple[KvBackend[K, V, KBLOB, VBLOB, COLL], K]]) -> int:
        return self.batch_delete_across(pairs)

    def _field_as_kblob(self, field: RedisWire, prefix: KBLOB) -> KBLOB:
        if isinstance(prefix, bytes) and isinstance(field, str):
            return cast(KBLOB, field.encode("utf-8"))
        if isinstance(prefix, str) and isinstance(field, bytes):
            return cast(KBLOB, field.decode("utf-8"))
        return cast(KBLOB, field)

    def scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        if query.prefix is not None and not supports_prefix_scan(self.kvset.key_layout):
            raise KvStoreScanUnsupported(
                f"prefix scan unsupported for layout {self.kvset.key_layout!r}"
            )
        prefix = self._scan_prefix_blob(query.prefix)
        ops = self.kvset.blob_ops
        page_size = query.page_size

        if self._use_hash:
            cursor = 0
            match: str | None = None
            if isinstance(prefix, bytes) and ops.len(prefix) > 0:
                try:
                    match = prefix.decode("utf-8") + "*"
                except UnicodeDecodeError:
                    match = None
            elif isinstance(prefix, str) and ops.len(prefix) > 0:
                match = prefix + "*"
            while True:
                cursor, pairs = self._client.hscan(
                    self._collection_key(),
                    cursor=cursor,
                    match=match,
                    count=page_size,
                )
                fields = cast(Mapping[RedisWire, RedisWire], pairs)
                for field, raw in fields.items():
                    pk = self._field_as_kblob(field, prefix)
                    if not ops.startswith(pk, prefix):
                        continue
                    decoded = self._decode_key_from_physical(pk)
                    if decoded is None:
                        continue
                    if query.include_values:
                        yield decoded, self._deserialize_value(raw)
                    else:
                        yield decoded, None
                if cursor == 0:
                    break
            return

        match_pattern: RedisWire
        if isinstance(prefix, bytes):
            match_pattern = prefix + b"*"
        else:
            match_pattern = str(prefix) + "*"
        cursor = 0
        while True:
            cursor, keys = self._client.scan(
                cursor=cursor, match=match_pattern, count=page_size
            )
            if not keys:
                if cursor == 0:
                    break
                continue
            if query.include_values:
                values = self._client.mget(keys)
                for i, rk in enumerate(keys):
                    value_blob: RedisWire | None = values[i]
                    pk = self._field_as_kblob(rk, prefix)
                    if not ops.startswith(pk, prefix):
                        continue
                    decoded = self._decode_key_from_physical(pk)
                    if decoded is None or value_blob is None:
                        continue
                    yield decoded, self._deserialize_value(value_blob)
            else:
                for rk in keys:
                    pk = self._field_as_kblob(rk, prefix)
                    if not ops.startswith(pk, prefix):
                        continue
                    decoded = self._decode_key_from_physical(pk)
                    if decoded is None:
                        continue
                    yield decoded, None
            if cursor == 0:
                break


class RedisKvStore[K, V, KBLOB, VBLOB, COLL](LeafKvStore[K, V, KBLOB, VBLOB, COLL]):
    """Sync redis-py client store.

    Prefer the factories:

    - ``RedisKvStore.flat(...)`` — ``KeyPrefixBinder`` (``COLL=None``)
    - ``RedisKvStore.hash_collection(...)`` — one HASH per set (``COLL=str``)
    """

    def __init__(
        self,
        kvset: KvSet[K, V, KBLOB, VBLOB],
        client: Redis,
        *,
        scope: Scope | None = None,
        binder: NamespaceBinder[KBLOB, COLL] | None = None,
        binding: CollectionBinding[KBLOB, COLL] | None = None,
        batch_size: int = 500,
    ) -> None:
        super().__init__(
            RedisKvBackend(
                kvset,
                client,
                scope=scope,
                binder=binder,
                binding=binding,
                batch_size=batch_size,
            )
        )

    @classmethod
    def flat(
        cls,
        kvset: KvSet[K, V, KBLOB, VBLOB],
        client: Redis,
        *,
        scope: Scope | None = None,
        batch_size: int = 500,
    ) -> RedisKvStore[K, V, KBLOB, VBLOB, None]:
        return RedisKvStore[K, V, KBLOB, VBLOB, None](
            kvset,
            client,
            scope=scope,
            binder=KeyPrefixBinder(),
            batch_size=batch_size,
        )

    @classmethod
    def hash_collection(
        cls,
        kvset: KvSet[K, V, KBLOB, VBLOB],
        client: Redis,
        *,
        scope: Scope | None = None,
        batch_size: int = 500,
    ) -> RedisKvStore[K, V, KBLOB, VBLOB, str]:
        return RedisKvStore[K, V, KBLOB, VBLOB, str](
            kvset,
            client,
            scope=scope,
            binder=NativeStrCollectionBinder(),
            batch_size=batch_size,
        )

