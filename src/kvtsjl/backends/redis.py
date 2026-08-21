"""Redis-backed KvStore (string keys or HASH collections).

Install with::

    pip install 'kvtsjl[redis]'
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import cast

import redis

from kvtsjl.batching import chunk_sequence
from kvtsjl.exceptions import KvStoreScanUnsupported
from kvtsjl.key_layout import ScanQuery, supports_prefix_scan
from kvtsjl.kvset import KvSet
from kvtsjl.namespace import (
    CollectionBinding,
    KeyPrefixBinder,
    NamespaceBinder,
    NativeStrCollectionBinder,
)
from kvtsjl.scope import Scope
from kvtsjl.store import KvStore

RedisWire = str | bytes


def _as_redis_key(blob: object, *, what: str) -> RedisWire:
    if isinstance(blob, (str, bytes)):
        return blob
    raise TypeError(f"{what} requires str|bytes KBLOB, got {type(blob).__name__}")


def _as_redis_value(blob: object, *, what: str) -> RedisWire:
    if isinstance(blob, (str, bytes)):
        return blob
    raise TypeError(f"{what} requires str|bytes VBLOB, got {type(blob).__name__}")


class RedisKvStore[K, V, KBLOB, VBLOB, COLL](KvStore[K, V, KBLOB, VBLOB, COLL]):
    """Sync redis-py client store.

    Prefer the factories:

    - ``RedisKvStore.flat(...)`` — ``KeyPrefixBinder`` (``COLL=None``)
    - ``RedisKvStore.hash_collection(...)`` — one HASH per set (``COLL=str``)
    """

    def __init__(
        self,
        kvset: KvSet[K, V, KBLOB, VBLOB],
        client: redis.Redis,
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

    @classmethod
    def flat(
        cls,
        kvset: KvSet[K, V, KBLOB, VBLOB],
        client: redis.Redis,
        *,
        scope: Scope | None = None,
        batch_size: int = 500,
    ) -> RedisKvStore[K, V, KBLOB, VBLOB, None]:
        return RedisKvStore(
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
        client: redis.Redis,
        *,
        scope: Scope | None = None,
        batch_size: int = 500,
    ) -> RedisKvStore[K, V, KBLOB, VBLOB, str]:
        return RedisKvStore(
            kvset,
            client,
            scope=scope,
            binder=NativeStrCollectionBinder(),
            batch_size=batch_size,
        )

    def _clone_with_scope(self, scope: Scope) -> RedisKvStore[K, V, KBLOB, VBLOB, COLL]:
        return RedisKvStore(
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

    def set(self, key: K, value: V) -> None:
        pk = self._pk(key)
        raw = self._serialize_value(value)
        ttl = self.ttl_seconds()
        if self._use_hash:
            self._client.hset(self._collection_key(), pk, raw)
            if ttl is not None:
                self._client.expire(self._collection_key(), ttl)
        else:
            self._client.set(pk, raw, ex=ttl)

    def delete(self, key: K) -> bool:
        pk = self._pk(key)
        if self._use_hash:
            return bool(self._client.hdel(self._collection_key(), pk))
        return bool(self._client.delete(pk))

    def batch_get(self, keys: Sequence[K]) -> dict[K, V]:
        out: dict[K, V] = {}
        for chunk in chunk_sequence(keys, self.batch_size):
            if not chunk:
                continue
            physical = [self._pk(k) for k in chunk]
            if self._use_hash:
                values = self._client.hmget(self._collection_key(), physical)
                for key, raw in zip(chunk, values, strict=True):
                    if raw is not None:
                        out[key] = self._deserialize_value(raw)
            else:
                values = self._client.mget(physical)
                for key, raw in zip(chunk, values, strict=True):
                    if raw is not None:
                        out[key] = self._deserialize_value(raw)
        return out

    def batch_set(self, items: Mapping[K, V]) -> None:
        pairs = list(items.items())
        ttl = self.ttl_seconds()
        for chunk in chunk_sequence(pairs, self.batch_size):
            if self._use_hash:
                mapping = {self._pk(k): self._serialize_value(v) for k, v in chunk}
                if mapping:
                    self._client.hset(self._collection_key(), mapping=mapping)
                    if ttl is not None:
                        self._client.expire(self._collection_key(), ttl)
            else:
                pipe = self._client.pipeline(transaction=False)
                for k, v in chunk:
                    pipe.set(self._pk(k), self._serialize_value(v), ex=ttl)
                pipe.execute()

    def batch_delete(self, keys: Sequence[K]) -> int:
        deleted = 0
        for chunk in chunk_sequence(keys, self.batch_size):
            if not chunk:
                continue
            physical = [self._pk(k) for k in chunk]
            if self._use_hash:
                deleted += int(
                    self._client.hdel(self._collection_key(), *physical) or 0
                )
            else:
                deleted += int(self._client.unlink(*physical) or 0)
        return deleted

    def _field_as_kblob(self, field: RedisWire, prefix: KBLOB) -> KBLOB:
        if isinstance(prefix, bytes) and isinstance(field, str):
            return cast(KBLOB, field.encode("utf-8"))
        if isinstance(prefix, str) and isinstance(field, bytes):
            return cast(KBLOB, field.decode("utf-8"))
        return cast(KBLOB, field)

    def _scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
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
                for field, raw in pairs.items():
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
                for rk, raw in zip(keys, values, strict=True):
                    pk = self._field_as_kblob(rk, prefix)
                    if not ops.startswith(pk, prefix):
                        continue
                    decoded = self._decode_key_from_physical(pk)
                    if decoded is None or raw is None:
                        continue
                    yield decoded, self._deserialize_value(raw)
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
