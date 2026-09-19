"""Read-only logical store wrapper."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from datetime import datetime

from kvtsjl.exceptions import KvStoreReadOnlyError
from kvtsjl.scope import Scope
from kvtsjl.store.logical import KvStore
from kvtsjl.store.readable import ReadableKvStore
from kvtsjl.store.schema.layout import KeyLayout, ScanQuery
from kvtsjl.store.schema.stat import RecordStat
from kvtsjl.store.schema.ttl import TtlPolicy


class ReadonlyKvStore[K, V](ReadableKvStore[K, V]):
    """Runtime write guard returned by ``KvStore.readonly()``.

    Declared as ``ReadableKvStore`` (no writes in the type). Write / GC methods
    exist only so untyped callers get ``KvStoreReadOnlyError`` instead of
    ``AttributeError`` — the documented product contract for ``.readonly()``.
    """

    def __init__(self, underlying: KvStore[K, V]) -> None:
        self.scope = underlying.scope
        self.batch_size = underlying.batch_size
        self._underlying = underlying

    def key_layout(self) -> KeyLayout:
        return self._underlying.key_layout()

    def get(self, key: K) -> V | None:
        return self._underlying.get(key)

    def exists(self, key: K) -> bool:
        return self._underlying.exists(key)

    def batch_exists(self, keys: Sequence[K]) -> set[K]:
        return self._underlying.batch_exists(keys)

    def stat(self, key: K) -> RecordStat | None:
        return self._underlying.stat(key)

    def batch_stat(self, keys: Sequence[K]) -> dict[K, RecordStat]:
        return self._underlying.batch_stat(keys)

    def batch_get(self, keys: Sequence[K]) -> dict[K, V]:
        return self._underlying.batch_get(keys)

    def set(
        self,
        key: K,
        value: V,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        raise KvStoreReadOnlyError("set on readonly store")

    def delete(self, key: K) -> bool:
        raise KvStoreReadOnlyError("delete on readonly store")

    def batch_set(
        self,
        items: Mapping[K, V],
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        raise KvStoreReadOnlyError("batch_set on readonly store")

    def batch_delete(self, keys: Sequence[K]) -> int:
        raise KvStoreReadOnlyError("batch_delete on readonly store")

    def gc_expired_keys(self, *, max_entries: int) -> list[K]:
        raise KvStoreReadOnlyError("gc_expired on readonly store")

    def gc_expired(self, *, max_entries: int) -> int:
        raise KvStoreReadOnlyError("gc_expired on readonly store")

    def scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        yield from self._underlying.scan_entries(query)

    def clone_with_scope(self, scope: Scope) -> ReadableKvStore[K, V]:
        return ReadonlyKvStore(self._underlying.clone_with_scope(scope))

    def readonly(self) -> ReadableKvStore[K, V]:
        return self

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import compose_repr

        return compose_repr("ReadonlyKvStore", underlying=self._underlying)
