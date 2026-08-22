"""Read-only logical store wrapper."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence

from kvtsjl.exceptions import KvStoreReadOnlyError
from kvtsjl.scope import Scope
from kvtsjl.store.compose.delegating import DelegatingKvStore
from kvtsjl.store.logical import KvStore
from kvtsjl.store.schema.layout import ScanQuery


class ReadonlyKvStore[K, V](DelegatingKvStore[K, V]):
    def get(self, key: K) -> V | None:
        return self._underlying.get(key)

    def set(self, key: K, value: V) -> None:
        raise KvStoreReadOnlyError("set on readonly store")

    def delete(self, key: K) -> bool:
        raise KvStoreReadOnlyError("delete on readonly store")

    def batch_get(self, keys: Sequence[K]) -> dict[K, V]:
        return self._underlying.batch_get(keys)

    def batch_set(self, items: Mapping[K, V]) -> None:
        raise KvStoreReadOnlyError("batch_set on readonly store")

    def batch_delete(self, keys: Sequence[K]) -> int:
        raise KvStoreReadOnlyError("batch_delete on readonly store")

    def _scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        yield from self._underlying._scan_entries(query)

    def _clone_with_scope(self, scope: Scope) -> KvStore[K, V]:
        return ReadonlyKvStore(self._underlying._clone_with_scope(scope))

    def readonly(self) -> KvStore[K, V]:
        return self

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import compose_repr

        return compose_repr("ReadonlyKvStore", underlying=self._underlying)
