"""Index ABC and search hits."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, overload

from kvtsjl.exceptions import KvStoreIndexError
from kvtsjl.keymap import KeyMap

if TYPE_CHECKING:
    from kvtsjl.store.compose.indexed import IndexedKvStore


@dataclass(frozen=True, slots=True)
class IndexHit[K, M]:
    """One document key from an index search, plus index-specific metadata."""

    key: K
    meta: M


class Index[Q, K, V, M](KeyMap[K, M], ABC):
    """Logical search facet: query document keys, metadata as ``KeyMap[K, M]``.

    Leaf implementations live in ``kvtsjl.backends.index`` as ``IndexBackend``
    subclasses (``Index`` + ``IndexSet`` + binding). Attach via
    ``KvStore.indexed(...)``.
    """

    sync_on_write: bool = True

    _store: IndexedKvStore[K, V, Any] | None = None

    def _bind(self, store: IndexedKvStore[K, V, Any]) -> None:
        self._store = store

    @abstractmethod
    def search(self, query: Q, *, limit: int = 100) -> Sequence[IndexHit[K, M]]:
        """Return matching hits (ordered as defined by the index)."""

    @abstractmethod
    def meta_of(self, key: K, value: V, *, previous: M | None) -> M:
        """Build metadata for a store ``sync``."""

    @abstractmethod
    def upsert(self, key: K, value: V, meta: M) -> None:
        """Insert or update ``key`` (index structures) and stored metadata."""

    def batch_upsert(self, items: Mapping[K, tuple[V, M]]) -> None:
        for key, (value, meta) in items.items():
            self.upsert(key, value, meta)

    def sync(self, key: K, value: V) -> None:
        previous = self.get(key)
        self.upsert(key, value, self.meta_of(key, value, previous=previous))

    def batch_sync(self, items: Mapping[K, V]) -> None:
        prepared: dict[K, tuple[V, M]] = {}
        for key, value in items.items():
            previous = self.get(key)
            prepared[key] = (value, self.meta_of(key, value, previous=previous))
        self.batch_upsert(prepared)

    @overload
    def __call__(
        self,
        query: Q,
        *,
        limit: int = 100,
        include_keys: Literal[False] = False,
    ) -> list[V]: ...

    @overload
    def __call__(
        self,
        query: Q,
        *,
        limit: int = 100,
        include_keys: Literal[True],
    ) -> list[tuple[K, V]]: ...

    def __call__(
        self,
        query: Q,
        *,
        limit: int = 100,
        include_keys: bool = False,
    ) -> list[V] | list[tuple[K, V]]:
        store = self._store
        if store is None:
            raise KvStoreIndexError(
                "index is not bound to a store; use store.via.<field> or "
                "store.search(index, query)"
            )
        return store.search(self, query, limit=limit, include_keys=include_keys)
