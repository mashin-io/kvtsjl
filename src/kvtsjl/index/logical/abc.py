"""Index ABC and search hits."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, overload

from kvtsjl.exceptions import KvStoreIndexError
from kvtsjl.keymap import KeyMap
from kvtsjl.scope import Scope
from kvtsjl.store.capabilities import ScopedMixin

if TYPE_CHECKING:
    from kvtsjl.store.compose.indexed import IndexedKvStore


@dataclass(frozen=True, slots=True)
class IndexHit[K, M]:
    """One document key from an index search, plus index-specific metadata."""

    key: K
    meta: M


class Index[Q, K, V, M](KeyMap[K, M], ScopedMixin, ABC):
    """Logical search facet: query document keys, metadata as ``KeyMap[K, M]``.

    Leaf implementations live in ``kvtsjl.backends.index`` as ``IndexBackend``
    subclasses (``Index`` + ``IndexSet`` + binding). Attach via
    ``KvStore.indexed(...)``.

    ``ScopedMixin`` supplies ``scoped()`` / ``prefixed()`` and a raising
    ``clone_with_scope`` default (P1 fail-closed); scopable indexes override.
    """

    sync_on_write: bool = True

    _store: IndexedKvStore[K, V, Any] | None = None

    # Indexes are not scoped until attached / cloned; default empty prefix.
    scope: Scope = Scope.empty()

    def bind_store(self, store: IndexedKvStore[K, V, Any]) -> None:
        if self._store is not None and self._store is not store:
            raise KvStoreIndexError(
                f"{type(self).__name__} is already bound to another store"
            )
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
        # One read for the whole batch. meta_of needs the previous record for
        # every key, and asking per key is a round trip per key; batch_get is
        # already the contract for reading several, and a backend that can do
        # it in one call overrides it there.
        previous_by_key = self.batch_get(list(items))
        prepared: dict[K, tuple[V, M]] = {
            key: (value, self.meta_of(key, value, previous=previous_by_key.get(key)))
            for key, value in items.items()
        }
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
