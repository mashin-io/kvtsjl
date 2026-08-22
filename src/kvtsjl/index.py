"""Indexes for search over a KvStore (exact / term / later semantic)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, fields, is_dataclass
from typing import TYPE_CHECKING, Any, Literal, overload

from kvtsjl.exceptions import KvStoreIndexError
from kvtsjl.keymap import KeyMap

if TYPE_CHECKING:
    from kvtsjl.compose import IndexedKvStore


@dataclass(frozen=True, slots=True)
class IndexHit[K, M]:
    """One document key from an index search, plus index-specific metadata.

    ``M`` is defined by the index. Use ``None`` when the index carries no
    per-hit metadata.
    """

    key: K
    meta: M


class Index[Q, K, V, M](KeyMap[K, M], ABC):
    """Search over document keys, with per-key metadata as a ``KeyMap[K, M]``.

    ``KeyMap`` ops address index-owned metadata ``M`` (``get`` / ``set`` /
    ``delete`` / batch forms). ``set`` updates metadata only and should raise
    ``KvStoreIndexError`` if the key is not indexed. Document bodies stay on
    the attached ``KvStore`` (``V``).

    Write paths beyond ``KeyMap``:

    - Store sync: ``sync`` / ``batch_sync`` → ``meta_of`` + ``upsert`` /
      ``batch_upsert`` (may rebuild index structures).
    - Explicit write: ``upsert`` / ``batch_upsert`` with caller-supplied meta.

    On ``sync``, ``meta_of`` receives ``previous`` (from ``get``) so value
    mutations can keep fields that are not derived from ``value``.
    """

    sync_on_write: bool = True
    """When True, ``IndexedKvStore`` calls ``sync``/``delete`` on mutations."""

    _store: IndexedKvStore[K, V, Any, Any, Any, Any] | None = None

    def _bind(self, store: IndexedKvStore[K, V, Any, Any, Any, Any]) -> None:
        self._store = store

    @abstractmethod
    def search(self, query: Q, *, limit: int = 100) -> Sequence[IndexHit[K, M]]:
        """Return matching hits (ordered as defined by the index)."""

    @abstractmethod
    def meta_of(self, key: K, value: V, *, previous: M | None) -> M:
        """Build metadata for a store ``sync``.

        - ``previous is None``: first time this key is indexed — defaults +
          value-derived fields.
        - ``previous is not None``: KV value changed — retain non-value-derived
          fields from ``previous``, refresh value-derived fields from ``value``.
        """

    @abstractmethod
    def upsert(self, key: K, value: V, meta: M) -> None:
        """Insert or update ``key`` (index structures) and stored metadata.

        Prefer ``sync`` for normal KV writes. Prefer ``set`` when only ``M``
        changes and a full re-index is unnecessary.
        """

    def batch_upsert(self, items: Mapping[K, tuple[V, M]]) -> None:
        """Insert or update many keys with explicit ``(value, meta)`` pairs.

        Default loops ``upsert``. Override when the backend supports bulk
        writes (``batch_sync`` builds pairs then calls this).
        """
        for key, (value, meta) in items.items():
            self.upsert(key, value, meta)

    def sync(self, key: K, value: V) -> None:
        """Insert or update this key from the current KV ``value`` (store write path)."""
        previous = self.get(key)
        self.upsert(key, value, self.meta_of(key, value, previous=previous))

    def batch_sync(self, items: Mapping[K, V]) -> None:
        """Insert or update many keys from a store ``batch_set``.

        Builds ``(value, meta)`` via ``meta_of`` then ``batch_upsert``. Override
        ``batch_upsert`` for native bulk APIs.
        """
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
        """Hydrated search; requires the index be bound via ``indexed`` / ``indexed_as``."""
        store = self._store
        if store is None:
            raise KvStoreIndexError(
                "index is not bound to a store; use store.via.<field> or "
                "store.search(index, query)"
            )
        return store.search(self, query, limit=limit, include_keys=include_keys)


@dataclass
class MemoryKeyIndex[K, V](Index[K, K, V, None]):
    """Exact membership index: query type is the document key ``K``.

    ``K`` must be hashable. Hits carry no metadata (``meta=None``).
    """

    sync_on_write: bool = True
    _keys: set[K] = field(default_factory=set)

    def search(self, query: K, *, limit: int = 100) -> Sequence[IndexHit[K, None]]:
        if limit <= 0:
            return []
        if query in self._keys:
            return [IndexHit(key=query, meta=None)]
        return []

    def get(self, key: K) -> None:
        return None

    def meta_of(self, key: K, value: V, *, previous: None) -> None:
        return None

    def upsert(self, key: K, value: V, meta: None) -> None:
        self._keys.add(key)

    def set(self, key: K, value: None) -> None:
        if key not in self._keys:
            raise KvStoreIndexError("key is not in the index")

    def delete(self, key: K) -> bool:
        if key not in self._keys:
            return False
        self._keys.discard(key)
        return True


@dataclass
class MemoryTermIndex[K, V](Index[str, K, V, None]):
    """Exact term → keys multimaps (in-memory).

    ``terms_of(key, value)`` extracts index terms on upsert. ``K`` must be
    hashable. Search is equality on a single term (not full-text ranking).
    Hits carry no metadata (``meta=None``); terms are posting state, not ``M``.
    """

    terms_of: Callable[[K, V], Sequence[str]]
    sync_on_write: bool = True
    _term_to_keys: dict[str, set[K]] = field(default_factory=lambda: defaultdict(set))
    _key_to_terms: dict[K, set[str]] = field(default_factory=dict)

    def search(self, query: str, *, limit: int = 100) -> Sequence[IndexHit[K, None]]:
        if limit <= 0:
            return []
        keys = self._term_to_keys.get(query, set())
        out: list[IndexHit[K, None]] = []
        for key in keys:
            out.append(IndexHit(key=key, meta=None))
            if len(out) >= limit:
                break
        return out

    def get(self, key: K) -> None:
        return None

    def meta_of(self, key: K, value: V, *, previous: None) -> None:
        return None

    def upsert(self, key: K, value: V, meta: None) -> None:
        new_terms = set(self.terms_of(key, value))
        old_terms = self._key_to_terms.get(key, set())
        for term in old_terms - new_terms:
            bucket = self._term_to_keys.get(term)
            if bucket is not None:
                bucket.discard(key)
                if not bucket:
                    del self._term_to_keys[term]
        for term in new_terms - old_terms:
            self._term_to_keys[term].add(key)
        if new_terms:
            self._key_to_terms[key] = new_terms
        else:
            self._key_to_terms.pop(key, None)

    def set(self, key: K, value: None) -> None:
        if key not in self._key_to_terms:
            raise KvStoreIndexError("key is not in the index")

    def delete(self, key: K) -> bool:
        terms = self._key_to_terms.pop(key, None)
        if not terms:
            return False
        for term in terms:
            bucket = self._term_to_keys.get(term)
            if bucket is not None:
                bucket.discard(key)
                if not bucket:
                    del self._term_to_keys[term]
        return True


def indexes_from_bundle(bundle: object) -> tuple[Index[Any, Any, Any, Any], ...]:
    """Collect ``Index`` fields from a dataclass instance (top-level only)."""
    if not is_dataclass(bundle) or isinstance(bundle, type):
        raise TypeError("indexed_as expects a dataclass instance")
    found: list[Index[Any, Any, Any, Any]] = []
    for f in fields(bundle):
        value = getattr(bundle, f.name)
        if isinstance(value, Index):
            found.append(value)
    if not found:
        raise ValueError("dataclass bundle has no Index fields")
    return tuple(found)
