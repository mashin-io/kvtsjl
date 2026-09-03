"""Indexed logical store with search and sync."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
import logging
from typing import Any, Literal, overload

from kvtsjl.exceptions import KvStoreIndexError
from kvtsjl.index.logical.abc import Index, IndexHit
from kvtsjl.scope import Scope
from kvtsjl.store.compose.delegating import DelegatingKvStore
from kvtsjl.store.logical import KvStore
from kvtsjl.store.schema.layout import ScanQuery
from kvtsjl.store.schema.ttl import TtlPolicy

logger = logging.getLogger(__name__)


class IndexedKvStore[K, V, ViaT](DelegatingKvStore[K, V]):
    """Primary store plus attached indexes; mutations sync all indexes."""

    def __init__(
        self,
        underlying: KvStore[K, V],
        indexes: Sequence[Index[Any, K, V, Any]],
        *,
        default_index: Index[Any, K, V, Any] | None = None,
        bundle: ViaT | None = None,
    ) -> None:
        super().__init__(underlying)
        self._indexes: tuple[Index[Any, K, V, Any], ...] = tuple(indexes)
        self._index_ids = {id(index) for index in self._indexes}
        if default_index is not None and id(default_index) not in self._index_ids:
            raise KvStoreIndexError("default_index must be one of the attached indexes")
        self._default_index = default_index
        self._bundle = bundle
        for index in self._indexes:
            index._bind(self)

    @property
    def via(self) -> ViaT | None:
        if self._bundle is None:
            return None
        for index in self._indexes:
            index._bind(self)
        return self._bundle

    @property
    def indexes(self) -> tuple[Index[Any, K, V, Any], ...]:
        return self._indexes

    @property
    def default_index(self) -> Index[Any, K, V, Any] | None:
        return self._default_index

    def get(self, key: K) -> V | None:
        return self._underlying.get(key)

    def set(self, key: K, value: V, *, ttl: TtlPolicy | None = None) -> None:
        self._underlying.set(key, value, ttl=ttl)
        self._sync_upsert(key, value)

    def delete(self, key: K) -> bool:
        deleted = self._underlying.delete(key)
        if deleted:
            self._sync_delete(key)
        return deleted

    def batch_get(self, keys: Sequence[K]) -> dict[K, V]:
        return self._underlying.batch_get(keys)

    def batch_set(
        self, items: Mapping[K, V], *, ttl: TtlPolicy | None = None
    ) -> None:
        self._underlying.batch_set(items, ttl=ttl)
        self._sync_batch_upsert(items)

    def batch_delete(self, keys: Sequence[K]) -> int:
        deleted = self._underlying.batch_delete(keys)
        self._sync_batch_delete(keys)
        return deleted

    def _scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        yield from self._underlying._scan_entries(query)

    def _clone_with_scope(self, scope: Scope) -> IndexedKvStore[K, V, ViaT]:
        return IndexedKvStore(
            self._underlying._clone_with_scope(scope),
            self._indexes,
            default_index=self._default_index,
            bundle=self._bundle,
        )

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import compose_repr

        return compose_repr(
            "IndexedKvStore",
            underlying=self._underlying,
            indexes=list(self._indexes),
        )

    def _require_attached[Q, M](self, index: Index[Q, K, V, M]) -> Index[Q, K, V, M]:
        if id(index) not in self._index_ids:
            raise KvStoreIndexError("index is not attached to this store")
        return index

    def _sync_upsert(self, key: K, value: V) -> None:
        for index in self._indexes:
            if not index.sync_on_write:
                continue
            try:
                index.sync(key, value)
            except Exception:
                logger.exception("index upsert failed (%s)", type(index).__name__)

    def _sync_batch_upsert(self, items: Mapping[K, V]) -> None:
        if not items:
            return
        for index in self._indexes:
            if not index.sync_on_write:
                continue
            try:
                index.batch_sync(items)
            except Exception:
                logger.exception("index batch upsert failed (%s)", type(index).__name__)

    def _sync_delete(self, key: K) -> None:
        for index in self._indexes:
            if not index.sync_on_write:
                continue
            try:
                index.delete(key)
            except Exception:
                logger.exception("index delete failed (%s)", type(index).__name__)

    def _sync_batch_delete(self, keys: Sequence[K]) -> None:
        if not keys:
            return
        for index in self._indexes:
            if not index.sync_on_write:
                continue
            try:
                index.batch_delete(keys)
            except Exception:
                logger.exception("index batch delete failed (%s)", type(index).__name__)

    def search_hits[Q, M](
        self, index: Index[Q, K, V, M], query: Q, *, limit: int = 100
    ) -> list[IndexHit[K, M]]:
        return list(self._require_attached(index).search(query, limit=limit))

    def _hydrate(
        self,
        index: Index[Any, K, V, Any],
        query: object,
        *,
        limit: int,
        include_keys: bool,
    ) -> list[V] | list[tuple[K, V]]:
        hits = self.search_hits(index, query, limit=limit)
        if not hits:
            return []
        keys = [hit.key for hit in hits]
        found = self.batch_get(keys)
        if include_keys:
            return [(key, found[key]) for key in keys if key in found]
        return [found[key] for key in keys if key in found]

    @overload
    def search[Q, M](
        self,
        index: Index[Q, K, V, M],
        query: Q,
        /,
        *,
        limit: int = 100,
        include_keys: Literal[False] = False,
    ) -> list[V]: ...

    @overload
    def search[Q, M](
        self,
        index: Index[Q, K, V, M],
        query: Q,
        /,
        *,
        limit: int = 100,
        include_keys: Literal[True],
    ) -> list[tuple[K, V]]: ...

    @overload
    def search(
        self,
        query: object,
        /,
        *,
        limit: int = 100,
        include_keys: Literal[False] = False,
    ) -> list[V]: ...

    @overload
    def search(
        self,
        query: object,
        /,
        *,
        limit: int = 100,
        include_keys: Literal[True],
    ) -> list[tuple[K, V]]: ...

    def search(
        self,
        index_or_query: Index[Any, K, V, Any] | object,
        query: object | None = None,
        /,
        *,
        limit: int = 100,
        include_keys: bool = False,
    ) -> list[V] | list[tuple[K, V]]:
        if query is None:
            if self._default_index is None:
                raise KvStoreIndexError(
                    "no default index; use search(index, query) or indexed(single)"
                )
            return self._hydrate(
                self._default_index,
                index_or_query,
                limit=limit,
                include_keys=include_keys,
            )
        if not isinstance(index_or_query, Index):
            raise TypeError("search(index, query) requires an Index as the first argument")
        return self._hydrate(
            index_or_query,
            query,
            limit=limit,
            include_keys=include_keys,
        )
