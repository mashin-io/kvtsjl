"""Composable KvStore wrappers: fallback_read, mirror, readonly, indexed."""

from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping, Sequence
from typing import Any, Literal, overload

from kvtsjl.exceptions import KvStoreIndexError, KvStoreReadOnlyError
from kvtsjl.index import Index, IndexHit
from kvtsjl.key_layout import ScanQuery
from kvtsjl.scope import Scope
from kvtsjl.store import KvStore

logger = logging.getLogger(__name__)


class _DelegatingKvStore[K, V, KBLOB, VBLOB, COLL](KvStore[K, V, KBLOB, VBLOB, COLL]):
    """Base for wrappers that hold an underlying store."""

    def __init__(self, underlying: KvStore[K, V, KBLOB, VBLOB, COLL]) -> None:
        self.kvset = underlying.kvset
        self.scope = underlying.scope
        self.batch_size = underlying.batch_size
        self._binding = underlying.binding
        self._underlying = underlying

    def _clone_with_scope(self, scope: Scope) -> KvStore[K, V, KBLOB, VBLOB, COLL]:
        raise NotImplementedError


class ReadonlyKvStore[K, V, KBLOB, VBLOB, COLL](
    _DelegatingKvStore[K, V, KBLOB, VBLOB, COLL]
):
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

    def _clone_with_scope(self, scope: Scope) -> KvStore[K, V, KBLOB, VBLOB, COLL]:
        return ReadonlyKvStore(self._underlying._clone_with_scope(scope))

    def readonly(self) -> KvStore[K, V, KBLOB, VBLOB, COLL]:
        return self


class FallbackReadKvStore[K, V, KBLOB, VBLOB, COLL, SEC_COLL](
    _DelegatingKvStore[K, V, KBLOB, VBLOB, COLL]
):
    """Read primary then secondary; writes go to primary only. Scan: primary only."""

    def __init__(
        self,
        primary: KvStore[K, V, KBLOB, VBLOB, COLL],
        secondary: KvStore[K, V, KBLOB, VBLOB, SEC_COLL],
        *,
        promote: bool = True,
    ) -> None:
        super().__init__(primary)
        self._primary = primary
        self._secondary = secondary
        self._promote = promote

    def get(self, key: K) -> V | None:
        value = self._primary.get(key)
        if value is not None:
            return value
        value = self._secondary.get(key)
        if value is not None and self._promote:
            try:
                self._primary.set(key, value)
            except Exception:
                logger.exception("fallback promote failed for key")
        return value

    def set(self, key: K, value: V) -> None:
        self._primary.set(key, value)

    def delete(self, key: K) -> bool:
        return self._primary.delete(key)

    def batch_get(self, keys: Sequence[K]) -> dict[K, V]:
        found = self._primary.batch_get(keys)
        missing = [k for k in keys if k not in found]
        if not missing:
            return found
        secondary_found = self._secondary.batch_get(missing)
        if secondary_found and self._promote:
            try:
                self._primary.batch_set(secondary_found)
            except Exception:
                logger.exception("fallback batch promote failed")
        found.update(secondary_found)
        return found

    def batch_set(self, items: Mapping[K, V]) -> None:
        self._primary.batch_set(items)

    def batch_delete(self, keys: Sequence[K]) -> int:
        return self._primary.batch_delete(keys)

    def _scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        yield from self._primary._scan_entries(query)

    def _clone_with_scope(self, scope: Scope) -> KvStore[K, V, KBLOB, VBLOB, COLL]:
        return FallbackReadKvStore(
            self._primary._clone_with_scope(scope),
            self._secondary._clone_with_scope(scope),
            promote=self._promote,
        )


class MirrorKvStore[K, V, KBLOB, VBLOB, COLL, SEC_COLL](
    _DelegatingKvStore[K, V, KBLOB, VBLOB, COLL]
):
    """Reads/scans from primary; writes/deletes go to both (secondary best-effort)."""

    def __init__(
        self,
        primary: KvStore[K, V, KBLOB, VBLOB, COLL],
        secondary: KvStore[K, V, KBLOB, VBLOB, SEC_COLL],
    ) -> None:
        super().__init__(primary)
        self._primary = primary
        self._secondary = secondary

    def get(self, key: K) -> V | None:
        return self._primary.get(key)

    def set(self, key: K, value: V) -> None:
        self._primary.set(key, value)
        try:
            self._secondary.set(key, value)
        except Exception:
            logger.exception("mirror set to secondary failed")

    def delete(self, key: K) -> bool:
        deleted = self._primary.delete(key)
        try:
            self._secondary.delete(key)
        except Exception:
            logger.exception("mirror delete on secondary failed")
        return deleted

    def batch_get(self, keys: Sequence[K]) -> dict[K, V]:
        return self._primary.batch_get(keys)

    def batch_set(self, items: Mapping[K, V]) -> None:
        self._primary.batch_set(items)
        try:
            self._secondary.batch_set(items)
        except Exception:
            logger.exception("mirror batch_set to secondary failed")

    def batch_delete(self, keys: Sequence[K]) -> int:
        n = self._primary.batch_delete(keys)
        try:
            self._secondary.batch_delete(keys)
        except Exception:
            logger.exception("mirror batch_delete on secondary failed")
        return n

    def _scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        yield from self._primary._scan_entries(query)

    def _clone_with_scope(self, scope: Scope) -> KvStore[K, V, KBLOB, VBLOB, COLL]:
        return MirrorKvStore(
            self._primary._clone_with_scope(scope),
            self._secondary._clone_with_scope(scope),
        )


class IndexedKvStore[K, V, KBLOB, VBLOB, COLL, ViaT](
    _DelegatingKvStore[K, V, KBLOB, VBLOB, COLL]
):
    """Primary store plus attached indexes; mutations sync all indexes.

    Construction:

    - ``store.indexed(one)`` — default index; ``search(query)`` uses it.
    - ``store.indexed_as(bundle)`` — ``via`` is the same dataclass; each ``Index``
      field is callable for hydrated search.
    - ``search(index, query)`` — always available (explicit index).

    Prefix ``scan`` stays on the backend.
    """

    def __init__(
        self,
        underlying: KvStore[K, V, KBLOB, VBLOB, COLL],
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
        """Index dataclass from ``indexed_as`` (``None`` for plain ``indexed``)."""
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

    def set(self, key: K, value: V) -> None:
        self._underlying.set(key, value)
        self._sync_upsert(key, value)

    def delete(self, key: K) -> bool:
        deleted = self._underlying.delete(key)
        if deleted:
            self._sync_delete(key)
        return deleted

    def batch_get(self, keys: Sequence[K]) -> dict[K, V]:
        return self._underlying.batch_get(keys)

    def batch_set(self, items: Mapping[K, V]) -> None:
        self._underlying.batch_set(items)
        self._sync_batch_upsert(items)

    def batch_delete(self, keys: Sequence[K]) -> int:
        deleted = self._underlying.batch_delete(keys)
        self._sync_batch_delete(keys)
        return deleted

    def _scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        yield from self._underlying._scan_entries(query)

    def _clone_with_scope(
        self, scope: Scope
    ) -> IndexedKvStore[K, V, KBLOB, VBLOB, COLL, ViaT]:
        return IndexedKvStore(
            self._underlying._clone_with_scope(scope),
            self._indexes,
            default_index=self._default_index,
            bundle=self._bundle,
        )

    def _require_attached[Q, M](
        self, index: Index[Q, K, V, M]
    ) -> Index[Q, K, V, M]:
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
        """Raw index hits (document keys + index metadata) without hydration."""
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
        """Search and hydrate.

        - ``search(index, query)`` — explicit attached index (always available).
        - ``search(query)`` — uses the default index from ``indexed(one)`` (or a
          single-index ``indexed_as`` bundle).
        """
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
