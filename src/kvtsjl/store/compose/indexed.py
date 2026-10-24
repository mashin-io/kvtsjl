"""Indexed logical store with search and sync."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import fields, is_dataclass
from typing import Any, Literal, Never, cast, overload

from kvtsjl.exceptions import KvStoreIndexError, PartialSyncError
from kvtsjl.index.logical.abc import Index, IndexHit
from kvtsjl.keymap_algebra.topology import Role, Target, Topology, WriteOptions
from kvtsjl.scope import Scope
from kvtsjl.store.compose.delegating import DelegatingKvStore
from kvtsjl.store.compose.topology import StoreTopologyMixin
from kvtsjl.store.logical import KvStore
from kvtsjl.store.schema.policy import SyncPolicy


def _index_fanout_label(i: int, index: Index[Any, Any, Any, Any]) -> str:
    """Stable unique label; must not raise (runs on the failure path)."""
    name = getattr(getattr(index, "index_set", None), "name", None) or type(index).__name__
    return f"{i}:{name}"


class _IndexesTarget[K, V](Target[K, V, Never]):
    """The attached indexes, as one dependent of the record.

    It holds nothing a read could return — hence ``Never`` — so it is never
    looked up, only written and deleted.

    Every index is attempted even when one fails, and the failures are
    reported together, labelled by index; the topology applies the policy.
    """

    def __init__(
        self, indexes: Sequence[Index[Any, K, V, Any]], *, policy: SyncPolicy
    ) -> None:
        super().__init__("indexes", Role.DEPENDENT, policy=policy)
        self._indexes = indexes

    def _each(self, op: Callable[[Index[Any, K, V, Any]], object], what: str) -> None:
        failures: dict[str, Exception] = {}
        for i, index in enumerate(self._indexes):
            if not index.sync_on_write:
                continue
            try:
                op(index)
            except Exception as exc:
                failures[_index_fanout_label(i, index)] = exc
        if failures:
            first = next(iter(failures.values()))
            raise PartialSyncError(
                f"index {what} failed for {len(failures)} target(s)",
                failures=failures,
            ) from first

    def write(self, key: K, value: V, options: WriteOptions) -> None:
        self._each(lambda index: index.sync(key, value), "upsert")

    def batch_write(self, items: Mapping[K, V], options: WriteOptions) -> None:
        self._each(lambda index: index.batch_sync(items), "batch upsert")

    def delete(self, key: K) -> bool:
        self._each(lambda index: index.delete(key), "delete")
        return False

    def batch_delete(self, keys: Sequence[K]) -> int:
        self._each(lambda index: index.batch_delete(keys), "batch delete")
        return 0


class IndexedKvStore[K, V, ViaT](StoreTopologyMixin[K, V], DelegatingKvStore[K, V]):
    """Primary store plus attached indexes; mutations sync all indexes.

    The store is the authority and the indexes one dependent, so every
    operation's order follows from those roles — see
    :mod:`kvtsjl.keymap_algebra.topology`. Where the store's writes are
    deferred to the caller's commit, index syncs wait for
    ``flush_index_syncs``; GC syncs are best-effort, since leftover index
    entries are phantoms that ``search`` filters out.
    """

    def __init__(
        self,
        underlying: KvStore[K, V],
        indexes: Sequence[Index[Any, K, V, Any]],
        *,
        default_index: Index[Any, K, V, Any] | None = None,
        bundle: ViaT | None = None,
        sync_policy: SyncPolicy = SyncPolicy.REQUIRED,
    ) -> None:
        super().__init__(underlying)
        self._indexes: tuple[Index[Any, K, V, Any], ...] = tuple(indexes)
        self._index_ids = {id(index) for index in self._indexes}
        if default_index is not None and id(default_index) not in self._index_ids:
            raise KvStoreIndexError("default_index must be one of the attached indexes")
        self._default_index = default_index
        self._bundle = bundle
        self._sync_policy = sync_policy
        for index in self._indexes:
            index.bind_store(self)

    @property
    def sync_policy(self) -> SyncPolicy:
        return self._sync_policy

    @property
    def via(self) -> ViaT | None:
        if self._bundle is None:
            return None
        for index in self._indexes:
            index.bind_store(self)
        return self._bundle

    @property
    def indexes(self) -> tuple[Index[Any, K, V, Any], ...]:
        return self._indexes

    @property
    def default_index(self) -> Index[Any, K, V, Any] | None:
        return self._default_index

    def topology(self) -> Topology[K, V]:
        """The store is the authority; the indexes a dependent derivation."""
        return Topology(
            (
                self._target("store", Role.AUTHORITY, self._underlying),
                _IndexesTarget(self._indexes, policy=self._sync_policy),
            )
        )

    def clone_with_scope(self, scope: Scope) -> IndexedKvStore[K, V, ViaT]:
        clones = [idx.clone_with_scope(scope) for idx in self._indexes]
        remap = {id(o): c for o, c in zip(self._indexes, clones, strict=True)}
        return IndexedKvStore(
            self._underlying.clone_with_scope(scope),
            clones,
            default_index=(
                remap[id(self._default_index)]
                if self._default_index is not None
                else None
            ),
            bundle=self._remap_bundle(remap),
            sync_policy=self._sync_policy,
        )

    def _remap_bundle(
        self, remap: dict[int, Index[Any, K, V, Any]]
    ) -> ViaT | None:
        bundle = self._bundle
        if bundle is None:
            return None
        # Reconstruct over init fields only — dataclasses.replace rejects
        # init=False fields, and Index fields are already required to be init.
        if not is_dataclass(bundle) or isinstance(bundle, type):
            return bundle
        kwargs = {
            f.name: (
                remap[id(getattr(bundle, f.name))]
                if id(getattr(bundle, f.name)) in remap
                else getattr(bundle, f.name)
            )
            for f in fields(bundle)
            if f.init
        }
        return cast(ViaT, type(bundle)(**kwargs))

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import compose_repr

        return compose_repr(
            "IndexedKvStore",
            underlying=self._underlying,
            indexes=list(self._indexes),
            sync_policy=self._sync_policy,
        )

    def _require_attached[Q, M](self, index: Index[Q, K, V, M]) -> Index[Q, K, V, M]:
        if id(index) not in self._index_ids:
            raise KvStoreIndexError("index is not attached to this store")
        return index

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
            cast(Index[Any, K, V, Any], index_or_query),
            query,
            limit=limit,
            include_keys=include_keys,
        )
