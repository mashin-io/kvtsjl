"""Indexed logical store with search and sync."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import fields, is_dataclass
from datetime import datetime
import logging
from typing import Any, Literal, cast, overload

from kvtsjl.exceptions import KvStoreIndexError, PartialSyncError
from kvtsjl.index.logical.abc import Index, IndexHit
from kvtsjl.scope import Scope
from kvtsjl.store.compose.delegating import DelegatingKvStore
from kvtsjl.store.logical import KvStore
from kvtsjl.store.schema.layout import ScanQuery
from kvtsjl.store.schema.policy import SyncPolicy
from kvtsjl.store.schema.scan import Predicate
from kvtsjl.store.schema.ttl import TtlPolicy

logger = logging.getLogger(__name__)


def _index_fanout_label(i: int, index: Index[Any, Any, Any, Any]) -> str:
    """Stable unique label; must not raise (runs on the failure path)."""
    name = getattr(getattr(index, "index_set", None), "name", None) or type(index).__name__
    return f"{i}:{name}"


class IndexedKvStore[K, V, ViaT](DelegatingKvStore[K, V]):
    """Primary store plus attached indexes; mutations sync all indexes."""

    _pending: list[
        tuple[Callable[[Index[Any, Any, Any, Any]], object], str, SyncPolicy]
    ]

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
        self._pending = []
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

    def get(self, key: K) -> V | None:
        return self._underlying.get(key)

    def set(
        self,
        key: K,
        value: V,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        self._underlying.set(key, value, ttl=ttl, created_at=created_at)
        self._sync_upsert(key, value)

    def delete(self, key: K) -> bool:
        deleted = self._underlying.delete(key)
        if deleted:
            self._sync_delete(key)
        return deleted

    def batch_get(self, keys: Sequence[K]) -> dict[K, V]:
        return self._underlying.batch_get(keys)

    def batch_set(
        self,
        items: Mapping[K, V],
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        self._underlying.batch_set(items, ttl=ttl, created_at=created_at)
        self._sync_batch_upsert(items)

    def batch_delete(self, keys: Sequence[K]) -> int:
        deleted = self._underlying.batch_delete(keys)
        self._sync_batch_delete(keys)
        return deleted

    def scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        yield from self._underlying.scan_entries(query)

    def delete_where(
        self,
        where: Predicate[K],
        *,
        limit: int | None = None,
        sync_policy: SyncPolicy | None = None,
    ) -> int:
        from kvtsjl.store.schema.scan import Projection, ScanSpec, is_gc_predicate

        if is_gc_predicate(where):
            max_entries = limit if limit is not None else 2_147_483_647
            if max_entries < 1:
                raise ValueError(f"limit must be >= 1 for GC, got {limit}")
            keys = self._underlying.gc_expired_keys(max_entries=max_entries)
            if keys:
                # Always BEST_EFFORT unless overridden: raising would fail a
                # successful cleanup; leftover index entries are phantoms
                # filtered by _hydrate.
                policy = (
                    sync_policy
                    if sync_policy is not None
                    else SyncPolicy.BEST_EFFORT
                )
                self._sync_batch_delete(keys, policy=policy)
            return len(keys)
        keys = [
            row.key
            for row in self.scan_rows(
                ScanSpec(where=where, project=Projection.KEYS, limit=limit)
            )
        ]
        if not keys:
            return 0
        deleted = self._underlying.batch_delete(keys)
        policy = sync_policy if sync_policy is not None else self._sync_policy
        self._sync_batch_delete(keys, policy=policy)
        return deleted

    def gc_expired_keys(self, *, max_entries: int) -> list[K]:
        keys = self._underlying.gc_expired_keys(max_entries=max_entries)
        if keys:
            # Always BEST_EFFORT: raising would fail a successful cleanup;
            # leftover index entries are phantoms filtered by _hydrate.
            self._sync_batch_delete(keys, policy=SyncPolicy.BEST_EFFORT)
        return keys

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

    def _finish_fanout(
        self,
        failures: dict[str, Exception],
        *,
        policy: SyncPolicy,
        what: str,
    ) -> None:
        if not failures:
            return
        if policy is SyncPolicy.BEST_EFFORT:
            for label, exc in failures.items():
                try:
                    logger.exception(
                        "index %s failed (%s)",
                        what,
                        label,
                        exc_info=exc,
                    )
                except Exception:
                    # Logging must not replace the sync failure already in `failures`.
                    pass
            return
        first = next(iter(failures.values()))
        raise PartialSyncError(
            f"index {what} failed for {len(failures)} target(s)",
            failures=failures,
        ) from first

    def _run_index_ops(
        self,
        op: Callable[[Index[Any, K, V, Any]], object],
        *,
        what: str,
        policy: SyncPolicy | None = None,
    ) -> None:
        effective = policy if policy is not None else self._sync_policy
        if self._underlying.writes_are_deferred:
            # The caller owns the commit, so the write is not durable yet.
            # Running now would embed inside their transaction — holding the
            # write lock across a network call — and a rollback would leave
            # the index entry behind. Hold it until they say the commit landed.
            self._pending.append((op, what, effective))
            return
        self._apply_index_ops(op, what=what, policy=effective)

    def _apply_index_ops(
        self,
        op: Callable[[Index[Any, K, V, Any]], object],
        *,
        what: str,
        policy: SyncPolicy,
    ) -> None:
        failures: dict[str, Exception] = {}
        for i, index in enumerate(self._indexes):
            if not index.sync_on_write:
                continue
            try:
                op(index)
            except Exception as exc:
                label = _index_fanout_label(i, index)
                failures[label] = exc
        self._finish_fanout(failures, policy=policy, what=what)

    # --- deferred syncs -----------------------------------------------------

    @property
    def pending_index_syncs(self) -> int:
        """Index operations waiting for the caller's commit."""
        return len(self._pending)

    def flush_index_syncs(self) -> None:
        """Run the queued index operations; call this after the commit.

        Failures surface here rather than at the write, under the same policy:
        ``REQUIRED`` raises ``PartialSyncError``, ``BEST_EFFORT`` logs. The
        queue is cleared either way, so a failed flush is not retried silently.
        """
        queued, self._pending = self._pending, []
        for op, what, policy in queued:
            self._apply_index_ops(op, what=what, policy=policy)

    def discard_index_syncs(self) -> None:
        """Drop the queued operations; call this after a rollback."""
        self._pending = []

    def _sync_upsert(self, key: K, value: V) -> None:
        self._run_index_ops(
            lambda index: index.sync(key, value),
            what="upsert",
        )

    def _sync_batch_upsert(self, items: Mapping[K, V]) -> None:
        if not items:
            return
        self._run_index_ops(
            lambda index: index.batch_sync(items),
            what="batch upsert",
        )

    def _sync_delete(self, key: K) -> None:
        self._run_index_ops(
            lambda index: index.delete(key),
            what="delete",
        )

    def _sync_batch_delete(
        self,
        keys: Sequence[K],
        *,
        policy: SyncPolicy | None = None,
    ) -> None:
        if not keys:
            return
        self._run_index_ops(
            lambda index: index.batch_delete(keys),
            what="batch delete",
            policy=policy,
        )

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
