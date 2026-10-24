"""Store operations derived from a record's topology.

:mod:`kvtsjl.keymap_algebra.topology` defines the roles and the order every
operation takes over them. This module gives those operations their store
shape: targets that are stores, the write clock, streams, scans, garbage
collection and the deferred syncs a caller flushes after committing.
"""

from __future__ import annotations

from abc import ABC
from collections.abc import Callable, Iterator, Sequence
from datetime import datetime

from kvtsjl.exceptions import PartialSyncError
from kvtsjl.keymap import KeyMap
from kvtsjl.keymap_algebra.topology import (
    FillStats,
    KeyMapTarget,
    Lookup,
    RecordTopologyMixin,
    Role,
    Target,
    WriteOptions,
    whole,
)
from kvtsjl.store.fanout import (
    explain_parts,
    iter_present,
    iter_union_keys,
    iter_union_keys_spec,
    needs_values,
    part_key_spec,
    require_unordered,
)
from kvtsjl.store.logical import KvStore
from kvtsjl.store.schema.layout import KeyLayout, ScanQuery
from kvtsjl.store.schema.policy import SyncPolicy
from kvtsjl.store.schema.scan import (
    Predicate,
    Projection,
    ScanPlanNode,
    ScanRow,
    ScanSpec,
    apply_cascade,
    is_gc_predicate,
    predicate_label,
    split_key_predicates,
)
from kvtsjl.store.schema.stat import RecordStat
from kvtsjl.store.schema.ttl import TtlPolicy, require_write_clock_supported
from kvtsjl.store.streaming import StreamSource

_NO_LIMIT = 2_147_483_647


class StoreTarget[K, V, P](KeyMapTarget[K, V, P]):
    """A target that is a store: it has a write clock, streams and expires."""

    def __init__(
        self,
        name: str,
        role: Role,
        store: KvStore[K, P],
        slot: Callable[[V], P | None],
        *,
        lookup: Lookup | None = None,
        fill: bool = False,
        policy: SyncPolicy = SyncPolicy.REQUIRED,
    ) -> None:
        super().__init__(
            name, role, store, slot, lookup=lookup, fill=fill, policy=policy
        )
        self.store = store

    @property
    def supports_write_clock(self) -> bool:
        return self.store.supports_write_clock

    @property
    def expires(self) -> bool:
        return True

    def gc(self, max_entries: int) -> list[K]:
        return self.store.gc_expired_keys(max_entries=max_entries)

    @property
    def writes_are_deferred(self) -> bool:
        return self.store.writes_are_deferred

    def read_stream[C: (bytes, str)](
        self: StoreTarget[K, V, C], key: K
    ) -> Iterator[C] | None:
        return self.store.read_stream(key)

    def keys(self, page_size: int) -> Iterator[K]:
        query = ScanQuery[K](include_values=False, page_size=page_size)
        return (key for key, _ in self.store.scan_entries(query))

    def write_stream[C: (bytes, str)](
        self: StoreTarget[K, C, C],
        key: K,
        source: StreamSource,
        options: WriteOptions,
        size: int | None,
    ) -> None:
        self.store.set_stream(
            key, source, ttl=options.ttl, created_at=options.created_at, size=size
        )


class StoreTopologyMixin[K, V](RecordTopologyMixin[K, V], KvStore[K, V], ABC):
    """A store whose operations over several targets derive from its topology.

    Mix in ahead of the store base. Targets built with ``_target`` and
    ``_part`` are :class:`StoreTarget`; a target that is not a store — an
    index — subclasses :class:`~kvtsjl.keymap_algebra.topology.Target`
    directly.

    Beyond the keymap operations this derives the write clock, metadata,
    streams, scans, expiry, ``delete_where`` and the deferred syncs. Scans read
    what decides existence: the authority's own scan, whole, when it holds
    the record; otherwise the keys of the authority — or of every component,
    when none decides alone — with each record assembled from its parts.
    """

    def _target(
        self,
        name: str,
        role: Role,
        keymap: KeyMap[K, V],
        *,
        lookup: Lookup | None = None,
        fill: bool = False,
        policy: SyncPolicy = SyncPolicy.REQUIRED,
    ) -> Target[K, V, V]:
        return StoreTarget(
            name,
            role,
            _require_store(name, keymap),
            whole,
            lookup=lookup,
            fill=fill,
            policy=policy,
        )

    def _part[P](
        self,
        name: str,
        role: Role,
        keymap: KeyMap[K, P],
        slot: Callable[[V], P | None],
    ) -> Target[K, V, P]:
        return StoreTarget(name, role, _require_store(name, keymap), slot)

    def _store_of[P](self, target: Target[K, V, P]) -> KvStore[K, P]:
        if not isinstance(target, StoreTarget):
            raise TypeError(f"{target.name} is read for the record but is not a store")
        return target.store

    def _scanned(self) -> list[KvStore[K, object]]:
        """The stores whose keys are the record's keys."""
        topology = self.topology()
        authority = topology.authority
        targets = [authority] if authority is not None else topology.components
        return [self._store_of(t) for t in targets]

    # --- the record's medium -------------------------------------------------

    def _admit(self, options: WriteOptions) -> None:
        require_write_clock_supported(
            options.created_at,
            allowed=self.supports_write_clock,
            backend=type(self).__name__,
        )

    @property
    def supports_write_clock(self) -> bool:
        return self.topology().supports_write_clock

    @property
    def writes_are_deferred(self) -> bool:
        return self.topology().deferred

    def key_layout(self) -> KeyLayout:
        return self._store_of(self.topology().anchor).key_layout()

    def stream_chunk_type(self) -> type[object]:
        return self._store_of(self.topology().anchor).stream_chunk_type()

    # --- metadata and streams ------------------------------------------------

    def stat(self, key: K) -> RecordStat | None:
        topology = self.topology()
        if topology.assembled:
            return super().stat(key)
        return topology.stat(key)

    def batch_stat(self, keys: Sequence[K]) -> dict[K, RecordStat]:
        topology = self.topology()
        if topology.assembled:
            return super().batch_stat(keys)
        return topology.batch_stat(keys)

    def read_stream[Chunkable: (bytes, str)](
        self: StoreTopologyMixin[K, Chunkable], key: K
    ) -> Iterator[Chunkable] | None:
        return self.topology().read_stream(key)

    def set_stream[Chunkable: (bytes, str)](
        self: StoreTopologyMixin[K, Chunkable],
        key: K,
        source: StreamSource,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
        size: int | None = None,
    ) -> None:
        options = WriteOptions(ttl=ttl, created_at=created_at)
        self._admit(options)
        self.topology().write_stream(key, source, options, size, self._dependent_syncs())

    # --- scans ---------------------------------------------------------------

    def scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        topology = self.topology()
        whole_record = topology.whole_authority
        if whole_record is not None:
            yield from self._store_of(whole_record).scan_entries(query)
            return
        yield from iter_present(
            self,
            iter_union_keys(self._scanned(), query),
            page_size=query.page_size,
            include_values=query.include_values,
        )

    def scan_rows(self, spec: ScanSpec[K]) -> Iterator[ScanRow[K, V]]:
        topology = self.topology()
        whole_record = topology.whole_authority
        if whole_record is not None:
            yield from self._store_of(whole_record).scan_rows(spec)
            return
        scanned = self._scanned()
        if len(scanned) > 1:
            require_unordered(spec, type(self).__name__)
        key_where, residual = split_key_predicates(spec.where)
        keys = iter_union_keys_spec(scanned, part_key_spec(spec, key_where))
        raw = (
            ScanRow(key=k, value=v)
            for k, v in iter_present(
                self,
                keys,
                page_size=spec.page_size,
                include_values=needs_values(spec, residual),
            )
        )
        yield from apply_cascade(
            raw,
            residual=residual,
            store=self,
            refine=spec.refine,
            limit=spec.limit,
            project=spec.project,
        )

    def explain_node(self, spec: ScanSpec[K]) -> ScanPlanNode:
        topology = self.topology()
        whole_record = topology.whole_authority
        if whole_record is not None:
            child = self._store_of(whole_record).explain_node(spec)
            return ScanPlanNode(
                name=type(self).__name__,
                pushed=frozenset({"all→authority"}),
                children=(child,),
            )
        scanned = self._scanned()
        if len(scanned) > 1:
            require_unordered(spec, type(self).__name__)
        key_where, residual = split_key_predicates(spec.where)
        return explain_parts(
            scanned,
            part_key_spec(spec, key_where),
            name=type(self).__name__,
            residual_label=predicate_label(residual),
        )

    # --- what the fallbacks hold -------------------------------------------

    def missing_keys(self, *, page_size: int | None = None) -> Iterator[K]:
        return self.topology().missing_keys(page_size or self.batch_size)

    def fill_missing(
        self, *, page_size: int | None = None, limit: int | None = None
    ) -> FillStats:
        return self.topology().fill_missing(
            page_size or self.batch_size, limit, self._dependent_syncs()
        )

    # --- expiry and bulk deletes ---------------------------------------------

    def expired_keys(self, *, now: datetime | None = None, limit: int) -> list[K]:
        """Only the authority knows when the record expires."""
        authority = self.topology().authority
        if authority is None:
            return super().expired_keys(now=now, limit=limit)
        return self._store_of(authority).expired_keys(now=now, limit=limit)

    def gc_expired_keys(self, *, max_entries: int) -> list[K]:
        return self.topology().gc(max_entries, self._dependent_syncs())

    def delete_where(
        self,
        where: Predicate[K],
        *,
        limit: int | None = None,
        sync_policy: SyncPolicy | None = None,
    ) -> int:
        """Delete the records matching ``where``, in the topology's order.

        ``sync_policy`` overrides each dependent's own for this call; GC
        defaults to best-effort, since the cleanup itself already succeeded.
        """
        if is_gc_predicate(where):
            max_entries = limit if limit is not None else _NO_LIMIT
            if max_entries < 1:
                raise ValueError(f"limit must be >= 1 for GC, got {limit}")
            policy = sync_policy if sync_policy is not None else SyncPolicy.BEST_EFFORT
            return len(
                self.topology().gc(max_entries, self._dependent_syncs(), policy=policy)
            )
        keys = [
            row.key
            for row in self.scan_rows(
                ScanSpec(where=where, project=Projection.KEYS, limit=limit)
            )
        ]
        return self.topology().batch_delete(
            keys, self._dependent_syncs(), policy=sync_policy
        )

    # --- deferred syncs ------------------------------------------------------

    @property
    def pending_index_syncs(self) -> int:
        """This node's queued operations plus any beneath it."""
        return self._dependent_syncs().pending + super().pending_index_syncs

    def flush_index_syncs(self) -> None:
        """Run this node's queued operations, then those beneath it.

        Every node is visited even when one fails; failures are reported
        together, under the policy each operation was queued with.
        """
        failures: dict[str, Exception] = {}
        for flush in (self._dependent_syncs().flush, super().flush_index_syncs):
            try:
                flush()
            except PartialSyncError as exc:
                failures.update(exc.failures)
        if failures:
            raise PartialSyncError(
                "deferred index syncs failed after commit", failures=failures
            )

    def discard_index_syncs(self) -> None:
        """Drop this node's queued operations and those beneath it."""
        self._dependent_syncs().discard()
        super().discard_index_syncs()


def _require_store[K, P](name: str, keymap: KeyMap[K, P]) -> KvStore[K, P]:
    if not isinstance(keymap, KvStore):
        raise TypeError(f"{name}: a store composition's targets are stores")
    return keymap
