"""Logical ``KvStore`` ABC: domain K/V, scope, scan, composition."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Self, cast

from kvtsjl.batching import chunk_sequence
from kvtsjl.exceptions import PartialSyncError
from kvtsjl.keymap import KeyMap
from kvtsjl.scope import Scope
from kvtsjl.store.capabilities import ExpiringMixin, TtlWritesMixin
from kvtsjl.store.readable import ReadableKvStore
from kvtsjl.store.schema.policy import SyncPolicy
from kvtsjl.store.schema.ttl import TtlPolicy
from kvtsjl.store.streaming import StreamingView, StreamSource

if TYPE_CHECKING:
    from kvtsjl.index.logical.abc import Index
    from kvtsjl.keymap_algebra.topology import MissingPage
    from kvtsjl.store.compose.indexed import IndexedKvStore
    from kvtsjl.store.readable import ReadableKvStore as ReadableKvStoreType
    from kvtsjl.store.schema.stat import RecordStat


@dataclass(frozen=True, slots=True)
class FillStats:
    """What a ``fill_missing`` run saw and did, key by key.

    ``scanned`` keys were listed by a fallback; ``missing`` of them the store
    lacked. Of those, ``written`` were copied; ``lost_race`` were written by
    someone else between the check and the conditional write, and kept;
    ``source_miss`` were gone from the fallback by the time they were read.
    """

    scanned: int = 0
    missing: int = 0
    written: int = 0
    lost_race: int = 0
    source_miss: int = 0


def _page_size(asked: int | None, default: int) -> int:
    size = default if asked is None else asked
    if size < 1:
        raise ValueError(f"page_size must be >= 1, got {size}")
    return size


class SyncTreeNode(ABC):
    """A node of a composed tree, reached by operations over the whole tree.

    Flushing and discarding deferred syncs touch every node and nothing about
    its keys or values, so the walk is typed by this, not by ``KvStore[K, V]``
    — whose parameters differ from node to node.
    """

    def child_stores(self) -> tuple[object, ...]:
        """What lies directly beneath this node; empty for a leaf.

        A composition answers this once, and every operation that must reach
        the whole tree — flushing deferred index syncs, for one — walks it.
        """
        return ()

    def _child_nodes(self) -> list[SyncTreeNode]:
        return [c for c in self.child_stores() if isinstance(c, SyncTreeNode)]

    @property
    def pending_index_syncs(self) -> int:
        """Index operations anywhere beneath this store awaiting a commit."""
        return sum(child.pending_index_syncs for child in self._child_nodes())

    def flush_index_syncs(self) -> None:
        """Apply deferred index syncs anywhere in the tree; call after commit.

        Every node is visited even when one fails, so one failing index does
        not strand the others' queued work. Failures are reported together.
        The record is committed by then, so this raises the committed side:
        ``PartialSyncError``.
        """
        failures: dict[str, Exception] = {}
        for child in self._child_nodes():
            try:
                child.flush_index_syncs()
            except PartialSyncError as exc:
                failures.update(exc.failures)
        if failures:
            raise PartialSyncError(
                "deferred index syncs failed after commit", failures=failures
            )

    def discard_index_syncs(self) -> None:
        """Drop deferred index syncs anywhere in the tree; call after rollback."""
        for child in self._child_nodes():
            child.discard_index_syncs()


class KvStore[K, V](
    ReadableKvStore[K, V],
    KeyMap[K, V],
    TtlWritesMixin[K, V],
    ExpiringMixin[K],
    SyncTreeNode,
    ABC,
):
    """Logical document store: read + write + scope + scan + TTL + GC.

    Physical schema (``KvSet``, blob serdes, collection binding) lives on
    ``KvBackend`` / ``PhysicalBackend`` only. Logical wrappers delegate I/O to a leaf.

    Batch contract: default ``batch_get`` may loop; leaf overrides should use
    native bulk. Composed stores (``zip``, ``fallback``, ``mirror``, …) must
    fan out to part ``batch_get``, not ``N×get``.
    """

    # --- streaming (byte-valued stores only) ---------------------------------
    #
    # ``self: KvStore[K, bytes]`` restricts these to stores whose value *is* the
    # a chunkable surface type. Calling them on any other value type is a type
    # error, not a method that exists and refuses — the distinction this
    # codebase keeps coming back to. NotImplementedError is left for a real
    # backend limit.
    #
    # ``Chunkable`` is bytes-or-str because those are the value types that can
    # be split and rejoined at all; whether the *serde* can also be applied
    # chunk by chunk is a separate question, answered at runtime by
    # ``require_streaming_serde`` (a str store with a UTF-8 serde streams; one
    # with a non-incremental codec does not). The type rules out the cases that
    # can never work; the guard rules out the rest.

    @abstractmethod
    def missing_pages(self, page_size: int) -> Iterator[MissingPage[K, V]]:
        """Pages of keys a fallback holds and this store lacks, at any depth.

        Every store answers, so none can answer by accident: a leaf lacks
        nothing, a composition asks down its chain of authorities and adds its
        own fallbacks, and a view translates what the store behind it says.
        ``missing_keys`` and ``fill_missing`` are built on this alone.
        """

    def missing_keys(self, *, page_size: int | None = None) -> Iterator[K]:
        """Keys a read finds through a fallback that the store itself lacks.

        Fallbacks anywhere down the chain of authorities count — ``fallback_read``
        nested inside ``indexed`` or ``mirror`` still reports what it lacks —
        and each is listed a page at a time, so memory stays one page.
        """
        for page in self.missing_pages(_page_size(page_size, self.batch_size)):
            yield from page.missing

    def fill_missing(
        self, *, page_size: int | None = None, limit: int | None = None
    ) -> FillStats:
        """Copy in what the fallbacks hold and this store lacks; never overwrite.

        Each copy is this store's own ``set_if_absent``, so it is written at
        the level the fill was asked for: indexes and copies on the way down
        receive it, and nothing the store holds is ever replaced — a writer
        that got there first wins and is counted in ``lost_race``. The
        fallback's write clock is carried where the store keeps one, so a
        copy keeps its remaining life. A failure raises: this is an explicit
        write, not a read's best-effort fill. ``limit`` bounds the records
        written, and a later call continues, since what was copied is no
        longer missing::

            records.fallback_read(live_turns).fill_missing()
            records.fallback_read(history).indexed(index).fill_missing(limit=500)
        """
        if limit is not None and limit < 1:
            raise ValueError(f"limit must be >= 1, got {limit}")
        scanned = missing = written = lost_race = source_miss = 0
        for page in self.missing_pages(_page_size(page_size, self.batch_size)):
            wanted = set(page.missing)
            found: Mapping[K, V] = page.read(page.missing) if page.missing else {}
            clocks: Mapping[K, RecordStat] = (
                page.stat(list(found)) if found and self.supports_write_clock else {}
            )
            for key in page.listed:
                if limit is not None and written >= limit:
                    return FillStats(scanned, missing, written, lost_race, source_miss)
                scanned += 1
                if key not in wanted:
                    continue
                missing += 1
                value = found.get(key)
                if value is None:
                    source_miss += 1
                    continue
                meta = clocks.get(key)
                clock = None if meta is None else meta.write_clock
                if self.set_if_absent(key, value, created_at=clock):
                    written += 1
                else:
                    lost_race += 1
        return FillStats(scanned, missing, written, lost_race, source_miss)

    @property
    def writes_are_deferred(self) -> bool:
        """Whether a write is durable only after something the caller does.

        ``True`` where the caller owns the transaction, so an index bound to
        this store must queue its sync rather than run it beside the write.
        """
        return False

    def streaming[C](self, chunk_type: type[C]) -> StreamingView[K, C]:
        """A writable streaming view typed in ``chunk_type``.

        Narrows ``ReadableKvStore.streaming`` — a store that can be written to
        yields a view that can write.
        """
        self._check_chunk_type(chunk_type)
        return StreamingView[K, C](self, chunk_type)

    def set_stream[Chunkable: (bytes, str)](
        self: KvStore[K, Chunkable],
        key: K,
        source: StreamSource,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
        size: int | None = None,
    ) -> None:
        """Write ``source`` to ``key`` without holding the whole value.

        ``source`` is a path, an open stream whose ``read(n)`` returns chunks
        of the value type, or an iterable of such chunks. ``ttl`` and ``created_at``
        behave exactly as on ``set``, so a stream-written value expires, scans
        and garbage-collects identically to a ``set`` one.

        ``size`` is the **exact** encoded byte count, or ``None`` when it is
        not known. It is not a hint: GCS receives it as ``size=`` and Azure as
        ``length=``, and both upload exactly that many bytes — too small
        truncates the object, too large fails the upload. ``None`` is always
        safe, and a serde that changes the length reports ``None`` from
        ``encoded_size`` for exactly this reason.

        Stores that cannot stream raise rather than buffering: buffering is
        precisely what callers use this for. Small-value leaves (memory, Redis)
        may buffer, but they override and say so.
        """
        raise NotImplementedError(f"{type(self).__name__} does not support set_stream")

    @abstractmethod
    def set(
        self,
        key: K,
        value: V,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        """Insert or replace ``key``. ``ttl=None`` uses the ``KvSet`` policy.

        ``created_at`` is only accepted when ``supports_write_clock`` is True;
        otherwise raise (same family as unsupported per-write ``ttl=``).
        """

    def clone_with_scope(self, scope: Scope) -> Self:
        raise NotImplementedError(
            f"{type(self).__name__} must implement clone_with_scope"
        )

    def get_or_set(self, key: K, compute_value_fn: Callable[[], V]) -> V:
        existing = self.get(key)
        if existing is not None:
            return existing
        value = compute_value_fn()
        self.set(key, value)
        return value

    def batch_get_or_set(
        self,
        keys: Sequence[K],
        compute_missing_values_fn: Callable[[Sequence[K]], Mapping[K, V]],
    ) -> dict[K, V]:
        result: dict[K, V] = {}
        for chunk in chunk_sequence(keys, self.batch_size):
            found = self.batch_get(chunk)
            result.update(found)
            missing = [k for k in chunk if k not in found]
            if not missing:
                continue
            computed = compute_missing_values_fn(missing)
            to_set = {k: computed[k] for k in missing if k in computed}
            if to_set:
                self.batch_set(to_set)
                result.update(to_set)
        return result

    def readonly(self) -> ReadableKvStoreType[K, V]:
        from kvtsjl.store.compose.readonly import ReadonlyKvStore

        return ReadonlyKvStore(self)

    @classmethod
    def readonly_of(cls, store: KvStore[K, V]) -> ReadableKvStoreType[K, V]:
        from kvtsjl.store.compose.readonly import ReadonlyKvStore

        return ReadonlyKvStore(store)

    def fallback_read(
        self,
        secondary: KvStore[K, V],
        *,
        promote: bool = True,
    ) -> KvStore[K, V]:
        """Read-through secondary on miss (e.g. new version → old version)."""
        from kvtsjl.keymap_algebra.build import build_coalesce

        return cast(KvStore[K, V], build_coalesce(self, secondary, promote=promote))

    def group_by(self, by: str) -> KvStore[str, Mapping[K, V]]:
        """These rows grouped by the scope segment ``by``, each group one value.

        ``rows.group_by("turn_id")`` reads and writes the rows under a turn as
        one mapping: ``set`` makes them equal the given ones, ``delete``
        removes them all. See :class:`~kvtsjl.store.compose.grouped.GroupedKvStore`.
        """
        from kvtsjl.store.compose.grouped import GroupedKvStore

        return GroupedKvStore(self, by)

    def mirror(
        self,
        secondary: KvStore[K, V],
        *,
        sync_policy: SyncPolicy = SyncPolicy.REQUIRED,
    ) -> KvStore[K, V]:
        """Write-through to secondary; reads stay on primary."""
        from kvtsjl.store.compose.mirror import MirrorKvStore

        return MirrorKvStore(self, secondary, sync_policy=sync_policy)

    def indexed(
        self,
        index: Index[Any, K, V, Any],
        /,
        *more: Index[Any, K, V, Any],
        sync_policy: SyncPolicy = SyncPolicy.REQUIRED,
    ) -> IndexedKvStore[K, V, None]:
        from kvtsjl.store.compose.indexed import IndexedKvStore

        indexes = (index, *more)
        default = index if not more else None
        return IndexedKvStore(
            self, indexes, default_index=default, sync_policy=sync_policy
        )

    def indexed_as[ViaT](
        self,
        bundle: ViaT,
        *,
        sync_policy: SyncPolicy = SyncPolicy.REQUIRED,
    ) -> IndexedKvStore[K, V, ViaT]:
        from kvtsjl.index.logical.bundle import indexes_from_bundle
        from kvtsjl.store.compose.indexed import IndexedKvStore

        indexes = indexes_from_bundle(bundle)
        default = indexes[0] if len(indexes) == 1 else None
        return IndexedKvStore(
            self,
            cast(Any, indexes),
            default_index=cast(Any, default),
            bundle=bundle,
            sync_policy=sync_policy,
        )
