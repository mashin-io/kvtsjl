"""Adapt a physical ``KvBackend`` into a logical ``KvStore``."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from datetime import datetime
from typing import Any, Self, cast

from kvtsjl.bind.namespace import CollectionBinding
from kvtsjl.keymap_algebra.topology import MissingPage
from kvtsjl.scope import Scope
from kvtsjl.store.backend import KvBackend
from kvtsjl.store.logical import KvStore
from kvtsjl.store.schema.kvset import KvSet
from kvtsjl.store.schema.layout import KeyLayout, ScanQuery
from kvtsjl.store.schema.scan import (
    CompiledScan,
    KeyPrefix,
    ScanCapabilities,
    ScanOrder,
    ScanRow,
    ScanSpec,
    apply_cascade,
    compile_from_scan_entries,
    enforce_order_by_supported,
)
from kvtsjl.store.schema.stat import RecordStat
from kvtsjl.store.schema.ttl import TtlPolicy
from kvtsjl.store.streaming import StreamSource, require_streaming_serde


class LeafKvStore[K, V, KBLOB, VBLOB, COLL](KvStore[K, V]):
    """Public leaf store: full ``KvStore`` algebra over a non-store ``KvBackend``.

    Always forwards the enumerated backend surface used by tests, compose, and
    ``backend_repr``. Leaf-specific attributes (``_client``, ``io_concurrency``,
    …) are resolved via ``__getattr__`` when present on the concrete backend.
    """

    def __init__(self, backend: KvBackend[K, V, KBLOB, VBLOB, COLL]) -> None:
        self._backend = backend
        self.scope = backend.scope
        self.batch_size = backend.batch_size

    @classmethod
    def from_backend(
        cls, backend: KvBackend[K, V, KBLOB, VBLOB, COLL]
    ) -> Self:
        """Wrap an already-built backend (used by ``clone_with_scope``)."""
        inst = object.__new__(cls)
        inst._backend = backend
        inst.scope = backend.scope
        inst.batch_size = backend.batch_size
        return inst

    @property
    def backend(self) -> KvBackend[K, V, KBLOB, VBLOB, COLL]:
        return self._backend

    # --- always forward / expose ---

    @property
    def kvset(self) -> KvSet[K, V, KBLOB, VBLOB]:
        return self._backend.kvset

    @property
    def physical(self) -> KvSet[K, V, KBLOB, VBLOB]:
        return self._backend.physical

    @property
    def binding(self) -> CollectionBinding[KBLOB, COLL]:
        return self._backend.binding

    @property
    def _binding(self) -> CollectionBinding[KBLOB, COLL]:
        return self._backend.binding

    @property
    def supports_write_clock(self) -> bool:
        return self._backend.supports_write_clock

    def key_layout(self) -> KeyLayout:
        return self._backend.key_layout()

    def get(self, key: K) -> V | None:
        return self._backend.get(key)

    def exists(self, key: K) -> bool:
        return self._backend.exists(key)

    def batch_exists(self, keys: Sequence[K]) -> set[K]:
        return self._backend.batch_exists(keys)

    def stat(self, key: K) -> RecordStat | None:
        return self._backend.stat(key)

    def batch_stat(self, keys: Sequence[K]) -> dict[K, RecordStat]:
        return self._backend.batch_stat(keys)

    def batch_get(self, keys: Sequence[K]) -> dict[K, V]:
        return self._backend.batch_get(keys)

    def set(
        self,
        key: K,
        value: V,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        self._backend.set(key, value, ttl=ttl, created_at=created_at)

    def delete(self, key: K) -> bool:
        return self._backend.delete(key)

    def batch_set(
        self,
        items: Mapping[K, V],
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        self._backend.batch_set(items, ttl=ttl, created_at=created_at)

    def batch_delete(self, keys: Sequence[K]) -> int:
        return self._backend.batch_delete(keys)

    def set_stream(
        self,
        key: K,
        source: StreamSource,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
        size: int | None = None,
    ) -> None:
        stream_fn = getattr(self._backend, "set_stream", None)
        if not callable(stream_fn):
            raise NotImplementedError(f"{type(self._backend).__name__} does not support set_stream")
        stream_fn(key, source, ttl=ttl, created_at=created_at, size=size)

    def stream_chunk_type(self) -> type[Any]:
        return require_streaming_serde(
            self._backend.kvset.value_serde,
            op="streaming",
            backend=type(self._backend).__name__,
        ).chunk_type

    def read_stream[Chunkable: (bytes, str)](
        self: LeafKvStore[K, Chunkable, KBLOB, VBLOB, COLL], key: K
    ) -> Iterator[Chunkable] | None:
        stream_fn = getattr(self._backend, "read_stream", None)
        if not callable(stream_fn):
            raise NotImplementedError(
                f"{type(self._backend).__name__} does not support read_stream"
            )
        return cast("Iterator[Chunkable] | None", stream_fn(key))

    def scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        return self._backend.scan_entries(query)

    def scan_capabilities(self) -> ScanCapabilities:
        caps_fn = getattr(self._backend, "scan_capabilities", None)
        if callable(caps_fn):
            return cast(ScanCapabilities, caps_fn())
        return ScanCapabilities(
            pushdown=frozenset({KeyPrefix}),
            orders=frozenset({ScanOrder.UNORDERED}),
            resumable=False,
            stats=False,
        )

    def compile_scan(self, spec: ScanSpec[K]) -> CompiledScan[K]:
        compile_fn = getattr(self._backend, "compile_scan", None)
        if callable(compile_fn):
            return cast(CompiledScan[K], compile_fn(spec))
        return compile_from_scan_entries(
            self._backend.scan_entries, spec, backend=type(self).__name__
        )

    def scan_rows(self, spec: ScanSpec[K]) -> Iterator[ScanRow[K, V]]:
        enforce_order_by_supported(
            spec, self.scan_capabilities(), backend=type(self._backend).__name__
        )
        compiled = self.compile_scan(spec)
        yield from apply_cascade(
            compiled.rows,
            residual=compiled.residual,
            store=self,
            refine=spec.refine,
            limit=spec.limit,
            project=spec.project,
        )

    def clone_with_scope(self, scope: Scope) -> Self:
        return type(self).from_backend(self._backend.clone_with_scope(scope))

    def set_if_absent(
        self,
        key: K,
        value: V,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> bool:
        return self._backend.set_if_absent(key, value, ttl=ttl, created_at=created_at)

    def unset(self, key: K) -> bool:
        return self._backend.unset(key)

    def batch_unset(self, keys: Sequence[K]) -> int:
        return self._backend.batch_unset(keys)

    def missing_pages(self, page_size: int) -> Iterator[MissingPage[K, V]]:
        """A leaf has no fallback, so it lacks nothing a read through it finds."""
        return iter(())

    @property
    def writes_are_deferred(self) -> bool:
        return getattr(self._backend, "writes_are_deferred", False)

    def expired_keys(self, *, now: datetime | None = None, limit: int) -> list[K]:
        return self._backend.expired_keys(now=now, limit=limit)

    def gc_expired_keys(self, *, max_entries: int) -> list[K]:
        return self._backend.gc_expired_keys(max_entries=max_entries)

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import backend_repr

        return backend_repr(self._backend, class_name=type(self).__name__)

    def __getattr__(self, name: str) -> Any:
        # Leaf-specific backend attrs (``_client``, ``io_concurrency``, …).
        return getattr(self._backend, name)
