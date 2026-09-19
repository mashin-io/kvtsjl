"""Filesystem-backed KvStore (native collection dirs).

Default ``ttl_mode=MTIME`` is footprint-free: one data file per key, TTL is
``mtime + KvSet.ttl_policy``. Opt in to ``SIDECAR`` only when you need per-write
``ttl=`` (writes ``{path}.expires``).

``batch_get`` / ``batch_set`` / ``batch_delete`` run concurrent point ops capped by
``io_concurrency`` (default 8). For local/dev or FS-heavy zip with SQL, pass
``io_concurrency=1``.
"""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path

from kvtsjl.backends.object.binding import filesystem_collection_binder
from kvtsjl.backends.object.fs_driver import FilesystemBlobDriver
from kvtsjl.backends.object.store import ObjectStoreKvBackend
from kvtsjl.backends.object.ttl import (
    FilesystemMtimeTtl,
    FilesystemSidecarTtl,
    TtlSource,
)
from kvtsjl.batching import DEFAULT_FS_IO_CONCURRENCY
from kvtsjl.bind import CollectionBinding, NamespaceBinder
from kvtsjl.bind.namespace import resolve_collection_binding
from kvtsjl.schema.ref import WireRef
from kvtsjl.scope import Scope
from kvtsjl.store.schema.kvset import KvSet
from kvtsjl.store.schema.ttl import ExpiryGc


class FilesystemTtlMode(StrEnum):
    """How ``FilesystemKvStore`` applies TTL."""

    MTIME = "mtime"
    """``now >= mtime + kvset.ttl``; no sidecar files; per-write ``ttl=`` raises."""

    SIDECAR = "sidecar"
    """Honor per-write ``ttl=`` via ``{data_path}.expires``."""


def _ttl_source_for(mode: FilesystemTtlMode) -> TtlSource:
    if mode is FilesystemTtlMode.SIDECAR:
        return FilesystemSidecarTtl()
    return FilesystemMtimeTtl()


class FilesystemKvStore[K, V, KBLOB: str | bytes](ObjectStoreKvBackend[K, V, KBLOB]):
    """Light FS store: ``{root}/{name}/v{version}/`` + one data file per key.

    - ``VBLOB`` is ``bytes`` from ``value_serde`` (encoding is the caller's SerDe).
    - ``KBLOB`` is logical in-key material (``str`` / ``bytes``), not ``Path`` —
      paths are derived via ``KeyLayout``.
    - TTL: see ``ttl_mode`` / ``FilesystemTtlMode`` (default ``mtime``, no sidecars).
    - ``expiry_gc``: delete expired files on get/scan (default) or only hide them.
    - ``io_concurrency``: max concurrent point I/O in ``batch_get`` / ``batch_set`` /
      ``batch_delete`` (default 8).
    - Scan is supported only for ``KeyLayout.LITERAL`` (path ↔ key is reversible).
    """

    def __init__(
        self,
        kvset: KvSet[K, V, KBLOB, bytes],
        *,
        root: Path | str,
        ttl_mode: FilesystemTtlMode = FilesystemTtlMode.MTIME,
        expiry_gc: ExpiryGc = ExpiryGc.LAZY_DELETE,
        scope: Scope | None = None,
        binder: NamespaceBinder[KBLOB, str] | None = None,
        binding: CollectionBinding[KBLOB, str] | None = None,
        batch_size: int = 500,
        io_concurrency: int = DEFAULT_FS_IO_CONCURRENCY,
    ) -> None:
        root_path = Path(root)
        if binder is None and binding is None:
            binder = filesystem_collection_binder(root_path)
        resolved = resolve_collection_binding(
            WireRef.from_kvset(kvset),
            binder=binder,
            binding=binding,
        )
        coll = resolved.collection
        if coll is None:
            raise TypeError("FilesystemKvStore requires a non-None collection binding")
        collection_prefix = coll if coll.endswith("/") else f"{coll}/"
        Path(collection_prefix).mkdir(parents=True, exist_ok=True)
        self._root = root_path
        self._ttl_mode = ttl_mode
        super().__init__(
            kvset,
            driver=FilesystemBlobDriver(collection_prefix),
            ttl_source=_ttl_source_for(ttl_mode),
            expiry_gc=expiry_gc,
            scope=scope,
            binder=None,
            binding=resolved,
            batch_size=batch_size,
            io_concurrency=io_concurrency,
            collection_prefix=collection_prefix,
        )

    def _clone_with_scope(self, scope: Scope) -> FilesystemKvStore[K, V, KBLOB]:
        return FilesystemKvStore(
            self.kvset,
            root=self._root,
            ttl_mode=self._ttl_mode,
            expiry_gc=self._expiry_gc,
            scope=scope,
            binding=self._binding,
            batch_size=self.batch_size,
            io_concurrency=self.io_concurrency,
        )
