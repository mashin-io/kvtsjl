"""Filesystem-backed KvStore (native collection dirs).

Default ``ttl_mode=MTIME`` is footprint-free: one data file per key, TTL is
``mtime + KvSet.ttl_policy``. Opt in to ``SIDECAR`` only when you need per-write
``ttl=`` (writes ``{path}.expires``).
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from enum import Enum
import os
from pathlib import Path
import time
from typing import cast

from kvtsjl.batching import chunk_sequence
from kvtsjl.bind import (
    CollectionBinding,
    NamespaceBinder,
    NativeCollectionBinder,
)
from kvtsjl.exceptions import KvStoreScanUnsupported
from kvtsjl.scope import Scope
from kvtsjl.store import KvBackend
from kvtsjl.store.schema.kvset import KvSet
from kvtsjl.store.schema.layout import (
    KeyLayout,
    ScanQuery,
    layout_decode_for_fs,
    layout_encode_for_fs,
)
from kvtsjl.store.schema.ttl import (
    ExpiryGc,
    TtlPolicy,
    require_explicit_ttl_supported,
)


class FilesystemTtlMode(str, Enum):
    """How ``FilesystemKvStore`` applies TTL."""

    MTIME = "mtime"
    """``now >= mtime + kvset.ttl``; no sidecar files; per-write ``ttl=`` raises."""

    SIDECAR = "sidecar"
    """Honor per-write ``ttl=`` via ``{data_path}.expires``."""


def _safe_name(name: str) -> str:
    return (
        (name or "_")
        .replace("/", "_")
        .replace("\\", "_")
        .replace("..", "_")
        .replace("\0", "")
    )


class FilesystemKvStore[K, V, KBLOB: str | bytes](KvBackend[K, V, KBLOB, bytes, str]):
    """Light FS store: ``{root}/{name}/v{version}/`` + one data file per key.

    - ``VBLOB`` is ``bytes`` from ``value_serde`` (encoding is the caller's SerDe).
    - ``KBLOB`` is logical in-key material (``str`` / ``bytes``), not ``Path`` —
      paths are derived via ``KeyLayout``.
    - TTL: see ``ttl_mode`` / ``FilesystemTtlMode`` (default ``mtime``, no sidecars).
    - ``expiry_gc``: delete expired files on get/scan (default) or only hide them.
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
    ) -> None:
        root_path = Path(root)
        if binder is None and binding is None:
            binder = NativeCollectionBinder[KBLOB, str](
                collection_formatter=lambda ref: str(
                    root_path / _safe_name(ref.name) / ref.version_label()
                )
            )
        super().__init__(
            kvset,
            scope=scope,
            binder=binder,
            binding=binding,
            batch_size=batch_size,
        )
        self._root = root_path
        self._ttl_mode = ttl_mode
        self._expiry_gc = expiry_gc
        assert self._binding.collection is not None
        self._collection_dir = Path(self._binding.collection)
        self._collection_dir.mkdir(parents=True, exist_ok=True)

    def _clone_with_scope(self, scope: Scope) -> FilesystemKvStore[K, V, KBLOB]:
        return FilesystemKvStore(
            self.kvset,
            root=self._root,
            ttl_mode=self._ttl_mode,
            expiry_gc=self._expiry_gc,
            scope=scope,
            binding=self._binding,
            batch_size=self.batch_size,
        )

    def _path_for_physical(self, physical_key: KBLOB) -> Path:
        rel = layout_encode_for_fs(self.kvset.key_layout, physical_key)
        return self._collection_dir / rel

    def _write_atomic(self, path: Path, data: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = Path(str(path) + ".tmp")
        tmp.write_bytes(data)
        os.replace(tmp, path)

    def _expires_sidecar(self, path: Path) -> Path:
        return Path(str(path) + ".expires")

    def _delete_with_sidecar(self, path: Path) -> None:
        path.unlink(missing_ok=True)
        self._expires_sidecar(path).unlink(missing_ok=True)

    def _read_live(self, path: Path) -> bytes | None:
        """Read file bytes, or ``None`` if missing / TTL-expired (lazy delete)."""
        if not path.is_file():
            return None
        sidecar = self._expires_sidecar(path)
        if sidecar.is_file():
            try:
                token = sidecar.read_text(encoding="utf-8").strip()
            except OSError:
                return None
            if token != "none":
                try:
                    expires_at = float(token)
                except ValueError:
                    expires_at = 0.0
                if time.time() >= expires_at:
                    if self._expiry_gc is ExpiryGc.LAZY_DELETE:
                        self._delete_with_sidecar(path)
                    return None
        else:
            ttl = self.ttl_seconds()
            if ttl is not None:
                try:
                    mtime = path.stat().st_mtime
                except OSError:
                    return None
                if time.time() >= mtime + ttl:
                    if self._expiry_gc is ExpiryGc.LAZY_DELETE:
                        self._delete_with_sidecar(path)
                    return None
        try:
            return path.read_bytes()
        except OSError:
            return None

    def get(self, key: K) -> V | None:
        raw = self._read_live(self._path_for_physical(self._physical_key_blob(key)))
        if raw is None:
            return None
        return self.kvset.value_serde.deserialize(raw)

    def set(self, key: K, value: V, *, ttl: TtlPolicy | None = None) -> None:
        require_explicit_ttl_supported(
            ttl,
            allowed=self._ttl_mode is FilesystemTtlMode.SIDECAR,
            backend="FilesystemKvStore",
        )
        path = self._path_for_physical(self._physical_key_blob(key))
        self._write_atomic(path, self.kvset.value_serde.serialize(value))
        sidecar = self._expires_sidecar(path)
        if ttl is None:
            sidecar.unlink(missing_ok=True)
            return
        secs = self.resolve_ttl_seconds(ttl)
        sidecar.write_text("none" if secs is None else str(time.time() + secs), encoding="utf-8")

    def delete(self, key: K) -> bool:
        path = self._path_for_physical(self._physical_key_blob(key))
        existed = path.is_file()
        self._delete_with_sidecar(path)
        return existed

    def batch_get(self, keys: Sequence[K]) -> dict[K, V]:
        out: dict[K, V] = {}
        for chunk in chunk_sequence(keys, self.batch_size):
            for key in chunk:
                value = self.get(key)
                if value is not None:
                    out[key] = value
        return out

    def batch_set(
        self, items: Mapping[K, V], *, ttl: TtlPolicy | None = None
    ) -> None:
        for chunk in chunk_sequence(list(items.items()), self.batch_size):
            for key, value in chunk:
                self.set(key, value, ttl=ttl)

    def batch_delete(self, keys: Sequence[K]) -> int:
        deleted = 0
        for chunk in chunk_sequence(keys, self.batch_size):
            for key in chunk:
                if self.delete(key):
                    deleted += 1
        return deleted

    def _iter_data_files(self) -> Iterator[Path]:
        if not self._collection_dir.is_dir():
            return
        for dirpath, _dirnames, filenames in os.walk(self._collection_dir):
            for name in filenames:
                if name.endswith(".tmp") or name.endswith(".expires"):
                    continue
                yield Path(dirpath) / name

    def _physical_from_path(self, path: Path) -> KBLOB:
        rel = path.relative_to(self._collection_dir).as_posix()
        blob_type = cast(type[str] | type[bytes], self.kvset.str_serde.blob_type)
        return cast(
            KBLOB,
            layout_decode_for_fs(self.kvset.key_layout, rel, blob_type=blob_type),
        )

    def _scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        if self.kvset.key_layout is not KeyLayout.LITERAL:
            raise KvStoreScanUnsupported(
                "FilesystemKvStore scan requires KeyLayout.LITERAL "
                f"(got {self.kvset.key_layout!r}; no meta sidecar to recover keys)"
            )
        prefix = self._scan_prefix_blob(query.prefix)
        ops = self.kvset.blob_ops
        for path in self._iter_data_files():
            raw = self._read_live(path)
            if raw is None:
                continue
            try:
                pk = self._physical_from_path(path)
            except (ValueError, OSError):
                continue
            if not ops.startswith(pk, prefix):
                continue
            decoded = self._decode_key_from_physical(pk)
            if decoded is None:
                continue
            if query.include_values:
                yield decoded, self.kvset.value_serde.deserialize(raw)
            else:
                yield decoded, None
