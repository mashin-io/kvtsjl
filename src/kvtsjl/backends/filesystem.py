"""Filesystem-backed KvStore (native collection dirs).

Default ``ttl_mode=MTIME`` is footprint-free: one data file per key, TTL is
``mtime + KvSet.ttl_policy``. Opt in to ``SIDECAR`` only when you need per-write
``ttl=`` (writes ``{path}.expires``).

``batch_get`` / ``batch_set`` / ``batch_delete`` run concurrent point ops capped by
``io_concurrency`` (default 8). For local/dev or FS-heavy zip with SQL, pass
``io_concurrency=1``.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, datetime
from enum import Enum
import os
from pathlib import Path
import time
from typing import cast

from kvtsjl.batching import DEFAULT_FS_IO_CONCURRENCY, chunk_sequence, map_concurrent
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
from kvtsjl.store.schema.stat import RecordStat
from kvtsjl.store.schema.ttl import (
    ExpiryGc,
    TtlPolicy,
    require_explicit_ttl_supported,
    require_write_clock_supported,
)


def _utc_from_epoch(epoch: float | None) -> datetime | None:
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=UTC)


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
        if io_concurrency < 1:
            raise ValueError(f"io_concurrency must be >= 1, got {io_concurrency}")
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
        self.io_concurrency = io_concurrency
        assert self._binding.collection is not None
        self._collection_dir = Path(self._binding.collection)
        self._collection_dir.mkdir(parents=True, exist_ok=True)

    @property
    def supports_write_clock(self) -> bool:
        return True

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

    def _clock_base(self, created_at: datetime | None) -> float:
        if created_at is None:
            return time.time()
        if created_at.tzinfo is None:
            return created_at.replace(tzinfo=UTC).timestamp()
        return created_at.timestamp()

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

    def _sidecar_expires_at(self, path: Path) -> float | None | str:
        """Absolute expiry epoch, ``\"none\"`` for persistent sidecar, or ``None`` if absent."""
        sidecar = self._expires_sidecar(path)
        if not sidecar.is_file():
            return None
        try:
            token = sidecar.read_text(encoding="utf-8").strip()
        except OSError:
            return None
        if token == "none":
            return "none"
        try:
            return float(token)
        except ValueError:
            return 0.0

    def _path_is_expired(self, path: Path) -> bool:
        if not path.is_file():
            return False
        sidecar_exp = self._sidecar_expires_at(path)
        if sidecar_exp == "none":
            return False
        if isinstance(sidecar_exp, float):
            return time.time() >= sidecar_exp
        ttl = self.ttl_seconds()
        if ttl is None:
            return False
        try:
            mtime = path.stat().st_mtime
        except OSError:
            return False
        return time.time() >= mtime + ttl

    def _live_path(self, path: Path) -> Path | None:
        """Return ``path`` if present and not expired; honour ``ExpiryGc`` (no body read)."""
        if not path.is_file():
            return None
        if self._path_is_expired(path):
            if self._expiry_gc is ExpiryGc.LAZY_DELETE:
                self._delete_with_sidecar(path)
            return None
        return path

    def _read_live(self, path: Path) -> bytes | None:
        """Read file bytes, or ``None`` if missing / TTL-expired (lazy delete)."""
        if self._live_path(path) is None:
            return None
        try:
            return path.read_bytes()
        except OSError:
            return None

    def _stat_for_path(self, path: Path) -> RecordStat | None:
        if self._live_path(path) is None:
            return None
        try:
            st = path.stat()
        except OSError:
            return None
        mtime = st.st_mtime
        sidecar_exp = self._sidecar_expires_at(path)
        expires_at: float | None
        persistent: bool
        if sidecar_exp == "none":
            expires_at = None
            persistent = True
        elif isinstance(sidecar_exp, float):
            expires_at = sidecar_exp
            persistent = False
        else:
            ttl = self.ttl_seconds()
            if ttl is None:
                expires_at = None
                persistent = True
            else:
                expires_at = mtime + ttl
                persistent = False
        return RecordStat(
            created_at=_utc_from_epoch(mtime),
            modified_at=_utc_from_epoch(mtime),
            expires_at=_utc_from_epoch(expires_at),
            size=st.st_size,
            persistent=persistent,
        )

    def get(self, key: K) -> V | None:
        raw = self._read_live(self._path_for_physical(self._physical_key_blob(key)))
        if raw is None:
            return None
        return self.kvset.value_serde.deserialize(raw)

    def exists(self, key: K) -> bool:
        return (
            self._live_path(self._path_for_physical(self._physical_key_blob(key)))
            is not None
        )

    def batch_exists(self, keys: Sequence[K]) -> set[K]:
        if not keys:
            return set()
        out: set[K] = set()
        for chunk in chunk_sequence(keys, self.batch_size):
            flags = map_concurrent(
                chunk, self.exists, max_workers=self.io_concurrency
            )
            for key, ok in zip(chunk, flags, strict=True):
                if ok:
                    out.add(key)
        return out

    def stat(self, key: K) -> RecordStat | None:
        return self._stat_for_path(
            self._path_for_physical(self._physical_key_blob(key))
        )

    def batch_stat(self, keys: Sequence[K]) -> dict[K, RecordStat]:
        if not keys:
            return {}
        out: dict[K, RecordStat] = {}
        for chunk in chunk_sequence(keys, self.batch_size):
            stats = map_concurrent(
                chunk, self.stat, max_workers=self.io_concurrency
            )
            for key, meta in zip(chunk, stats, strict=True):
                if meta is not None:
                    out[key] = meta
        return out

    def set(
        self,
        key: K,
        value: V,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        require_explicit_ttl_supported(
            ttl,
            allowed=self._ttl_mode is FilesystemTtlMode.SIDECAR,
            backend="FilesystemKvStore",
        )
        require_write_clock_supported(
            created_at, allowed=True, backend="FilesystemKvStore"
        )
        path = self._path_for_physical(self._physical_key_blob(key))
        self._write_atomic(path, self.kvset.value_serde.serialize(value))
        base = self._clock_base(created_at)
        if created_at is not None:
            os.utime(path, (base, base))
        sidecar = self._expires_sidecar(path)
        if self._ttl_mode is FilesystemTtlMode.SIDECAR and ttl is not None:
            secs = self.resolve_ttl_seconds(ttl)
            sidecar.write_text(
                "none" if secs is None else str(base + secs),
                encoding="utf-8",
            )
            return
        sidecar.unlink(missing_ok=True)

    def delete(self, key: K) -> bool:
        path = self._path_for_physical(self._physical_key_blob(key))
        existed = path.is_file()
        self._delete_with_sidecar(path)
        return existed

    def batch_get(self, keys: Sequence[K]) -> dict[K, V]:
        if not keys:
            return {}
        out: dict[K, V] = {}
        for chunk in chunk_sequence(keys, self.batch_size):
            values = map_concurrent(
                chunk, self.get, max_workers=self.io_concurrency
            )
            for key, value in zip(chunk, values, strict=True):
                if value is not None:
                    out[key] = value
        return out

    def batch_set(
        self,
        items: Mapping[K, V],
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        pairs = list(items.items())
        if not pairs:
            return

        def write(pair: tuple[K, V]) -> None:
            self.set(pair[0], pair[1], ttl=ttl, created_at=created_at)

        for chunk in chunk_sequence(pairs, self.batch_size):
            map_concurrent(chunk, write, max_workers=self.io_concurrency)

    def batch_delete(self, keys: Sequence[K]) -> int:
        if not keys:
            return 0
        unique = list(dict.fromkeys(keys))
        deleted = 0
        for chunk in chunk_sequence(unique, self.batch_size):
            flags = map_concurrent(
                chunk, self.delete, max_workers=self.io_concurrency
            )
            deleted += sum(1 for ok in flags if ok)
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

    def _gc_expired_keys(self, *, max_entries: int) -> list[K]:
        if max_entries < 1:
            raise ValueError(f"max_entries must be >= 1, got {max_entries}")
        prefix = self._scan_prefix_blob(None)
        ops = self.kvset.blob_ops
        deleted: list[K] = []
        for path in list(self._iter_data_files()):
            if len(deleted) >= max_entries:
                break
            if not self._path_is_expired(path):
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
            self._delete_with_sidecar(path)
            deleted.append(decoded)
        return deleted
