"""Filesystem ``BlobDriver`` (one data file per key; optional ``.expires`` sidecar)."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from datetime import UTC, datetime
import os
from pathlib import Path
from typing import Any

from kvtsjl.backends.object.driver import BlobDriver, BlobStat
from kvtsjl.store.schema.ttl import TTL_NONE_EXPIRES_AT


def _expires_sidecar(path: Path) -> Path:
    return Path(str(path) + ".expires")


def _native_expires(path: Path) -> datetime | None:
    sidecar = _expires_sidecar(path)
    if not sidecar.is_file():
        return None
    try:
        token = sidecar.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    if token == "none":
        return TTL_NONE_EXPIRES_AT
    try:
        return datetime.fromtimestamp(float(token), tz=UTC)
    except ValueError:
        return datetime.fromtimestamp(0, tz=UTC)


def _stat_for_path(path: Path, *, key: str) -> BlobStat | None:
    if not path.is_file():
        return None
    try:
        st = path.stat()
    except OSError:
        return None
    return BlobStat(
        key=key,
        modified_at=datetime.fromtimestamp(st.st_mtime, tz=UTC),
        native_expires_at=_native_expires(path),
        size=st.st_size,
    )


def _write_atomic(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(str(path) + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


def _delete_with_sidecar(path: Path) -> None:
    path.unlink(missing_ok=True)
    _expires_sidecar(path).unlink(missing_ok=True)


class FilesystemBlobDriver(BlobDriver):
    def __init__(self, collection_prefix: str) -> None:
        self._prefix = collection_prefix

    @property
    def max_bulk_delete(self) -> int:
        # No native bulk API — template still parallelizes size-1 chunks.
        return 1

    def head(self, key: str) -> BlobStat | None:
        return _stat_for_path(Path(key), key=key)

    def read(self, key: str) -> tuple[bytes, BlobStat] | None:
        path = Path(key)
        stat = _stat_for_path(path, key=key)
        if stat is None:
            return None
        try:
            data = path.read_bytes()
        except OSError:
            return None
        return data, stat

    def write(self, key: str, data: bytes, *, headers: dict[str, Any] | None = None) -> None:
        path = Path(key)
        _write_atomic(path, data)
        headers = headers or {}
        mtime = headers.get("mtime")
        if isinstance(mtime, (int, float)):
            os.utime(path, (float(mtime), float(mtime)))
        if "expires_sidecar" in headers:
            sidecar = _expires_sidecar(path)
            token = headers["expires_sidecar"]
            if token is None:
                sidecar.unlink(missing_ok=True)
            else:
                sidecar.write_text(str(token), encoding="utf-8")

    def delete(self, key: str) -> bool:
        path = Path(key)
        existed = path.is_file()
        _delete_with_sidecar(path)
        return existed

    def list(self, *, prefix: str) -> Iterator[BlobStat]:
        """List objects under ``prefix`` (directory walk or string-prefix filter).

        LITERAL layout encodes keys as percent-encoded filenames, so a user
        ``KeyPrefix`` often ends mid-filename (e.g. ``…/inv%2F``) rather than
        on a directory boundary. Walk the nearest existing *directory* ancestor
        and filter by ``str.startswith(prefix)``.

        Critical: ``Path(prefix).exists()`` is True when a *file* equals the
        listing prefix (key ``al`` while scanning ``KeyPrefix("al")``). That
        file must not become the walk root — otherwise the scan silently
        returns zero rows.
        """
        root = Path(prefix)
        if root.is_dir():
            walk_root = root
        else:
            walk_root = root.parent
            while not walk_root.is_dir() and walk_root != walk_root.parent:
                walk_root = walk_root.parent
            if not walk_root.is_dir():
                return

        for dirpath, _dirnames, filenames in os.walk(walk_root):
            for name in filenames:
                if name.endswith(".tmp") or name.endswith(".expires"):
                    continue
                path = Path(dirpath) / name
                key = path.as_posix()
                if prefix.endswith("/") and not key.startswith(prefix):
                    # Align with KeyCodec encode_physical (trailing-slash prefix).
                    base = prefix.rstrip("/")
                    if key.startswith(base + "/") or key == base:
                        key = prefix + key[len(base) + 1 :]
                if not key.startswith(prefix):
                    continue
                stat = _stat_for_path(path, key=key)
                if stat is not None:
                    yield stat

    def bulk_delete(self, keys: Sequence[str]) -> int:
        deleted = 0
        for key in keys:
            if self.delete(key):
                deleted += 1
        return deleted
