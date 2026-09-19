"""GCS ``BlobDriver``."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any

from kvtsjl.backends.object.driver import BlobDriver, BlobStat
from kvtsjl.batching import GCS_BATCH_OPS_MAX

if TYPE_CHECKING:
    from google.cloud.storage import Blob, Bucket  # google-cloud-storage is not py.typed


def _ref_time(blob: Blob) -> datetime | None:
    updated = blob.updated
    if isinstance(updated, datetime):
        return updated
    created = blob.time_created
    if isinstance(created, datetime):
        return created
    return None


def _stat_from_blob(blob: Blob) -> BlobStat:
    name = blob.name
    if not isinstance(name, str):
        raise TypeError(f"GCS blob missing string name: {blob!r}")
    custom = blob.custom_time
    size = blob.size
    etag = blob.etag
    return BlobStat(
        key=name,
        modified_at=_ref_time(blob),
        native_expires_at=custom if isinstance(custom, datetime) else None,
        size=int(size) if isinstance(size, int) else None,
        etag=etag if isinstance(etag, str) else None,
    )


class GcsBlobDriver(BlobDriver):
    def __init__(self, bucket: Bucket) -> None:
        self._bucket = bucket

    @property
    def max_bulk_delete(self) -> int:
        return GCS_BATCH_OPS_MAX

    def head(self, key: str) -> BlobStat | None:
        blob = self._bucket.get_blob(key)
        if blob is None:
            return None
        return _stat_from_blob(blob)

    def read(self, key: str) -> tuple[bytes, BlobStat] | None:
        blob = self._bucket.get_blob(key)
        if blob is None:
            return None
        data = blob.download_as_bytes()
        raw = data if isinstance(data, bytes) else bytes(data)
        return raw, _stat_from_blob(blob)

    def write(self, key: str, data: bytes, *, headers: dict[str, Any] | None = None) -> None:
        blob = self._bucket.blob(key)
        if headers is not None and "custom_time" in headers:
            blob.custom_time = headers["custom_time"]
        blob.upload_from_string(data)

    def delete(self, key: str) -> bool:
        blob = self._bucket.get_blob(key)
        if blob is None:
            return False
        blob.delete()
        return True

    def list(self, *, prefix: str) -> Iterator[BlobStat]:
        for blob in self._bucket.list_blobs(prefix=prefix):
            yield _stat_from_blob(blob)

    def bulk_delete(self, keys: Sequence[str]) -> int:
        if not keys:
            return 0
        with self._bucket.client.batch():
            for key in keys:
                self._bucket.delete_blob(key)
        return len(keys)
