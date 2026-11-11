"""GCS ``BlobDriver``."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any, BinaryIO, cast

from kvtsjl.backends.object.driver import BlobDriver, BlobStat
from kvtsjl.batching import GCS_BATCH_OPS_MAX
from kvtsjl.serde.compress import STREAM_CHUNK

# google-cloud-storage uploads a known size up to this in one multipart
# request; above it, or with no size, it takes a resumable upload that seeks.
_ONE_REQUEST_UPLOAD = 8 * 1024 * 1024

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
    generation = blob.generation
    return BlobStat(
        key=name,
        modified_at=_ref_time(blob),
        native_expires_at=custom if isinstance(custom, datetime) else None,
        size=int(size) if isinstance(size, int) else None,
        etag=etag if isinstance(etag, str) else None,
        version=str(generation) if isinstance(generation, int) else None,
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

    def write_if_version(
        self,
        key: str,
        data: bytes,
        version: str | None,
        *,
        headers: dict[str, Any] | None = None,
    ) -> bool:
        """Upload with ``ifGenerationMatch``; generation 0 is the one no object has."""
        from google.api_core.exceptions import PreconditionFailed

        blob = self._bucket.blob(key)
        if headers is not None and "custom_time" in headers:
            blob.custom_time = headers["custom_time"]
        try:
            blob.upload_from_string(data, if_generation_match=int(version or 0))
        except PreconditionFailed:
            return False
        return True

    def delete_if_version(self, key: str, version: str) -> bool:
        from google.api_core.exceptions import NotFound, PreconditionFailed

        try:
            self._bucket.delete_blob(key, if_generation_match=int(version))
        except (PreconditionFailed, NotFound):
            return False
        return True

    def write_stream(
        self,
        key: str,
        src: BinaryIO,
        *,
        size: int | None = None,
        headers: dict[str, Any] | None = None,
    ) -> None:
        """Upload ``src``, reading it once, front to back.

        A known size up to the client's multipart limit goes in one request,
        which only reads. Anything larger, or of unknown size — every
        compressed value — would take the client's resumable upload, which
        seeks the stream, and a stream kvtsjl encodes as it goes cannot seek;
        it is written through the blob's own writer instead, which buffers its
        chunks itself. The properties set on the blob first, ``custom_time``
        among them, start the writer's session, and a failure inside it
        cancels the session, so nothing is committed — including a ``size``
        the stream does not have.
        """
        blob = self._bucket.blob(key)
        if headers is not None and "custom_time" in headers:
            blob.custom_time = headers["custom_time"]
        if size is not None and size <= _ONE_REQUEST_UPLOAD:
            blob.upload_from_file(src, size=size)
            return
        written = 0
        with cast(BinaryIO, blob.open("wb")) as sink:
            while chunk := src.read(STREAM_CHUNK):
                sink.write(chunk)
                written += len(chunk)
            if size is not None and size != written:
                raise ValueError(f"write_stream size={size} but stream had {written} bytes")

    def read_chunks(self, key: str) -> tuple[Iterator[bytes], BlobStat] | None:
        blob = self._bucket.get_blob(key)
        if blob is None:
            return None

        def stream() -> Iterator[bytes]:
            with blob.open("rb") as handle:
                while chunk := handle.read(STREAM_CHUNK):
                    yield cast(bytes, chunk)

        return stream(), _stat_from_blob(blob)

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
