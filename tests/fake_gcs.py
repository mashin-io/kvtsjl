"""In-memory GCS bucket stand-in for tests (no emulator / GCP)."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from datetime import datetime, timezone
import io
import itertools
from types import TracebackType

from google.api_core.exceptions import NotFound, PreconditionFailed

_generations = itertools.count(1)
# The client uploads a known size up to this in one multipart request, which
# only reads; anything else takes the resumable upload, which seeks.
_MAX_MULTIPART_SIZE = 8 * 1024 * 1024


@dataclass
class _StoredObject:
    body: bytes
    custom_time: datetime | None
    updated: datetime
    time_created: datetime
    generation: int = field(default_factory=lambda: next(_generations))


def _require_generation(store: dict[str, _StoredObject], name: str, wanted: int | None) -> None:
    """The service's ``ifGenerationMatch``: 0 means absent, else the generation held."""
    if wanted is None:
        return
    stored = store.get(name)
    held = stored.generation if stored is not None else 0
    if held != wanted:
        raise PreconditionFailed(f"{name}: generation {held}, not {wanted}")


class _FakeBlobWriter(io.BytesIO):
    """Mirror ``BlobWriter``: commits on close, and an exception cancels it."""

    def __init__(self, blob: FakeGcsBlob, custom_time: datetime | None) -> None:
        super().__init__()
        self._blob, self._custom_time = blob, custom_time

    def close(self) -> None:
        if not self.closed:
            body = self.getvalue()
            self._blob.custom_time = self._custom_time
            self._blob.upload_from_string(body)
        super().close()

    def __exit__(self, exc_type: object, *_rest: object) -> None:
        if exc_type is not None:
            super().close()  # terminate: the session is dropped, nothing stored
            return
        self.close()


@dataclass
class FakeGcsBlob:
    name: str
    _store: dict[str, _StoredObject]
    custom_time: datetime | None = None
    updated: datetime | None = None
    time_created: datetime | None = None
    etag: str | None = None
    generation: int | None = None

    @property
    def size(self) -> int | None:
        stored = self._store.get(self.name)
        if stored is None:
            return None
        return len(stored.body)

    def download_as_bytes(self) -> bytes:
        return self._store[self.name].body

    def upload_from_file(self, file_obj: BinaryIO, *, size: int | None = None) -> None:
        """Mirror ``Blob.upload_from_file``; reads in chunks.

        As the client does, the resumable path — size unknown or above the
        multipart limit — asks the stream where it is, which a stream that
        cannot seek refuses.
        """
        if (size is None or size > _MAX_MULTIPART_SIZE) and file_obj.tell() != 0:
            raise ValueError("Stream must be at beginning.")
        chunks: list[bytes] = []
        while chunk := file_obj.read(1 << 16):
            chunks.append(chunk)
        body = b"".join(chunks)
        if size is not None and size != len(body):
            # ``size`` is exact for the real client, not a hint — see the
            # matching check in the Azure fake.
            raise ValueError(
                f"upload_from_file size={size} but stream had {len(body)} bytes"
            )
        self.upload_from_string(body)

    def download_to_file(self, file_obj: BinaryIO) -> None:
        """Mirror ``Blob.download_to_file``; writes in chunks."""
        body = self._store[self.name].body
        for start in range(0, len(body), 1 << 16):
            file_obj.write(body[start : start + (1 << 16)])

    def open(self, mode: str = "rb") -> BinaryIO:
        """Mirror ``Blob.open``: a reader, or a writer that uploads on close.

        The writer takes the blob's properties as they are when it opens, as
        the client's resumable session does.
        """
        if mode == "wb":
            return _FakeBlobWriter(self, self.custom_time)
        assert mode == "rb", f"fake only supports 'rb' and 'wb', got {mode!r}"
        return io.BytesIO(self._store[self.name].body)

    def upload_from_string(
        self, data: bytes | str, *, if_generation_match: int | None = None
    ) -> None:
        # Generation 0 is the one no existing object has: the precondition
        # holds only while the name is free, else the service answers 412.
        _require_generation(self._store, self.name, if_generation_match)
        body = data if isinstance(data, bytes) else data.encode("utf-8")
        now = datetime.now(timezone.utc)
        prev = self._store.get(self.name)
        created = prev.time_created if prev is not None else now
        self._store[self.name] = _StoredObject(
            body=body,
            custom_time=self.custom_time,
            updated=now,
            time_created=created,
        )

    def delete(self) -> None:
        self._store.pop(self.name, None)


class _FakeGcsBatch(AbstractContextManager[None]):
    def __enter__(self) -> None:
        return None

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        return False


@dataclass
class FakeGcsClient:
    def batch(self) -> _FakeGcsBatch:
        return _FakeGcsBatch()


@dataclass
class FakeGcsBucket:
    _objects: dict[str, _StoredObject] = field(default_factory=dict)
    client: FakeGcsClient = field(default_factory=FakeGcsClient)

    def blob(self, blob_name: str) -> FakeGcsBlob:
        return FakeGcsBlob(name=blob_name, _store=self._objects)

    def get_blob(self, blob_name: str) -> FakeGcsBlob | None:
        stored = self._objects.get(blob_name)
        if stored is None:
            return None
        return FakeGcsBlob(
            name=blob_name,
            _store=self._objects,
            custom_time=stored.custom_time,
            updated=stored.updated,
            time_created=stored.time_created,
            generation=stored.generation,
        )

    def delete_blob(self, blob_name: str, *, if_generation_match: int | None = None) -> None:
        if if_generation_match is not None and blob_name not in self._objects:
            raise NotFound(blob_name)
        _require_generation(self._objects, blob_name, if_generation_match)
        self._objects.pop(blob_name, None)

    def list_blobs(self, *, prefix: str | None = None) -> Iterator[FakeGcsBlob]:
        prefix = prefix or ""
        for name, stored in sorted(self._objects.items()):
            if name.startswith(prefix):
                yield FakeGcsBlob(
                    name=name,
                    _store=self._objects,
                    custom_time=stored.custom_time,
                    updated=stored.updated,
                    time_created=stored.time_created,
                )
