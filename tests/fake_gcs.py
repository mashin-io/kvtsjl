"""In-memory GCS bucket stand-in for tests (no emulator / GCP)."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from datetime import datetime, timezone
import io
from types import TracebackType

from google.api_core.exceptions import PreconditionFailed


@dataclass
class _StoredObject:
    body: bytes
    custom_time: datetime | None
    updated: datetime
    time_created: datetime


@dataclass
class FakeGcsBlob:
    name: str
    _store: dict[str, _StoredObject]
    custom_time: datetime | None = None
    updated: datetime | None = None
    time_created: datetime | None = None
    etag: str | None = None

    @property
    def size(self) -> int | None:
        stored = self._store.get(self.name)
        if stored is None:
            return None
        return len(stored.body)

    def download_as_bytes(self) -> bytes:
        return self._store[self.name].body

    def upload_from_file(self, file_obj: BinaryIO, *, size: int | None = None) -> None:
        """Mirror ``Blob.upload_from_file``; reads in chunks."""
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
        """Mirror ``Blob.open``; the pull-side read."""
        assert mode == "rb", f"fake only supports 'rb', got {mode!r}"
        return io.BytesIO(self._store[self.name].body)

    def upload_from_string(
        self, data: bytes | str, *, if_generation_match: int | None = None
    ) -> None:
        # Generation 0 is the one no existing object has: the precondition
        # holds only while the name is free, else the service answers 412.
        if if_generation_match == 0 and self.name in self._store:
            raise PreconditionFailed(f"object already exists: {self.name}")
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
        )

    def delete_blob(self, blob_name: str) -> None:
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
