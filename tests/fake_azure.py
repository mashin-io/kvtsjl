"""In-memory Azure Blob container stand-in for tests (no Azurite / Azure)."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import BinaryIO

from azure.core.exceptions import ResourceNotFoundError


@dataclass
class _StoredBlob:
    body: bytes
    metadata: dict[str, str]
    last_modified: datetime


@dataclass
class _FakeDownload:
    _body: bytes

    def readall(self) -> bytes:
        return self._body

    def readinto(self, stream: BinaryIO) -> int:
        """Mirror ``StorageStreamDownloader.readinto``; writes in chunks."""
        for start in range(0, len(self._body), 1 << 16):
            stream.write(self._body[start : start + (1 << 16)])
        return len(self._body)

    def chunks(self) -> Iterator[bytes]:
        """Mirror ``StorageStreamDownloader.chunks``; the pull-side read."""
        for start in range(0, len(self._body), 1 << 16):
            yield self._body[start : start + (1 << 16)]


@dataclass
class FakeAzureBlobProperties:
    last_modified: datetime
    metadata: dict[str, str]
    size: int | None = None
    etag: str | None = None
    name: str | None = None


@dataclass
class FakeAzureBlobItem:
    name: str
    last_modified: datetime
    metadata: dict[str, str]
    size: int | None = None
    etag: str | None = None


@dataclass
class FakeAzureBlobClient:
    name: str
    _store: dict[str, _StoredBlob]

    def upload_blob(
        self,
        data: bytes | BinaryIO,
        *,
        overwrite: bool = False,
        metadata: Mapping[str, str] | None = None,
        length: int | None = None,
    ) -> None:
        if not isinstance(data, bytes):
            chunks: list[bytes] = []
            while chunk := data.read(1 << 16):
                chunks.append(chunk)
            data = b"".join(chunks)
        if length is not None and length != len(data):
            # The real service writes exactly ``length`` bytes: too small
            # truncates the blob, too large fails the request. Refusing here
            # makes a wrong length a test failure instead of silent data loss.
            raise ValueError(
                f"upload_blob length={length} but stream had {len(data)} bytes"
            )
        if not overwrite and self.name in self._store:
            raise ValueError(f"blob already exists: {self.name}")
        body = data if isinstance(data, bytes) else bytes(data)
        self._store[self.name] = _StoredBlob(
            body=body,
            metadata=dict(metadata or {}),
            last_modified=datetime.now(timezone.utc),
        )

    def download_blob(self) -> _FakeDownload:
        stored = self._store.get(self.name)
        if stored is None:
            raise ResourceNotFoundError(self.name)
        return _FakeDownload(stored.body)

    def delete_blob(self) -> None:
        if self.name not in self._store:
            raise ResourceNotFoundError(self.name)
        del self._store[self.name]

    def get_blob_properties(self) -> FakeAzureBlobProperties:
        stored = self._store.get(self.name)
        if stored is None:
            raise ResourceNotFoundError(self.name)
        return FakeAzureBlobProperties(
            last_modified=stored.last_modified,
            metadata=dict(stored.metadata),
            size=len(stored.body),
            name=self.name,
        )


@dataclass
class FakeAzureContainer:
    """Duck-types enough of ``ContainerClient`` for ``AzureBlobKvStore`` tests."""

    _objects: dict[str, _StoredBlob] = field(default_factory=dict)

    def get_blob_client(self, blob: str) -> FakeAzureBlobClient:
        return FakeAzureBlobClient(name=blob, _store=self._objects)

    def delete_blobs(self, *blobs: str | FakeAzureBlobItem) -> Iterator[None]:
        for blob in blobs:
            name = blob if isinstance(blob, str) else blob.name
            self._objects.pop(name, None)
            yield None

    def list_blobs(
        self, *, name_starts_with: str | None = None
    ) -> Iterator[FakeAzureBlobItem]:
        prefix = name_starts_with or ""
        for name, stored in sorted(self._objects.items()):
            if name.startswith(prefix):
                yield FakeAzureBlobItem(
                    name=name,
                    last_modified=stored.last_modified,
                    metadata=dict(stored.metadata),
                    size=len(stored.body),
                )
