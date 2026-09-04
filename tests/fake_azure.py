"""In-memory Azure Blob container stand-in for tests (no Azurite / Azure)."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone

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


@dataclass
class FakeAzureBlobProperties:
    last_modified: datetime
    metadata: dict[str, str]


@dataclass
class FakeAzureBlobItem:
    name: str
    last_modified: datetime
    metadata: dict[str, str]


@dataclass
class FakeAzureBlobClient:
    name: str
    _store: dict[str, _StoredBlob]

    def upload_blob(
        self,
        data: bytes,
        *,
        overwrite: bool = False,
        metadata: Mapping[str, str] | None = None,
    ) -> None:
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
        )


@dataclass
class FakeAzureContainer:
    """Duck-types enough of ``ContainerClient`` for ``AzureBlobKvStore`` tests."""

    _objects: dict[str, _StoredBlob] = field(default_factory=dict)

    def get_blob_client(self, blob: str) -> FakeAzureBlobClient:
        return FakeAzureBlobClient(name=blob, _store=self._objects)

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
                )
