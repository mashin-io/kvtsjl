"""In-memory GCS bucket stand-in for tests (no emulator / GCP)."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, timezone


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

    def download_as_bytes(self) -> bytes:
        return self._store[self.name].body

    def upload_from_string(self, data: bytes | str) -> None:
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


@dataclass
class FakeGcsBucket:
    _objects: dict[str, _StoredObject] = field(default_factory=dict)

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
