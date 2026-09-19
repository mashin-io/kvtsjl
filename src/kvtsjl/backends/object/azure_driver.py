"""Azure Blob Storage ``BlobDriver``."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from azure.core.exceptions import ResourceNotFoundError

from kvtsjl.backends.object.driver import BlobDriver, BlobStat
from kvtsjl.batching import AZURE_DELETE_BLOBS_MAX
from kvtsjl.store.schema.ttl import TTL_NONE_EXPIRES_AT

if TYPE_CHECKING:
    from azure.storage.blob import BlobProperties, ContainerClient

_EXPIRES_META_KEY = "expires"


def _native_expires(metadata: Mapping[str, str] | None) -> datetime | None:
    if not metadata:
        return None
    token = metadata.get(_EXPIRES_META_KEY)
    if token is None:
        return None
    if token == "none":
        return TTL_NONE_EXPIRES_AT
    try:
        return datetime.fromtimestamp(float(token), tz=UTC)
    except ValueError:
        return datetime.fromtimestamp(0, tz=UTC)


def _stat_from_props(props: BlobProperties, *, key: str) -> BlobStat:
    name = props.name if isinstance(props.name, str) else key
    size = props.size
    etag = props.etag
    if isinstance(etag, str):
        etag = etag.strip('"')
    else:
        etag = None
    return BlobStat(
        key=name,
        modified_at=(
            props.last_modified if isinstance(props.last_modified, datetime) else None
        ),
        native_expires_at=_native_expires(props.metadata),
        size=int(size) if isinstance(size, int) else None,
        etag=etag,
    )


class AzureBlobDriver(BlobDriver):
    def __init__(self, container: ContainerClient) -> None:
        self._container = container

    @property
    def max_bulk_delete(self) -> int:
        return AZURE_DELETE_BLOBS_MAX

    def head(self, key: str) -> BlobStat | None:
        blob = self._container.get_blob_client(key)
        try:
            props = blob.get_blob_properties()
        except ResourceNotFoundError:
            return None
        return _stat_from_props(props, key=key)

    def read(self, key: str) -> tuple[bytes, BlobStat] | None:
        blob = self._container.get_blob_client(key)
        try:
            props = blob.get_blob_properties()
        except ResourceNotFoundError:
            return None
        try:
            data = blob.download_blob().readall()
        except ResourceNotFoundError:
            return None
        raw = data if isinstance(data, bytes) else bytes(data)
        return raw, _stat_from_props(props, key=key)

    def write(self, key: str, data: bytes, *, headers: dict[str, Any] | None = None) -> None:
        blob = self._container.get_blob_client(key)
        metadata: Mapping[str, str] | None = None
        if headers is not None:
            raw_meta = headers.get("metadata")
            if isinstance(raw_meta, Mapping):
                metadata = {str(k): str(v) for k, v in raw_meta.items()}
        blob.upload_blob(data, overwrite=True, metadata=metadata)

    def delete(self, key: str) -> bool:
        blob = self._container.get_blob_client(key)
        try:
            blob.get_blob_properties()
        except ResourceNotFoundError:
            return False
        blob.delete_blob()
        return True

    def list(self, *, prefix: str) -> Iterator[BlobStat]:
        for item in self._container.list_blobs(name_starts_with=prefix):
            name = item.name
            if not isinstance(name, str):
                raise TypeError(f"Azure blob item missing string name: {item!r}")
            yield _stat_from_props(item, key=name)

    def bulk_delete(self, keys: Sequence[str]) -> int:
        if not keys:
            return 0
        # Consume the response iterator so the batch request completes.
        list(self._container.delete_blobs(*keys))
        return len(keys)
