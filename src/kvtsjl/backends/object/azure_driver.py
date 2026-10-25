"""Azure Blob Storage ``BlobDriver``."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, BinaryIO, cast

from azure.core import MatchConditions
from azure.core.exceptions import (
    ResourceExistsError,
    ResourceModifiedError,
    ResourceNotFoundError,
)

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
    sent = props.etag if isinstance(props.etag, str) else None
    etag = sent.strip('"') if sent is not None else None
    return BlobStat(
        key=name,
        modified_at=(
            props.last_modified if isinstance(props.last_modified, datetime) else None
        ),
        native_expires_at=_native_expires(props.metadata),
        size=int(size) if isinstance(size, int) else None,
        etag=etag,
        version=sent,
    )


def _metadata(headers: dict[str, Any] | None) -> Mapping[str, str] | None:
    raw = None if headers is None else headers.get("metadata")
    if not isinstance(raw, Mapping):
        return None
    return {str(k): str(v) for k, v in cast(Mapping[object, object], raw).items()}


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
        blob.upload_blob(data, overwrite=True, metadata=_metadata(headers))

    def write_if_version(
        self,
        key: str,
        data: bytes,
        version: str | None,
        *,
        headers: dict[str, Any] | None = None,
    ) -> bool:
        """Upload without ``overwrite`` (``If-None-Match: *``), or against the ETag seen."""
        blob = self._container.get_blob_client(key)
        try:
            if version is None:
                blob.upload_blob(data, overwrite=False, metadata=_metadata(headers))
            else:
                blob.upload_blob(
                    data,
                    overwrite=True,
                    metadata=_metadata(headers),
                    etag=version,
                    match_condition=MatchConditions.IfNotModified,
                )
        except (ResourceExistsError, ResourceModifiedError, ResourceNotFoundError):
            return False
        return True

    def delete_if_version(self, key: str, version: str) -> bool:
        blob = self._container.get_blob_client(key)
        try:
            blob.delete_blob(etag=version, match_condition=MatchConditions.IfNotModified)
        except (ResourceModifiedError, ResourceNotFoundError):
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
        blob = self._container.get_blob_client(key)
        blob.upload_blob(src, overwrite=True, metadata=_metadata(headers), length=size)

    def read_chunks(self, key: str) -> tuple[Iterator[bytes], BlobStat] | None:
        blob = self._container.get_blob_client(key)
        try:
            props = blob.get_blob_properties()
            downloader = blob.download_blob()
        except ResourceNotFoundError:
            return None
        def stream() -> Iterator[bytes]:
            # As in the S3 driver: release the response if the caller stops
            # early. ``StorageStreamDownloader`` has not always exposed
            # ``close``, so it is called only when present.
            try:
                yield from cast(Iterator[bytes], downloader.chunks())
            finally:
                closer = getattr(downloader, "close", None)
                if callable(closer):
                    closer()

        return stream(), _stat_from_props(props, key=key)

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
