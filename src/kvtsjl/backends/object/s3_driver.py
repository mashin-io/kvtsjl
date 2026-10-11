"""S3 ``BlobDriver``."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any, BinaryIO, cast

from botocore.exceptions import ClientError

from kvtsjl.backends.object.driver import BlobDriver, BlobStat
from kvtsjl.batching import S3_DELETE_OBJECTS_MAX
from kvtsjl.serde.compress import STREAM_CHUNK

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


def _not_found(exc: ClientError) -> bool:
    code = exc.response.get("Error", {}).get("Code", "")
    return code in {"404", "NoSuchKey", "NotFound"}


class S3BlobDriver(BlobDriver):
    def __init__(self, client: S3Client, bucket: str) -> None:
        self._client = client
        self._bucket = bucket

    @property
    def max_bulk_delete(self) -> int:
        return S3_DELETE_OBJECTS_MAX

    def head(self, key: str) -> BlobStat | None:
        try:
            resp = self._client.head_object(Bucket=self._bucket, Key=key)
        except ClientError as exc:
            if _not_found(exc):
                return None
            raise
        return self._stat_from_response(key, resp)

    def read(self, key: str) -> tuple[bytes, BlobStat] | None:
        try:
            resp = self._client.get_object(Bucket=self._bucket, Key=key)
        except ClientError as exc:
            if _not_found(exc):
                return None
            raise
        body = resp.get("Body")
        if body is None or not hasattr(body, "read"):
            return None
        data = body.read()
        raw = data if isinstance(data, bytes) else bytes(data)
        return raw, self._stat_from_response(key, resp)

    def write(self, key: str, data: bytes, *, headers: dict[str, Any] | None = None) -> None:
        kwargs: dict[str, object] = {
            "Bucket": self._bucket,
            "Key": key,
            "Body": data,
        }
        if headers:
            kwargs.update(headers)
        self._client.put_object(**cast(Any, kwargs))

    def write_stream(
        self,
        key: str,
        src: BinaryIO,
        *,
        size: int | None = None,
        headers: dict[str, Any] | None = None,
    ) -> None:
        extra: dict[str, Any] = dict(headers) if headers else {}
        # upload_fileobj manages multipart itself; it reads src in parts.
        self._client.upload_fileobj(src, self._bucket, key, ExtraArgs=extra or None)

    def read_chunks(self, key: str) -> tuple[Iterator[bytes], BlobStat] | None:
        try:
            resp = self._client.get_object(Bucket=self._bucket, Key=key)
        except ClientError as exc:
            if _not_found(exc):
                return None
            raise
        body = resp.get("Body")
        if body is None or not hasattr(body, "read"):
            return None
        return body.iter_chunks(chunk_size=STREAM_CHUNK), self._stat_from_response(key, resp)

    def delete(self, key: str) -> bool:
        try:
            self._client.head_object(Bucket=self._bucket, Key=key)
        except ClientError as exc:
            if _not_found(exc):
                return False
            raise
        self._client.delete_object(Bucket=self._bucket, Key=key)
        return True

    def list(self, *, prefix: str) -> Iterator[BlobStat]:
        token: str | None = None
        while True:
            if token:
                resp = self._client.list_objects_v2(
                    Bucket=self._bucket,
                    Prefix=prefix,
                    ContinuationToken=token,
                )
            else:
                resp = self._client.list_objects_v2(
                    Bucket=self._bucket,
                    Prefix=prefix,
                )
            contents = resp.get("Contents")
            if isinstance(contents, list):
                for obj in contents:
                    if not isinstance(obj, Mapping):
                        continue
                    key = obj.get("Key")
                    if not isinstance(key, str):
                        continue
                    last_modified = obj.get("LastModified")
                    size = obj.get("Size")
                    etag = obj.get("ETag")
                    if isinstance(etag, str):
                        etag = etag.strip('"')
                    else:
                        etag = None
                    yield BlobStat(
                        key=key,
                        modified_at=(
                            last_modified
                            if isinstance(last_modified, datetime)
                            else None
                        ),
                        size=int(size) if isinstance(size, int) else None,
                        etag=etag,
                    )
            if not resp.get("IsTruncated"):
                break
            next_token = resp.get("NextContinuationToken")
            if not isinstance(next_token, str) or not next_token:
                break
            token = next_token

    def bulk_delete(self, keys: Sequence[str]) -> int:
        if not keys:
            return 0
        objects = [{"Key": k} for k in keys]
        self._client.delete_objects(
            Bucket=self._bucket,
            Delete=cast(Any, {"Objects": objects, "Quiet": True}),
        )
        return len(keys)

    def _stat_from_response(self, key: str, resp: Mapping[str, Any]) -> BlobStat:
        last_modified = resp.get("LastModified")
        expires = resp.get("Expires")
        size = resp.get("ContentLength")
        etag = resp.get("ETag")
        if isinstance(etag, str):
            etag = etag.strip('"')
        else:
            etag = None
        return BlobStat(
            key=key,
            modified_at=last_modified if isinstance(last_modified, datetime) else None,
            native_expires_at=expires if isinstance(expires, datetime) else None,
            size=int(size) if isinstance(size, int) else None,
            etag=etag,
        )
