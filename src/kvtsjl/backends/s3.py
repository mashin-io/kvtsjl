"""S3-backed KvStore (AWS S3 and S3-compatible stores such as MinIO).

Install with::

    pip install 'kvtsjl[s3]'

TTL is lazy-delete on get/scan. How expiry is stored is set at construction via
``ttl_mode``:

- ``S3TtlMode.OBJECT_TIME`` (default) — ``LastModified + KvSet.ttl_policy``;
  does not write HTTP ``Expires``; per-write ``ttl=`` raises.
- ``S3TtlMode.EXPIRES`` — write HTTP ``Expires`` on explicit ``ttl=`` (RFC 7231).
  Opt in only if you do not also use ``Expires`` for CDN caching.

This is not S3 Lifecycle (day-granularity bucket rules).
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, datetime
from enum import Enum
import time
from typing import TYPE_CHECKING, Any, cast

from botocore.exceptions import ClientError

from kvtsjl.batching import chunk_sequence
from kvtsjl.bind import (
    CollectionBinding,
    NamespaceBinder,
    NativeCollectionBinder,
)
from kvtsjl.exceptions import KvStoreScanUnsupported
from kvtsjl.scope import Scope
from kvtsjl.store import KvBackend
from kvtsjl.store.schema.kvset import KvSet
from kvtsjl.store.schema.layout import (
    KeyLayout,
    ScanQuery,
    layout_decode_for_fs,
    layout_encode_for_fs,
)
from kvtsjl.store.schema.ttl import (
    TTL_NONE_EXPIRES_AT,
    TtlPolicy,
    require_explicit_ttl_supported,
)

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


class S3TtlMode(str, Enum):
    """How ``S3KvStore`` applies TTL."""

    OBJECT_TIME = "object_time"
    """``now >= LastModified + ttl`` (no ``Expires`` writes)."""

    EXPIRES = "expires"
    """Write HTTP ``Expires`` on per-write ``ttl=``; ``now >= Expires``."""


def _safe_name(name: str) -> str:
    return (
        (name or "_")
        .replace("/", "_")
        .replace("\\", "_")
        .replace("..", "_")
        .replace("\0", "")
    )


def _normalize_prefix(prefix: str) -> str:
    prefix = prefix.strip().lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return prefix


class S3KvStore[K, V, KBLOB: str | bytes](KvBackend[K, V, KBLOB, bytes, str]):
    """KvStore over an S3 bucket (or MinIO / other S3-compatible endpoint).

    - ``VBLOB`` is ``bytes`` from ``value_serde``.
    - Collection = key prefix ``{key_prefix}{name}/v{version}/``.
    - Pass a boto3 S3 client (configure ``endpoint_url`` for MinIO).
    - TTL: see ``ttl_mode`` / ``S3TtlMode``.
    - Scan requires ``KeyLayout.LITERAL``.
    """

    def __init__(
        self,
        kvset: KvSet[K, V, KBLOB, bytes],
        *,
        client: S3Client,
        bucket: str,
        key_prefix: str = "",
        ttl_mode: S3TtlMode = S3TtlMode.OBJECT_TIME,
        scope: Scope | None = None,
        binder: NamespaceBinder[KBLOB, str] | None = None,
        binding: CollectionBinding[KBLOB, str] | None = None,
        batch_size: int = 500,
    ) -> None:
        base = _normalize_prefix(key_prefix)
        if binder is None and binding is None:
            binder = NativeCollectionBinder[KBLOB, str](
                collection_formatter=lambda ref: (
                    f"{base}{_safe_name(ref.name)}/{ref.version_label()}/"
                )
            )
        super().__init__(
            kvset,
            scope=scope,
            binder=binder,
            binding=binding,
            batch_size=batch_size,
        )
        self._client = client
        self._bucket = bucket
        self._key_prefix = base
        self._ttl_mode = ttl_mode
        assert self._binding.collection is not None
        self._collection_prefix = self._binding.collection

    def _clone_with_scope(self, scope: Scope) -> S3KvStore[K, V, KBLOB]:
        return S3KvStore(
            self.kvset,
            client=self._client,
            bucket=self._bucket,
            key_prefix=self._key_prefix,
            ttl_mode=self._ttl_mode,
            scope=scope,
            binding=self._binding,
            batch_size=self.batch_size,
        )

    def _object_key(self, physical_key: KBLOB) -> str:
        rel = layout_encode_for_fs(self.kvset.key_layout, physical_key)
        return f"{self._collection_prefix}{rel}"

    def _expired(
        self, last_modified: datetime | None, expires: datetime | None = None
    ) -> bool:
        if self._ttl_mode is S3TtlMode.EXPIRES and expires is not None:
            return time.time() >= expires.timestamp()
        ttl = self.ttl_seconds()
        if ttl is None or last_modified is None:
            return False
        return time.time() >= last_modified.timestamp() + ttl

    def _expires_from_response(self, resp: Mapping[str, Any]) -> datetime | None:
        expires = resp.get("Expires")
        return expires if isinstance(expires, datetime) else None

    def _read_live(self, object_key: str) -> bytes | None:
        try:
            resp = self._client.get_object(Bucket=self._bucket, Key=object_key)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise
        last_modified = resp.get("LastModified")
        last_modified_dt = last_modified if isinstance(last_modified, datetime) else None
        if self._expired(last_modified_dt, self._expires_from_response(resp)):
            self._client.delete_object(Bucket=self._bucket, Key=object_key)
            return None
        body = resp.get("Body")
        if body is None or not hasattr(body, "read"):
            return None
        data = body.read()
        return data if isinstance(data, bytes) else bytes(data)

    def get(self, key: K) -> V | None:
        raw = self._read_live(self._object_key(self._physical_key_blob(key)))
        if raw is None:
            return None
        return self.kvset.value_serde.deserialize(raw)

    def set(self, key: K, value: V, *, ttl: TtlPolicy | None = None) -> None:
        require_explicit_ttl_supported(
            ttl,
            allowed=self._ttl_mode is S3TtlMode.EXPIRES,
            backend="S3KvStore",
        )
        kwargs: dict[str, object] = {
            "Bucket": self._bucket,
            "Key": self._object_key(self._physical_key_blob(key)),
            "Body": self.kvset.value_serde.serialize(value),
        }
        if ttl is not None:
            secs = self.resolve_ttl_seconds(ttl)
            kwargs["Expires"] = (
                TTL_NONE_EXPIRES_AT
                if secs is None
                else datetime.fromtimestamp(time.time() + secs, tz=UTC)
            )
        self._client.put_object(**kwargs)  # type: ignore[arg-type]

    def delete(self, key: K) -> bool:
        object_key = self._object_key(self._physical_key_blob(key))
        try:
            self._client.head_object(Bucket=self._bucket, Key=object_key)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise
        self._client.delete_object(Bucket=self._bucket, Key=object_key)
        return True

    def batch_get(self, keys: Sequence[K]) -> dict[K, V]:
        out: dict[K, V] = {}
        for chunk in chunk_sequence(keys, self.batch_size):
            for key in chunk:
                value = self.get(key)
                if value is not None:
                    out[key] = value
        return out

    def batch_set(
        self, items: Mapping[K, V], *, ttl: TtlPolicy | None = None
    ) -> None:
        for chunk in chunk_sequence(list(items.items()), self.batch_size):
            for key, value in chunk:
                self.set(key, value, ttl=ttl)

    def batch_delete(self, keys: Sequence[K]) -> int:
        deleted = 0
        for chunk in chunk_sequence(keys, self.batch_size):
            for key in chunk:
                if self.delete(key):
                    deleted += 1
        return deleted

    def _iter_object_keys(self) -> Iterator[tuple[str, datetime | None]]:
        token: str | None = None
        while True:
            if token:
                resp = self._client.list_objects_v2(
                    Bucket=self._bucket,
                    Prefix=self._collection_prefix,
                    ContinuationToken=token,
                )
            else:
                resp = self._client.list_objects_v2(
                    Bucket=self._bucket,
                    Prefix=self._collection_prefix,
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
                    yield (
                        key,
                        last_modified if isinstance(last_modified, datetime) else None,
                    )
            if not resp.get("IsTruncated"):
                break
            next_token = resp.get("NextContinuationToken")
            if not isinstance(next_token, str) or not next_token:
                break
            token = next_token

    def _physical_from_object_key(self, object_key: str) -> KBLOB:
        if not object_key.startswith(self._collection_prefix):
            raise ValueError("object key outside collection prefix")
        rel = object_key[len(self._collection_prefix) :]
        blob_type = cast(type[str] | type[bytes], self.kvset.str_serde.blob_type)
        return cast(
            KBLOB,
            layout_decode_for_fs(self.kvset.key_layout, rel, blob_type=blob_type),
        )

    def _scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        if self.kvset.key_layout is not KeyLayout.LITERAL:
            raise KvStoreScanUnsupported(
                "S3KvStore scan requires KeyLayout.LITERAL "
                f"(got {self.kvset.key_layout!r})"
            )
        prefix = self._scan_prefix_blob(query.prefix)
        ops = self.kvset.blob_ops
        for object_key, last_modified in self._iter_object_keys():
            expires: datetime | None = None
            if self._ttl_mode is S3TtlMode.EXPIRES:
                try:
                    head = self._client.head_object(Bucket=self._bucket, Key=object_key)
                except ClientError as exc:
                    code = exc.response.get("Error", {}).get("Code", "")
                    if code in {"404", "NoSuchKey", "NotFound"}:
                        continue
                    raise
                expires = self._expires_from_response(head)
            if self._expired(last_modified, expires):
                self._client.delete_object(Bucket=self._bucket, Key=object_key)
                continue
            try:
                pk = self._physical_from_object_key(object_key)
            except ValueError:
                continue
            if not ops.startswith(pk, prefix):
                continue
            decoded = self._decode_key_from_physical(pk)
            if decoded is None:
                continue
            if query.include_values:
                raw = self._read_live(object_key)
                if raw is None:
                    continue
                yield decoded, self.kvset.value_serde.deserialize(raw)
            else:
                yield decoded, None
