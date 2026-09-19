"""S3-backed KvStore (AWS S3 and S3-compatible stores such as MinIO).

Install with::

    pip install 'kvtsjl[s3]'

TTL is lazy-delete on get/scan. How expiry is stored is set at construction via
``ttl_mode``:

- ``S3TtlMode.OBJECT_TIME`` (default) — ``LastModified + KvSet.ttl_policy``;
  does not write HTTP ``Expires``; per-write ``ttl=`` raises.
- ``S3TtlMode.EXPIRES`` — write HTTP ``Expires`` on explicit ``ttl=`` (RFC 7231).
  Opt in only if you do not also use ``Expires`` for CDN caching.

This is not S3 Lifecycle (day-granularity bucket rules). Use ``expiry_gc=HIDE``
to treat expired objects as absent without deleting them on get/scan.

``batch_get`` / ``batch_set`` run concurrent point ops capped by ``io_concurrency``
(default 32). ``batch_delete`` uses S3 ``delete_objects`` (up to 1000 keys/request)
with concurrent chunks; existence probe keeps the return count matching point
``delete``.
"""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING

from kvtsjl.backends.object.binding import normalize_key_prefix, object_collection_binder
from kvtsjl.backends.object.s3_driver import S3BlobDriver
from kvtsjl.backends.object.store import ObjectStoreKvBackend
from kvtsjl.backends.object.ttl import ObjectTimeTtl, S3ExpiresTtl, TtlSource
from kvtsjl.batching import DEFAULT_IO_CONCURRENCY
from kvtsjl.bind import CollectionBinding, NamespaceBinder
from kvtsjl.scope import Scope
from kvtsjl.store.leaf import LeafKvStore
from kvtsjl.store.schema.kvset import KvSet
from kvtsjl.store.schema.ttl import ExpiryGc

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


class S3TtlMode(StrEnum):
    """How ``S3KvStore`` applies TTL."""

    OBJECT_TIME = "object_time"
    """``now >= LastModified + ttl`` (no ``Expires`` writes)."""

    EXPIRES = "expires"
    """Write HTTP ``Expires`` on per-write ``ttl=``; ``now >= Expires``."""


def _ttl_source_for(mode: S3TtlMode) -> TtlSource:
    if mode is S3TtlMode.EXPIRES:
        return S3ExpiresTtl()
    return ObjectTimeTtl()


class S3KvStore[K, V, KBLOB: str | bytes](LeafKvStore[K, V, KBLOB, bytes, str]):
    """KvStore over an S3 bucket (or MinIO / other S3-compatible endpoint).

    - ``VBLOB`` is ``bytes`` from ``value_serde``.
    - Collection = key prefix ``{key_prefix}{name}/v{version}/``.
    - Pass a boto3 S3 client (configure ``endpoint_url`` for MinIO).
    - TTL: see ``ttl_mode`` / ``S3TtlMode``.
    - ``io_concurrency``: max concurrent GETs/SETs / bulk-delete chunks (default 32).
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
        expiry_gc: ExpiryGc = ExpiryGc.LAZY_DELETE,
        scope: Scope | None = None,
        binder: NamespaceBinder[KBLOB, str] | None = None,
        binding: CollectionBinding[KBLOB, str] | None = None,
        batch_size: int = 500,
        io_concurrency: int = DEFAULT_IO_CONCURRENCY,
    ) -> None:
        base = normalize_key_prefix(key_prefix)
        if binder is None and binding is None:
            binder = object_collection_binder(base)
        backend = ObjectStoreKvBackend(
            kvset,
            driver=S3BlobDriver(client, bucket),
            ttl_source=_ttl_source_for(ttl_mode),
            expiry_gc=expiry_gc,
            scope=scope,
            binder=binder,
            binding=binding,
            batch_size=batch_size,
            io_concurrency=io_concurrency,
        )
        backend.bind_leaf_state(
            _client=client,
            _bucket=bucket,
            _key_prefix=base,
            _ttl_mode=ttl_mode,
        )
        super().__init__(backend)

    def clone_with_scope(self, scope: Scope) -> S3KvStore[K, V, KBLOB]:
        return S3KvStore(
            self.kvset,
            client=self._client,
            bucket=self._bucket,
            key_prefix=self._key_prefix,
            ttl_mode=self._ttl_mode,
            expiry_gc=self._expiry_gc,
            scope=scope,
            binding=self._binding,
            batch_size=self.batch_size,
            io_concurrency=self.io_concurrency,
        )
