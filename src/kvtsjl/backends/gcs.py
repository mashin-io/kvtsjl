"""GCS-backed KvStore (Google Cloud Storage).

Install with::

    pip install 'kvtsjl[gcs]'

TTL is lazy-delete on get/scan. How the expiry clock is chosen is set at
construction via ``ttl_mode``:

- ``GcsTtlMode.OBJECT_TIME`` (default) — derive from object ``updated``
  (fallback ``time_created``) + ``TtlPolicy``; does not write ``customTime``.
- ``GcsTtlMode.CUSTOM_TIME`` — write absolute expiry to native ``customTime``;
  expire when ``now >= customTime``. Per-write ``ttl=`` requires this mode.
  Prefer only when you do not also use lifecycle rules (or other features)
  that depend on ``customTime``.

Default ``OBJECT_TIME`` writes nothing extra. This is not GCS Object Lifecycle.
Use ``expiry_gc=HIDE`` to treat expired objects as absent without deleting them.

``batch_get`` / ``batch_set`` use concurrent point ops (capped by ``io_concurrency``,
default 32), not ``transfer_manager`` multi-file helpers. ``batch_delete`` uses the
GCS JSON API ``client.batch()`` for multi-delete (after a concurrent existence probe).
"""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING

from kvtsjl.backends.object.binding import normalize_key_prefix, object_collection_binder
from kvtsjl.backends.object.gcs_driver import GcsBlobDriver
from kvtsjl.backends.object.store import ObjectStoreKvBackend
from kvtsjl.backends.object.ttl import GcsCustomTimeTtl, ObjectTimeTtl, TtlSource
from kvtsjl.batching import DEFAULT_IO_CONCURRENCY
from kvtsjl.bind import CollectionBinding, NamespaceBinder
from kvtsjl.scope import Scope
from kvtsjl.store.schema.kvset import KvSet
from kvtsjl.store.schema.ttl import ExpiryGc

if TYPE_CHECKING:
    from google.cloud.storage import Bucket  # google-cloud-storage is not py.typed


class GcsTtlMode(StrEnum):
    """How ``GcsKvStore`` applies ``TtlPolicy``."""

    OBJECT_TIME = "object_time"
    """``now >= updated + ttl`` (no ``customTime`` writes)."""

    CUSTOM_TIME = "custom_time"
    """Write absolute expiry to ``customTime``; ``now >= customTime``."""


def _ttl_source_for(mode: GcsTtlMode) -> TtlSource:
    if mode is GcsTtlMode.CUSTOM_TIME:
        return GcsCustomTimeTtl()
    return ObjectTimeTtl()


class GcsKvStore[K, V, KBLOB: str | bytes](ObjectStoreKvBackend[K, V, KBLOB]):
    """KvStore over a GCS bucket.

    - ``VBLOB`` is ``bytes`` from ``value_serde``.
    - Collection = object prefix ``{key_prefix}{name}/v{version}/``.
    - Pass a ``google.cloud.storage.Bucket``.
    - TTL: see ``ttl_mode`` / ``GcsTtlMode``.
    - ``io_concurrency``: max concurrent GETs/SETs / existence probes (default 32).
      ``batch_delete`` uses GCS ``client.batch()`` multi-delete.
    - Scan requires ``KeyLayout.LITERAL``.
    """

    def __init__(
        self,
        kvset: KvSet[K, V, KBLOB, bytes],
        *,
        bucket: Bucket,
        key_prefix: str = "",
        ttl_mode: GcsTtlMode = GcsTtlMode.OBJECT_TIME,
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
        self._bucket = bucket
        self._key_prefix = base
        self._ttl_mode = ttl_mode
        super().__init__(
            kvset,
            driver=GcsBlobDriver(bucket),
            ttl_source=_ttl_source_for(ttl_mode),
            expiry_gc=expiry_gc,
            scope=scope,
            binder=binder,
            binding=binding,
            batch_size=batch_size,
            io_concurrency=io_concurrency,
        )

    def _clone_with_scope(self, scope: Scope) -> GcsKvStore[K, V, KBLOB]:
        return GcsKvStore(
            self.kvset,
            bucket=self._bucket,
            key_prefix=self._key_prefix,
            ttl_mode=self._ttl_mode,
            expiry_gc=self._expiry_gc,
            scope=scope,
            binding=self._binding,
            batch_size=self.batch_size,
            io_concurrency=self.io_concurrency,
        )
