"""Azure Blob Storage-backed KvStore.

Install with::

    pip install 'kvtsjl[azure]'

TTL is lazy-delete on get/scan. How expiry is stored is set at construction via
``ttl_mode``:

- ``AzureTtlMode.OBJECT_TIME`` (default) — ``last_modified + KvSet.ttl_policy``;
  does not write blob metadata; per-write ``ttl=`` raises.
- ``AzureTtlMode.METADATA`` — on explicit ``ttl=`` only, set metadata key
  ``expires`` (unix timestamp or ``none``). Opt in only when you accept that
  metadata footprint.

Use ``expiry_gc=HIDE`` to treat expired blobs as absent without deleting them.

``batch_get`` / ``batch_set`` run concurrent point ops capped by ``io_concurrency``
(default 32). ``batch_delete`` uses Azure ``delete_blobs`` (up to 256/request) after
a concurrent existence probe so the return count matches point ``delete``.
"""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING

from kvtsjl.backends.object.azure_driver import AzureBlobDriver
from kvtsjl.backends.object.binding import normalize_key_prefix, object_collection_binder
from kvtsjl.backends.object.store import ObjectStoreKvBackend
from kvtsjl.backends.object.ttl import AzureMetadataTtl, ObjectTimeTtl, TtlSource
from kvtsjl.batching import DEFAULT_IO_CONCURRENCY
from kvtsjl.bind import CollectionBinding, NamespaceBinder
from kvtsjl.scope import Scope
from kvtsjl.store.leaf import LeafKvStore
from kvtsjl.store.schema.kvset import KvSet
from kvtsjl.store.schema.ttl import ExpiryGc

if TYPE_CHECKING:
    from azure.storage.blob import ContainerClient


class AzureTtlMode(StrEnum):
    """How ``AzureBlobKvStore`` applies TTL."""

    OBJECT_TIME = "object_time"
    """``now >= last_modified + ttl`` (no metadata writes)."""

    METADATA = "metadata"
    """Write ``expires`` metadata on per-write ``ttl=``; honor that clock."""


def _ttl_source_for(mode: AzureTtlMode) -> TtlSource:
    if mode is AzureTtlMode.METADATA:
        return AzureMetadataTtl()
    return ObjectTimeTtl()


class AzureBlobKvStore[K, V, KBLOB: str | bytes](LeafKvStore[K, V, KBLOB, bytes, str]):
    """KvStore over an Azure Blob container.

    - ``VBLOB`` is ``bytes`` from ``value_serde``.
    - Collection = blob prefix ``{key_prefix}{name}/v{version}/``.
    - Pass an ``azure.storage.blob.ContainerClient``.
    - TTL: see ``ttl_mode`` / ``AzureTtlMode``.
    - ``io_concurrency``: max concurrent GETs/SETs / existence probes (default 32).
      ``batch_delete`` uses native ``delete_blobs``.
    - Scan requires ``KeyLayout.LITERAL``.
    """

    def __init__(
        self,
        kvset: KvSet[K, V, KBLOB, bytes],
        *,
        container: ContainerClient,
        key_prefix: str = "",
        ttl_mode: AzureTtlMode = AzureTtlMode.OBJECT_TIME,
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
            driver=AzureBlobDriver(container),
            ttl_source=_ttl_source_for(ttl_mode),
            expiry_gc=expiry_gc,
            scope=scope,
            binder=binder,
            binding=binding,
            batch_size=batch_size,
            io_concurrency=io_concurrency,
        )
        backend.bind_leaf_state(
            _container=container,
            _key_prefix=base,
            _ttl_mode=ttl_mode,
        )
        super().__init__(backend)

    def clone_with_scope(self, scope: Scope) -> AzureBlobKvStore[K, V, KBLOB]:
        return AzureBlobKvStore(
            self.kvset,
            container=self._container,
            key_prefix=self._key_prefix,
            ttl_mode=self._ttl_mode,
            expiry_gc=self._expiry_gc,
            scope=scope,
            binding=self._binding,
            batch_size=self.batch_size,
            io_concurrency=self.io_concurrency,
        )
