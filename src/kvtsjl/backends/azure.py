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
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from datetime import datetime
from enum import Enum
import time
from typing import TYPE_CHECKING, cast

from azure.core.exceptions import ResourceNotFoundError

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
from kvtsjl.store.schema.ttl import ExpiryGc, TtlPolicy, require_explicit_ttl_supported

if TYPE_CHECKING:
    from azure.storage.blob import ContainerClient

_EXPIRES_META_KEY = "expires"


class AzureTtlMode(str, Enum):
    """How ``AzureBlobKvStore`` applies TTL."""

    OBJECT_TIME = "object_time"
    """``now >= last_modified + ttl`` (no metadata writes)."""

    METADATA = "metadata"
    """Write ``expires`` metadata on per-write ``ttl=``; honor that clock."""


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


class AzureBlobKvStore[K, V, KBLOB: str | bytes](KvBackend[K, V, KBLOB, bytes, str]):
    """KvStore over an Azure Blob container.

    - ``VBLOB`` is ``bytes`` from ``value_serde``.
    - Collection = blob prefix ``{key_prefix}{name}/v{version}/``.
    - Pass an ``azure.storage.blob.ContainerClient``.
    - TTL: see ``ttl_mode`` / ``AzureTtlMode``.
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
        self._container = container
        self._key_prefix = base
        self._ttl_mode = ttl_mode
        self._expiry_gc = expiry_gc
        assert self._binding.collection is not None
        self._collection_prefix = self._binding.collection

    def _clone_with_scope(self, scope: Scope) -> AzureBlobKvStore[K, V, KBLOB]:
        return AzureBlobKvStore(
            self.kvset,
            container=self._container,
            key_prefix=self._key_prefix,
            ttl_mode=self._ttl_mode,
            expiry_gc=self._expiry_gc,
            scope=scope,
            binding=self._binding,
            batch_size=self.batch_size,
        )

    def _object_key(self, physical_key: KBLOB) -> str:
        rel = layout_encode_for_fs(self.kvset.key_layout, physical_key)
        return f"{self._collection_prefix}{rel}"

    def _expires_meta_value(self, ttl: TtlPolicy) -> str:
        secs = self.resolve_ttl_seconds(ttl)
        if secs is None:
            return "none"
        return str(time.time() + secs)

    def _expired_from_props(
        self,
        last_modified: datetime | None,
        metadata: Mapping[str, str] | None,
    ) -> bool:
        meta = metadata or {}
        token = meta.get(_EXPIRES_META_KEY)
        if token is not None:
            if token == "none":
                return False
            try:
                expires_at = float(token)
            except ValueError:
                expires_at = 0.0
            return time.time() >= expires_at
        ttl = self.ttl_seconds()
        if ttl is None or last_modified is None:
            return False
        return time.time() >= last_modified.timestamp() + ttl

    def _read_live(self, object_key: str) -> bytes | None:
        blob = self._container.get_blob_client(object_key)
        try:
            props = blob.get_blob_properties()
        except ResourceNotFoundError:
            return None
        if self._expired_from_props(props.last_modified, props.metadata):
            if self._expiry_gc is ExpiryGc.LAZY_DELETE:
                try:
                    blob.delete_blob()
                except ResourceNotFoundError:
                    pass
            return None
        try:
            data = blob.download_blob().readall()
        except ResourceNotFoundError:
            return None
        return data if isinstance(data, bytes) else bytes(data)

    def get(self, key: K) -> V | None:
        raw = self._read_live(self._object_key(self._physical_key_blob(key)))
        if raw is None:
            return None
        return self.kvset.value_serde.deserialize(raw)

    def set(self, key: K, value: V, *, ttl: TtlPolicy | None = None) -> None:
        require_explicit_ttl_supported(
            ttl,
            allowed=self._ttl_mode is AzureTtlMode.METADATA,
            backend="AzureBlobKvStore",
        )
        object_key = self._object_key(self._physical_key_blob(key))
        blob = self._container.get_blob_client(object_key)
        body = self.kvset.value_serde.serialize(value)
        metadata: dict[str, str] | None = None
        if ttl is not None:
            metadata = {_EXPIRES_META_KEY: self._expires_meta_value(ttl)}
        blob.upload_blob(body, overwrite=True, metadata=metadata)

    def delete(self, key: K) -> bool:
        object_key = self._object_key(self._physical_key_blob(key))
        blob = self._container.get_blob_client(object_key)
        try:
            blob.get_blob_properties()
        except ResourceNotFoundError:
            return False
        blob.delete_blob()
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
                "AzureBlobKvStore scan requires KeyLayout.LITERAL "
                f"(got {self.kvset.key_layout!r})"
            )
        prefix = self._scan_prefix_blob(query.prefix)
        ops = self.kvset.blob_ops
        for item in self._container.list_blobs(name_starts_with=self._collection_prefix):
            if self._expired_from_props(item.last_modified, item.metadata):
                if self._expiry_gc is ExpiryGc.LAZY_DELETE:
                    try:
                        self._container.get_blob_client(item.name).delete_blob()
                    except ResourceNotFoundError:
                        pass
                continue
            try:
                pk = self._physical_from_object_key(item.name)
            except ValueError:
                continue
            if not ops.startswith(pk, prefix):
                continue
            decoded = self._decode_key_from_physical(pk)
            if decoded is None:
                continue
            if query.include_values:
                raw = self._read_live(item.name)
                if raw is None:
                    continue
                yield decoded, self.kvset.value_serde.deserialize(raw)
            else:
                yield decoded, None

    def _gc_expired_keys(self, *, max_entries: int) -> list[K]:
        if max_entries < 1:
            raise ValueError(f"max_entries must be >= 1, got {max_entries}")
        prefix = self._scan_prefix_blob(None)
        ops = self.kvset.blob_ops
        deleted: list[K] = []
        for item in self._container.list_blobs(name_starts_with=self._collection_prefix):
            if len(deleted) >= max_entries:
                break
            if not self._expired_from_props(item.last_modified, item.metadata):
                continue
            try:
                pk = self._physical_from_object_key(item.name)
            except ValueError:
                continue
            if not ops.startswith(pk, prefix):
                continue
            decoded = self._decode_key_from_physical(pk)
            if decoded is None:
                continue
            try:
                self._container.get_blob_client(item.name).delete_blob()
            except ResourceNotFoundError:
                continue
            deleted.append(decoded)
        return deleted
