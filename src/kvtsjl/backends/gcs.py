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
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, datetime
from enum import Enum
import time
from typing import TYPE_CHECKING, cast

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
    from google.cloud.storage import Blob, Bucket  # google-cloud-storage is not py.typed


class GcsTtlMode(str, Enum):
    """How ``GcsKvStore`` applies ``TtlPolicy``."""

    OBJECT_TIME = "object_time"
    """``now >= updated + ttl`` (no ``customTime`` writes)."""

    CUSTOM_TIME = "custom_time"
    """Write absolute expiry to ``customTime``; ``now >= customTime``."""


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


class GcsKvStore[K, V, KBLOB: str | bytes](KvBackend[K, V, KBLOB, bytes, str]):
    """KvStore over a GCS bucket.

    - ``VBLOB`` is ``bytes`` from ``value_serde``.
    - Collection = object prefix ``{key_prefix}{name}/v{version}/``.
    - Pass a ``google.cloud.storage.Bucket``.
    - TTL: see ``ttl_mode`` / ``GcsTtlMode``.
    - Scan requires ``KeyLayout.LITERAL``.
    """

    def __init__(
        self,
        kvset: KvSet[K, V, KBLOB, bytes],
        *,
        bucket: Bucket,
        key_prefix: str = "",
        ttl_mode: GcsTtlMode = GcsTtlMode.OBJECT_TIME,
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
        self._bucket = bucket
        self._key_prefix = base
        self._ttl_mode = ttl_mode
        assert self._binding.collection is not None
        self._collection_prefix = self._binding.collection

    def _clone_with_scope(self, scope: Scope) -> GcsKvStore[K, V, KBLOB]:
        return GcsKvStore(
            self.kvset,
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

    def _custom_time_expires_at(self, ttl: TtlPolicy | None = None) -> datetime | None:
        secs = self.resolve_ttl_seconds(ttl)
        if secs is None:
            if ttl is not None:
                return TTL_NONE_EXPIRES_AT
            return None
        return datetime.fromtimestamp(time.time() + secs, tz=UTC)

    def _object_reference_time(self, blob: Blob) -> datetime | None:
        updated = blob.updated
        if isinstance(updated, datetime):
            return updated
        created = blob.time_created
        if isinstance(created, datetime):
            return created
        return None

    def _expired(self, blob: Blob) -> bool:
        if self._ttl_mode is GcsTtlMode.CUSTOM_TIME:
            custom_time = blob.custom_time
            if isinstance(custom_time, datetime):
                return time.time() >= custom_time.timestamp()
            ttl = self.ttl_seconds()
            if ttl is None:
                return False
            ref = self._object_reference_time(blob)
            if ref is None:
                return False
            return time.time() >= ref.timestamp() + ttl
        ttl = self.ttl_seconds()
        if ttl is None:
            return False
        ref = self._object_reference_time(blob)
        if ref is None:
            return False
        return time.time() >= ref.timestamp() + ttl

    def _read_live(self, object_key: str) -> bytes | None:
        blob = self._bucket.get_blob(object_key)
        if blob is None:
            return None
        if self._expired(blob):
            blob.delete()
            return None
        data = blob.download_as_bytes()
        return data if isinstance(data, bytes) else bytes(data)

    def get(self, key: K) -> V | None:
        raw = self._read_live(self._object_key(self._physical_key_blob(key)))
        if raw is None:
            return None
        return self.kvset.value_serde.deserialize(raw)

    def set(self, key: K, value: V, *, ttl: TtlPolicy | None = None) -> None:
        require_explicit_ttl_supported(
            ttl,
            allowed=self._ttl_mode is GcsTtlMode.CUSTOM_TIME,
            backend="GcsKvStore",
        )
        object_key = self._object_key(self._physical_key_blob(key))
        blob = self._bucket.blob(object_key)
        if self._ttl_mode is GcsTtlMode.CUSTOM_TIME:
            blob.custom_time = self._custom_time_expires_at(ttl)
        blob.upload_from_string(self.kvset.value_serde.serialize(value))

    def delete(self, key: K) -> bool:
        object_key = self._object_key(self._physical_key_blob(key))
        blob = self._bucket.get_blob(object_key)
        if blob is None:
            return False
        blob.delete()
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
                "GcsKvStore scan requires KeyLayout.LITERAL "
                f"(got {self.kvset.key_layout!r})"
            )
        prefix = self._scan_prefix_blob(query.prefix)
        ops = self.kvset.blob_ops
        for blob in self._bucket.list_blobs(prefix=self._collection_prefix):
            if self._expired(blob):
                blob.delete()
                continue
            try:
                pk = self._physical_from_object_key(blob.name)
            except ValueError:
                continue
            if not ops.startswith(pk, prefix):
                continue
            decoded = self._decode_key_from_physical(pk)
            if decoded is None:
                continue
            if query.include_values:
                raw = self._read_live(blob.name)
                if raw is None:
                    continue
                yield decoded, self.kvset.value_serde.deserialize(raw)
            else:
                yield decoded, None
