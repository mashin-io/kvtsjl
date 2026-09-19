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

from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, datetime
from enum import Enum
import time
from typing import TYPE_CHECKING, cast

from kvtsjl.batching import (
    DEFAULT_IO_CONCURRENCY,
    GCS_BATCH_OPS_MAX,
    chunk_sequence,
    map_concurrent,
)
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
    decode_path,
    encode_path,
)
from kvtsjl.store.schema.stat import RecordStat
from kvtsjl.store.schema.ttl import (
    TTL_NONE_EXPIRES_AT,
    ExpiryGc,
    TtlPolicy,
    require_explicit_ttl_supported,
    require_write_clock_supported,
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
        if io_concurrency < 1:
            raise ValueError(f"io_concurrency must be >= 1, got {io_concurrency}")
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
        self._expiry_gc = expiry_gc
        self.io_concurrency = io_concurrency
        assert self._binding.collection is not None
        self._collection_prefix = self._binding.collection

    @property
    def supports_write_clock(self) -> bool:
        return self._ttl_mode is GcsTtlMode.CUSTOM_TIME

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

    def _clock_base(self, created_at: datetime | None) -> float:
        if created_at is None:
            return time.time()
        if created_at.tzinfo is None:
            return created_at.replace(tzinfo=UTC).timestamp()
        return created_at.timestamp()

    def _object_key(self, physical_key: KBLOB) -> str:
        rel = encode_path(self.kvset.key_layout, physical_key)
        return f"{self._collection_prefix}{rel}"

    def _custom_time_expires_at(
        self,
        ttl: TtlPolicy | None = None,
        *,
        created_at: datetime | None = None,
    ) -> datetime | None:
        secs = self.resolve_ttl_seconds(ttl)
        if secs is None:
            if ttl is not None:
                return TTL_NONE_EXPIRES_AT
            return None
        return datetime.fromtimestamp(self._clock_base(created_at) + secs, tz=UTC)

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

    def _live_blob(self, object_key: str) -> Blob | None:
        blob = self._bucket.get_blob(object_key)
        if blob is None:
            return None
        if self._expired(blob):
            if self._expiry_gc is ExpiryGc.LAZY_DELETE:
                blob.delete()
            return None
        return blob

    def _stat_from_blob(self, blob: Blob) -> RecordStat:
        ref = self._object_reference_time(blob)
        custom = blob.custom_time if isinstance(blob.custom_time, datetime) else None
        size = blob.size if isinstance(blob.size, int) else None
        etag = blob.etag if isinstance(blob.etag, str) else None
        expires_at: datetime | None = None
        persistent = False
        if self._ttl_mode is GcsTtlMode.CUSTOM_TIME and custom is not None:
            if custom.year >= 2099:
                persistent = True
            else:
                expires_at = custom
        else:
            ttl = self.ttl_seconds()
            if ttl is None or ref is None:
                persistent = True
            else:
                expires_at = datetime.fromtimestamp(ref.timestamp() + ttl, tz=UTC)
        return RecordStat(
            created_at=ref,
            modified_at=ref,
            expires_at=expires_at,
            size=size,
            etag=etag,
            persistent=persistent,
        )

    def _read_live(self, object_key: str) -> bytes | None:
        blob = self._live_blob(object_key)
        if blob is None:
            return None
        data = blob.download_as_bytes()
        return data if isinstance(data, bytes) else bytes(data)

    def get(self, key: K) -> V | None:
        raw = self._read_live(self._object_key(self._physical_key_blob(key)))
        if raw is None:
            return None
        return self.kvset.value_serde.deserialize(raw)

    def exists(self, key: K) -> bool:
        return self._live_blob(self._object_key(self._physical_key_blob(key))) is not None

    def batch_exists(self, keys: Sequence[K]) -> set[K]:
        if not keys:
            return set()
        out: set[K] = set()
        for chunk in chunk_sequence(keys, self.batch_size):
            flags = map_concurrent(
                chunk, self.exists, max_workers=self.io_concurrency
            )
            for key, ok in zip(chunk, flags, strict=True):
                if ok:
                    out.add(key)
        return out

    def stat(self, key: K) -> RecordStat | None:
        blob = self._live_blob(self._object_key(self._physical_key_blob(key)))
        if blob is None:
            return None
        return self._stat_from_blob(blob)

    def batch_stat(self, keys: Sequence[K]) -> dict[K, RecordStat]:
        if not keys:
            return {}
        out: dict[K, RecordStat] = {}
        for chunk in chunk_sequence(keys, self.batch_size):
            stats = map_concurrent(
                chunk, self.stat, max_workers=self.io_concurrency
            )
            for key, meta in zip(chunk, stats, strict=True):
                if meta is not None:
                    out[key] = meta
        return out

    def set(
        self,
        key: K,
        value: V,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        require_explicit_ttl_supported(
            ttl,
            allowed=self._ttl_mode is GcsTtlMode.CUSTOM_TIME,
            backend="GcsKvStore",
        )
        require_write_clock_supported(
            created_at,
            allowed=self.supports_write_clock,
            backend="GcsKvStore",
        )
        object_key = self._object_key(self._physical_key_blob(key))
        blob = self._bucket.blob(object_key)
        if self._ttl_mode is GcsTtlMode.CUSTOM_TIME:
            blob.custom_time = self._custom_time_expires_at(ttl, created_at=created_at)
        blob.upload_from_string(self.kvset.value_serde.serialize(value))

    def delete(self, key: K) -> bool:
        object_key = self._object_key(self._physical_key_blob(key))
        blob = self._bucket.get_blob(object_key)
        if blob is None:
            return False
        blob.delete()
        return True

    def batch_get(self, keys: Sequence[K]) -> dict[K, V]:
        if not keys:
            return {}
        out: dict[K, V] = {}
        for chunk in chunk_sequence(keys, self.batch_size):
            values = map_concurrent(
                chunk, self.get, max_workers=self.io_concurrency
            )
            for key, value in zip(chunk, values, strict=True):
                if value is not None:
                    out[key] = value
        return out

    def batch_set(
        self,
        items: Mapping[K, V],
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        pairs = list(items.items())
        if not pairs:
            return

        def write(pair: tuple[K, V]) -> None:
            self.set(pair[0], pair[1], ttl=ttl, created_at=created_at)

        for chunk in chunk_sequence(pairs, self.batch_size):
            map_concurrent(chunk, write, max_workers=self.io_concurrency)

    def batch_delete(self, keys: Sequence[K]) -> int:
        if not keys:
            return 0
        unique = list(dict.fromkeys(keys))
        deleted = 0
        limit = min(self.batch_size, GCS_BATCH_OPS_MAX)
        for chunk in chunk_sequence(unique, limit):
            present = self.batch_exists(chunk)
            to_delete = [
                self._object_key(self._physical_key_blob(key))
                for key in chunk
                if key in present
            ]
            if to_delete:
                with self._bucket.client.batch():
                    for object_key in to_delete:
                        self._bucket.delete_blob(object_key)
            deleted += len(to_delete)
        return deleted

    def _physical_from_object_key(self, object_key: str) -> KBLOB:
        if not object_key.startswith(self._collection_prefix):
            raise ValueError("object key outside collection prefix")
        rel = object_key[len(self._collection_prefix) :]
        blob_type = cast(type[str] | type[bytes], self.kvset.str_serde.blob_type)
        return cast(
            KBLOB,
            decode_path(self.kvset.key_layout, rel, blob_type=blob_type),
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
                if self._expiry_gc is ExpiryGc.LAZY_DELETE:
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

    def _gc_expired_keys(self, *, max_entries: int) -> list[K]:
        if max_entries < 1:
            raise ValueError(f"max_entries must be >= 1, got {max_entries}")
        prefix = self._scan_prefix_blob(None)
        ops = self.kvset.blob_ops
        deleted: list[K] = []
        for blob in self._bucket.list_blobs(prefix=self._collection_prefix):
            if len(deleted) >= max_entries:
                break
            if not self._expired(blob):
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
            blob.delete()
            deleted.append(decoded)
        return deleted
