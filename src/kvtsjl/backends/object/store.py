"""Shared object-store KvBackend template."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, datetime
import time

from kvtsjl.backends.object.driver import BlobDriver, BlobStat
from kvtsjl.backends.object.ttl import TtlSource
from kvtsjl.batching import chunk_sequence, map_concurrent
from kvtsjl.bind import CollectionBinding, NamespaceBinder
from kvtsjl.exceptions import KvStoreScanUnsupported
from kvtsjl.scope import Scope
from kvtsjl.store import KvBackend
from kvtsjl.store.schema.key_codec import BindingLayoutKeyCodec, KeyCodec
from kvtsjl.store.schema.kvset import KvSet
from kvtsjl.store.schema.layout import KeyLayout, ScanQuery
from kvtsjl.store.schema.scan import (
    CompiledScan,
    KeyPrefix,
    Projection,
    ScanCapabilities,
    ScanOrder,
    ScanRow,
    ScanSpec,
    ScanStat,
    enforce_strict,
    peel_key_prefix,
)
from kvtsjl.store.schema.stat import RecordStat
from kvtsjl.store.schema.ttl import (
    ExpiryGc,
    TtlPolicy,
    require_explicit_ttl_supported,
    require_write_clock_supported,
)


class ObjectStoreKvBackend[K, V, KBLOB: str | bytes](KvBackend[K, V, KBLOB, bytes, str]):
    """Shared CRUD / scan / GC over ``BlobDriver`` + ``TtlSource`` + ``KeyCodec``."""

    # Optional leaf-facing attrs set by thin ``*KvStore`` wrappers (forwarded via LeafKvStore).
    _client: object | None = None
    _bucket: object | None = None
    _container: object | None = None
    _key_prefix: str = ""
    _ttl_mode: object | None = None
    _root: object | None = None

    def bind_leaf_state(self, **attrs: object) -> None:
        """Attach thin-leaf identity fields used by ``clone_with_scope`` / tests."""
        for name, value in attrs.items():
            object.__setattr__(self, name, value)

    def __init__(
        self,
        kvset: KvSet[K, V, KBLOB, bytes],
        *,
        driver: BlobDriver,
        ttl_source: TtlSource,
        expiry_gc: ExpiryGc = ExpiryGc.LAZY_DELETE,
        scope: Scope | None = None,
        binder: NamespaceBinder[KBLOB, str] | None = None,
        binding: CollectionBinding[KBLOB, str] | None = None,
        batch_size: int = 500,
        io_concurrency: int = 32,
        collection_prefix: str | None = None,
        key_codec: KeyCodec[K] | None = None,
    ) -> None:
        if io_concurrency < 1:
            raise ValueError(f"io_concurrency must be >= 1, got {io_concurrency}")
        super().__init__(
            kvset,
            scope=scope,
            binder=binder,
            binding=binding,
            batch_size=batch_size,
        )
        coll = self._binding.collection
        if coll is None:
            raise TypeError(
                f"{type(self).__name__} requires a non-None collection binding"
            )
        self._collection = coll
        self._driver = driver
        self._ttl_source = ttl_source
        self._expiry_gc = expiry_gc
        self.io_concurrency = io_concurrency
        prefix = collection_prefix if collection_prefix is not None else coll
        self._collection_prefix = prefix
        self._key_codec: KeyCodec[K] = key_codec or BindingLayoutKeyCodec(
            kvset,
            self._binding,
            scope=self.scope,
            collection_prefix=prefix,
        )

    @property
    def supports_write_clock(self) -> bool:
        return self._ttl_source.supports_write_clock

    def _clock_base(self, created_at: datetime | None) -> float:
        if created_at is None:
            return time.time()
        if created_at.tzinfo is None:
            return created_at.replace(tzinfo=UTC).timestamp()
        return created_at.timestamp()

    def _live_stat(self, object_key: str) -> BlobStat | None:
        stat = self._driver.head(object_key)
        if stat is None:
            return None
        if self._ttl_source.is_expired(
            stat, collection_ttl_seconds=self.ttl_seconds()
        ):
            if self._expiry_gc is ExpiryGc.LAZY_DELETE:
                self._driver.delete(object_key)
            return None
        return stat

    def _read_live(self, object_key: str) -> bytes | None:
        got = self._driver.read(object_key)
        if got is None:
            return None
        data, stat = got
        if self._ttl_source.is_expired(
            stat, collection_ttl_seconds=self.ttl_seconds()
        ):
            if self._expiry_gc is ExpiryGc.LAZY_DELETE:
                self._driver.delete(object_key)
            return None
        return data

    def get(self, key: K) -> V | None:
        raw = self._read_live(self._key_codec.encode_physical(key))
        if raw is None:
            return None
        return self.kvset.value_serde.deserialize(raw)

    def exists(self, key: K) -> bool:
        return self._live_stat(self._key_codec.encode_physical(key)) is not None

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
        head = self._live_stat(self._key_codec.encode_physical(key))
        if head is None:
            return None
        return self._ttl_source.to_record_stat(
            head, collection_ttl_seconds=self.ttl_seconds()
        )

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
            allowed=self._ttl_source.supports_per_write_ttl,
            backend=type(self).__name__,
        )
        require_write_clock_supported(
            created_at,
            allowed=self.supports_write_clock,
            backend=type(self).__name__,
        )
        headers = self._ttl_source.write_headers(
            ttl_seconds=self.resolve_ttl_seconds(ttl),
            clock_base=self._clock_base(created_at),
            explicit_ttl=ttl is not None,
            created_at_set=created_at is not None,
        )
        self._driver.write(
            self._key_codec.encode_physical(key),
            self.kvset.value_serde.serialize(value),
            headers=headers or None,
        )

    def delete(self, key: K) -> bool:
        return self._driver.delete(self._key_codec.encode_physical(key))

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
        # max_bulk_delete governs request size; io_concurrency governs
        # parallelism across chunks (FS max_bulk_delete=1 must not go serial).
        limit = min(self.batch_size, self._driver.max_bulk_delete)

        def delete_chunk(chunk: Sequence[K]) -> int:
            present = self.batch_exists(chunk)
            to_delete = [
                self._key_codec.encode_physical(key)
                for key in chunk
                if key in present
            ]
            if not to_delete:
                return 0
            return self._driver.bulk_delete(to_delete)

        chunks = list(chunk_sequence(unique, limit))
        counts = map_concurrent(
            chunks, delete_chunk, max_workers=self.io_concurrency
        )
        return sum(counts)

    def _enrich_list_stat(self, stat: BlobStat) -> BlobStat | None:
        if not self._ttl_source.needs_head_for_list_expiry():
            return stat
        headed = self._driver.head(stat.key)
        return headed

    def _listing_prefix(self, key_prefix: K | None) -> str:
        return self._key_codec.listing_prefix(key_prefix)

    def _scan_stat(self, blob: BlobStat) -> ScanStat:
        record = self._ttl_source.to_record_stat(
            blob, collection_ttl_seconds=self.ttl_seconds()
        )
        return ScanStat(
            size=blob.size,
            modified_at=record.modified_at,
            expires_at=record.expires_at,
            etag=blob.etag,
        )

    def _iter_listed_rows(
        self,
        *,
        listing_prefix: str,
        user_prefix: K | None,
        include_values: bool,
        limit: int | None = None,
    ) -> Iterator[ScanRow[K, V]]:
        yielded = 0
        for listed in self._driver.list(prefix=listing_prefix):
            if limit is not None and yielded >= limit:
                return
            stat = self._enrich_list_stat(listed)
            if stat is None:
                continue
            if self._ttl_source.is_expired(
                stat, collection_ttl_seconds=self.ttl_seconds()
            ):
                if self._expiry_gc is ExpiryGc.LAZY_DELETE:
                    self._driver.delete(stat.key)
                continue
            # Safety net: wrong listing prefix yields zero rows, not wrong rows.
            decoded = self._key_codec.decode_if_prefix(stat.key, user_prefix)
            if decoded is None:
                continue
            scan_stat = self._scan_stat(stat)
            if include_values:
                raw = self._read_live(stat.key)
                if raw is None:
                    continue
                yield ScanRow(
                    key=decoded,
                    value=self.kvset.value_serde.deserialize(raw),
                    stat=scan_stat,
                )
            else:
                yield ScanRow(key=decoded, value=None, stat=scan_stat)
            yielded += 1

    def scan_capabilities(self) -> ScanCapabilities:
        return ScanCapabilities(
            pushdown=frozenset({KeyPrefix}),
            orders=frozenset({ScanOrder.UNORDERED, ScanOrder.KEY_ASC}),
            resumable=True,
            stats=True,
        )

    def compile_scan(self, spec: ScanSpec[K]) -> CompiledScan[K]:
        if self.kvset.key_layout is not KeyLayout.LITERAL:
            raise KvStoreScanUnsupported(
                f"{type(self).__name__} scan requires KeyLayout.LITERAL "
                f"(got {self.kvset.key_layout!r})"
            )
        user_prefix, residual = peel_key_prefix(spec.where)
        pushed: set[str] = set()
        if user_prefix is not None:
            pushed.add("KeyPrefix")
        listing = self._listing_prefix(user_prefix)
        include_values = spec.project is Projection.ENTRIES
        # Over-fetch a little when residual may filter; limit applied in cascade.
        rows = self._iter_listed_rows(
            listing_prefix=listing,
            user_prefix=user_prefix,
            include_values=include_values,
            limit=None,
        )
        enforce_strict(spec, residual, backend=type(self).__name__)
        return CompiledScan(
            rows=rows,
            residual=residual,
            pushed=frozenset(pushed),
        )

    def scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        if self.kvset.key_layout is not KeyLayout.LITERAL:
            raise KvStoreScanUnsupported(
                f"{type(self).__name__} scan requires KeyLayout.LITERAL "
                f"(got {self.kvset.key_layout!r})"
            )
        listing = self._listing_prefix(query.prefix)
        for row in self._iter_listed_rows(
            listing_prefix=listing,
            user_prefix=query.prefix,
            include_values=query.include_values,
        ):
            yield row.key, row.value

    def clone_with_scope(self, scope: Scope) -> ObjectStoreKvBackend[K, V, KBLOB]:
        cloned = ObjectStoreKvBackend(
            self.kvset,
            driver=self._driver,
            ttl_source=self._ttl_source,
            expiry_gc=self._expiry_gc,
            scope=scope,
            binding=self._binding,
            batch_size=self.batch_size,
            io_concurrency=self.io_concurrency,
            collection_prefix=self._collection_prefix,
        )
        for attr in (
            "_client",
            "_bucket",
            "_container",
            "_key_prefix",
            "_ttl_mode",
            "_root",
            "_read_live",
        ):
            if hasattr(self, attr):
                setattr(cloned, attr, getattr(self, attr))
        return cloned

    def gc_expired_keys(self, *, max_entries: int) -> list[K]:
        if max_entries < 1:
            raise ValueError(f"max_entries must be >= 1, got {max_entries}")
        deleted: list[K] = []
        listing = self._listing_prefix(None)
        for listed in self._driver.list(prefix=listing):
            if len(deleted) >= max_entries:
                break
            stat = self._enrich_list_stat(listed)
            if stat is None:
                continue
            if not self._ttl_source.is_expired(
                stat, collection_ttl_seconds=self.ttl_seconds()
            ):
                continue
            decoded = self._key_codec.decode_if_prefix(stat.key, None)
            if decoded is None:
                continue
            self._driver.delete(stat.key)
            deleted.append(decoded)
        return deleted
