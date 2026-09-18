"""Read-through fallback logical store."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from datetime import datetime
import logging

from kvtsjl.scope import Scope
from kvtsjl.store.compose.delegating import DelegatingKvStore
from kvtsjl.store.logical import KvStore
from kvtsjl.store.schema.layout import ScanQuery
from kvtsjl.store.schema.stat import RecordStat
from kvtsjl.store.schema.ttl import TtlPolicy

logger = logging.getLogger(__name__)


class FallbackReadKvStore[K, V](DelegatingKvStore[K, V]):
    """Read primary then secondary; writes go to primary only. Scan: primary only.

    ``exists`` / ``stat`` probe secondary on primary miss but **do not promote**
    — only ``get`` / ``batch_get`` may write a secondary hit into the primary.
    """

    def __init__(
        self,
        primary: KvStore[K, V],
        secondary: KvStore[K, V],
        *,
        promote: bool = True,
    ) -> None:
        super().__init__(primary)
        self._primary = primary
        self._secondary = secondary
        self._promote = promote

    @property
    def supports_write_clock(self) -> bool:
        return self._primary.supports_write_clock

    def get(self, key: K) -> V | None:
        value = self._primary.get(key)
        if value is not None:
            return value
        value = self._secondary.get(key)
        if value is not None and self._promote:
            try:
                self._promote_one(key, value)
            except Exception:
                logger.exception("fallback promote failed for key")
        return value

    def exists(self, key: K) -> bool:
        if self._primary.exists(key):
            return True
        return self._secondary.exists(key)

    def batch_exists(self, keys: Sequence[K]) -> set[K]:
        found = self._primary.batch_exists(keys)
        missing = [k for k in keys if k not in found]
        if missing:
            found |= self._secondary.batch_exists(missing)
        return found

    def stat(self, key: K) -> RecordStat | None:
        meta = self._primary.stat(key)
        if meta is not None:
            return meta
        return self._secondary.stat(key)

    def batch_stat(self, keys: Sequence[K]) -> dict[K, RecordStat]:
        out = self._primary.batch_stat(keys)
        missing = [k for k in keys if k not in out]
        if missing:
            out.update(self._secondary.batch_stat(missing))
        return out

    def set(
        self,
        key: K,
        value: V,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        self._primary.set(key, value, ttl=ttl, created_at=created_at)

    def delete(self, key: K) -> bool:
        return self._primary.delete(key)

    def batch_get(self, keys: Sequence[K]) -> dict[K, V]:
        found = self._primary.batch_get(keys)
        missing = [k for k in keys if k not in found]
        if not missing:
            return found
        secondary_found = self._secondary.batch_get(missing)
        if secondary_found and self._promote:
            try:
                self._promote_many(secondary_found)
            except Exception:
                logger.exception("fallback batch promote failed")
        found.update(secondary_found)
        return found

    def batch_set(
        self,
        items: Mapping[K, V],
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        self._primary.batch_set(items, ttl=ttl, created_at=created_at)

    def batch_delete(self, keys: Sequence[K]) -> int:
        return self._primary.batch_delete(keys)

    def _promote_one(self, key: K, value: V) -> None:
        if self._primary.supports_write_clock:
            meta = self._secondary.stat(key)
            clock = None
            if meta is not None:
                clock = meta.created_at or meta.modified_at
            self._primary.set(key, value, created_at=clock)
        else:
            self._primary.set(key, value)

    def _promote_many(self, items: Mapping[K, V]) -> None:
        if not self._primary.supports_write_clock:
            self._primary.batch_set(items)
            return
        stats = self._secondary.batch_stat(list(items.keys()))
        for key, value in items.items():
            meta = stats.get(key)
            clock = None
            if meta is not None:
                clock = meta.created_at or meta.modified_at
            self._primary.set(key, value, created_at=clock)

    def _scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        yield from self._primary._scan_entries(query)

    def _gc_expired_keys(self, *, max_entries: int) -> list[K]:
        if max_entries < 1:
            raise ValueError(f"max_entries must be >= 1, got {max_entries}")
        deleted = self._primary._gc_expired_keys(max_entries=max_entries)
        remaining = max_entries - len(deleted)
        if remaining < 1:
            return deleted
        deleted.extend(self._secondary._gc_expired_keys(max_entries=remaining))
        return deleted

    def _clone_with_scope(self, scope: Scope) -> KvStore[K, V]:
        return FallbackReadKvStore(
            self._primary._clone_with_scope(scope),
            self._secondary._clone_with_scope(scope),
            promote=self._promote,
        )

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import compose_repr

        return compose_repr(
            "FallbackReadKvStore",
            primary=self._primary,
            secondary=self._secondary,
            promote=self._promote,
        )
