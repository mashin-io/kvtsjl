"""Write-through mirror logical store."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
import logging

from kvtsjl.scope import Scope
from kvtsjl.store.compose.delegating import DelegatingKvStore
from kvtsjl.store.logical import KvStore
from kvtsjl.store.schema.layout import ScanQuery
from kvtsjl.store.schema.ttl import TtlPolicy

logger = logging.getLogger(__name__)


class MirrorKvStore[K, V](DelegatingKvStore[K, V]):
    """Reads/scans from primary; writes/deletes go to both (secondary best-effort)."""

    def __init__(
        self,
        primary: KvStore[K, V],
        secondary: KvStore[K, V],
    ) -> None:
        super().__init__(primary)
        self._primary = primary
        self._secondary = secondary

    def get(self, key: K) -> V | None:
        return self._primary.get(key)

    def set(self, key: K, value: V, *, ttl: TtlPolicy | None = None) -> None:
        self._primary.set(key, value, ttl=ttl)
        try:
            self._secondary.set(key, value, ttl=ttl)
        except Exception:
            logger.exception("mirror set to secondary failed")

    def delete(self, key: K) -> bool:
        deleted = self._primary.delete(key)
        try:
            self._secondary.delete(key)
        except Exception:
            logger.exception("mirror delete on secondary failed")
        return deleted

    def batch_get(self, keys: Sequence[K]) -> dict[K, V]:
        return self._primary.batch_get(keys)

    def batch_set(
        self, items: Mapping[K, V], *, ttl: TtlPolicy | None = None
    ) -> None:
        self._primary.batch_set(items, ttl=ttl)
        try:
            self._secondary.batch_set(items, ttl=ttl)
        except Exception:
            logger.exception("mirror batch_set to secondary failed")

    def batch_delete(self, keys: Sequence[K]) -> int:
        n = self._primary.batch_delete(keys)
        try:
            self._secondary.batch_delete(keys)
        except Exception:
            logger.exception("mirror batch_delete on secondary failed")
        return n

    def _scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        yield from self._primary._scan_entries(query)

    def _gc_expired_keys(self, *, max_entries: int) -> list[K]:
        if max_entries < 1:
            raise ValueError(f"max_entries must be >= 1, got {max_entries}")
        deleted = self._primary._gc_expired_keys(max_entries=max_entries)
        remaining = max_entries - len(deleted)
        if remaining < 1:
            return deleted
        try:
            deleted.extend(self._secondary._gc_expired_keys(max_entries=remaining))
        except Exception:
            logger.exception("mirror gc_expired on secondary failed")
        return deleted

    def _clone_with_scope(self, scope: Scope) -> KvStore[K, V]:
        return MirrorKvStore(
            self._primary._clone_with_scope(scope),
            self._secondary._clone_with_scope(scope),
        )

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import compose_repr

        return compose_repr(
            "MirrorKvStore",
            primary=self._primary,
            secondary=self._secondary,
        )
