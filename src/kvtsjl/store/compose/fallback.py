"""Read-through fallback logical store."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
import logging

from kvtsjl.scope import Scope
from kvtsjl.store.compose.delegating import DelegatingKvStore
from kvtsjl.store.logical import KvStore
from kvtsjl.store.schema.layout import ScanQuery
from kvtsjl.store.schema.ttl import TtlPolicy

logger = logging.getLogger(__name__)


class FallbackReadKvStore[K, V](DelegatingKvStore[K, V]):
    """Read primary then secondary; writes go to primary only. Scan: primary only."""

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

    def get(self, key: K) -> V | None:
        value = self._primary.get(key)
        if value is not None:
            return value
        value = self._secondary.get(key)
        if value is not None and self._promote:
            try:
                self._primary.set(key, value)
            except Exception:
                logger.exception("fallback promote failed for key")
        return value

    def set(self, key: K, value: V, *, ttl: TtlPolicy | None = None) -> None:
        self._primary.set(key, value, ttl=ttl)

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
                self._primary.batch_set(secondary_found)
            except Exception:
                logger.exception("fallback batch promote failed")
        found.update(secondary_found)
        return found

    def batch_set(
        self, items: Mapping[K, V], *, ttl: TtlPolicy | None = None
    ) -> None:
        self._primary.batch_set(items, ttl=ttl)

    def batch_delete(self, keys: Sequence[K]) -> int:
        return self._primary.batch_delete(keys)

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
