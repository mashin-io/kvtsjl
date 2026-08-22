"""Write-through mirror logical store."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
import logging

from kvtsjl.scope import Scope
from kvtsjl.store.compose.delegating import DelegatingKvStore
from kvtsjl.store.logical import KvStore
from kvtsjl.store.schema.layout import ScanQuery

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

    def set(self, key: K, value: V) -> None:
        self._primary.set(key, value)
        try:
            self._secondary.set(key, value)
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

    def batch_set(self, items: Mapping[K, V]) -> None:
        self._primary.batch_set(items)
        try:
            self._secondary.batch_set(items)
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

    def _clone_with_scope(self, scope: Scope) -> KvStore[K, V]:
        return MirrorKvStore(
            self._primary._clone_with_scope(scope),
            self._secondary._clone_with_scope(scope),
        )
