"""Composable KvStore wrappers: fallback_read, mirror, readonly."""

from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping, Sequence

from kvtsjl.exceptions import KvStoreReadOnlyError
from kvtsjl.key_layout import ScanQuery
from kvtsjl.scope import Scope
from kvtsjl.store import KvStore

logger = logging.getLogger(__name__)


class _DelegatingKvStore[K, V, KBLOB, VBLOB, COLL](KvStore[K, V, KBLOB, VBLOB, COLL]):
    """Base for wrappers that hold an underlying store."""

    def __init__(self, underlying: KvStore[K, V, KBLOB, VBLOB, COLL]) -> None:
        self.kvset = underlying.kvset
        self.scope = underlying.scope
        self.batch_size = underlying.batch_size
        self._binding = underlying.binding
        self._underlying = underlying

    def _clone_with_scope(self, scope: Scope) -> KvStore[K, V, KBLOB, VBLOB, COLL]:
        raise NotImplementedError


class ReadonlyKvStore[K, V, KBLOB, VBLOB, COLL](
    _DelegatingKvStore[K, V, KBLOB, VBLOB, COLL]
):
    def get(self, key: K) -> V | None:
        return self._underlying.get(key)

    def set(self, key: K, value: V) -> None:
        raise KvStoreReadOnlyError("set on readonly store")

    def delete(self, key: K) -> bool:
        raise KvStoreReadOnlyError("delete on readonly store")

    def batch_get(self, keys: Sequence[K]) -> dict[K, V]:
        return self._underlying.batch_get(keys)

    def batch_set(self, items: Mapping[K, V]) -> None:
        raise KvStoreReadOnlyError("batch_set on readonly store")

    def batch_delete(self, keys: Sequence[K]) -> int:
        raise KvStoreReadOnlyError("batch_delete on readonly store")

    def _scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        yield from self._underlying._scan_entries(query)

    def _clone_with_scope(self, scope: Scope) -> KvStore[K, V, KBLOB, VBLOB, COLL]:
        return ReadonlyKvStore(self._underlying._clone_with_scope(scope))

    def readonly(self) -> KvStore[K, V, KBLOB, VBLOB, COLL]:
        return self


class FallbackReadKvStore[K, V, KBLOB, VBLOB, COLL, SEC_COLL](
    _DelegatingKvStore[K, V, KBLOB, VBLOB, COLL]
):
    """Read primary then secondary; writes go to primary only. Scan: primary only."""

    def __init__(
        self,
        primary: KvStore[K, V, KBLOB, VBLOB, COLL],
        secondary: KvStore[K, V, KBLOB, VBLOB, SEC_COLL],
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
            except Exception:  # noqa: BLE001
                logger.exception("fallback promote failed for key")
        return value

    def set(self, key: K, value: V) -> None:
        self._primary.set(key, value)

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
            except Exception:  # noqa: BLE001
                logger.exception("fallback batch promote failed")
        found.update(secondary_found)
        return found

    def batch_set(self, items: Mapping[K, V]) -> None:
        self._primary.batch_set(items)

    def batch_delete(self, keys: Sequence[K]) -> int:
        return self._primary.batch_delete(keys)

    def _scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        yield from self._primary._scan_entries(query)

    def _clone_with_scope(self, scope: Scope) -> KvStore[K, V, KBLOB, VBLOB, COLL]:
        return FallbackReadKvStore(
            self._primary._clone_with_scope(scope),
            self._secondary._clone_with_scope(scope),
            promote=self._promote,
        )


class MirrorKvStore[K, V, KBLOB, VBLOB, COLL, SEC_COLL](
    _DelegatingKvStore[K, V, KBLOB, VBLOB, COLL]
):
    """Reads/scans from primary; writes/deletes go to both (secondary best-effort)."""

    def __init__(
        self,
        primary: KvStore[K, V, KBLOB, VBLOB, COLL],
        secondary: KvStore[K, V, KBLOB, VBLOB, SEC_COLL],
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
        except Exception:  # noqa: BLE001
            logger.exception("mirror set to secondary failed")

    def delete(self, key: K) -> bool:
        deleted = self._primary.delete(key)
        try:
            self._secondary.delete(key)
        except Exception:  # noqa: BLE001
            logger.exception("mirror delete on secondary failed")
        return deleted

    def batch_get(self, keys: Sequence[K]) -> dict[K, V]:
        return self._primary.batch_get(keys)

    def batch_set(self, items: Mapping[K, V]) -> None:
        self._primary.batch_set(items)
        try:
            self._secondary.batch_set(items)
        except Exception:  # noqa: BLE001
            logger.exception("mirror batch_set to secondary failed")

    def batch_delete(self, keys: Sequence[K]) -> int:
        n = self._primary.batch_delete(keys)
        try:
            self._secondary.batch_delete(keys)
        except Exception:  # noqa: BLE001
            logger.exception("mirror batch_delete on secondary failed")
        return n

    def _scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        yield from self._primary._scan_entries(query)

    def _clone_with_scope(self, scope: Scope) -> KvStore[K, V, KBLOB, VBLOB, COLL]:
        return MirrorKvStore(
            self._primary._clone_with_scope(scope),
            self._secondary._clone_with_scope(scope),
        )
