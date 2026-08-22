"""KvStore algebra: map / imap / imap_keys with scan."""

from __future__ import annotations

from collections.abc import Callable, Iterator

from kvtsjl.exceptions import KvStoreScanUnsupported
from kvtsjl.keymap_algebra.map import IMappedKeyMap, IMappedKeysKeyMap, MappedKeyMap
from kvtsjl.keymap_algebra.util import raise_readonly
from kvtsjl.scope import Scope
from kvtsjl.store.logical import KvStore
from kvtsjl.store.schema.layout import KeyLayout, ScanQuery


class MappedKvStore[K, V, U](KvStore[K, U]):
    """Read-only value ``map`` over a ``KvStore``."""

    def __init__(self, underlying: KvStore[K, V], forward: Callable[[V], U]) -> None:
        self.scope = underlying.scope
        self.batch_size = underlying.batch_size
        self._src = underlying
        self._view = MappedKeyMap(underlying, forward)
        self._forward = forward

    def key_layout(self) -> KeyLayout:
        return self._src.key_layout()

    def get(self, key: K) -> U | None:
        return self._view.get(key)

    def set(self, key: K, value: U) -> None:
        raise_readonly("set")

    def delete(self, key: K) -> bool:
        raise_readonly("delete")
        return False  # pragma: no cover

    def _scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, U | None]]:
        for key, value in self._src._scan_entries(
            ScanQuery(
                prefix=query.prefix,
                include_values=True,
                page_size=query.page_size,
            )
        ):
            if value is None:
                yield key, None
            else:
                yield key, self._forward(value)

    def _clone_with_scope(self, scope: Scope) -> KvStore[K, U]:
        return MappedKvStore(self._src._clone_with_scope(scope), self._forward)


class IMappedKvStore[K, V, U](KvStore[K, U]):
    """Invertible value ``imap`` over a ``KvStore``."""

    def __init__(
        self,
        underlying: KvStore[K, V],
        forward: Callable[[V], U],
        inverse: Callable[[U], V],
    ) -> None:
        self.scope = underlying.scope
        self.batch_size = underlying.batch_size
        self._src = underlying
        self._view = IMappedKeyMap(underlying, forward, inverse)
        self._forward = forward
        self._inverse = inverse

    def key_layout(self) -> KeyLayout:
        return self._src.key_layout()

    def get(self, key: K) -> U | None:
        return self._view.get(key)

    def set(self, key: K, value: U) -> None:
        self._view.set(key, value)

    def delete(self, key: K) -> bool:
        return self._view.delete(key)

    def _scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, U | None]]:
        for key, value in self._src._scan_entries(
            ScanQuery(
                prefix=query.prefix,
                include_values=True,
                page_size=query.page_size,
            )
        ):
            if value is None:
                yield key, None
            else:
                yield key, self._forward(value)

    def _clone_with_scope(self, scope: Scope) -> KvStore[K, U]:
        return IMappedKvStore(
            self._src._clone_with_scope(scope), self._forward, self._inverse
        )


class IMappedKeysKvStore[K, SK, V](KvStore[K, V]):
    """Caller keys ``K`` over storage-keyed ``KvStore[SK, V]``."""

    def __init__(
        self,
        underlying: KvStore[SK, V],
        to_store: Callable[[K], SK],
        from_store: Callable[[SK], K] | None = None,
    ) -> None:
        self.scope = underlying.scope
        self.batch_size = underlying.batch_size
        self._src = underlying
        self._view = IMappedKeysKeyMap(underlying, to_store, from_store)
        self._to_store = to_store
        self._from_store = from_store

    def key_layout(self) -> KeyLayout:
        return self._src.key_layout()

    def get(self, key: K) -> V | None:
        return self._view.get(key)

    def set(self, key: K, value: V) -> None:
        self._view.set(key, value)

    def delete(self, key: K) -> bool:
        return self._view.delete(key)

    def _scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        from_store = self._view.require_from_store()
        if query.prefix is not None:
            raise KvStoreScanUnsupported(
                "prefix scan unsupported through imap_keys (prefix is in caller key space)"
            )
        src_query = ScanQuery(
            prefix=None,
            include_values=query.include_values,
            page_size=query.page_size,
        )
        for sk, value in self._src._scan_entries(src_query):
            yield from_store(sk), value

    def _clone_with_scope(self, scope: Scope) -> KvStore[K, V]:
        return IMappedKeysKvStore(
            self._src._clone_with_scope(scope), self._to_store, self._from_store
        )
