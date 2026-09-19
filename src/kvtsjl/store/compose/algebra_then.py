"""KvStore ``then`` / ``then_with`` (read-only FK lookup)."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence

from kvtsjl.keymap_algebra.then import ThenKeyMap, ThenWithKeyMap
from kvtsjl.scope import Scope
from kvtsjl.store.logical import KvStore
from kvtsjl.store.readable import ReadableKvStore
from kvtsjl.store.schema.layout import KeyLayout, ScanQuery
from kvtsjl.store.schema.stat import RecordStat


class ThenKvStore[K, J, V](ReadableKvStore[K, V]):
    def __init__(self, left: KvStore[K, J], right: KvStore[J, V]) -> None:
        self.scope = left.scope
        self.batch_size = left.batch_size
        self._left = left
        self._right = right
        self._view = ThenKeyMap(left, right)

    def key_layout(self) -> KeyLayout:
        return self._left.key_layout()

    def get(self, key: K) -> V | None:
        return self._view.get(key)

    def exists(self, key: K) -> bool:
        mid = self._left.get(key)
        if mid is None:
            return False
        return self._right.exists(mid)

    def batch_exists(self, keys: Sequence[K]) -> set[K]:
        return {key for key in keys if self.exists(key)}

    def stat(self, key: K) -> RecordStat | None:
        mid = self._left.get(key)
        if mid is None:
            return None
        return self._right.stat(mid)

    def batch_stat(self, keys: Sequence[K]) -> dict[K, RecordStat]:
        out: dict[K, RecordStat] = {}
        for key in keys:
            meta = self.stat(key)
            if meta is not None:
                out[key] = meta
        return out



    def scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        for key, _ in self._left.scan_entries(
            ScanQuery(
                prefix=query.prefix,
                include_values=False,
                page_size=query.page_size,
            )
        ):
            if query.include_values:
                yield key, self.get(key)
            else:
                yield key, None

    def clone_with_scope(self, scope: Scope) -> ReadableKvStore[K, V]:
        return ThenKvStore(
            self._left.clone_with_scope(scope),
            self._right.clone_with_scope(scope),
        )



    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import compose_repr

        return compose_repr("ThenKvStore", left=self._left, right=self._right)


class ThenWithKvStore[K, T, J, V](ReadableKvStore[K, V]):
    def __init__(
        self,
        left: KvStore[K, T],
        key_of: Callable[[K, T], J],
        right: KvStore[J, V],
    ) -> None:
        self.scope = left.scope
        self.batch_size = left.batch_size
        self._left = left
        self._key_of = key_of
        self._right = right
        self._view = ThenWithKeyMap(left, key_of, right)

    def key_layout(self) -> KeyLayout:
        return self._left.key_layout()

    def get(self, key: K) -> V | None:
        return self._view.get(key)

    def exists(self, key: K) -> bool:
        mid = self._left.get(key)
        if mid is None:
            return False
        return self._right.exists(self._key_of(key, mid))

    def batch_exists(self, keys: Sequence[K]) -> set[K]:
        return {key for key in keys if self.exists(key)}

    def stat(self, key: K) -> RecordStat | None:
        mid = self._left.get(key)
        if mid is None:
            return None
        return self._right.stat(self._key_of(key, mid))

    def batch_stat(self, keys: Sequence[K]) -> dict[K, RecordStat]:
        out: dict[K, RecordStat] = {}
        for key in keys:
            meta = self.stat(key)
            if meta is not None:
                out[key] = meta
        return out



    def scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        for key, _ in self._left.scan_entries(
            ScanQuery(
                prefix=query.prefix,
                include_values=False,
                page_size=query.page_size,
            )
        ):
            if query.include_values:
                yield key, self.get(key)
            else:
                yield key, None

    def clone_with_scope(self, scope: Scope) -> ReadableKvStore[K, V]:
        return ThenWithKvStore(
            self._left.clone_with_scope(scope),
            self._key_of,
            self._right.clone_with_scope(scope),
        )



    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import callable_label, compose_repr

        return compose_repr(
            "ThenWithKvStore",
            left=self._left,
            key_of=callable_label(self._key_of),
            right=self._right,
        )
