"""KvStore ``then`` / ``then_with`` (read-only FK lookup)."""

from __future__ import annotations

from collections.abc import Callable, Iterator

from kvtsjl.keymap_algebra.then import ThenKeyMap, ThenWithKeyMap
from kvtsjl.keymap_algebra.util import raise_readonly
from kvtsjl.scope import Scope
from kvtsjl.store.logical import KvStore
from kvtsjl.store.schema.layout import KeyLayout, ScanQuery


class ThenKvStore[K, J, V](KvStore[K, V]):
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

    def set(self, key: K, value: V) -> None:
        raise_readonly("set")

    def delete(self, key: K) -> bool:
        raise_readonly("delete")
        return False  # pragma: no cover

    def _scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        for key, _ in self._left._scan_entries(
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

    def _clone_with_scope(self, scope: Scope) -> KvStore[K, V]:
        return ThenKvStore(
            self._left._clone_with_scope(scope),
            self._right._clone_with_scope(scope),
        )

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import compose_repr

        return compose_repr("ThenKvStore", left=self._left, right=self._right)


class ThenWithKvStore[K, T, J, V](KvStore[K, V]):
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

    def set(self, key: K, value: V) -> None:
        raise_readonly("set")

    def delete(self, key: K) -> bool:
        raise_readonly("delete")
        return False  # pragma: no cover

    def _scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        for key, _ in self._left._scan_entries(
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

    def _clone_with_scope(self, scope: Scope) -> KvStore[K, V]:
        return ThenWithKvStore(
            self._left._clone_with_scope(scope),
            self._key_of,
            self._right._clone_with_scope(scope),
        )

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import callable_label, compose_repr

        return compose_repr(
            "ThenWithKvStore",
            left=self._left,
            key_of=callable_label(self._key_of),
            right=self._right,
        )
