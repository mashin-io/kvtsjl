"""KvStore ``then`` / ``then_with`` — cores + fan-out mixins."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import cast

from kvtsjl.keymap_algebra.then import ThenKeyMap, ThenWithKeyMap
from kvtsjl.scope import Scope
from kvtsjl.store.fanout import ScanPassthroughMixin, ScopeFanoutMixin
from kvtsjl.store.logical import KvStore
from kvtsjl.store.readable import ReadableKvStore
from kvtsjl.store.schema.layout import KeyLayout
from kvtsjl.store.schema.stat import RecordStat


class ThenKvStore[K, J, V](
    ThenKeyMap[K, J, V],
    ScanPassthroughMixin[K, V],
    ScopeFanoutMixin,
    ReadableKvStore[K, V],
):
    def __init__(self, left: KvStore[K, J], right: KvStore[J, V]) -> None:
        super().__init__(left, right)
        self.scope = left.scope
        self.batch_size = left.batch_size
        self._scan_src = left

    def _left_store(self) -> KvStore[K, J]:
        return cast(KvStore[K, J], self._left)

    def _right_store(self) -> KvStore[J, V]:
        return cast(KvStore[J, V], self._right)

    def key_layout(self) -> KeyLayout:
        return self._left_store().key_layout()

    def exists(self, key: K) -> bool:
        mid = self._left.get(key)
        if mid is None:
            return False
        return self._right_store().exists(mid)

    def batch_exists(self, keys: Sequence[K]) -> set[K]:
        return {key for key in keys if self.exists(key)}

    def stat(self, key: K) -> RecordStat | None:
        mid = self._left.get(key)
        if mid is None:
            return None
        return self._right_store().stat(mid)

    def batch_stat(self, keys: Sequence[K]) -> dict[K, RecordStat]:
        out: dict[K, RecordStat] = {}
        for key in keys:
            meta = self.stat(key)
            if meta is not None:
                out[key] = meta
        return out

    def _clone_fanout(self, scope: Scope) -> ThenKvStore[K, J, V]:
        return ThenKvStore(
            self._left_store().clone_with_scope(scope),
            self._right_store().clone_with_scope(scope),
        )

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import compose_repr

        return compose_repr(
            "ThenKvStore", left=self._left_store(), right=self._right_store()
        )


class ThenWithKvStore[K, T, J, V](
    ThenWithKeyMap[K, T, J, V],
    ScanPassthroughMixin[K, V],
    ScopeFanoutMixin,
    ReadableKvStore[K, V],
):
    def __init__(
        self,
        left: KvStore[K, T],
        key_of: Callable[[K, T], J],
        right: KvStore[J, V],
    ) -> None:
        super().__init__(left, key_of, right)
        self.scope = left.scope
        self.batch_size = left.batch_size
        self._scan_src = left

    def _left_store(self) -> KvStore[K, T]:
        return cast(KvStore[K, T], self._left)

    def _right_store(self) -> KvStore[J, V]:
        return cast(KvStore[J, V], self._right)

    def key_layout(self) -> KeyLayout:
        return self._left_store().key_layout()

    def exists(self, key: K) -> bool:
        mid = self._left.get(key)
        if mid is None:
            return False
        return self._right_store().exists(self._key_of(key, mid))

    def batch_exists(self, keys: Sequence[K]) -> set[K]:
        return {key for key in keys if self.exists(key)}

    def stat(self, key: K) -> RecordStat | None:
        mid = self._left.get(key)
        if mid is None:
            return None
        return self._right_store().stat(self._key_of(key, mid))

    def batch_stat(self, keys: Sequence[K]) -> dict[K, RecordStat]:
        out: dict[K, RecordStat] = {}
        for key in keys:
            meta = self.stat(key)
            if meta is not None:
                out[key] = meta
        return out

    def _clone_fanout(self, scope: Scope) -> ThenWithKvStore[K, T, J, V]:
        return ThenWithKvStore(
            self._left_store().clone_with_scope(scope),
            self._key_of,
            self._right_store().clone_with_scope(scope),
        )

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import callable_label, compose_repr

        return compose_repr(
            "ThenWithKvStore",
            left=self._left_store(),
            key_of=callable_label(self._key_of),
            right=self._right_store(),
        )
