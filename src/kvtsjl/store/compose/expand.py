"""KvStore ``expand`` / ``expand_map`` — cores + fan-out mixins."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import cast

from kvtsjl.keymap import ReadableKeyMap
from kvtsjl.keymap_algebra.expand import ExpandKeyMap, ExpandMapKeyMap
from kvtsjl.scope import Scope
from kvtsjl.store.fanout import ScanPassthroughMixin, ScopeFanoutMixin
from kvtsjl.store.logical import KvStore
from kvtsjl.store.readable import ReadableKvStore
from kvtsjl.store.schema.layout import KeyLayout
from kvtsjl.store.schema.stat import RecordStat


class ExpandKvStore[K, V, SK, SV](
    ExpandKeyMap[K, V, SK, SV],
    ScanPassthroughMixin[K, ReadableKeyMap[SK, SV]],
    ScopeFanoutMixin,
    ReadableKvStore[K, ReadableKeyMap[SK, SV]],
):
    def __init__(
        self,
        underlying: KvStore[K, V],
        expander: Callable[
            [K, V],
            ReadableKeyMap[SK, SV] | Mapping[SK, SV] | Sequence[tuple[SK, SV]],
        ],
    ) -> None:
        super().__init__(underlying, expander)
        self.scope = underlying.scope
        self.batch_size = underlying.batch_size
        self._scan_src = underlying

    def _src(self) -> KvStore[K, V]:
        return cast(KvStore[K, V], self._underlying)

    def key_layout(self) -> KeyLayout:
        return self._src().key_layout()

    def exists(self, key: K) -> bool:
        return self._src().exists(key)

    def batch_exists(self, keys: Sequence[K]) -> set[K]:
        return self._src().batch_exists(keys)

    def stat(self, key: K) -> RecordStat | None:
        return self._src().stat(key)

    def batch_stat(self, keys: Sequence[K]) -> dict[K, RecordStat]:
        return self._src().batch_stat(keys)

    def _clone_fanout(self, scope: Scope) -> ExpandKvStore[K, V, SK, SV]:
        return ExpandKvStore(self._src().clone_with_scope(scope), self._expander)

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import callable_label, compose_repr

        return compose_repr(
            "ExpandKvStore",
            src=self._src(),
            expander=callable_label(self._expander),
        )


class ExpandMapKvStore[K, V, SK, SV, U](
    ExpandMapKeyMap[K, V, SK, SV, U],
    ScanPassthroughMixin[K, U],
    ScopeFanoutMixin,
    ReadableKvStore[K, U],
):
    def __init__(
        self,
        underlying: KvStore[K, V],
        expander: Callable[
            [K, V],
            ReadableKeyMap[SK, SV] | Mapping[SK, SV] | Sequence[tuple[SK, SV]],
        ],
        aggregate: Callable[[K, V, ReadableKeyMap[SK, SV]], U],
    ) -> None:
        super().__init__(underlying, expander, aggregate)
        self.scope = underlying.scope
        self.batch_size = underlying.batch_size
        self._scan_src = underlying

    def _src(self) -> KvStore[K, V]:
        return cast(KvStore[K, V], self._underlying)

    def key_layout(self) -> KeyLayout:
        return self._src().key_layout()

    def exists(self, key: K) -> bool:
        return self._src().exists(key)

    def batch_exists(self, keys: Sequence[K]) -> set[K]:
        return self._src().batch_exists(keys)

    def stat(self, key: K) -> RecordStat | None:
        return self._src().stat(key)

    def batch_stat(self, keys: Sequence[K]) -> dict[K, RecordStat]:
        return self._src().batch_stat(keys)

    def _clone_fanout(self, scope: Scope) -> ExpandMapKvStore[K, V, SK, SV, U]:
        return ExpandMapKvStore(
            self._src().clone_with_scope(scope),
            self._expander,
            self._aggregate,
        )

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import callable_label, compose_repr

        return compose_repr(
            "ExpandMapKvStore",
            src=self._src(),
            expander=callable_label(self._expander),
            aggregate=callable_label(self._aggregate),
        )
