"""KvStore ``expand`` / ``expand_map``."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence

from kvtsjl.keymap import ReadableKeyMap
from kvtsjl.keymap_algebra.expand import ExpandKeyMap, ExpandMapKeyMap
from kvtsjl.scope import Scope
from kvtsjl.store.logical import KvStore
from kvtsjl.store.readable import ReadableKvStore
from kvtsjl.store.schema.layout import KeyLayout, ScanQuery
from kvtsjl.store.schema.stat import RecordStat


class ExpandKvStore[K, V, SK, SV](ReadableKvStore[K, ReadableKeyMap[SK, SV]]):
    def __init__(
        self,
        underlying: KvStore[K, V],
        expander: Callable[
            [K, V],
            ReadableKeyMap[SK, SV] | Mapping[SK, SV] | Sequence[tuple[SK, SV]],
        ],
    ) -> None:
        self.scope = underlying.scope
        self.batch_size = underlying.batch_size
        self._src = underlying
        self._expander = expander
        self._view = ExpandKeyMap(underlying, expander)

    def key_layout(self) -> KeyLayout:
        return self._src.key_layout()

    def get(self, key: K) -> ReadableKeyMap[SK, SV] | None:
        return self._view.get(key)

    def exists(self, key: K) -> bool:
        return self._src.exists(key)

    def batch_exists(self, keys: Sequence[K]) -> set[K]:
        return self._src.batch_exists(keys)

    def stat(self, key: K) -> RecordStat | None:
        return self._src.stat(key)

    def batch_stat(self, keys: Sequence[K]) -> dict[K, RecordStat]:
        return self._src.batch_stat(keys)

    def scan_entries(
        self, query: ScanQuery[K]
    ) -> Iterator[tuple[K, ReadableKeyMap[SK, SV] | None]]:
        for key, _ in self._src.scan_entries(
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

    def clone_with_scope(self, scope: Scope) -> ReadableKvStore[K, ReadableKeyMap[SK, SV]]:
        return ExpandKvStore(self._src.clone_with_scope(scope), self._expander)

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import callable_label, compose_repr

        return compose_repr(
            "ExpandKvStore",
            src=self._src,
            expander=callable_label(self._expander),
        )


class ExpandMapKvStore[K, V, SK, SV, U](ReadableKvStore[K, U]):
    def __init__(
        self,
        underlying: KvStore[K, V],
        expander: Callable[
            [K, V],
            ReadableKeyMap[SK, SV] | Mapping[SK, SV] | Sequence[tuple[SK, SV]],
        ],
        aggregate: Callable[[K, V, ReadableKeyMap[SK, SV]], U],
    ) -> None:
        self.scope = underlying.scope
        self.batch_size = underlying.batch_size
        self._src = underlying
        self._expander = expander
        self._aggregate = aggregate
        self._view = ExpandMapKeyMap(underlying, expander, aggregate)

    def key_layout(self) -> KeyLayout:
        return self._src.key_layout()

    def get(self, key: K) -> U | None:
        return self._view.get(key)

    def exists(self, key: K) -> bool:
        return self._src.exists(key)

    def batch_exists(self, keys: Sequence[K]) -> set[K]:
        return self._src.batch_exists(keys)

    def stat(self, key: K) -> RecordStat | None:
        return self._src.stat(key)

    def batch_stat(self, keys: Sequence[K]) -> dict[K, RecordStat]:
        return self._src.batch_stat(keys)

    def scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, U | None]]:
        for key, _ in self._src.scan_entries(
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

    def clone_with_scope(self, scope: Scope) -> ReadableKvStore[K, U]:
        return ExpandMapKvStore(
            self._src.clone_with_scope(scope),
            self._expander,
            self._aggregate,
        )

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import callable_label, compose_repr

        return compose_repr(
            "ExpandMapKvStore",
            src=self._src,
            expander=callable_label(self._expander),
            aggregate=callable_label(self._aggregate),
        )
