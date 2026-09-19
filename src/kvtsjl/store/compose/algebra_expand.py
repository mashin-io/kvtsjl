"""KvStore ``expand`` / ``expand_map``."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from datetime import datetime

from kvtsjl.keymap import KeyMap
from kvtsjl.keymap_algebra.expand import ExpandKeyMap, ExpandMapKeyMap
from kvtsjl.keymap_algebra.util import raise_readonly
from kvtsjl.scope import Scope
from kvtsjl.store.logical import KvStore
from kvtsjl.store.schema.layout import KeyLayout, ScanQuery
from kvtsjl.store.schema.stat import RecordStat
from kvtsjl.store.schema.ttl import TtlPolicy


class ExpandKvStore[K, V, SK, SV](KvStore[K, KeyMap[SK, SV]]):
    def __init__(
        self,
        underlying: KvStore[K, V],
        expander: Callable[
            [K, V], KeyMap[SK, SV] | Mapping[SK, SV] | Sequence[tuple[SK, SV]]
        ],
    ) -> None:
        self.scope = underlying.scope
        self.batch_size = underlying.batch_size
        self._src = underlying
        self._expander = expander
        self._view = ExpandKeyMap(underlying, expander)

    def key_layout(self) -> KeyLayout:
        return self._src.key_layout()

    def get(self, key: K) -> KeyMap[SK, SV] | None:
        return self._view.get(key)

    def exists(self, key: K) -> bool:
        return self._src.exists(key)

    def batch_exists(self, keys: Sequence[K]) -> set[K]:
        return self._src.batch_exists(keys)

    def stat(self, key: K) -> RecordStat | None:
        return self._src.stat(key)

    def batch_stat(self, keys: Sequence[K]) -> dict[K, RecordStat]:
        return self._src.batch_stat(keys)

    def set(
        self,
        key: K,
        value: KeyMap[SK, SV],
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        raise_readonly("set")

    def delete(self, key: K) -> bool:
        raise_readonly("delete")
        return False  # pragma: no cover

    def scan_entries(
        self, query: ScanQuery[K]
    ) -> Iterator[tuple[K, KeyMap[SK, SV] | None]]:
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

    def clone_with_scope(self, scope: Scope) -> KvStore[K, KeyMap[SK, SV]]:
        return ExpandKvStore(self._src.clone_with_scope(scope), self._expander)

    def gc_expired_keys(self, *, max_entries: int) -> list[K]:
        return self._src.gc_expired_keys(max_entries=max_entries)

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import callable_label, compose_repr

        return compose_repr(
            "ExpandKvStore",
            src=self._src,
            expander=callable_label(self._expander),
        )


class ExpandMapKvStore[K, V, SK, SV, U](KvStore[K, U]):
    def __init__(
        self,
        underlying: KvStore[K, V],
        expander: Callable[
            [K, V], KeyMap[SK, SV] | Mapping[SK, SV] | Sequence[tuple[SK, SV]]
        ],
        aggregate: Callable[[K, V, KeyMap[SK, SV]], U],
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

    def set(
        self,
        key: K,
        value: U,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        raise_readonly("set")

    def delete(self, key: K) -> bool:
        raise_readonly("delete")
        return False  # pragma: no cover

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

    def clone_with_scope(self, scope: Scope) -> KvStore[K, U]:
        return ExpandMapKvStore(
            self._src.clone_with_scope(scope),
            self._expander,
            self._aggregate,
        )

    def gc_expired_keys(self, *, max_entries: int) -> list[K]:
        return self._src.gc_expired_keys(max_entries=max_entries)

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import callable_label, compose_repr

        return compose_repr(
            "ExpandMapKvStore",
            src=self._src,
            expander=callable_label(self._expander),
            aggregate=callable_label(self._aggregate),
        )
