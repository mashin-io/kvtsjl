"""KvStore ``map`` / ``imap`` / ``imap_keys`` — cores + fan-out mixins."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from datetime import datetime
from typing import cast

from kvtsjl.exceptions import KvStoreScanUnsupported
from kvtsjl.keymap_algebra.map import IMappedKeyMap, IMappedKeysKeyMap, MappedKeyMap
from kvtsjl.keymap_algebra.topology import MissingPage
from kvtsjl.scope import Scope
from kvtsjl.store.fanout import GcPassthroughMixin, ScopeFanoutMixin
from kvtsjl.store.logical import KvStore
from kvtsjl.store.readable import ReadableKvStore
from kvtsjl.store.schema.layout import KeyLayout, ScanQuery
from kvtsjl.store.schema.stat import RecordStat
from kvtsjl.store.schema.ttl import TtlPolicy


class MappedKvStore[K, V, U](
    MappedKeyMap[K, V, U],
    ScopeFanoutMixin,
    ReadableKvStore[K, U],
):
    """Read-only value ``map`` over a ``KvStore`` (inherits ``MappedKeyMap``)."""

    def __init__(self, underlying: KvStore[K, V], forward: Callable[[V], U]) -> None:
        super().__init__(underlying, forward)
        self.scope = underlying.scope
        self.batch_size = underlying.batch_size

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

    def scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, U | None]]:
        # Stream underlying values and transform locally (no re-fetch).
        for key, value in self._src().scan_entries(
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

    def _clone_fanout(self, scope: Scope) -> MappedKvStore[K, V, U]:
        return MappedKvStore(self._src().clone_with_scope(scope), self._forward)

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import callable_label, compose_repr

        return compose_repr(
            "MappedKvStore",
            src=self._src(),
            forward=callable_label(self._forward),
        )


class IMappedKvStore[K, V, U](
    IMappedKeyMap[K, V, U],
    ScopeFanoutMixin,
    GcPassthroughMixin[K],
    KvStore[K, U],
):
    """Invertible value ``imap`` over a ``KvStore``."""

    def __init__(
        self,
        underlying: KvStore[K, V],
        forward: Callable[[V], U],
        inverse: Callable[[U], V],
    ) -> None:
        super().__init__(underlying, forward, inverse)
        self.scope = underlying.scope
        self.batch_size = underlying.batch_size
        self._gc_src = underlying

    def _src(self) -> KvStore[K, V]:
        return cast(KvStore[K, V], self._underlying)

    @property
    def supports_write_clock(self) -> bool:
        return self._src().supports_write_clock

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

    def set(
        self,
        key: K,
        value: U,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        self._src().set(key, self._inverse(value), ttl=ttl, created_at=created_at)

    def child_stores(self) -> tuple[object, ...]:
        return (self._src(),)

    def set_if_absent(
        self,
        key: K,
        value: U,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> bool:
        return self._src().set_if_absent(
            key, self._inverse(value), ttl=ttl, created_at=created_at
        )

    def missing_pages(self, page_size: int) -> Iterator[MissingPage[K, U]]:
        return (page.map_values(self._forward) for page in self._src().missing_pages(page_size))

    def scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, U | None]]:
        for key, value in self._src().scan_entries(
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

    def _clone_fanout(self, scope: Scope) -> IMappedKvStore[K, V, U]:
        return IMappedKvStore(
            self._src().clone_with_scope(scope), self._forward, self._inverse
        )

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import callable_label, compose_repr

        return compose_repr(
            "IMappedKvStore",
            src=self._src(),
            forward=callable_label(self._forward),
            inverse=callable_label(self._inverse),
        )


class IMappedKeysKvStore[K, SK, V](
    IMappedKeysKeyMap[K, SK, V],
    ScopeFanoutMixin,
    KvStore[K, V],
):
    """Caller keys ``K`` over storage-keyed ``KvStore[SK, V]``."""

    def __init__(
        self,
        underlying: KvStore[SK, V],
        to_store: Callable[[K], SK],
        from_store: Callable[[SK], K] | None = None,
    ) -> None:
        super().__init__(underlying, to_store, from_store)
        self.scope = underlying.scope
        self.batch_size = underlying.batch_size

    def _src(self) -> KvStore[SK, V]:
        return cast(KvStore[SK, V], self._underlying)

    @property
    def supports_write_clock(self) -> bool:
        return self._src().supports_write_clock

    def key_layout(self) -> KeyLayout:
        return self._src().key_layout()

    def exists(self, key: K) -> bool:
        return self._src().exists(self._to_store(key))

    def batch_exists(self, keys: Sequence[K]) -> set[K]:
        sk_to_k = {self._to_store(key): key for key in keys}
        return {
            sk_to_k[sk]
            for sk in self._src().batch_exists(list(sk_to_k.keys()))
            if sk in sk_to_k
        }

    def stat(self, key: K) -> RecordStat | None:
        return self._src().stat(self._to_store(key))

    def batch_stat(self, keys: Sequence[K]) -> dict[K, RecordStat]:
        sk_to_k = {self._to_store(key): key for key in keys}
        return {
            sk_to_k[sk]: meta
            for sk, meta in self._src().batch_stat(list(sk_to_k.keys())).items()
            if sk in sk_to_k
        }

    def set(
        self,
        key: K,
        value: V,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        self._src().set(self._to_store(key), value, ttl=ttl, created_at=created_at)

    def child_stores(self) -> tuple[object, ...]:
        return (self._src(),)

    def set_if_absent(
        self,
        key: K,
        value: V,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> bool:
        return self._src().set_if_absent(
            self._to_store(key), value, ttl=ttl, created_at=created_at
        )

    def missing_pages(self, page_size: int) -> Iterator[MissingPage[K, V]]:
        """What the store behind lacks, under the caller's keys.

        Needs ``from_store``: without it a stored key cannot be shown, so this
        refuses as ``scan`` does.
        """
        from_store = self.require_from_store()
        return (page.map_keys(from_store) for page in self._src().missing_pages(page_size))

    def scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        from_store = self.require_from_store()
        if query.prefix is not None:
            raise KvStoreScanUnsupported(
                "prefix scan unsupported through imap_keys (prefix is in caller key space)"
            )
        src_query: ScanQuery[SK] = ScanQuery(
            prefix=None,
            include_values=query.include_values,
            page_size=query.page_size,
        )
        for sk, value in self._src().scan_entries(src_query):
            yield from_store(sk), value

    def _clone_fanout(self, scope: Scope) -> IMappedKeysKvStore[K, SK, V]:
        return IMappedKeysKvStore(
            self._src().clone_with_scope(scope), self._to_store, self._from_store
        )

    def expired_keys(self, *, now: datetime | None = None, limit: int) -> list[K]:
        from_store = self.require_from_store()
        return [
            from_store(sk) for sk in self._src().expired_keys(now=now, limit=limit)
        ]

    def gc_expired_keys(self, *, max_entries: int) -> list[K]:
        from_store = self.require_from_store()
        return [
            from_store(sk)
            for sk in self._src().gc_expired_keys(max_entries=max_entries)
        ]

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import callable_label, compose_repr

        fields: dict[str, object] = {
            "src": self._src(),
            "to_store": callable_label(self._to_store),
        }
        if self._from_store is not None:
            fields["from_store"] = callable_label(self._from_store)
        return compose_repr("IMappedKeysKvStore", **fields)
