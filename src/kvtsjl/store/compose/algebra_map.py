"""KvStore algebra: map / imap / imap_keys with scan."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from datetime import datetime

from kvtsjl.exceptions import KvStoreScanUnsupported
from kvtsjl.keymap_algebra.map import IMappedKeyMap, IMappedKeysKeyMap, MappedKeyMap
from kvtsjl.keymap_algebra.util import raise_readonly
from kvtsjl.scope import Scope
from kvtsjl.store.logical import KvStore
from kvtsjl.store.schema.layout import KeyLayout, ScanQuery
from kvtsjl.store.schema.stat import RecordStat
from kvtsjl.store.schema.ttl import TtlPolicy


class MappedKvStore[K, V, U](KvStore[K, U]):
    """Read-only value ``map`` over a ``KvStore``."""

    def __init__(self, underlying: KvStore[K, V], forward: Callable[[V], U]) -> None:
        self.scope = underlying.scope
        self.batch_size = underlying.batch_size
        self._src = underlying
        self._view = MappedKeyMap(underlying, forward)
        self._forward = forward

    @property
    def supports_write_clock(self) -> bool:
        return self._src.supports_write_clock

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
        for key, value in self._src.scan_entries(
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

    def clone_with_scope(self, scope: Scope) -> KvStore[K, U]:
        return MappedKvStore(self._src.clone_with_scope(scope), self._forward)

    def gc_expired_keys(self, *, max_entries: int) -> list[K]:
        return self._src.gc_expired_keys(max_entries=max_entries)

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import callable_label, compose_repr

        return compose_repr(
            "MappedKvStore",
            src=self._src,
            forward=callable_label(self._forward),
        )


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

    @property
    def supports_write_clock(self) -> bool:
        return self._src.supports_write_clock

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
        self._src.set(key, self._inverse(value), ttl=ttl, created_at=created_at)

    def delete(self, key: K) -> bool:
        return self._view.delete(key)

    def scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, U | None]]:
        for key, value in self._src.scan_entries(
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

    def clone_with_scope(self, scope: Scope) -> KvStore[K, U]:
        return IMappedKvStore(
            self._src.clone_with_scope(scope), self._forward, self._inverse
        )

    def gc_expired_keys(self, *, max_entries: int) -> list[K]:
        return self._src.gc_expired_keys(max_entries=max_entries)

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import callable_label, compose_repr

        return compose_repr(
            "IMappedKvStore",
            src=self._src,
            forward=callable_label(self._forward),
            inverse=callable_label(self._inverse),
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

    @property
    def supports_write_clock(self) -> bool:
        return self._src.supports_write_clock

    def key_layout(self) -> KeyLayout:
        return self._src.key_layout()

    def get(self, key: K) -> V | None:
        return self._view.get(key)

    def exists(self, key: K) -> bool:
        return self._src.exists(self._to_store(key))

    def batch_exists(self, keys: Sequence[K]) -> set[K]:
        sk_to_k = {self._to_store(key): key for key in keys}
        return {
            sk_to_k[sk]
            for sk in self._src.batch_exists(list(sk_to_k.keys()))
            if sk in sk_to_k
        }

    def stat(self, key: K) -> RecordStat | None:
        return self._src.stat(self._to_store(key))

    def batch_stat(self, keys: Sequence[K]) -> dict[K, RecordStat]:
        sk_to_k = {self._to_store(key): key for key in keys}
        return {
            sk_to_k[sk]: meta
            for sk, meta in self._src.batch_stat(list(sk_to_k.keys())).items()
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
        self._src.set(self._to_store(key), value, ttl=ttl, created_at=created_at)

    def delete(self, key: K) -> bool:
        return self._view.delete(key)

    def scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
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
        for sk, value in self._src.scan_entries(src_query):
            yield from_store(sk), value

    def clone_with_scope(self, scope: Scope) -> KvStore[K, V]:
        return IMappedKeysKvStore(
            self._src.clone_with_scope(scope), self._to_store, self._from_store
        )

    def gc_expired_keys(self, *, max_entries: int) -> list[K]:
        from_store = self._view.require_from_store()
        return [
            from_store(sk)
            for sk in self._src.gc_expired_keys(max_entries=max_entries)
        ]

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import callable_label, compose_repr

        fields: dict[str, object] = {
            "src": self._src,
            "to_store": callable_label(self._to_store),
        }
        if self._from_store is not None:
            fields["from_store"] = callable_label(self._from_store)
        return compose_repr("IMappedKeysKvStore", **fields)
