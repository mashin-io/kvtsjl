"""Delegating logical store base."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from kvtsjl.scope import Scope
from kvtsjl.store.logical import KvStore
from kvtsjl.store.schema.layout import KeyLayout
from kvtsjl.store.schema.stat import RecordStat


class DelegatingKvStore[K, V](KvStore[K, V]):
    """Base for logical wrappers that delegate to an underlying store (not ``KvBackend``)."""

    def __init__(self, underlying: KvStore[K, V]) -> None:
        self.scope = underlying.scope
        self.batch_size = underlying.batch_size
        self._underlying = underlying

    @property
    def supports_write_clock(self) -> bool:
        return self._underlying.supports_write_clock

    def key_layout(self) -> KeyLayout:
        return self._underlying.key_layout()

    def exists(self, key: K) -> bool:
        return self._underlying.exists(key)

    def batch_exists(self, keys: Sequence[K]) -> set[K]:
        return self._underlying.batch_exists(keys)

    def stat(self, key: K) -> RecordStat | None:
        return self._underlying.stat(key)

    def batch_stat(self, keys: Sequence[K]) -> dict[K, RecordStat]:
        return self._underlying.batch_stat(keys)

    def clone_with_scope(self, scope: Scope) -> KvStore[K, V]:
        raise NotImplementedError

    @property
    def writes_are_deferred(self) -> bool:
        return self._underlying.writes_are_deferred

    def expired_keys(self, *, now: datetime | None = None, limit: int) -> list[K]:
        return self._underlying.expired_keys(now=now, limit=limit)

    def gc_expired_keys(self, *, max_entries: int) -> list[K]:
        return self._underlying.gc_expired_keys(max_entries=max_entries)

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import compose_repr

        return compose_repr(type(self).__name__, underlying=self._underlying)
