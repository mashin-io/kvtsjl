"""Delegating logical store base."""

from __future__ import annotations

from kvtsjl.scope import Scope
from kvtsjl.store.logical import KvStore
from kvtsjl.wire.layout import KeyLayout


class DelegatingKvStore[K, V](KvStore[K, V]):
    """Base for logical wrappers that delegate to an underlying store (not ``KvBackend``)."""

    def __init__(self, underlying: KvStore[K, V]) -> None:
        self.scope = underlying.scope
        self.batch_size = underlying.batch_size
        self._underlying = underlying

    def key_layout(self) -> KeyLayout:
        return self._underlying.key_layout()

    def _clone_with_scope(self, scope: Scope) -> KvStore[K, V]:
        raise NotImplementedError
