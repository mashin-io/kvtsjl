"""Read-only document store ABC (scope + scan, no writes)."""

from __future__ import annotations

from abc import ABC

from kvtsjl.keymap import ReadableKeyMap
from kvtsjl.store.capabilities import ScannableMixin, ScopedMixin


class ReadableKvStore[K, V](ReadableKeyMap[K, V], ScopedMixin, ScannableMixin[K, V], ABC):
    """Logical read-only store: ``ReadableKeyMap`` + scope + scan.

    Returned by ``readonly()``, ``map``, ``then``, and ``expand``. Mutations are
    absent from the type; ``ReadonlyKvStore`` still raises ``KvStoreReadOnlyError``
    at runtime for accidental untyped writes.
    """
