"""Minimal keyed map: get/set/delete plus default batch variants."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence


class KeyMap[K, T](ABC):
    """Keyed entries ``K → T`` with single- and batch-form mutations.

    ``KvStore`` uses ``T = V`` (documents). ``Index`` uses ``T = M`` (per-key
    metadata; ``set`` is metadata-only and typically errors if the key is not
    already indexed). Batch methods default to looping; override for native
    bulk APIs.
    """

    @abstractmethod
    def get(self, key: K) -> T | None:
        """Return the value for ``key``, or ``None`` if absent."""

    @abstractmethod
    def set(self, key: K, value: T) -> None:
        """Insert or replace the value for ``key``."""

    @abstractmethod
    def delete(self, key: K) -> bool:
        """Remove ``key`` if present. Return whether it was present."""

    def batch_get(self, keys: Sequence[K]) -> dict[K, T]:
        """Return ``{key: value}`` for keys that exist."""
        out: dict[K, T] = {}
        for key in keys:
            value = self.get(key)
            if value is not None:
                out[key] = value
        return out

    def batch_set(self, items: Mapping[K, T]) -> None:
        """Insert or replace many entries."""
        for key, value in items.items():
            self.set(key, value)

    def batch_delete(self, keys: Sequence[K]) -> int:
        """Remove many keys. Return how many were present."""
        n = 0
        for key in keys:
            if self.delete(key):
                n += 1
        return n
