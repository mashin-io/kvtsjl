"""``then`` / ``then_with`` — compose partial maps (``other[self[k]]``)."""

from __future__ import annotations

from collections.abc import Callable

from kvtsjl.keymap import ReadableKeyMap


class ThenKeyMap[K, J, V](ReadableKeyMap[K, V]):
    """``get(k) = other.get(self.get(k))``."""

    def __init__(
        self, left: ReadableKeyMap[K, J], right: ReadableKeyMap[J, V]
    ) -> None:
        self._left = left
        self._right = right

    def get(self, key: K) -> V | None:
        join = self._left.get(key)
        if join is None:
            return None
        return self._right.get(join)


class ThenWithKeyMap[K, T, J, V](ReadableKeyMap[K, V]):
    """``get(k) = other.get(f(k, self.get(k)))``."""

    def __init__(
        self,
        left: ReadableKeyMap[K, T],
        key_of: Callable[[K, T], J],
        right: ReadableKeyMap[J, V],
    ) -> None:
        self._left = left
        self._key_of = key_of
        self._right = right

    def get(self, key: K) -> V | None:
        value = self._left.get(key)
        if value is None:
            return None
        return self._right.get(self._key_of(key, value))
