"""``then`` / ``then_with`` — compose partial maps (``other[self[k]]``)."""

from __future__ import annotations

from collections.abc import Callable

from kvtsjl.keymap import KeyMap
from kvtsjl.keymap_algebra.util import raise_readonly


class ThenKeyMap[K, J, V](KeyMap[K, V]):
    """``get(k) = other.get(self.get(k))``; mutations unsupported."""

    def __init__(self, left: KeyMap[K, J], right: KeyMap[J, V]) -> None:
        self._left = left
        self._right = right

    def get(self, key: K) -> V | None:
        join = self._left.get(key)
        if join is None:
            return None
        return self._right.get(join)

    def set(self, key: K, value: V) -> None:
        raise_readonly("set")

    def delete(self, key: K) -> bool:
        raise_readonly("delete")
        return False  # pragma: no cover


class ThenWithKeyMap[K, T, J, V](KeyMap[K, V]):
    """``get(k) = other.get(f(k, self.get(k)))``; mutations unsupported."""

    def __init__(
        self,
        left: KeyMap[K, T],
        key_of: Callable[[K, T], J],
        right: KeyMap[J, V],
    ) -> None:
        self._left = left
        self._key_of = key_of
        self._right = right

    def get(self, key: K) -> V | None:
        value = self._left.get(key)
        if value is None:
            return None
        return self._right.get(self._key_of(key, value))

    def set(self, key: K, value: V) -> None:
        raise_readonly("set")

    def delete(self, key: K) -> bool:
        raise_readonly("delete")
        return False  # pragma: no cover
