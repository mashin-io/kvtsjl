"""In-memory ``KeyMap`` for nested expand collections and tests."""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from kvtsjl.keymap import KeyMap


class DictKeyMap[K, T](KeyMap[K, T]):
    """Process-local mutable ``KeyMap`` backed by a ``dict``."""

    def __init__(self, items: Mapping[K, T] | None = None) -> None:
        self._data: dict[K, T] = dict(items) if items is not None else {}

    @classmethod
    def from_pairs(cls, pairs: Iterable[tuple[K, T]]) -> DictKeyMap[K, T]:
        return cls(dict(pairs))

    def get(self, key: K) -> T | None:
        return self._data.get(key)

    def set(self, key: K, value: T) -> None:
        self._data[key] = value

    def delete(self, key: K) -> bool:
        if key not in self._data:
            return False
        del self._data[key]
        return True

    def keys(self) -> list[K]:
        return list(self._data.keys())

    def items(self) -> list[tuple[K, T]]:
        return list(self._data.items())

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return f"DictKeyMap({self._data!r})"
