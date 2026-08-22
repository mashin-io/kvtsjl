"""``expand`` / ``expand_map`` — nested collections under the same outer key."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence

from kvtsjl.keymap import KeyMap
from kvtsjl.keymap_algebra.util import as_keymap, raise_readonly


class ExpandKeyMap[K, T, SK, SV](KeyMap[K, KeyMap[SK, SV]]):
    """``get(k)`` returns expander collection when left hits; mutations unsupported."""

    def __init__(
        self,
        underlying: KeyMap[K, T],
        expander: Callable[
            [K, T], KeyMap[SK, SV] | Mapping[SK, SV] | Sequence[tuple[SK, SV]]
        ],
    ) -> None:
        self._underlying = underlying
        self._expander = expander

    def get(self, key: K) -> KeyMap[SK, SV] | None:
        value = self._underlying.get(key)
        if value is None:
            return None
        return as_keymap(self._expander(key, value))

    def set(self, key: K, value: KeyMap[SK, SV]) -> None:
        raise_readonly("set")

    def delete(self, key: K) -> bool:
        raise_readonly("delete")
        return False  # pragma: no cover


class ExpandMapKeyMap[K, T, SK, SV, U](KeyMap[K, U]):
    """Expand then fold: ``agg(k, v, col)`` when left hits."""

    def __init__(
        self,
        underlying: KeyMap[K, T],
        expander: Callable[
            [K, T], KeyMap[SK, SV] | Mapping[SK, SV] | Sequence[tuple[SK, SV]]
        ],
        aggregate: Callable[[K, T, KeyMap[SK, SV]], U],
    ) -> None:
        self._underlying = underlying
        self._expander = expander
        self._aggregate = aggregate

    def get(self, key: K) -> U | None:
        value = self._underlying.get(key)
        if value is None:
            return None
        col = as_keymap(self._expander(key, value))
        return self._aggregate(key, value, col)

    def set(self, key: K, value: U) -> None:
        raise_readonly("set")

    def delete(self, key: K) -> bool:
        raise_readonly("delete")
        return False  # pragma: no cover
