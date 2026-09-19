"""``expand`` / ``expand_map`` — nested collections under the same outer key."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence

from kvtsjl.keymap import ReadableKeyMap
from kvtsjl.keymap_algebra.util import as_keymap


class ExpandKeyMap[K, T, SK, SV](ReadableKeyMap[K, ReadableKeyMap[SK, SV]]):
    """``get(k)`` returns expander collection when left hits."""

    def __init__(
        self,
        underlying: ReadableKeyMap[K, T],
        expander: Callable[
            [K, T],
            ReadableKeyMap[SK, SV] | Mapping[SK, SV] | Sequence[tuple[SK, SV]],
        ],
    ) -> None:
        self._underlying = underlying
        self._expander = expander

    def get(self, key: K) -> ReadableKeyMap[SK, SV] | None:
        value = self._underlying.get(key)
        if value is None:
            return None
        return as_keymap(self._expander(key, value))

    def batch_get(self, keys: Sequence[K]) -> dict[K, ReadableKeyMap[SK, SV]]:
        """One underlying ``batch_get``; expander is pure (no I/O)."""
        if not keys:
            return {}
        unique = list(dict.fromkeys(keys))
        found = self._underlying.batch_get(unique)
        return {k: as_keymap(self._expander(k, v)) for k, v in found.items()}


class ExpandMapKeyMap[K, T, SK, SV, U](ReadableKeyMap[K, U]):
    """Expand then fold: ``agg(k, v, col)`` when left hits."""

    def __init__(
        self,
        underlying: ReadableKeyMap[K, T],
        expander: Callable[
            [K, T],
            ReadableKeyMap[SK, SV] | Mapping[SK, SV] | Sequence[tuple[SK, SV]],
        ],
        aggregate: Callable[[K, T, ReadableKeyMap[SK, SV]], U],
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

    def batch_get(self, keys: Sequence[K]) -> dict[K, U]:
        """One underlying ``batch_get``; expander + aggregate are pure (no I/O)."""
        if not keys:
            return {}
        unique = list(dict.fromkeys(keys))
        found = self._underlying.batch_get(unique)
        out: dict[K, U] = {}
        for k, v in found.items():
            u = self._aggregate(k, v, as_keymap(self._expander(k, v)))
            if u is not None:
                out[k] = u
        return out
