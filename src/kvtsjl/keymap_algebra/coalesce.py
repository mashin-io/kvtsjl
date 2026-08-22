"""``coalesce`` — left-biased merge (same as store ``fallback_read``)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from kvtsjl.keymap import KeyMap


class CoalescedKeyMap[K, T](KeyMap[K, T]):
    """``get`` prefers left; writes go to left only."""

    def __init__(
        self,
        left: KeyMap[K, T],
        right: KeyMap[K, T],
        *,
        promote: bool = True,
    ) -> None:
        self._left = left
        self._right = right
        self._promote = promote

    def get(self, key: K) -> T | None:
        value = self._left.get(key)
        if value is not None:
            return value
        value = self._right.get(key)
        if value is not None and self._promote:
            self._left.set(key, value)
        return value

    def set(self, key: K, value: T) -> None:
        self._left.set(key, value)

    def delete(self, key: K) -> bool:
        return self._left.delete(key)

    def batch_get(self, keys: Sequence[K]) -> dict[K, T]:
        found = self._left.batch_get(keys)
        missing = [k for k in keys if k not in found]
        if not missing:
            return found
        secondary = self._right.batch_get(missing)
        if secondary and self._promote:
            self._left.batch_set(secondary)
        found.update(secondary)
        return found

    def batch_set(self, items: Mapping[K, T]) -> None:
        self._left.batch_set(items)

    def batch_delete(self, keys: Sequence[K]) -> int:
        return self._left.batch_delete(keys)
