"""``then`` / ``then_with`` — compose partial maps (``other[self[k]]``)."""

from __future__ import annotations

from collections.abc import Callable, Sequence

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

    def batch_get(self, keys: Sequence[K]) -> dict[K, V]:
        """Two part ``batch_get`` calls; mid keys are deduped before the right hop."""
        if not keys:
            return {}
        unique = list(dict.fromkeys(keys))
        left_found = self._left.batch_get(unique)
        mids = list(dict.fromkeys(j for j in left_found.values() if j is not None))
        right_found = self._right.batch_get(mids)
        return {k: right_found[j] for k, j in left_found.items() if j in right_found}


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

    def batch_get(self, keys: Sequence[K]) -> dict[K, V]:
        """Two part ``batch_get`` calls; derived mids are deduped before the right hop."""
        if not keys:
            return {}
        unique = list(dict.fromkeys(keys))
        left_found = self._left.batch_get(unique)
        mid_of = {k: self._key_of(k, v) for k, v in left_found.items()}
        mids = list(dict.fromkeys(mid_of.values()))
        right_found = self._right.batch_get(mids)
        return {k: right_found[j] for k, j in mid_of.items() if j in right_found}
