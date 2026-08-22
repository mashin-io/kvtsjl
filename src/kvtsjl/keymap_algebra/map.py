"""Value ``map`` / ``imap`` and key ``imap_keys`` wrappers."""

from __future__ import annotations

from collections.abc import Callable

from kvtsjl.exceptions import KvStoreScanUnsupported
from kvtsjl.keymap import KeyMap
from kvtsjl.keymap_algebra.util import raise_readonly


class MappedKeyMap[K, T, U](KeyMap[K, U]):
    """Read-only value view: ``get`` applies ``forward``; mutations unsupported."""

    def __init__(self, underlying: KeyMap[K, T], forward: Callable[[T], U]) -> None:
        self._underlying = underlying
        self._forward = forward

    def get(self, key: K) -> U | None:
        value = self._underlying.get(key)
        if value is None:
            return None
        return self._forward(value)

    def set(self, key: K, value: U) -> None:
        raise_readonly("set")

    def delete(self, key: K) -> bool:
        raise_readonly("delete")
        return False  # pragma: no cover


class IMappedKeyMap[K, T, U](KeyMap[K, U]):
    """Invertible value codec: store ``inverse(u)``, expose ``forward(t)``."""

    def __init__(
        self,
        underlying: KeyMap[K, T],
        forward: Callable[[T], U],
        inverse: Callable[[U], T],
    ) -> None:
        self._underlying = underlying
        self._forward = forward
        self._inverse = inverse

    def get(self, key: K) -> U | None:
        value = self._underlying.get(key)
        if value is None:
            return None
        return self._forward(value)

    def set(self, key: K, value: U) -> None:
        self._underlying.set(key, self._inverse(value))

    def delete(self, key: K) -> bool:
        return self._underlying.delete(key)


class IMappedKeysKeyMap[K, SK, T](KeyMap[K, T]):
    """Caller keys ``K`` mapped to storage keys ``SK`` via ``to_store``."""

    def __init__(
        self,
        underlying: KeyMap[SK, T],
        to_store: Callable[[K], SK],
        from_store: Callable[[SK], K] | None = None,
    ) -> None:
        self._underlying = underlying
        self._to_store = to_store
        self._from_store = from_store

    def get(self, key: K) -> T | None:
        return self._underlying.get(self._to_store(key))

    def set(self, key: K, value: T) -> None:
        self._underlying.set(self._to_store(key), value)

    def delete(self, key: K) -> bool:
        return self._underlying.delete(self._to_store(key))

    def require_from_store(self) -> Callable[[SK], K]:
        if self._from_store is None:
            raise KvStoreScanUnsupported(
                "imap_keys without from_store cannot recover caller keys (e.g. for scan)"
            )
        return self._from_store
