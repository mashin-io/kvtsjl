"""Value ``map`` / ``imap`` and key ``imap_keys`` wrappers."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import datetime

from kvtsjl.exceptions import KvStoreScanUnsupported
from kvtsjl.keymap import ConditionalWrite, KeyMap, Put, ReadableKeyMap, Versioned
from kvtsjl.store.schema.ttl import TtlPolicy


class MappedKeyMap[K, T, U](ReadableKeyMap[K, U]):
    """Read-only value view: ``get`` applies ``forward``."""

    def __init__(self, underlying: ReadableKeyMap[K, T], forward: Callable[[T], U]) -> None:
        self._underlying = underlying
        self._forward = forward

    def get(self, key: K) -> U | None:
        value = self._underlying.get(key)
        if value is None:
            return None
        return self._forward(value)

    def batch_get(self, keys: Sequence[K]) -> dict[K, U]:
        """One underlying ``batch_get``; ``None`` forward means absent (matches ``get``)."""
        if not keys:
            return {}
        unique = list(dict.fromkeys(keys))
        found = self._underlying.batch_get(unique)
        return {
            k: u for k, v in found.items() if (u := self._forward(v)) is not None
        }


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

    def batch_get(self, keys: Sequence[K]) -> dict[K, U]:
        """One underlying ``batch_get``; ``None`` forward means absent (matches ``get``)."""
        if not keys:
            return {}
        unique = list(dict.fromkeys(keys))
        found = self._underlying.batch_get(unique)
        return {
            k: u for k, v in found.items() if (u := self._forward(v)) is not None
        }

    def set(self, key: K, value: U, *, ttl: TtlPolicy | None = None, created_at: datetime | None = None) -> None:
        self._underlying.set(key, self._inverse(value), ttl=ttl, created_at=created_at)

    def batch_read_versioned(self, keys: Sequence[K]) -> dict[K, Versioned[U]]:
        return {
            k: Versioned(None if seen.value is None else self._forward(seen.value), seen.version)
            for k, seen in self._underlying.batch_read_versioned(keys).items()
        }

    def batch_write_if_unchanged(
        self, writes: Mapping[K, ConditionalWrite[U]], *, ttl: TtlPolicy | None = None
    ) -> set[K]:
        return self._underlying.batch_write_if_unchanged(
            {
                k: ConditionalWrite(
                    Versioned(None, w.seen.version),
                    Put(self._inverse(w.change.value)) if isinstance(w.change, Put) else w.change,
                    w.created_at,
                )
                for k, w in writes.items()
            },
            ttl=ttl,
        )

    def delete(self, key: K) -> bool:
        return self._underlying.delete(key)

    @property
    def creates_entries(self) -> bool:
        return self._underlying.creates_entries

    def unset(self, key: K) -> bool:
        return self._underlying.unset(key)

    def batch_unset(self, keys: Sequence[K]) -> int:
        return self._underlying.batch_unset(keys)


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

    def batch_get(self, keys: Sequence[K]) -> dict[K, T]:
        """Batch via ``to_store`` only — no ``from_store`` (hashed / no-inverse safe)."""
        if not keys:
            return {}
        unique = list(dict.fromkeys(keys))
        to_sk = {key: self._to_store(key) for key in unique}
        found = self._underlying.batch_get(list(dict.fromkeys(to_sk.values())))
        return {k: found[sk] for k, sk in to_sk.items() if sk in found}

    def set(self, key: K, value: T, *, ttl: TtlPolicy | None = None, created_at: datetime | None = None) -> None:
        self._underlying.set(self._to_store(key), value, ttl=ttl, created_at=created_at)

    def batch_read_versioned(self, keys: Sequence[K]) -> dict[K, Versioned[T]]:
        to_sk = {key: self._to_store(key) for key in dict.fromkeys(keys)}
        found = self._underlying.batch_read_versioned(list(dict.fromkeys(to_sk.values())))
        return {k: found[sk] for k, sk in to_sk.items()}

    def batch_write_if_unchanged(
        self, writes: Mapping[K, ConditionalWrite[T]], *, ttl: TtlPolicy | None = None
    ) -> set[K]:
        to_sk = {key: self._to_store(key) for key in writes}
        written = self._underlying.batch_write_if_unchanged(
            {to_sk[k]: w for k, w in writes.items()}, ttl=ttl
        )
        return {k for k, sk in to_sk.items() if sk in written}

    def delete(self, key: K) -> bool:
        return self._underlying.delete(self._to_store(key))

    @property
    def creates_entries(self) -> bool:
        return self._underlying.creates_entries

    def unset(self, key: K) -> bool:
        return self._underlying.unset(self._to_store(key))

    def batch_unset(self, keys: Sequence[K]) -> int:
        return self._underlying.batch_unset([self._to_store(k) for k in keys])

    def require_from_store(self) -> Callable[[SK], K]:
        if self._from_store is None:
            raise KvStoreScanUnsupported(
                "imap_keys without from_store cannot recover caller keys (e.g. for scan)"
            )
        return self._from_store
