"""In-memory ``KeyMap`` for nested expand collections and tests."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime
import itertools

from kvtsjl.keymap import ConditionalWrite, KeyMap, Remove, Versioned
from kvtsjl.store.schema.ttl import TtlPolicy


class DictKeyMap[K, T](KeyMap[K, T]):
    """Process-local mutable ``KeyMap`` backed by a ``dict``."""

    def __init__(self, items: Mapping[K, T] | None = None) -> None:
        self._data: dict[K, T] = dict(items) if items is not None else {}
        self._versions = itertools.count(1)
        self._version: dict[K, int] = {k: next(self._versions) for k in self._data}

    @classmethod
    def from_pairs(cls, pairs: Iterable[tuple[K, T]]) -> DictKeyMap[K, T]:
        return cls(dict(pairs))

    def get(self, key: K) -> T | None:
        return self._data.get(key)

    def set(self, key: K, value: T, *, ttl: TtlPolicy | None = None, created_at: datetime | None = None) -> None:
        self._data[key] = value
        self._version[key] = next(self._versions)

    def batch_read_versioned(self, keys: Sequence[K]) -> dict[K, Versioned[T]]:
        """Each write takes a new number, so a version names one write."""
        return {
            k: Versioned.absent() if (v := self._version.get(k)) is None else Versioned(self._data[k], v)
            for k in dict.fromkeys(keys)
        }

    def batch_write_if_unchanged(
        self, writes: Mapping[K, ConditionalWrite[T]], *, ttl: TtlPolicy | None = None
    ) -> set[K]:
        # Dict reads and writes with no await or I/O between them: each is one step.
        written: set[K] = set()
        for key, w in writes.items():
            if self._version.get(key) != w.seen.version:
                continue
            if isinstance(w.change, Remove):
                if self.delete(key):
                    written.add(key)
                continue
            self.set(key, w.change.value)
            written.add(key)
        return written

    def delete(self, key: K) -> bool:
        if key not in self._data:
            return False
        del self._data[key]
        del self._version[key]
        return True

    def keys(self) -> list[K]:
        return list(self._data.keys())

    def items(self) -> list[tuple[K, T]]:
        return list(self._data.items())

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return f"DictKeyMap({self._data!r})"
