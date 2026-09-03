"""KvStore ``zip`` / ``zip_with`` with key-union scan."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any

from kvtsjl.keymap_algebra.zip import ZippedKeyMap, ZipWithKeyMap
from kvtsjl.scope import Scope
from kvtsjl.store.logical import KvStore
from kvtsjl.store.schema.layout import KeyLayout, ScanQuery
from kvtsjl.store.schema.ttl import TtlPolicy


def _union_keys[K](
    parts: Sequence[KvStore[K, Any]],
    query: ScanQuery[K],
) -> Iterator[K]:
    seen: set[K] = set()
    for part in parts:
        for key in part.scan(
            prefix=query.prefix,
            include_values=False,
            page_size=query.page_size,
        ):
            if key not in seen:
                seen.add(key)
                yield key


class ZippedKvStore[K](KvStore[K, tuple[Any, ...]]):
    def __init__(self, parts: Sequence[KvStore[K, Any]]) -> None:
        if len(parts) < 2:
            raise ValueError("zip requires at least two KvStores")
        self._parts = tuple(parts)
        self._view = ZippedKeyMap(parts)
        self.scope = parts[0].scope
        self.batch_size = parts[0].batch_size

    def key_layout(self) -> KeyLayout:
        return self._parts[0].key_layout()

    def get(self, key: K) -> tuple[Any, ...] | None:
        return self._view.get(key)

    def set(
        self, key: K, value: tuple[Any, ...], *, ttl: TtlPolicy | None = None
    ) -> None:
        if len(value) != len(self._parts):
            raise ValueError(
                f"zip set expects {len(self._parts)}-tuple, got {len(value)}"
            )
        for part, item in zip(self._parts, value, strict=True):
            if item is None:
                part.delete(key)
            else:
                part.set(key, item, ttl=ttl)

    def delete(self, key: K) -> bool:
        return self._view.delete(key)

    def _scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, tuple[Any, ...] | None]]:
        for key in _union_keys(self._parts, query):
            if query.include_values:
                yield key, self.get(key)
            else:
                yield key, None

    def _clone_with_scope(self, scope: Scope) -> KvStore[K, tuple[Any, ...]]:
        return ZippedKvStore([p._clone_with_scope(scope) for p in self._parts])

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import compose_repr

        return compose_repr("ZippedKvStore", parts=list(self._parts))


class ZipWithKvStore[K, V](KvStore[K, V]):
    def __init__(
        self,
        ctor: Callable[..., V],
        parts: Mapping[str, KvStore[K, Any]],
    ) -> None:
        if not parts:
            raise ValueError("zip_with requires at least one part")
        self._ctor = ctor
        self._parts = dict(parts)
        self._view = ZipWithKeyMap(ctor, parts)
        first = next(iter(parts.values()))
        self.scope = first.scope
        self.batch_size = first.batch_size

    def key_layout(self) -> KeyLayout:
        return next(iter(self._parts.values())).key_layout()

    def get(self, key: K) -> V | None:
        return self._view.get(key)

    def set(self, key: K, value: V, *, ttl: TtlPolicy | None = None) -> None:
        for name in self._view._field_names:
            item = getattr(value, name)
            part = self._parts[name]
            if item is None:
                part.delete(key)
            else:
                part.set(key, item, ttl=ttl)

    def delete(self, key: K) -> bool:
        return self._view.delete(key)

    def _scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        part_list = list(self._parts.values())
        for key in _union_keys(part_list, query):
            if query.include_values:
                yield key, self.get(key)
            else:
                yield key, None

    def _clone_with_scope(self, scope: Scope) -> KvStore[K, V]:
        return ZipWithKvStore(
            self._ctor,
            {name: store._clone_with_scope(scope) for name, store in self._parts.items()},
        )

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import callable_label, stores_mapping_repr

        ctor = callable_label(self._ctor)
        parts = stores_mapping_repr(self._parts)
        return f"ZipWithKvStore({ctor}, {parts})"
