"""KvStore ``zip`` / ``zip_with`` with key-union scan."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from datetime import datetime
from typing import Any

from kvtsjl.keymap_algebra.zip import ZippedKeyMap, ZipWithKeyMap
from kvtsjl.scope import Scope
from kvtsjl.store.logical import KvStore
from kvtsjl.store.schema.layout import KeyLayout, ScanQuery
from kvtsjl.store.schema.stat import RecordStat
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


def _merge_part_stats(stats: Sequence[RecordStat]) -> RecordStat:
    created = [s.created_at for s in stats if s.created_at is not None]
    expires = [s.expires_at for s in stats if s.expires_at is not None]
    return RecordStat(
        created_at=max(created) if created else None,
        expires_at=min(expires) if expires else None,
    )


class ZippedKvStore[K](KvStore[K, tuple[Any, ...]]):
    def __init__(self, parts: Sequence[KvStore[K, Any]]) -> None:
        if len(parts) < 2:
            raise ValueError("zip requires at least two KvStores")
        self._parts = tuple(parts)
        self._view = ZippedKeyMap(parts)
        self.scope = parts[0].scope
        self.batch_size = parts[0].batch_size

    @property
    def supports_write_clock(self) -> bool:
        return all(part.supports_write_clock for part in self._parts)

    def key_layout(self) -> KeyLayout:
        return self._parts[0].key_layout()

    def get(self, key: K) -> tuple[Any, ...] | None:
        return self._view.get(key)

    def batch_get(self, keys: Sequence[K]) -> dict[K, tuple[Any, ...]]:
        return self._view.batch_get(keys)

    def exists(self, key: K) -> bool:
        return any(part.exists(key) for part in self._parts)

    def batch_exists(self, keys: Sequence[K]) -> set[K]:
        found: set[K] = set()
        for part in self._parts:
            found |= part.batch_exists(keys)
            if found.issuperset(keys):
                break
        return found

    def stat(self, key: K) -> RecordStat | None:
        present = [meta for part in self._parts if (meta := part.stat(key)) is not None]
        if not present:
            return None
        return _merge_part_stats(present)

    def batch_stat(self, keys: Sequence[K]) -> dict[K, RecordStat]:
        per_key: dict[K, list[RecordStat]] = {}
        for part in self._parts:
            for key, meta in part.batch_stat(keys).items():
                per_key.setdefault(key, []).append(meta)
        return {key: _merge_part_stats(stats) for key, stats in per_key.items()}

    def set(
        self,
        key: K,
        value: tuple[Any, ...],
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        if len(value) != len(self._parts):
            raise ValueError(
                f"zip set expects {len(self._parts)}-tuple, got {len(value)}"
            )
        for part, item in zip(self._parts, value, strict=True):
            if item is None:
                part.delete(key)
            else:
                part.set(key, item, ttl=ttl, created_at=created_at)

    def batch_set(
        self,
        items: Mapping[K, tuple[Any, ...]],
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        if not items:
            return
        n = len(self._parts)
        for value in items.values():
            if len(value) != n:
                raise ValueError(f"zip set expects {n}-tuple, got {len(value)}")
        for index, part in enumerate(self._parts):
            to_set: dict[K, Any] = {}
            to_delete: list[K] = []
            for key, value in items.items():
                item = value[index]
                if item is None:
                    to_delete.append(key)
                else:
                    to_set[key] = item
            if to_set:
                part.batch_set(to_set, ttl=ttl, created_at=created_at)
            if to_delete:
                part.batch_delete(to_delete)

    def delete(self, key: K) -> bool:
        return self._view.delete(key)

    def batch_delete(self, keys: Sequence[K]) -> int:
        return self._view.batch_delete(keys)

    def _scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, tuple[Any, ...] | None]]:
        for key in _union_keys(self._parts, query):
            if query.include_values:
                yield key, self.get(key)
            else:
                yield key, None

    def _clone_with_scope(self, scope: Scope) -> KvStore[K, tuple[Any, ...]]:
        return ZippedKvStore([p._clone_with_scope(scope) for p in self._parts])

    def _gc_expired_keys(self, *, max_entries: int) -> list[K]:
        if max_entries < 1:
            raise ValueError(f"max_entries must be >= 1, got {max_entries}")
        deleted: list[K] = []
        remaining = max_entries
        for part in self._parts:
            if remaining < 1:
                break
            part_keys = part._gc_expired_keys(max_entries=remaining)
            deleted.extend(part_keys)
            remaining = max_entries - len(deleted)
        # Dedupe while preserving order (same K may be GC'd in multiple parts).
        seen: set[K] = set()
        unique: list[K] = []
        for key in deleted:
            if key not in seen:
                seen.add(key)
                unique.append(key)
        return unique[:max_entries]

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

    @property
    def supports_write_clock(self) -> bool:
        return all(part.supports_write_clock for part in self._parts.values())

    def key_layout(self) -> KeyLayout:
        return next(iter(self._parts.values())).key_layout()

    def get(self, key: K) -> V | None:
        return self._view.get(key)

    def batch_get(self, keys: Sequence[K]) -> dict[K, V]:
        return self._view.batch_get(keys)

    def exists(self, key: K) -> bool:
        return any(part.exists(key) for part in self._parts.values())

    def batch_exists(self, keys: Sequence[K]) -> set[K]:
        found: set[K] = set()
        for part in self._parts.values():
            found |= part.batch_exists(keys)
            if found.issuperset(keys):
                break
        return found

    def stat(self, key: K) -> RecordStat | None:
        present = [
            meta
            for part in self._parts.values()
            if (meta := part.stat(key)) is not None
        ]
        if not present:
            return None
        return _merge_part_stats(present)

    def batch_stat(self, keys: Sequence[K]) -> dict[K, RecordStat]:
        per_key: dict[K, list[RecordStat]] = {}
        for part in self._parts.values():
            for key, meta in part.batch_stat(keys).items():
                per_key.setdefault(key, []).append(meta)
        return {key: _merge_part_stats(stats) for key, stats in per_key.items()}

    def set(
        self,
        key: K,
        value: V,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        for name in self._view._field_names:
            item = getattr(value, name)
            part = self._parts[name]
            if item is None:
                part.delete(key)
            else:
                part.set(key, item, ttl=ttl, created_at=created_at)

    def batch_set(
        self,
        items: Mapping[K, V],
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        if not items:
            return
        for name in self._view._field_names:
            part = self._parts[name]
            to_set: dict[K, Any] = {}
            to_delete: list[K] = []
            for key, value in items.items():
                item = getattr(value, name)
                if item is None:
                    to_delete.append(key)
                else:
                    to_set[key] = item
            if to_set:
                part.batch_set(to_set, ttl=ttl, created_at=created_at)
            if to_delete:
                part.batch_delete(to_delete)

    def delete(self, key: K) -> bool:
        return self._view.delete(key)

    def batch_delete(self, keys: Sequence[K]) -> int:
        return self._view.batch_delete(keys)

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

    def _gc_expired_keys(self, *, max_entries: int) -> list[K]:
        if max_entries < 1:
            raise ValueError(f"max_entries must be >= 1, got {max_entries}")
        deleted: list[K] = []
        remaining = max_entries
        for part in self._parts.values():
            if remaining < 1:
                break
            deleted.extend(part._gc_expired_keys(max_entries=remaining))
            remaining = max_entries - len(deleted)
        seen: set[K] = set()
        unique: list[K] = []
        for key in deleted:
            if key not in seen:
                seen.add(key)
                unique.append(key)
        return unique[:max_entries]

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import callable_label, stores_mapping_repr

        ctor = callable_label(self._ctor)
        parts = stores_mapping_repr(self._parts)
        return f"ZipWithKvStore({ctor}, {parts})"
