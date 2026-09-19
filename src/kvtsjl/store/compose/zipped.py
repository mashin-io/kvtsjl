"""KvStore ``zip`` / ``zip_with`` — cores + fan-out mixins."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from typing import Any, cast

from kvtsjl.keymap_algebra.zip import ZippedKeyMap, ZipWithKeyMap
from kvtsjl.scope import Scope
from kvtsjl.store.fanout import (
    GcFanoutMixin,
    ScanUnionMixin,
    ScopeFanoutMixin,
    clone_parts,
    clone_parts_mapping,
)
from kvtsjl.store.logical import KvStore
from kvtsjl.store.schema.layout import KeyLayout
from kvtsjl.store.schema.stat import RecordStat


def merge_part_stats(
    stats: Sequence[RecordStat],
    *,
    created_at_merge: Callable[[Sequence[datetime | None]], datetime | None]
    | None = None,
) -> RecordStat:
    created_vals = [s.created_at for s in stats]
    if created_at_merge is not None:
        created_at = created_at_merge(created_vals)
    else:
        present_created = [c for c in created_vals if c is not None]
        created_at = max(present_created) if present_created else None
    expires = [s.expires_at for s in stats if s.expires_at is not None]
    return RecordStat(
        created_at=created_at,
        expires_at=min(expires) if expires else None,
    )


class ZippedKvStore[K](
    ZippedKeyMap[K],
    ScanUnionMixin[K, tuple[Any, ...]],
    GcFanoutMixin[K],
    ScopeFanoutMixin,
    KvStore[K, tuple[Any, ...]],
):
    def __init__(
        self,
        parts: Sequence[KvStore[K, Any]],
        *,
        mandatory: Sequence[bool] | None = None,
        created_at_merge: Callable[[Sequence[datetime | None]], datetime | None]
        | None = None,
    ) -> None:
        if len(parts) < 2:
            raise ValueError("zip requires at least two KvStores")
        super().__init__(parts, mandatory=mandatory)
        self.scope = parts[0].scope
        self.batch_size = parts[0].batch_size
        store_parts = tuple(parts)
        self._scan_parts = store_parts
        self._gc_parts = store_parts
        self._created_at_merge = created_at_merge

    def _store_parts(self) -> tuple[KvStore[K, Any], ...]:
        return cast(tuple[KvStore[K, Any], ...], self._parts)

    @property
    def supports_write_clock(self) -> bool:
        return all(part.supports_write_clock for part in self._store_parts())

    def key_layout(self) -> KeyLayout:
        return self._store_parts()[0].key_layout()

    def exists(self, key: K) -> bool:
        return self.get(key) is not None

    def batch_exists(self, keys: Sequence[K]) -> set[K]:
        return set(self.batch_get(keys).keys())

    def stat(self, key: K) -> RecordStat | None:
        if self.get(key) is None:
            return None
        present = [
            meta for part in self._store_parts() if (meta := part.stat(key)) is not None
        ]
        if not present:
            return None
        return merge_part_stats(present, created_at_merge=self._created_at_merge)

    def batch_stat(self, keys: Sequence[K]) -> dict[K, RecordStat]:
        present_keys = self.batch_get(keys)
        if not present_keys:
            return {}
        key_list = list(present_keys.keys())
        per_key: dict[K, list[RecordStat]] = {}
        for part in self._store_parts():
            for key, meta in part.batch_stat(key_list).items():
                per_key.setdefault(key, []).append(meta)
        return {
            key: merge_part_stats(stats, created_at_merge=self._created_at_merge)
            for key, stats in per_key.items()
        }

    def _clone_fanout(self, scope: Scope) -> ZippedKvStore[K]:
        return ZippedKvStore(
            clone_parts(self._store_parts(), scope),
            mandatory=self._mandatory,
            created_at_merge=self._created_at_merge,
        )

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import compose_repr

        return compose_repr("ZippedKvStore", parts=list(self._store_parts()))


class ZipWithKvStore[K, V](
    ZipWithKeyMap[K, V],
    ScanUnionMixin[K, V],
    GcFanoutMixin[K],
    ScopeFanoutMixin,
    KvStore[K, V],
):
    def __init__(
        self,
        ctor: Callable[..., V],
        parts: Mapping[str, KvStore[K, Any]],
        *,
        mandatory: Sequence[bool] | None = None,
        created_at_merge: Callable[[Sequence[datetime | None]], datetime | None]
        | None = None,
    ) -> None:
        if not parts:
            raise ValueError("zip_with requires at least one part")
        super().__init__(ctor, parts, mandatory=mandatory)
        first = next(iter(parts.values()))
        self.scope = first.scope
        self.batch_size = first.batch_size
        store_parts = dict(parts)
        self._scan_parts = list(store_parts.values())
        self._gc_parts = list(store_parts.values())
        self._created_at_merge = created_at_merge

    def _store_parts(self) -> dict[str, KvStore[K, Any]]:
        return cast(dict[str, KvStore[K, Any]], self._parts)

    @property
    def supports_write_clock(self) -> bool:
        return all(part.supports_write_clock for part in self._store_parts().values())

    def key_layout(self) -> KeyLayout:
        return next(iter(self._store_parts().values())).key_layout()

    def exists(self, key: K) -> bool:
        return self.get(key) is not None

    def batch_exists(self, keys: Sequence[K]) -> set[K]:
        return set(self.batch_get(keys).keys())

    def stat(self, key: K) -> RecordStat | None:
        if self.get(key) is None:
            return None
        present = [
            meta
            for part in self._store_parts().values()
            if (meta := part.stat(key)) is not None
        ]
        if not present:
            return None
        return merge_part_stats(present, created_at_merge=self._created_at_merge)

    def batch_stat(self, keys: Sequence[K]) -> dict[K, RecordStat]:
        present_keys = self.batch_get(keys)
        if not present_keys:
            return {}
        key_list = list(present_keys.keys())
        per_key: dict[K, list[RecordStat]] = {}
        for part in self._store_parts().values():
            for key, meta in part.batch_stat(key_list).items():
                per_key.setdefault(key, []).append(meta)
        return {
            key: merge_part_stats(stats, created_at_merge=self._created_at_merge)
            for key, stats in per_key.items()
        }

    def _clone_fanout(self, scope: Scope) -> ZipWithKvStore[K, V]:
        return ZipWithKvStore(
            self._ctor,
            clone_parts_mapping(self._store_parts(), scope),
            mandatory=self._mandatory,
            created_at_merge=self._created_at_merge,
        )

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import callable_label, stores_mapping_repr

        ctor = callable_label(self._ctor)
        parts = stores_mapping_repr(self._store_parts())
        return f"ZipWithKvStore({ctor}, {parts})"
