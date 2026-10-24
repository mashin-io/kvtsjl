"""KvStore ``zip`` / ``zip_with`` — cores + fan-out mixins."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from typing import Any, cast

from kvtsjl.keymap_algebra.zip import ZippedKeyMap, ZipWithKeyMap
from kvtsjl.scope import Scope
from kvtsjl.store.compose.topology import StoreTopologyMixin
from kvtsjl.store.fanout import ScopeFanoutMixin, clone_parts, clone_parts_mapping
from kvtsjl.store.logical import KvStore
from kvtsjl.store.schema.stat import RecordStat


def merge_part_stats(
    slot_stats: Sequence[RecordStat | None],
    *,
    mandatory: Sequence[bool],
    created_at_merge: Callable[[Sequence[datetime | None]], datetime | None]
    | None = None,
) -> RecordStat:
    """Merge per-slot part stats into the row's stat.

    ``slot_stats`` is aligned with the parts (``None`` where a part has no
    entry), because ``expires_at`` depends on which slots are ``mandatory``.

    ``expires_at`` must agree with ``row_present``, which decides how long the
    row actually lives:

    - any mandatory slot — the row dies with the first mandatory part, so take
      the earliest expiry among mandatory slots;
    - otherwise (default OR) — the row survives while any part survives, so
      take the latest expiry among present slots, and ``None`` (never expires)
      if any present part never expires.

    Taking ``min`` unconditionally made ``stat`` claim a row had expired while
    ``get`` and ``exists`` still returned it.
    """
    present = [s for s in slot_stats if s is not None]
    created_vals = [s.created_at for s in present]
    if created_at_merge is not None:
        created_at = created_at_merge(created_vals)
    else:
        present_created = [c for c in created_vals if c is not None]
        created_at = max(present_created) if present_created else None

    if any(mandatory):
        required = [
            s.expires_at
            for s, is_required in zip(slot_stats, mandatory, strict=True)
            if is_required and s is not None and s.expires_at is not None
        ]
        expires_at = min(required) if required else None
    elif any(s.expires_at is None for s in present):
        expires_at = None
    else:
        latest = [s.expires_at for s in present if s.expires_at is not None]
        expires_at = max(latest) if latest else None

    return RecordStat(created_at=created_at, expires_at=expires_at)


class ZippedKvStore[K](
    StoreTopologyMixin[K, tuple[Any, ...]],
    ZippedKeyMap[K],
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
        self._created_at_merge = created_at_merge

    def _store_parts(self) -> tuple[KvStore[K, Any], ...]:
        return cast(tuple[KvStore[K, Any], ...], self._parts)

    def stat(self, key: K) -> RecordStat | None:
        if self.get(key) is None:
            return None
        slot_stats = [part.stat(key) for part in self._store_parts()]
        if all(meta is None for meta in slot_stats):
            return None
        return merge_part_stats(
            slot_stats,
            mandatory=self._mandatory,
            created_at_merge=self._created_at_merge,
        )

    def batch_stat(self, keys: Sequence[K]) -> dict[K, RecordStat]:
        present_keys = self.batch_get(keys)
        if not present_keys:
            return {}
        key_list = list(present_keys.keys())
        parts = self._store_parts()
        per_key: dict[K, list[RecordStat | None]] = {
            key: [None] * len(parts) for key in key_list
        }
        for index, part in enumerate(parts):
            for key, meta in part.batch_stat(key_list).items():
                per_key[key][index] = meta
        return {
            key: merge_part_stats(
                slot_stats,
                mandatory=self._mandatory,
                created_at_merge=self._created_at_merge,
            )
            for key, slot_stats in per_key.items()
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
    StoreTopologyMixin[K, V],
    ZipWithKeyMap[K, V],
    ScopeFanoutMixin,
    KvStore[K, V],
):
    def __init__(
        self,
        ctor: Callable[..., V],
        parts: Mapping[str, KvStore[K, Any]],
        *,
        commit_part: str | None = None,
        mandatory: Sequence[bool] | None = None,
        created_at_merge: Callable[[Sequence[datetime | None]], datetime | None]
        | None = None,
    ) -> None:
        if not parts:
            raise ValueError("zip_with requires at least one part")
        super().__init__(ctor, parts, commit_part=commit_part, mandatory=mandatory)
        first = next(iter(parts.values()))
        self.scope = first.scope
        self.batch_size = first.batch_size
        self._created_at_merge = created_at_merge

    def _store_parts(self) -> dict[str, KvStore[K, Any]]:
        return cast(dict[str, KvStore[K, Any]], self._parts)

    def stat(self, key: K) -> RecordStat | None:
        if self.get(key) is None:
            return None
        slot_stats = [
            self._store_parts()[name].stat(key) for name in self.field_names
        ]
        if all(meta is None for meta in slot_stats):
            return None
        return merge_part_stats(
            slot_stats,
            mandatory=self._mandatory,
            created_at_merge=self._created_at_merge,
        )

    def batch_stat(self, keys: Sequence[K]) -> dict[K, RecordStat]:
        present_keys = self.batch_get(keys)
        if not present_keys:
            return {}
        key_list = list(present_keys.keys())
        parts = self._store_parts()
        per_key: dict[K, list[RecordStat | None]] = {
            key: [None] * len(self.field_names) for key in key_list
        }
        for index, name in enumerate(self.field_names):
            for key, meta in parts[name].batch_stat(key_list).items():
                per_key[key][index] = meta
        return {
            key: merge_part_stats(
                slot_stats,
                mandatory=self._mandatory,
                created_at_merge=self._created_at_merge,
            )
            for key, slot_stats in per_key.items()
        }

    def _clone_fanout(self, scope: Scope) -> ZipWithKvStore[K, V]:
        return ZipWithKvStore(
            self._ctor,
            clone_parts_mapping(self._store_parts(), scope),
            commit_part=self._commit_part,
            mandatory=self._mandatory,
            created_at_merge=self._created_at_merge,
        )

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import callable_label, stores_mapping_repr

        ctor = callable_label(self._ctor)
        parts = stores_mapping_repr(self._store_parts())
        return f"ZipWithKvStore({ctor}, {parts})"
