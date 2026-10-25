"""``group_by`` — a store of groups: one scope segment's members, read and written whole."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from datetime import datetime

from kvtsjl.exceptions import KvStoreScanUnsupported, KvStoreUnsupported
from kvtsjl.keymap_algebra.topology import MissingPage
from kvtsjl.scope import Scope
from kvtsjl.store.logical import KvStore
from kvtsjl.store.schema.layout import KeyLayout, ScanQuery
from kvtsjl.store.schema.ttl import TtlPolicy


class GroupedKvStore[K, V](KvStore[str, Mapping[K, V]]):
    """The rows of ``rows`` grouped by the scope segment ``by``.

    The group under ``g`` is ``rows.scoped(by=g)``, read and written as one
    mapping, so a parent's child rows — a turn's receipts, keyed under it —
    are one value:

    - ``get`` returns the members, an empty mapping when there are none: a
      group is never absent, only empty;
    - ``set`` makes the stored members equal the given ones, writing those
      present and deleting the rest;
    - ``delete`` removes every member.

    It is scoped like any store: the group's segment is appended to whatever
    scope the store is bound to. A group is many rows, so no single
    conditional write covers it, and it lists no groups — scan the rows. As a
    part of a record it refers to the record's key, so declare it
    ``after_commit``: it is then written only once the record commits.
    """

    def __init__(self, rows: KvStore[K, V], by: str) -> None:
        self._rows = rows
        self._by = by
        self.scope = rows.scope
        self.batch_size = rows.batch_size

    def _group(self, key: str) -> KvStore[K, V]:
        return self._rows.clone_with_scope(self._rows.scope.extend(self._by, key))

    def _members(self, key: str) -> list[K]:
        query = ScanQuery[K](include_values=False, page_size=self.batch_size)
        return [k for k, _ in self._group(key).scan_entries(query)]

    @property
    def supports_write_clock(self) -> bool:
        return self._rows.supports_write_clock

    @property
    def writes_are_deferred(self) -> bool:
        return self._rows.writes_are_deferred

    def key_layout(self) -> KeyLayout:
        return self._rows.key_layout()

    def get(self, key: str) -> Mapping[K, V]:
        query = ScanQuery[K](include_values=True, page_size=self.batch_size)
        return {k: v for k, v in self._group(key).scan_entries(query) if v is not None}

    def exists(self, key: str) -> bool:
        return True

    def batch_exists(self, keys: Sequence[str]) -> set[str]:
        return set(keys)

    def set(
        self,
        key: str,
        value: Mapping[K, V],
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        """Write the members given, then delete those no longer among them."""
        group = self._group(key)
        held = self._members(key)
        if value:
            group.batch_set(value, ttl=ttl, created_at=created_at)
        stale = [k for k in held if k not in value]
        if stale:
            group.batch_delete(stale)

    def set_if_absent(
        self,
        key: str,
        value: Mapping[K, V],
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> bool:
        raise KvStoreUnsupported(
            "a group is many rows, and no one conditional write covers them; as "
            "a part of a record, declare it after_commit so it is written only "
            "once the record commits"
        )

    def delete(self, key: str) -> bool:
        held = self._members(key)
        return bool(held) and self._group(key).batch_delete(held) > 0

    def scan_entries(
        self, query: ScanQuery[str]
    ) -> Iterator[tuple[str, Mapping[K, V] | None]]:
        raise KvStoreScanUnsupported(
            "a grouped store lists no groups; scan the rows it groups"
        )

    def missing_pages(self, page_size: int) -> Iterator[MissingPage[str, Mapping[K, V]]]:
        raise KvStoreUnsupported(
            "a grouped store lists nothing missing; ask the rows it groups"
        )

    def gc_expired_keys(self, *, max_entries: int) -> list[str]:
        """Collect the members' expired rows; a group itself never expires."""
        self._rows.gc_expired_keys(max_entries=max_entries)
        return []

    def child_stores(self) -> tuple[object, ...]:
        return (self._rows,)

    def clone_with_scope(self, scope: Scope) -> GroupedKvStore[K, V]:
        return GroupedKvStore(self._rows.clone_with_scope(scope), self._by)

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import compose_repr

        return compose_repr("GroupedKvStore", rows=self._rows, by=self._by)
