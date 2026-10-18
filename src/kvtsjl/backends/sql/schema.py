"""SQL row schema descriptors for ``SqlDbKvStore``."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any

from kvtsjl.store.schema.kvset import KvSet

SqlDbRow = dict[str, Any]


class ExpiryBinding(str, Enum):
    """How ``expires_at`` is interpreted for SQL expiry scans.

    ``LATE`` (default): two-branch predicate — concrete ``expires_at``, or
    ``updated_at +`` collection TTL when ``expires_at`` is NULL. Needed while
    legacy NULL rows remain after binding writes.

    ``EARLY``: single branch ``expires_at <= now``. Only safe after the client
    has backfilled NULL death times on relative-TTL tables; the store checks
    once and raises if un-backfilled rows remain.
    """

    LATE = "late"
    EARLY = "early"


@dataclass(frozen=True, slots=True)
class SqlDbField:
    """A value column on the SQL table (not key / TTL columns)."""

    name: str
    read_only: bool = False


def sql_table_name(name: str, version: int | str) -> str:
    """Sanitize KvSet name/version into a SQL table identifier."""
    safe = (
        (name or "_")
        .replace("-", "_")
        .replace(".", "_")
        .replace("/", "_")
        .replace("\\", "_")
        .replace(" ", "_")
    )
    if not safe or not (safe[0].isalpha() or safe[0] == "_"):
        safe = f"t_{safe}"
    if isinstance(version, int):
        if version > 0:
            return f"{safe}_{version}"
        return safe
    ver = str(version).strip()
    if ver and ver not in {"0", "v0"}:
        label = ver[1:] if ver.startswith("v") else ver
        return f"{safe}_{label}"
    return safe


@dataclass(frozen=True, slots=True)
class SqlDbKvSet[K, V]:
    """Table mapping for a row-oriented SQL collection.

    Physical primary key is ``scope_fields + leaf_key_fields`` (composite).
    Scope segments map 1:1 onto ``scope_fields`` (kind == column name).
    Leaf ``K`` maps onto ``leaf_key_fields`` (single column via ``key_serde``,
    or a sequence/mapping aligned to multiple columns).

    DDL is caller-owned.

    **Sharing a table.** Several ``SqlDbKvSet`` views may map onto one table,
    each owning a different set of value columns. ``set`` handles this already:
    both the INSERT and the ON CONFLICT DO UPDATE list only this view's
    columns, so a view never clobbers a sibling's data and a view writing
    second updates rather than replacing the row.

    The one requirement falls on the DDL: whichever view inserts a row first
    can only fill its own columns, so **every other value column must be
    nullable or carry a DEFAULT**. A ``NOT NULL`` column with no default makes
    that first insert impossible from any view that does not own it.

    That constraint is also a tool. Leaving the *creating* view's columns
    ``NOT NULL`` without a default enforces which view may create the row: a
    non-owning view that writes first fails loudly instead of leaving a hollow
    row behind for the owner to find.

    ``updated_at`` and ``expires_at`` are row-level, not per-view: the view
    that writes last sets the row's clock for everyone sharing it. Pass
    ``created_at`` to pin the base a relative TTL counts from, so several
    writes within one unit of work share a death time instead of each one
    pushing it out.
    """

    kvset: KvSet[K, V, str, SqlDbRow]
    leaf_key_fields: Sequence[str]
    fields: Sequence[SqlDbField]
    scope_fields: Sequence[str] = ()
    updated_at_field: str = "updated_at"
    expires_at_field: str = "expires_at"
    expiry_binding: ExpiryBinding = ExpiryBinding.LATE

    def __post_init__(self) -> None:
        if not self.leaf_key_fields:
            raise ValueError("leaf_key_fields must be non-empty")
        seen: set[str] = set()
        for name in [*self.scope_fields, *self.leaf_key_fields]:
            if name in seen:
                raise ValueError(f"duplicate key field: {name!r}")
            seen.add(name)

    @classmethod
    def create(
        cls,
        kvset: KvSet[K, V, str, SqlDbRow],
        *,
        fields: Sequence[SqlDbField],
        key_field: str | None = None,
        leaf_key_fields: Sequence[str] | None = None,
        scope_fields: Sequence[str] = (),
        updated_at_field: str = "updated_at",
        expires_at_field: str = "expires_at",
        expiry_binding: ExpiryBinding = ExpiryBinding.LATE,
    ) -> SqlDbKvSet[K, V]:
        """Build a descriptor; pass ``key_field`` for a single leaf PK column."""
        if leaf_key_fields is not None and key_field is not None:
            raise ValueError("pass key_field or leaf_key_fields, not both")
        if leaf_key_fields is None:
            if key_field is None:
                raise ValueError("key_field or leaf_key_fields is required")
            leaf_key_fields = (key_field,)
        return cls(
            kvset=kvset,
            leaf_key_fields=tuple(leaf_key_fields),
            fields=fields,
            scope_fields=tuple(scope_fields),
            updated_at_field=updated_at_field,
            expires_at_field=expires_at_field,
            expiry_binding=expiry_binding,
        )

    @property
    def key_fields(self) -> tuple[str, ...]:
        """Full primary-key column order: scope columns then leaf columns."""
        return tuple(self.scope_fields) + tuple(self.leaf_key_fields)

    @property
    def table_name(self) -> str:
        return sql_table_name(self.kvset.name, self.kvset.version)

    def select_columns(self) -> tuple[str, ...]:
        names = [f.name for f in self.fields]
        for col in (*self.key_fields, self.updated_at_field, self.expires_at_field):
            if col not in names:
                names.append(col)
        return tuple(names)

    def write_columns(self) -> tuple[str, ...]:
        names = [f.name for f in self.fields if not f.read_only]
        for col in (*self.key_fields, self.updated_at_field, self.expires_at_field):
            if col not in names:
                names.append(col)
        return tuple(names)
