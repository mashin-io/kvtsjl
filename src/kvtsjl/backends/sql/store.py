"""``SqlDbKvStore`` leaf backend."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from datetime import UTC, datetime
import json
import time
from typing import Any, cast

from kvtsjl.backends.sql.adapter import SqlDbClientAdapter, SqlLiveness, SqlOrder, SqlSelect
from kvtsjl.backends.sql.schema import ExpiryBinding, SqlDbKvSet, SqlDbRow
from kvtsjl.batching import chunk_sequence
from kvtsjl.bind import (
    CollectionBinding,
    NamespaceBinder,
    NativeCollectionBinder,
)
from kvtsjl.exceptions import (
    KvStoreError,
    KvStoreScanUnsupported,
    KvStoreScopeError,
    KvStoreUnsupported,
)
from kvtsjl.keymap import ConditionalWrite, Put, Remove, Versioned
from kvtsjl.scope import Scope, ScopeSegment
from kvtsjl.store import KvBackend
from kvtsjl.store.leaf import LeafKvStore
from kvtsjl.store.schema.layout import KeyLayout, ScanQuery
from kvtsjl.store.schema.scan import (
    After,
    AlwaysTrue,
    CompiledScan,
    FieldContains,
    FieldEq,
    FieldIn,
    FieldIsNull,
    FieldStartsWith,
    FieldTest,
    KeyPrefix,
    OrderBy,
    Predicate,
    Projection,
    ScanCapabilities,
    ScanCursor,
    ScanOrder,
    ScanRow,
    ScanSpec,
    ScanStat,
    compile_from_scan_entries,
    conjoin,
    enforce_strict,
    peel_key_prefix,
    split_by_groups,
    split_where,
)
from kvtsjl.store.schema.stat import RecordStat
from kvtsjl.store.schema.ttl import (
    TTL_NONE_EXPIRES_AT,
    ExpiryGc,
    TtlPolicy,
    require_write_clock_supported,
)


def _utc_from_epoch(epoch: float | None) -> datetime | None:
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=UTC)


def _clock_base(created_at: datetime | None) -> float:
    if created_at is None:
        return time.time()
    if created_at.tzinfo is None:
        return created_at.replace(tzinfo=UTC).timestamp()
    return created_at.timestamp()


def _as_timestamp(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return datetime.fromisoformat(value).timestamp()
    raise TypeError(f"unsupported timestamp type: {type(value).__name__}")


def _key_tuple(row: SqlDbRow, key_fields: Sequence[str]) -> tuple[Any, ...]:
    return tuple(row[f] for f in key_fields)


class SqlDbKvBackend[K, V](KvBackend[K, V, str, SqlDbRow, str]):
    """Row-oriented KvBackend over a SQL table via ``SqlDbClientAdapter``.

    Physical PK = ``scope_fields`` (from ``scoped(...)``) + ``leaf_key_fields``
    (from leaf ``K``). Composite leaf keys are sequences/mappings aligned to
    ``leaf_key_fields``. Scan uses per-part string prefixes (empty / omitted =
    unconstrained), matching other kvtsjl prefix-scan semantics.
    """

    def __init__(
        self,
        sql_kvset: SqlDbKvSet[K, V],
        *,
        adapter: SqlDbClientAdapter,
        expiry_gc: ExpiryGc = ExpiryGc.LAZY_DELETE,
        scope: Scope | None = None,
        binder: NamespaceBinder[str, str] | None = None,
        binding: CollectionBinding[str, str] | None = None,
        batch_size: int = 500,
    ) -> None:
        table = sql_kvset.table_name
        if binder is None and binding is None:
            binder = NativeCollectionBinder[str, str](
                collection_formatter=lambda _ref: table
            )
        super().__init__(
            sql_kvset.kvset,
            scope=scope,
            binder=binder,
            binding=binding,
            batch_size=batch_size,
        )
        self._sql_kvset = sql_kvset
        self._adapter = adapter
        self._expiry_gc = expiry_gc
        assert self._binding.collection is not None
        self._table = self._binding.collection
        self._early_binding_checked = False

    @property
    def supports_write_clock(self) -> bool:
        return True

    def _clone(
        self,
        *,
        scope: Scope | None = None,
        adapter: SqlDbClientAdapter | None = None,
    ) -> SqlDbKvBackend[K, V]:
        """Single clone path for scope and adapter rebinding."""
        return SqlDbKvBackend(
            self._sql_kvset,
            adapter=self._adapter if adapter is None else adapter,
            expiry_gc=self._expiry_gc,
            scope=self.scope if scope is None else scope,
            binding=self._binding,
            batch_size=self.batch_size,
        )

    def clone_with_scope(self, scope: Scope) -> SqlDbKvBackend[K, V]:
        return self._clone(scope=scope)

    def clone_with_adapter(
        self, adapter: SqlDbClientAdapter
    ) -> SqlDbKvBackend[K, V]:
        return self._clone(adapter=adapter)

    def _presence_columns(self) -> tuple[str, ...]:
        sk = self._sql_kvset
        return (*sk.key_fields, sk.updated_at_field, sk.expires_at_field, *self._held_by())

    def _held_by(self) -> tuple[str, ...]:
        """The columns whose being set means this view holds a row.

        Only on a shared table: there the row is its siblings' too, and the
        view holds it while one of its own columns is set. A view that owns
        its table holds every row in it.
        """
        sk = self._sql_kvset
        return sk.own_columns if sk.shares_table else ()

    def _holds(self, row: SqlDbRow) -> bool:
        held_by = self._held_by()
        return not held_by or any(row.get(c) is not None for c in held_by)

    def _expires_at_column(
        self,
        ttl: TtlPolicy | None,
        *,
        created_at: datetime | None = None,
    ) -> float | None:
        """Bind a concrete death time on every write.

        Relative collection TTL and per-write ``ttl=`` both store
        ``expires_at = clock + seconds``. Explicit ``TtlPolicy.none()`` stores
        the far-future sentinel. Only a kvset with **no** TTL policy writes
        NULL, which then means "never expires".
        """
        secs = self.resolve_ttl_seconds(ttl)
        if secs is not None:
            return _clock_base(created_at) + secs
        if ttl is not None:
            # Explicit never — keep distinct from "no policy" NULL.
            return TTL_NONE_EXPIRES_AT.timestamp()
        return None

    def _scope_by_kind(self) -> dict[str, str]:
        return {s.kind: s.id for s in self.scope.segments}

    def _scope_key_row(self, *, require_all: bool) -> SqlDbRow:
        sk = self._sql_kvset
        if not sk.scope_fields:
            return {}
        by_kind = self._scope_by_kind()
        row: SqlDbRow = {}
        for field in sk.scope_fields:
            if field in by_kind:
                row[field] = by_kind[field]
            elif require_all:
                raise KvStoreScopeError(
                    f"SqlDbKvStore requires scope field {field!r} "
                    f"(have {sorted(by_kind)!r})"
                )
        return row

    def _leaf_key_row(self, key: K) -> SqlDbRow:
        sk = self._sql_kvset
        fields = sk.leaf_key_fields
        if len(fields) == 1:
            if sk.scope_fields:
                blob = self.kvset.key_serde.serialize(key)
            else:
                # No scope columns: keep binder/scope-in-key blob behavior.
                blob = self._physical_key_blob(key)
            return {fields[0]: blob}
        if isinstance(key, Mapping):
            return {f: key[f] for f in fields}
        seq = cast(Sequence[Any], key)
        return {f: v for f, v in zip(fields, seq, strict=True)}

    def _full_key_row(self, key: K, *, require_scope: bool) -> SqlDbRow:
        row = self._scope_key_row(require_all=require_scope)
        row.update(self._leaf_key_row(key))
        return row

    def _decode_leaf_key(self, row: SqlDbRow) -> K | None:
        sk = self._sql_kvset
        fields = sk.leaf_key_fields
        if len(fields) == 1:
            blob = str(row[fields[0]])
            if sk.scope_fields:
                return self.kvset.key_serde.deserialize(blob)
            return self._decode_key_from_physical(blob)
        values = tuple(row[f] for f in fields)
        return cast(K, values)

    def _leaf_prefix_map(self, key_prefix: K | None) -> dict[str, str]:
        sk = self._sql_kvset
        fields = sk.leaf_key_fields
        if len(fields) == 1 and not sk.scope_fields:
            # Legacy: scope is embedded in the leaf string key.
            blob = self._scan_prefix_blob(key_prefix)
            return {fields[0]: blob} if blob else {}
        if key_prefix is None:
            return {}
        if len(fields) == 1:
            blob = self.kvset.key_serde.serialize(key_prefix)
            text = str(blob)
            return {fields[0]: text} if text else {}
        if isinstance(key_prefix, Mapping):
            out: dict[str, str] = {}
            for f in fields:
                if f not in key_prefix:
                    continue
                val = key_prefix[f]
                if val is None or val == "":
                    continue
                out[f] = str(val)
            return out
        seq = cast(Sequence[Any], key_prefix)
        out = {}
        for f, val in zip(fields, seq):
            if val is None or val == "":
                continue
            out[f] = str(val)
        return out

    def _row_expired_at(self, row: SqlDbRow, *, now: float) -> bool:
        expires = _as_timestamp(row.get(self._sql_kvset.expires_at_field))
        if expires is not None:
            return now >= expires
        ttl = self.ttl_seconds()
        if ttl is None:
            return False
        updated = _as_timestamp(row.get(self._sql_kvset.updated_at_field))
        if updated is None:
            return False
        return now >= updated + ttl

    def _row_expired(self, row: SqlDbRow) -> bool:
        return self._row_expired_at(row, now=time.time())

    def _death_time(self, row: SqlDbRow) -> float | None:
        """Epoch when the row dies, or ``None`` if it never expires."""
        expires = _as_timestamp(row.get(self._sql_kvset.expires_at_field))
        if expires is not None:
            return expires
        ttl = self.ttl_seconds()
        if ttl is None:
            return None
        updated = _as_timestamp(row.get(self._sql_kvset.updated_at_field))
        if updated is None:
            return None
        return updated + ttl

    def _ensure_early_binding_safe(self) -> None:
        """Refuse early binding while relative-TTL NULL rows remain."""
        if self._early_binding_checked:
            return
        self._early_binding_checked = True
        if self._sql_kvset.expiry_binding is not ExpiryBinding.EARLY:
            return
        if self.ttl_seconds() is None:
            # NULL legitimately means never expires.
            return
        exact = self._scope_key_row(require_all=False)
        if self._adapter.supports_expiry_scan():
            has_null = self._adapter.has_null_expires_at(
                table=self._table,
                exact=exact,
                expires_at_field=self._sql_kvset.expires_at_field,
            )
        else:
            has_null = False
            sk = self._sql_kvset
            for row in self._adapter.scan_by_key_parts(
                table=self._table,
                columns=(sk.expires_at_field, *sk.key_fields),
                key_fields=sk.key_fields,
                exact=exact,
                prefixes={},
            ):
                if row.get(sk.expires_at_field) is None:
                    has_null = True
                    break
        if has_null:
            raise KvStoreError(
                "expiry_binding='early' but un-backfilled rows remain"
            )

    def _relative_ttl_for_expiry_scan(self) -> int | None:
        """TTL seconds for the relative NULL branch, or ``None`` when unused."""
        if self._sql_kvset.expiry_binding is ExpiryBinding.EARLY:
            return None
        return self.ttl_seconds()

    def expired_keys(
        self, *, now: datetime | None = None, limit: int
    ) -> list[K]:
        """Oldest-expired keys first, bounded, under the bound scope. Read-only."""
        return [k for k in map(self._decode_leaf_key, self._expired_rows(now, limit)) if k is not None]

    def gc_expired_keys(self, *, max_entries: int) -> list[K]:
        """Delete expired rows by the full key each row carries.

        Under a partial scope a leaf key does not name one row — two parents
        may both hold child ``a`` — so the rows found are deleted by their own
        keys rather than by leaf keys re-scoped to the store.
        """
        rows = self._expired_rows(None, max_entries)
        if rows:
            sk = self._sql_kvset
            self._adapter.delete_by_keys(
                table=self._table,
                key_fields=sk.key_fields,
                keys=[{f: row[f] for f in sk.key_fields} for row in rows],
            )
        return [k for k in map(self._decode_leaf_key, rows) if k is not None]

    def _expired_rows(self, now: datetime | None, limit: int) -> list[SqlDbRow]:
        if limit < 1:
            raise ValueError(f"limit must be >= 1, got {limit}")
        self._ensure_early_binding_safe()
        now_epoch = _clock_base(now)
        sk = self._sql_kvset
        exact = self._scope_key_row(require_all=False)
        if self._adapter.supports_expiry_scan():
            return list(
                self._adapter.scan_expired(
                    table=self._table,
                    columns=sk.select_columns(),
                    key_fields=sk.key_fields,
                    exact=exact,
                    expires_at_field=sk.expires_at_field,
                    updated_at_field=sk.updated_at_field,
                    relative_ttl_seconds=self._relative_ttl_for_expiry_scan(),
                    now=now_epoch,
                    limit=limit,
                )
            )
        scored: list[tuple[float, SqlDbRow]] = []
        for row in self._adapter.scan_by_key_parts(
            table=self._table,
            columns=sk.select_columns(),
            key_fields=sk.key_fields,
            exact=exact,
            prefixes=self._leaf_prefix_map(None),
        ):
            if not self._row_expired_at(row, now=now_epoch):
                continue
            death = self._death_time(row)
            if death is None:
                continue
            scored.append((death, row))
        scored.sort(key=lambda item: item[0])
        return [row for _, row in scored[:limit]]

    def _live_row(self, row: SqlDbRow | None) -> SqlDbRow | None:
        if row is None:
            return None
        if not self._row_expired(row):
            return row if self._holds(row) else None
        if self._expiry_gc is ExpiryGc.LAZY_DELETE:
            sk = self._sql_kvset
            key_row = {f: row[f] for f in sk.key_fields}
            self._adapter.delete_by_keys(
                table=self._table,
                key_fields=sk.key_fields,
                keys=[key_row],
            )
        return None

    def _build_write_row(
        self,
        key: K,
        value: V,
        *,
        ttl: TtlPolicy | None,
        created_at: datetime | None = None,
    ) -> SqlDbRow:
        serialized = dict(self.kvset.value_serde.serialize(value))
        sk = self._sql_kvset
        serialized.update(self._full_key_row(key, require_scope=True))
        base = _clock_base(created_at)
        serialized[sk.updated_at_field] = base
        serialized[sk.expires_at_field] = self._expires_at_column(
            ttl, created_at=created_at
        )
        return {col: serialized.get(col) for col in sk.write_columns()}

    def _stat_from_row(self, row: SqlDbRow) -> RecordStat:
        sk = self._sql_kvset
        updated = _as_timestamp(row.get(sk.updated_at_field))
        expires = _as_timestamp(row.get(sk.expires_at_field))
        if expires is None:
            ttl = self.ttl_seconds()
            if ttl is not None and updated is not None:
                expires = updated + ttl
        persistent = expires is None
        return RecordStat(
            created_at=_utc_from_epoch(updated),
            modified_at=_utc_from_epoch(updated),
            expires_at=_utc_from_epoch(expires),
            persistent=persistent,
        )

    def get(self, key: K) -> V | None:
        sk = self._sql_kvset
        key_row = self._full_key_row(key, require_scope=True)
        found = self._adapter.fetch_by_keys(
            table=self._table,
            columns=sk.select_columns(),
            key_fields=sk.key_fields,
            keys=[key_row],
        )
        row = self._live_row(found[0] if found else None)
        if row is None:
            return None
        return self.kvset.value_serde.deserialize(row)

    def exists(self, key: K) -> bool:
        sk = self._sql_kvset
        key_row = self._full_key_row(key, require_scope=True)
        # Presence columns only (keys + TTL clock); still honour ExpiryGc via _live_row.
        found = self._adapter.exists_by_keys(
            table=self._table,
            key_fields=sk.key_fields,
            keys=[key_row],
            columns=self._presence_columns(),
        )
        return self._live_row(found[0] if found else None) is not None

    def batch_exists(self, keys: Sequence[K]) -> set[K]:
        keys = list(dict.fromkeys(keys))
        held = self.batch_exists_across([(self, k) for k in keys])
        return {k for k, h in zip(keys, held, strict=True) if h}

    def batch_exists_across(self, pairs: Sequence[tuple[KvBackend[K, V, str, SqlDbRow, str], K]]) -> list[bool]:
        """One presence query per batch, across scopes: the scope is in the key columns."""
        return [row is not None for row in self._live_rows(pairs, presence=True)]

    def _key_rows(self, pairs: Sequence[tuple[KvBackend[K, V, str, SqlDbRow, str], K]]) -> list[SqlDbRow]:
        """Each key's full key row, scope columns included, from its scope's clone."""
        return [self._own(b)._full_key_row(k, require_scope=True) for b, k in pairs]

    def _found_rows(
        self, pairs: Sequence[tuple[KvBackend[K, V, str, SqlDbRow, str], K]], *, presence: bool
    ) -> list[SqlDbRow | None]:
        """The stored row of each key, live or not, in order: one query per batch."""
        sk = self._sql_kvset
        out: list[SqlDbRow | None] = []
        for chunk in chunk_sequence(self._key_rows(pairs), self.batch_size):
            if presence:
                found = self._adapter.exists_by_keys(
                    table=self._table,
                    key_fields=sk.key_fields,
                    keys=chunk,
                    columns=self._presence_columns(),
                )
            else:
                found = self._adapter.fetch_by_keys(
                    table=self._table,
                    columns=sk.select_columns(),
                    key_fields=sk.key_fields,
                    keys=chunk,
                )
            by_tuple = {_key_tuple(r, sk.key_fields): r for r in found}
            out.extend(by_tuple.get(_key_tuple(r, sk.key_fields)) for r in chunk)
        return out

    def _live_rows(
        self, pairs: Sequence[tuple[KvBackend[K, V, str, SqlDbRow, str], K]], *, presence: bool
    ) -> list[SqlDbRow | None]:
        return [self._live_row(row) for row in self._found_rows(pairs, presence=presence)]

    def stat(self, key: K) -> RecordStat | None:
        sk = self._sql_kvset
        key_row = self._full_key_row(key, require_scope=True)
        found = self._adapter.exists_by_keys(
            table=self._table,
            key_fields=sk.key_fields,
            keys=[key_row],
            columns=self._presence_columns(),
        )
        row = self._live_row(found[0] if found else None)
        if row is None:
            return None
        return self._stat_from_row(row)

    def batch_stat(self, keys: Sequence[K]) -> dict[K, RecordStat]:
        keys = list(dict.fromkeys(keys))
        got = self.batch_stat_across([(self, k) for k in keys])
        return {k: m for k, m in zip(keys, got, strict=True) if m is not None}

    def batch_stat_across(self, pairs: Sequence[tuple[KvBackend[K, V, str, SqlDbRow, str], K]]) -> list[RecordStat | None]:
        return [
            None if row is None else self._stat_from_row(row)
            for row in self._live_rows(pairs, presence=True)
        ]

    def set(
        self,
        key: K,
        value: V,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        require_write_clock_supported(
            created_at, allowed=True, backend="SqlDbKvStore"
        )
        self._write_rows([self._build_write_row(key, value, ttl=ttl, created_at=created_at)])

    @property
    def writes_are_deferred(self) -> bool:
        return self._adapter.defers_commit()

    def _write_rows(self, rows: Sequence[SqlDbRow]) -> int:
        """One write path, choosing the shape the kvset's sharing demands.

        A view that does not own its shared table's rows only updates rows
        that exist: creating one would make a record its owner never made.
        Its write to a missing record is passed over, as a patch index view's
        is. Returns how many rows were written.
        """
        sk = self._sql_kvset
        if sk.shares_table and not sk.owns_rows:
            if not self._adapter.supports_update_rows():
                raise KvStoreUnsupported(
                    f"{type(self._adapter).__name__} does not support update_rows, "
                    "which a view of a shared table that does not own its rows "
                    "requires"
                )
            return self._adapter.update_rows(
                table=self._table,
                key_fields=sk.key_fields,
                write_columns=sk.write_columns(),
                rows=rows,
            )
        if not sk.shares_table:
            self._adapter.upsert_rows(
                table=self._table,
                key_fields=sk.key_fields,
                write_columns=sk.write_columns(),
                rows=rows,
            )
            return len(rows)
        if not self._adapter.supports_update_first():
            # Falling back to upsert is the write that cannot succeed on a
            # shared table, so name the adapter instead of failing later with
            # a NOT NULL error that points nowhere.
            raise KvStoreUnsupported(
                f"{type(self._adapter).__name__} does not support "
                "upsert_rows_update_first, which shares_table=True requires"
            )
        self._adapter.upsert_rows_update_first(
            table=self._table,
            key_fields=sk.key_fields,
            write_columns=sk.write_columns(),
            rows=rows,
        )
        return len(rows)

    def _version_columns(self) -> tuple[str, ...]:
        """What a version compares: the columns this view writes, clock included.

        The clock makes a rewrite of the same value a change all the same.
        """
        keys = set(self._sql_kvset.key_fields)
        return tuple(c for c in self._sql_kvset.write_columns() if c not in keys)

    def batch_read_versioned(self, keys: Sequence[K]) -> dict[K, Versioned[V]]:
        keys = list(dict.fromkeys(keys))
        return dict(zip(keys, self.batch_read_versioned_across([(self, k) for k in keys]), strict=True))

    def batch_read_versioned_across(self, pairs: Sequence[tuple[KvBackend[K, V, str, SqlDbRow, str], K]]) -> list[Versioned[V]]:
        """One fetch per batch; a row's version is its written columns, clock included."""
        out: list[Versioned[V]] = []
        for row in self._found_rows(pairs, presence=False):
            if row is None:
                out.append(Versioned.absent())
                continue
            live = not self._row_expired(row) and self._holds(row)
            value = self.kvset.value_serde.deserialize(row) if live else None
            out.append(Versioned(value, tuple(row.get(c) for c in self._version_columns())))
        return out

    def batch_write_if_unchanged(
        self, writes: Mapping[K, ConditionalWrite[V]], *, ttl: TtlPolicy | None = None
    ) -> set[K]:
        keys = list(writes)
        won = self.batch_write_if_unchanged_across([(self, k, writes[k]) for k in keys], ttl=ttl)
        return {k for k, w in zip(keys, won, strict=True) if w}

    def batch_write_if_unchanged_across(
        self,
        writes: Sequence[tuple[KvBackend[K, V, str, SqlDbRow, str], K, ConditionalWrite[V]]],
        *,
        ttl: TtlPolicy | None = None,
    ) -> list[bool]:
        """Insert where no row is, or replace or remove rows only if unchanged.

        One adapter call per kind of write per batch, across scopes: the
        inserts, the replacements and the removals. A view of a shared table
        that does not own its rows refuses to insert one — only an owner
        creates; removing through it clears its own columns, as ``unset`` does.
        """
        for _, _, w in writes:
            require_write_clock_supported(w.created_at, allowed=True, backend="SqlDbKvStore")
        sk = self._sql_kvset
        at = [(self._own(b), k, w) for b, k, w in writes]
        inserts = [
            (i, b._build_write_row(k, w.change.value, ttl=ttl, created_at=w.created_at))
            for i, (b, k, w) in enumerate(at)
            if w.seen.version is None and isinstance(w.change, Put)
        ]
        replaces = [
            (i, b._expected(k, w.seen), b._build_write_row(k, w.change.value, ttl=ttl, created_at=w.created_at))
            for i, (b, k, w) in enumerate(at)
            if w.seen.version is not None and isinstance(w.change, Put)
        ]
        removals = [
            (i, b._expected(k, w.seen))
            for i, (b, k, w) in enumerate(at)
            if w.seen.version is not None and isinstance(w.change, Remove)
        ]
        if inserts and not self.creates_entries:
            raise KvStoreUnsupported(
                f"{sk.table_name}: this view does not own its rows, so it cannot "
                f"create one for {at[inserts[0][0]][1]!r}; its owner creates the record"
            )
        if (replaces or removals) and not self._adapter.supports_compare_and_write():
            raise KvStoreUnsupported(
                f"{type(self._adapter).__name__} does not support compare-and-write, "
                "which writing against a version requires; it will not read and "
                "then write"
            )
        if removals and not sk.deletes_rows and not sk.own_columns:
            raise KvStoreUnsupported(
                f"{sk.table_name}: this view writes no columns of its own, so "
                "removing through it could only delete a row its siblings share"
            )
        if inserts and not self._adapter.supports_insert_if_absent():
            raise KvStoreUnsupported(
                f"{type(self._adapter).__name__} does not support "
                "insert_each_if_absent, which set_if_absent — inserting only "
                "where no row is — requires; it will not fall back to a get-then-set"
            )
        won = [False] * len(writes)
        for chunk in chunk_sequence(inserts, self.batch_size):
            flags = self._adapter.insert_each_if_absent(
                table=self._table,
                key_fields=sk.key_fields,
                write_columns=sk.write_columns(),
                rows=[row for _, row in chunk],
            )
            for (i, _), flag in zip(chunk, flags, strict=True):
                won[i] = flag
        for chunk in chunk_sequence(removals, self.batch_size):
            expected = [e for _, e in chunk]
            if sk.deletes_rows:
                flags = self._adapter.delete_each_if(
                    table=self._table, key_fields=sk.key_fields, expected=expected
                )
            else:
                flags = self._adapter.replace_each_if(
                    table=self._table,
                    key_fields=sk.key_fields,
                    write_columns=sk.own_columns,
                    expected=expected,
                    rows=[dict.fromkeys(sk.own_columns) for _ in chunk],
                )
            for (i, _), flag in zip(chunk, flags, strict=True):
                won[i] = flag
        for chunk in chunk_sequence(replaces, self.batch_size):
            flags = self._adapter.replace_each_if(
                table=self._table,
                key_fields=sk.key_fields,
                write_columns=sk.write_columns(),
                expected=[e for _, e, _ in chunk],
                rows=[row for _, _, row in chunk],
            )
            for (i, _, _), flag in zip(chunk, flags, strict=True):
                won[i] = flag
        return won

    def _expected(self, key: K, seen: Versioned[V]) -> SqlDbRow:
        """The key and the version's columns, which the row must still hold."""
        version = seen.version
        if not isinstance(version, tuple):
            raise KvStoreError(f"{version!r} is not a version this table gave out")
        return {
            **self._full_key_row(key, require_scope=True),
            **dict(zip(self._version_columns(), version, strict=True)),
        }

    def delete(self, key: K) -> bool:
        return self.batch_delete([key]) > 0

    @property
    def creates_entries(self) -> bool:
        return self._sql_kvset.owns_rows or not self._sql_kvset.shares_table

    def unset(self, key: K) -> bool:
        """Remove this view's value for ``key``, and nothing its siblings hold.

        A view that owns its table deletes the row. A view of a shared table
        nulls its own columns and leaves the row; with no columns of its own it
        refuses, since the only thing left to remove is a row it shares.
        """
        return self.batch_unset([key]) > 0

    def batch_unset(self, keys: Sequence[K]) -> int:
        return self.batch_unset_across([(self, k) for k in keys])

    def batch_unset_across(self, pairs: Sequence[tuple[KvBackend[K, V, str, SqlDbRow, str], K]]) -> int:
        if not self._sql_kvset.shares_table:
            return self._delete_rows(pairs)
        return self._clear_own_columns(pairs)

    def _clear_own_columns(self, pairs: Sequence[tuple[KvBackend[K, V, str, SqlDbRow, str], K]]) -> int:
        sk = self._sql_kvset
        if not sk.own_columns:
            raise KvStoreUnsupported(
                f"{sk.table_name}: this view writes no columns of its own, so "
                "clearing it could only delete a row its siblings share"
            )
        if not self._adapter.supports_clear_columns():
            raise KvStoreUnsupported(
                f"{type(self._adapter).__name__} does not support clear_columns, "
                "which unsetting a view of a shared table requires"
            )
        cleared = 0
        for chunk in chunk_sequence(self._key_rows(list(dict.fromkeys(pairs))), self.batch_size):
            cleared += self._adapter.clear_columns(
                table=self._table,
                key_fields=sk.key_fields,
                keys=chunk,
                columns=sk.own_columns,
            )
        return cleared

    def batch_get(self, keys: Sequence[K]) -> dict[K, V]:
        keys = list(dict.fromkeys(keys))
        got = self.batch_get_across([(self, k) for k in keys])
        return {k: v for k, v in zip(keys, got, strict=True) if v is not None}

    def batch_get_across(self, pairs: Sequence[tuple[KvBackend[K, V, str, SqlDbRow, str], K]]) -> list[V | None]:
        """One fetch per batch, across scopes: the scope is in the key columns."""
        return [
            None if row is None else self.kvset.value_serde.deserialize(row)
            for row in self._live_rows(pairs, presence=False)
        ]

    def batch_set(
        self,
        items: Mapping[K, V],
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        self.batch_set_across(
            [(self, k, v) for k, v in items.items()], ttl=ttl, created_at=created_at
        )

    def batch_set_across(
        self,
        items: Sequence[tuple[KvBackend[K, V, str, SqlDbRow, str], K, V]],
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        require_write_clock_supported(
            created_at, allowed=True, backend="SqlDbKvStore"
        )
        rows = [
            self._own(b)._build_write_row(k, v, ttl=ttl, created_at=created_at)
            for b, k, v in items
        ]
        for chunk in chunk_sequence(rows, self.batch_size):
            self._write_rows(chunk)

    def batch_delete(self, keys: Sequence[K]) -> int:
        """Remove the rows — or, from a view that does not own them, its columns.

        On a shared table only the view that ``owns_rows`` removes a row;
        deleting through any other view removes what that view wrote, so a
        sibling's columns and the record survive.
        """
        return self.batch_delete_across([(self, k) for k in keys])

    def batch_delete_across(self, pairs: Sequence[tuple[KvBackend[K, V, str, SqlDbRow, str], K]]) -> int:
        if self._sql_kvset.deletes_rows:
            return self._delete_rows(pairs)
        return self._clear_own_columns(pairs)

    def _delete_rows(self, pairs: Sequence[tuple[KvBackend[K, V, str, SqlDbRow, str], K]]) -> int:
        deleted = 0
        sk = self._sql_kvset
        for chunk in chunk_sequence(self._key_rows(list(dict.fromkeys(pairs))), self.batch_size):
            deleted += self._adapter.delete_by_keys(
                table=self._table,
                key_fields=sk.key_fields,
                keys=chunk,
            )
        return deleted

    def scan_capabilities(self) -> ScanCapabilities:
        """Report what SQL actually provides.

        ``KEY_ASC`` rests on the adapter contract: ``scan_by_key_parts`` and
        ``select`` yield rows in ascending ``key_fields`` order unless told
        otherwise. With ``select`` the adapter also takes the field tests on
        the view's declared columns, ``order_by`` over them and its
        ``after``, and keyset pages that a ``ScanCursor`` resumes; without it
        the store runs every filter itself and sorts ``order_by`` in process.
        """
        selects = self._adapter.supports_select()
        pushdown: set[type] = {KeyPrefix}
        if selects:
            pushdown |= {FieldEq, FieldIn, FieldStartsWith, FieldContains, FieldIsNull}
        return ScanCapabilities(
            pushdown=frozenset(pushdown),
            orders=frozenset({ScanOrder.UNORDERED, ScanOrder.KEY_ASC}),
            resumable=selects,
            # Only the ordered path fills ScanRow.stat; the default path goes
            # through compile_from_scan_entries, which has no stat to give.
            # Claiming True here would be the same wrong capability report this
            # change set out to fix.
            stats=False,
            orderable_columns=frozenset(self._sql_kvset.select_columns()) if selects else None,
            distinct_columns=(
                frozenset(self._sql_kvset.select_columns())
                if selects and self._adapter.supports_distinct_on()
                else None
            ),
        )

    def _peel_field_tests(
        self, where: Predicate[K]
    ) -> tuple[Predicate[K] | None, Predicate[K]]:
        """The tests of this view's own columns, for SQL to run, and the rest."""
        columns = {f.name for f in self._sql_kvset.fields}
        taken, rest = split_where(
            where, lambda p: isinstance(p, FieldTest) and p.of.name in columns
        )
        return (None if isinstance(taken, AlwaysTrue) else taken), rest

    def _resolve_order_by(self, order_by: Sequence[OrderBy[Any]]) -> list[SqlOrder]:
        """The columns to order by: ``order_by``'s, then the key fields ascending.

        The key fields make the order total, and ascending is the tiebreak
        every store sorts by in process — by key, ascending — so SQL and
        Python agree on which of two rows with equal values comes first.
        """
        allowed = frozenset(self._sql_kvset.select_columns())
        for item in order_by:
            if item.column not in allowed:
                raise KvStoreScanUnsupported(
                    f"order_by column {item.column!r} is not a declared column "
                    f"(have {sorted(allowed)!r})"
                )
        resolved = [SqlOrder(item.column, item.descending) for item in order_by]
        named = {item.column for item in resolved}
        for field_name in self._sql_kvset.key_fields:
            if field_name not in named:
                resolved.append(SqlOrder(field_name))
        return resolved

    def _after_row(self, after: After[K], order: Sequence[SqlOrder]) -> SqlDbRow:
        """``after`` as the leading columns of ``order``: its values, then its key's."""
        row: SqlDbRow = {
            item.column: value for item, value in zip(order, after.values, strict=False)
        }
        if after.key is not None:
            key_row = self._full_key_row(after.key, require_scope=True)
            for item in order[len(after.values) :]:
                row[item.column] = key_row[item.column]
        return row

    def compile_scan(self, spec: ScanSpec[K]) -> CompiledScan[K]:
        if not spec.order_by:
            if self._adapter.supports_select():
                return self._compile_keyset_scan(spec)
            return compile_from_scan_entries(
                self.scan_entries, spec, backend=type(self).__name__
            )
        if self.kvset.key_layout is not KeyLayout.LITERAL:
            raise KvStoreScanUnsupported(
                "SqlDbKvStore scan requires KeyLayout.LITERAL "
                f"(got {self.kvset.key_layout!r})"
            )
        order_by = self._resolve_order_by(spec.order_by)
        pushed = {"order_by"}
        kept: Predicate[K] | None = None
        if spec.distinct_on:
            # Ranking sees every live row of the view: only a test of the
            # group's columns runs before it; the rest tests the rows kept,
            # in SQL where it can, and a key prefix in Python.
            before, after_ranking = split_by_groups(spec)
            user_prefix = None
            tests, before_rest = self._peel_field_tests(before)
            kept, residual = self._peel_field_tests(after_ranking)
            residual = conjoin(before_rest, residual)
            pushed.add("distinct_on")
        else:
            user_prefix, rest = peel_key_prefix(spec.where)
            tests, residual = self._peel_field_tests(rest)
        if spec.after is not None:
            pushed.add("after")
        if user_prefix is not None:
            pushed.add("KeyPrefix")
        if tests is not None or kept is not None:
            pushed.add("FieldTest")
        # Push LIMIT only when nothing is left to filter client-side; otherwise
        # a pushed LIMIT 20 plus residual filtering returns fewer than 20 rows.
        limit: int | None = None
        if (
            spec.limit is not None
            and isinstance(residual, AlwaysTrue)
            and spec.refine is None
        ):
            limit = spec.limit
            pushed.add("limit")
        rows = self._iter_ordered_rows(
            user_prefix=user_prefix,
            tests=tests,
            order_by=order_by,
            after=None if spec.after is None else self._after_row(spec.after, order_by),
            distinct_on=[f.name for f in spec.distinct_on],
            kept=kept,
            limit=limit,
            include_values=spec.project is Projection.ENTRIES,
        )
        enforce_strict(spec, residual, backend=type(self).__name__)
        return CompiledScan(rows=rows, residual=residual, pushed=frozenset(pushed))

    def _compile_keyset_scan(self, spec: ScanSpec[K]) -> CompiledScan[K]:
        if self.kvset.key_layout is not KeyLayout.LITERAL:
            raise KvStoreScanUnsupported(
                "SqlDbKvStore scan requires KeyLayout.LITERAL "
                f"(got {self.kvset.key_layout!r})"
            )
        user_prefix, rest = peel_key_prefix(spec.where)
        tests, residual = self._peel_field_tests(rest)
        pushed = {"keyset"}
        if user_prefix is not None:
            pushed.add("KeyPrefix")
        if tests is not None:
            pushed.add("FieldTest")
        if spec.cursor is not None:
            pushed.add("cursor")
        after_key = (
            self._after_key_from_cursor(spec.cursor)
            if spec.cursor is not None
            else None
        )
        rows = self._iter_keyset_rows(
            user_prefix=user_prefix,
            tests=tests,
            after_key=after_key,
            page_size=spec.page_size,
            include_values=spec.project is Projection.ENTRIES,
            limit=spec.limit,
            decode=self._decode_leaf_key,
        )
        enforce_strict(spec, residual, backend=type(self).__name__)
        return CompiledScan(rows=rows, residual=residual, pushed=frozenset(pushed))

    def _after_key_from_cursor(self, cursor: ScanCursor) -> SqlDbRow:
        """Decode a ``ScanCursor`` into the full SQL key row for keyset resume.

        Single-leaf keys use the leaf string as the token (same shape as memory's
        ``ScanCursor(token=last_key)``). Composite leaves use a JSON array of
        leaf field values — opaque, stable, and independent of ``str(tuple)``.
        """
        sk = self._sql_kvset
        fields = sk.leaf_key_fields
        try:
            if len(fields) == 1:
                blob = cursor.token
                if sk.scope_fields:
                    key = self.kvset.key_serde.deserialize(blob)
                else:
                    decoded = self._decode_key_from_physical(blob)
                    if decoded is None:
                        raise KvStoreError(
                            f"invalid scan cursor token for SqlDbKvStore: "
                            f"{cursor.token!r}"
                        )
                    key = decoded
                return self._full_key_row(key, require_scope=True)
            values = json.loads(cursor.token)
            if not isinstance(values, list) or len(values) != len(fields):
                raise KvStoreError(
                    f"scan cursor token must be a JSON array of "
                    f"{len(fields)} leaf key parts"
                )
            key = cast(K, tuple(values))
            return self._full_key_row(key, require_scope=True)
        except (TypeError, ValueError, KeyError, json.JSONDecodeError) as exc:
            raise KvStoreError(
                f"invalid scan cursor token for SqlDbKvStore: {cursor.token!r}"
            ) from exc

    def _iter_keyset_rows[T](
        self,
        *,
        user_prefix: K | None,
        tests: Predicate[K] | None,
        after_key: SqlDbRow | None,
        page_size: int,
        include_values: bool,
        limit: int | None,
        decode: Callable[[SqlDbRow], T | None],
    ) -> Iterator[ScanRow[T, V]]:
        if page_size < 1:
            raise ValueError(f"page_size must be >= 1, got {page_size}")
        sk = self._sql_kvset
        exact = self._scope_key_row(require_all=False)
        prefixes = self._leaf_prefix_map(user_prefix)
        key_order = tuple(SqlOrder(f) for f in sk.key_fields)
        expired_keys: list[SqlDbRow] = []
        after = after_key
        yielded = 0
        while True:
            page = list(
                self._adapter.select(
                    SqlSelect(
                        table=self._table,
                        columns=sk.select_columns(),
                        key_fields=sk.key_fields,
                        order_by=key_order,
                        exact=exact,
                        prefixes=prefixes,
                        where=tests,
                        after=after,
                        limit=page_size,
                    )
                )
            )
            if not page:
                break
            for row in page:
                if self._row_expired(row):
                    if self._expiry_gc is ExpiryGc.LAZY_DELETE:
                        expired_keys.append({f: row[f] for f in sk.key_fields})
                    continue
                if not self._holds(row):
                    continue
                decoded = decode(row)
                if decoded is None:
                    continue
                value = (
                    self.kvset.value_serde.deserialize(row) if include_values else None
                )
                yield ScanRow(key=decoded, value=value)
                yielded += 1
                if limit is not None and yielded >= limit:
                    if expired_keys:
                        self._adapter.delete_by_keys(
                            table=self._table,
                            key_fields=sk.key_fields,
                            keys=expired_keys,
                        )
                    return
            advanced = {f: page[-1][f] for f in sk.key_fields}
            if after is not None and advanced == after:
                # A full page that ends where the last one ended means the
                # adapter ignored after_key. Looping would spin forever on the
                # same rows, so say which adapter is at fault instead.
                raise KvStoreError(
                    f"{type(self._adapter).__name__}.select "
                    "returned a page that does not advance past after; "
                    "the scan would never terminate"
                )
            after = advanced
            if len(page) < page_size:
                break
        if expired_keys:
            self._adapter.delete_by_keys(
                table=self._table,
                key_fields=sk.key_fields,
                keys=expired_keys,
            )

    def _iter_ordered_rows(
        self,
        *,
        user_prefix: K | None,
        tests: Predicate[K] | None,
        order_by: Sequence[SqlOrder],
        after: SqlDbRow | None,
        distinct_on: Sequence[str],
        kept: Predicate[K] | None,
        limit: int | None,
        include_values: bool,
    ) -> Iterator[ScanRow[K, V]]:
        sk = self._sql_kvset
        # Liveness must be pushed with LIMIT: expired rows live in the table
        # until GC runs, so filtering them in Python after a pushed LIMIT n
        # under-fills (and returns nothing when the newest n have expired).
        live = SqlLiveness(
            now=time.time(),
            ttl_seconds=self.ttl_seconds(),
            updated_at_field=sk.updated_at_field,
            expires_at_field=sk.expires_at_field,
            held_columns=self._held_by(),
        )
        query = SqlSelect(
            table=self._table,
            columns=sk.select_columns(),
            key_fields=sk.key_fields,
            order_by=order_by,
            exact=self._scope_key_row(require_all=False),
            prefixes=self._leaf_prefix_map(user_prefix),
            where=tests,
            after=after,
            limit=limit,
            live=live,
            distinct_on=distinct_on,
            kept_where=kept,
        )
        for row in self._adapter.select(query):
            # Defence in depth: SQL already excluded these.
            if self._row_expired(row) or not self._holds(row):
                continue
            decoded = self._decode_leaf_key(row)
            if decoded is None:
                continue
            value = self.kvset.value_serde.deserialize(row) if include_values else None
            record = self._stat_from_row(row)
            yield ScanRow(
                key=decoded,
                value=value,
                stat=ScanStat(
                    modified_at=record.modified_at, expires_at=record.expires_at
                ),
            )

    def scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        return self._scan(query, self._decode_leaf_key)

    def scan_scoped_entries(
        self, query: ScanQuery[K]
    ) -> Iterator[tuple[Scope, K, V | None]]:
        """Each row with its whole scope, read from its scope columns."""
        if not self._sql_kvset.scope_fields:
            yield from super().scan_scoped_entries(query)
            return
        for (scope, leaf), value in self._scan(query, self._decode_scoped):
            yield scope, leaf, value

    def expired_scoped_keys(
        self, *, now: datetime | None = None, limit: int
    ) -> list[tuple[Scope, K]]:
        if not self._sql_kvset.scope_fields:
            return super().expired_scoped_keys(now=now, limit=limit)
        found = map(self._decode_scoped, self._expired_rows(now, limit))
        return [f for f in found if f is not None]

    def _decode_scoped(self, row: SqlDbRow) -> tuple[Scope, K] | None:
        leaf = self._decode_leaf_key(row)
        if leaf is None:
            return None
        fields = self._sql_kvset.scope_fields
        return Scope(segments=tuple(ScopeSegment(f, str(row[f])) for f in fields)), leaf

    def _scan[T](
        self, query: ScanQuery[K], decode: Callable[[SqlDbRow], T | None]
    ) -> Iterator[tuple[T, V | None]]:
        if self.kvset.key_layout is not KeyLayout.LITERAL:
            raise KvStoreScanUnsupported(
                "SqlDbKvStore scan requires KeyLayout.LITERAL "
                f"(got {self.kvset.key_layout!r})"
            )
        if self._adapter.supports_select():
            for row in self._iter_keyset_rows(
                user_prefix=query.prefix,
                tests=None,
                after_key=None,
                page_size=query.page_size,
                include_values=query.include_values,
                limit=None,
                decode=decode,
            ):
                yield row.key, row.value
            return
        sk = self._sql_kvset
        exact = self._scope_key_row(require_all=False)
        prefixes = self._leaf_prefix_map(query.prefix)
        # Legacy single-column mode embeds scope in the leaf string: exact stays {}.
        expired_keys: list[SqlDbRow] = []
        for row in self._adapter.scan_by_key_parts(
            table=self._table,
            columns=sk.select_columns(),
            key_fields=sk.key_fields,
            exact=exact,
            prefixes=prefixes,
        ):
            if self._row_expired(row):
                if self._expiry_gc is ExpiryGc.LAZY_DELETE:
                    expired_keys.append({f: row[f] for f in sk.key_fields})
                continue
            if not self._holds(row):
                continue
            decoded = decode(row)
            if decoded is None:
                continue
            if query.include_values:
                yield decoded, self.kvset.value_serde.deserialize(row)
            else:
                yield decoded, None
        if expired_keys:
            self._adapter.delete_by_keys(
                table=self._table,
                key_fields=sk.key_fields,
                keys=expired_keys,
            )


class SqlDbKvStore[K, V](LeafKvStore[K, V, str, SqlDbRow, str]):
    """Row-oriented KvStore over a SQL table via ``SqlDbClientAdapter``.

    Physical PK = ``scope_fields`` (from ``scoped(...)``) + ``leaf_key_fields``
    (from leaf ``K``). Composite leaf keys are sequences/mappings aligned to
    ``leaf_key_fields``. Scan uses per-part string prefixes (empty / omitted =
    unconstrained), matching other kvtsjl prefix-scan semantics.
    """

    def __init__(
        self,
        sql_kvset: SqlDbKvSet[K, V],
        *,
        adapter: SqlDbClientAdapter,
        expiry_gc: ExpiryGc = ExpiryGc.LAZY_DELETE,
        scope: Scope | None = None,
        binder: NamespaceBinder[str, str] | None = None,
        binding: CollectionBinding[str, str] | None = None,
        batch_size: int = 500,
    ) -> None:
        super().__init__(
            SqlDbKvBackend(
                sql_kvset,
                adapter=adapter,
                expiry_gc=expiry_gc,
                scope=scope,
                binder=binder,
                binding=binding,
                batch_size=batch_size,
            )
        )

    def expired_keys(
        self, *, now: datetime | None = None, limit: int
    ) -> list[K]:
        """Oldest-expired keys first, bounded, under the bound scope. Read-only."""
        return cast(SqlDbKvBackend[K, V], self._backend).expired_keys(
            now=now, limit=limit
        )

    def rebind(self, adapter: SqlDbClientAdapter) -> SqlDbKvStore[K, V]:
        """Same store (scope, kvset, expiry, batch size) on a different adapter.

        Reads and writes both use ``adapter``, so a client-owned transaction
        on that connection covers ``get`` then ``set`` (and peers) atomically.
        """
        backend = cast(SqlDbKvBackend[K, V], self._backend).clone_with_adapter(
            adapter
        )
        return type(self).from_backend(backend)
