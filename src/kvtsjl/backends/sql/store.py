"""``SqlDbKvStore`` leaf backend."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, datetime
import time
from typing import Any, cast

from kvtsjl.backends.sql.adapter import SqlDbClientAdapter, SqlLiveness
from kvtsjl.backends.sql.schema import ExpiryBinding, SqlDbKvSet, SqlDbRow
from kvtsjl.batching import chunk_sequence
from kvtsjl.bind import (
    CollectionBinding,
    NamespaceBinder,
    NativeCollectionBinder,
)
from kvtsjl.exceptions import KvStoreError, KvStoreScanUnsupported, KvStoreScopeError
from kvtsjl.scope import Scope
from kvtsjl.store import KvBackend
from kvtsjl.store.leaf import LeafKvStore
from kvtsjl.store.schema.layout import KeyLayout, ScanQuery
from kvtsjl.store.schema.scan import (
    AlwaysTrue,
    CompiledScan,
    KeyPrefix,
    OrderBy,
    Projection,
    ScanCapabilities,
    ScanOrder,
    ScanRow,
    ScanSpec,
    ScanStat,
    compile_from_scan_entries,
    enforce_strict,
    peel_key_prefix,
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

    def clone_with_scope(self, scope: Scope) -> SqlDbKvBackend[K, V]:
        return SqlDbKvBackend(
            self._sql_kvset,
            adapter=self._adapter,
            expiry_gc=self._expiry_gc,
            scope=scope,
            binding=self._binding,
            batch_size=self.batch_size,
        )

    def _presence_columns(self) -> tuple[str, ...]:
        sk = self._sql_kvset
        return (*sk.key_fields, sk.updated_at_field, sk.expires_at_field)

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
        if limit < 1:
            raise ValueError(f"limit must be >= 1, got {limit}")
        self._ensure_early_binding_safe()
        now_epoch = _clock_base(now)
        sk = self._sql_kvset
        exact = self._scope_key_row(require_all=False)
        if self._adapter.supports_expiry_scan():
            rows = self._adapter.scan_expired(
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
        else:
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
            rows = [row for _, row in scored[:limit]]
        out: list[K] = []
        for row in rows:
            decoded = self._decode_leaf_key(row)
            if decoded is not None:
                out.append(decoded)
        return out

    def _live_row(self, row: SqlDbRow | None) -> SqlDbRow | None:
        if row is None:
            return None
        if not self._row_expired(row):
            return row
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
        out: set[K] = set()
        sk = self._sql_kvset
        for chunk in chunk_sequence(list(keys), self.batch_size):
            key_rows = [self._full_key_row(k, require_scope=True) for k in chunk]
            found = self._adapter.exists_by_keys(
                table=self._table,
                key_fields=sk.key_fields,
                keys=key_rows,
                columns=self._presence_columns(),
            )
            by_tuple = {_key_tuple(r, sk.key_fields): r for r in found}
            for key, key_row in zip(chunk, key_rows, strict=True):
                row = self._live_row(by_tuple.get(_key_tuple(key_row, sk.key_fields)))
                if row is not None:
                    out.add(key)
        return out

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
        out: dict[K, RecordStat] = {}
        sk = self._sql_kvset
        for chunk in chunk_sequence(list(keys), self.batch_size):
            key_rows = [self._full_key_row(k, require_scope=True) for k in chunk]
            found = self._adapter.exists_by_keys(
                table=self._table,
                key_fields=sk.key_fields,
                keys=key_rows,
                columns=self._presence_columns(),
            )
            by_tuple = {_key_tuple(r, sk.key_fields): r for r in found}
            for key, key_row in zip(chunk, key_rows, strict=True):
                row = self._live_row(by_tuple.get(_key_tuple(key_row, sk.key_fields)))
                if row is not None:
                    out[key] = self._stat_from_row(row)
        return out

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
        row = self._build_write_row(key, value, ttl=ttl, created_at=created_at)
        sk = self._sql_kvset
        self._adapter.upsert_rows(
            table=self._table,
            key_fields=sk.key_fields,
            write_columns=sk.write_columns(),
            rows=[row],
        )

    def delete(self, key: K) -> bool:
        sk = self._sql_kvset
        key_row = self._full_key_row(key, require_scope=True)
        return (
            self._adapter.delete_by_keys(
                table=self._table,
                key_fields=sk.key_fields,
                keys=[key_row],
            )
            > 0
        )

    def batch_get(self, keys: Sequence[K]) -> dict[K, V]:
        out: dict[K, V] = {}
        sk = self._sql_kvset
        for chunk in chunk_sequence(list(keys), self.batch_size):
            key_rows = [self._full_key_row(k, require_scope=True) for k in chunk]
            found = self._adapter.fetch_by_keys(
                table=self._table,
                columns=sk.select_columns(),
                key_fields=sk.key_fields,
                keys=key_rows,
            )
            by_tuple = {_key_tuple(r, sk.key_fields): r for r in found}
            for key, key_row in zip(chunk, key_rows, strict=True):
                row = self._live_row(by_tuple.get(_key_tuple(key_row, sk.key_fields)))
                if row is None:
                    continue
                out[key] = self.kvset.value_serde.deserialize(row)
        return out

    def batch_set(
        self,
        items: Mapping[K, V],
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        require_write_clock_supported(
            created_at, allowed=True, backend="SqlDbKvStore"
        )
        sk = self._sql_kvset
        pairs = list(items.items())
        for chunk in chunk_sequence(pairs, self.batch_size):
            rows = [
                self._build_write_row(k, v, ttl=ttl, created_at=created_at)
                for k, v in chunk
            ]
            self._adapter.upsert_rows(
                table=self._table,
                key_fields=sk.key_fields,
                write_columns=sk.write_columns(),
                rows=rows,
            )

    def batch_delete(self, keys: Sequence[K]) -> int:
        deleted = 0
        sk = self._sql_kvset
        for chunk in chunk_sequence(list(keys), self.batch_size):
            key_rows = [self._full_key_row(k, require_scope=True) for k in chunk]
            deleted += self._adapter.delete_by_keys(
                table=self._table,
                key_fields=sk.key_fields,
                keys=key_rows,
            )
        return deleted

    def scan_capabilities(self) -> ScanCapabilities:
        """Report what SQL actually provides.

        ``KEY_ASC`` rests on the adapter contract: ``scan_by_key_parts`` must
        yield rows in ascending ``key_fields`` order (the shipped SQLite adapter
        emits ``ORDER BY`` on them). ``orderable_columns`` is the declared
        column set, so an unknown column is rejected by name.
        """
        orderable: frozenset[str] | None = None
        if self._adapter.supports_scan_ordering():
            orderable = frozenset(self._sql_kvset.select_columns())
        return ScanCapabilities(
            pushdown=frozenset({KeyPrefix}),
            orders=frozenset({ScanOrder.UNORDERED, ScanOrder.KEY_ASC}),
            resumable=False,
            # Only the ordered path fills ScanRow.stat; the default path goes
            # through compile_from_scan_entries, which has no stat to give.
            # Claiming True here would be the same wrong capability report this
            # change set out to fix.
            stats=False,
            orderable_columns=orderable,
        )

    def _resolve_order_by(self, order_by: Sequence[OrderBy]) -> list[OrderBy]:
        """Validate columns and append key fields ascending for a total order.

        Without the tiebreak, SQL may return tied rows in any order, so repeated
        scans disagree on which row is "newest" whenever timestamps collide
        (fast tests, bulk imports). Appended fields are always ascending: the
        direction is irrelevant for determinism, and fixing it means two
        implementations cannot disagree.
        """
        if not self._adapter.supports_scan_ordering():
            raise KvStoreScanUnsupported(
                f"{type(self._adapter).__name__} does not support order_by; "
                "implement supports_scan_ordering() and scan_ordered()"
            )
        allowed = frozenset(self._sql_kvset.select_columns())
        for item in order_by:
            if item.column not in allowed:
                raise KvStoreScanUnsupported(
                    f"order_by column {item.column!r} is not a declared column "
                    f"(have {sorted(allowed)!r})"
                )
        resolved = list(order_by)
        named = {item.column for item in resolved}
        for field_name in self._sql_kvset.key_fields:
            if field_name not in named:
                resolved.append(OrderBy(field_name))
        return resolved

    def compile_scan(self, spec: ScanSpec[K]) -> CompiledScan[K]:
        if not spec.order_by:
            return compile_from_scan_entries(
                self.scan_entries, spec, backend=type(self).__name__
            )
        if self.kvset.key_layout is not KeyLayout.LITERAL:
            raise KvStoreScanUnsupported(
                "SqlDbKvStore scan requires KeyLayout.LITERAL "
                f"(got {self.kvset.key_layout!r})"
            )
        order_by = self._resolve_order_by(spec.order_by)
        user_prefix, residual = peel_key_prefix(spec.where)
        pushed = {"order_by"}
        if user_prefix is not None:
            pushed.add("KeyPrefix")
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
            order_by=order_by,
            limit=limit,
            include_values=spec.project is Projection.ENTRIES,
        )
        enforce_strict(spec, residual, backend=type(self).__name__)
        return CompiledScan(rows=rows, residual=residual, pushed=frozenset(pushed))

    def _iter_ordered_rows(
        self,
        *,
        user_prefix: K | None,
        order_by: Sequence[OrderBy],
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
        )
        for row in self._adapter.scan_ordered(
            table=self._table,
            columns=sk.select_columns(),
            key_fields=sk.key_fields,
            exact=self._scope_key_row(require_all=False),
            prefixes=self._leaf_prefix_map(user_prefix),
            order_by=order_by,
            limit=limit,
            live=live,
        ):
            # Defence in depth: SQL already excluded these.
            if self._row_expired(row):
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
        if self.kvset.key_layout is not KeyLayout.LITERAL:
            raise KvStoreScanUnsupported(
                "SqlDbKvStore scan requires KeyLayout.LITERAL "
                f"(got {self.kvset.key_layout!r})"
            )
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
            decoded = self._decode_leaf_key(row)
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
