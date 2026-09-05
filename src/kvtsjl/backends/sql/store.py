"""``SqlDbKvStore`` leaf backend."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from datetime import datetime
import time
from typing import Any, cast

from kvtsjl.backends.sql.adapter import SqlDbClientAdapter
from kvtsjl.backends.sql.schema import SqlDbKvSet, SqlDbRow
from kvtsjl.batching import chunk_sequence
from kvtsjl.bind import (
    CollectionBinding,
    NamespaceBinder,
    NativeCollectionBinder,
)
from kvtsjl.exceptions import KvStoreScanUnsupported, KvStoreScopeError
from kvtsjl.scope import Scope
from kvtsjl.store import KvBackend
from kvtsjl.store.schema.layout import KeyLayout, ScanQuery
from kvtsjl.store.schema.ttl import (
    TTL_NONE_EXPIRES_AT,
    ExpiryGc,
    TtlPolicy,
)


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


class SqlDbKvStore[K, V](KvBackend[K, V, str, SqlDbRow, str]):
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

    def _clone_with_scope(self, scope: Scope) -> SqlDbKvStore[K, V]:
        return SqlDbKvStore(
            self._sql_kvset,
            adapter=self._adapter,
            expiry_gc=self._expiry_gc,
            scope=scope,
            binding=self._binding,
            batch_size=self.batch_size,
        )

    def _expires_at_column(self, ttl: TtlPolicy | None) -> float | None:
        if ttl is None:
            return None
        secs = ttl.ttl_seconds()
        if secs is None:
            return TTL_NONE_EXPIRES_AT.timestamp()
        return time.time() + secs

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

    def _row_expired(self, row: SqlDbRow) -> bool:
        expires = _as_timestamp(row.get(self._sql_kvset.expires_at_field))
        if expires is not None:
            return time.time() >= expires
        ttl = self.ttl_seconds()
        if ttl is None:
            return False
        updated = _as_timestamp(row.get(self._sql_kvset.updated_at_field))
        if updated is None:
            return False
        return time.time() >= updated + ttl

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

    def _build_write_row(self, key: K, value: V, *, ttl: TtlPolicy | None) -> SqlDbRow:
        serialized = dict(self.kvset.value_serde.serialize(value))
        sk = self._sql_kvset
        serialized.update(self._full_key_row(key, require_scope=True))
        serialized[sk.updated_at_field] = time.time()
        serialized[sk.expires_at_field] = self._expires_at_column(ttl)
        return {col: serialized.get(col) for col in sk.write_columns()}

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

    def set(self, key: K, value: V, *, ttl: TtlPolicy | None = None) -> None:
        row = self._build_write_row(key, value, ttl=ttl)
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
        self, items: Mapping[K, V], *, ttl: TtlPolicy | None = None
    ) -> None:
        sk = self._sql_kvset
        pairs = list(items.items())
        for chunk in chunk_sequence(pairs, self.batch_size):
            rows = [self._build_write_row(k, v, ttl=ttl) for k, v in chunk]
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

    def _scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
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

    def _gc_expired_keys(self, *, max_entries: int) -> list[K]:
        if max_entries < 1:
            raise ValueError(f"max_entries must be >= 1, got {max_entries}")
        sk = self._sql_kvset
        exact = self._scope_key_row(require_all=False)
        prefixes = self._leaf_prefix_map(None)
        deleted: list[K] = []
        expired_rows: list[SqlDbRow] = []
        for row in self._adapter.scan_by_key_parts(
            table=self._table,
            columns=sk.select_columns(),
            key_fields=sk.key_fields,
            exact=exact,
            prefixes=prefixes,
        ):
            if len(deleted) >= max_entries:
                break
            if not self._row_expired(row):
                continue
            decoded = self._decode_leaf_key(row)
            if decoded is None:
                continue
            expired_rows.append({f: row[f] for f in sk.key_fields})
            deleted.append(decoded)
        if expired_rows:
            self._adapter.delete_by_keys(
                table=self._table,
                key_fields=sk.key_fields,
                keys=expired_rows,
            )
        return deleted
