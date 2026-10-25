"""SQLite ``SqlDbClientAdapter`` (stdlib ``sqlite3``)."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
import re
import sqlite3
from typing import Any

from kvtsjl.backends.sql.adapter import SqlDbClientAdapter, SqlLiveness
from kvtsjl.backends.sql.schema import SqlDbRow
from kvtsjl.exceptions import KvStoreError
from kvtsjl.store.schema.scan import OrderBy

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_ident(name: str) -> str:
    if not _IDENT.fullmatch(name):
        raise ValueError(f"invalid SQL identifier: {name!r}")
    return f'"{name}"'


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _row_from_sqlite(row: sqlite3.Row) -> SqlDbRow:
    return {k: row[k] for k in row.keys()}


class SqliteSqlDbClientAdapter(SqlDbClientAdapter):
    """First-party SQLite backend using a caller-owned ``sqlite3.Connection``.

    With ``autocommit=True`` (default), each write ends with ``commit()`` —
    today's behaviour. With ``autocommit=False``, this adapter never commits;
    the client must open a transaction before any write, or the write raises
    instead of vanishing on a connection with nothing to commit later.

    ``begin_on_write=True`` (with ``autocommit=False``) lets the first write
    open the transaction instead — ``BEGIN IMMEDIATE`` — which the client
    still commits or rolls back. A composition writes what comes before its
    commit point first, so a record's body uploads before the transaction,
    and the write lock is never held across that network call::

        records.set(key, record)   # body, then BEGIN at the row
        conn.commit(); records.flush_index_syncs()
    """

    def __init__(
        self,
        connection: sqlite3.Connection,
        *,
        db_name: str = "sqlite",
        autocommit: bool = True,
        begin_on_write: bool = False,
    ) -> None:
        if autocommit and begin_on_write:
            raise ValueError(
                "begin_on_write opens a transaction the client commits; "
                "autocommit commits each write itself"
            )
        self._conn = connection
        self._db_name = db_name
        self._autocommit = autocommit
        self._begin_on_write = begin_on_write
        self._conn.row_factory = sqlite3.Row

    def _ensure_write_txn(self) -> None:
        if self._autocommit or self._conn.in_transaction:
            return
        if self._begin_on_write:
            self._conn.execute("BEGIN IMMEDIATE")
            return
        raise KvStoreError("no open transaction; write would never commit")

    def _after_write(self) -> None:
        if self._autocommit:
            self._conn.commit()
    @property
    def db_name(self) -> str:
        return self._db_name

    def supports_insert_if_absent(self) -> bool:
        return True

    def defers_commit(self) -> bool:
        return not self._autocommit

    @property
    def autocommit(self) -> bool:
        """Whether this adapter commits after each write.

        ``False`` means the caller owns the transaction, and a write with none
        open is refused rather than silently lost.
        """
        return self._autocommit

    @property
    def connection(self) -> sqlite3.Connection:
        return self._conn

    def fetch_by_keys(
        self,
        *,
        table: str,
        columns: Sequence[str],
        key_fields: Sequence[str],
        keys: Sequence[SqlDbRow],
    ) -> Sequence[SqlDbRow]:
        if not keys:
            return []
        if not key_fields:
            raise ValueError("key_fields must be non-empty")
        cols = ", ".join(_quote_ident(c) for c in columns)
        t = _quote_ident(table)
        out: list[SqlDbRow] = []
        # SQLite has no row-IN for composites; OR of equality conjunctions.
        for key in keys:
            preds = " AND ".join(f"{_quote_ident(f)}=?" for f in key_fields)
            params = [key[f] for f in key_fields]
            cur = self._conn.execute(
                f"SELECT {cols} FROM {t} WHERE {preds}",
                params,
            )
            row = cur.fetchone()
            if row is not None:
                out.append(_row_from_sqlite(row))
        return out

    def exists_by_keys(
        self,
        *,
        table: str,
        key_fields: Sequence[str],
        keys: Sequence[SqlDbRow],
        columns: Sequence[str] | None = None,
    ) -> Sequence[SqlDbRow]:
        if not keys:
            return []
        if not key_fields:
            raise ValueError("key_fields must be non-empty")
        select = columns if columns is not None else key_fields
        cols = ", ".join(_quote_ident(c) for c in select)
        t = _quote_ident(table)
        out: list[SqlDbRow] = []
        for key in keys:
            preds = " AND ".join(f"{_quote_ident(f)}=?" for f in key_fields)
            params = [key[f] for f in key_fields]
            cur = self._conn.execute(
                f"SELECT {cols} FROM {t} WHERE {preds}",
                params,
            )
            row = cur.fetchone()
            if row is not None:
                out.append(_row_from_sqlite(row))
        return out

    def upsert_rows(
        self,
        *,
        table: str,
        key_fields: Sequence[str],
        write_columns: Sequence[str],
        rows: Sequence[SqlDbRow],
    ) -> None:
        if not rows:
            return
        self._ensure_write_txn()
        missing = [f for f in key_fields if f not in write_columns]
        if missing:
            raise ValueError(f"key_fields missing from write_columns: {missing}")
        t = _quote_ident(table)
        cols = [_quote_ident(c) for c in write_columns]
        col_list = ", ".join(cols)
        placeholders = ", ".join("?" for _ in write_columns)
        conflict = ", ".join(_quote_ident(f) for f in key_fields)
        key_set = set(key_fields)
        updates = ", ".join(
            f"{_quote_ident(c)}=excluded.{_quote_ident(c)}"
            for c in write_columns
            if c not in key_set
        )
        if not updates:
            sql = (
                f"INSERT INTO {t} ({col_list}) VALUES ({placeholders}) "
                f"ON CONFLICT({conflict}) DO NOTHING"
            )
        else:
            sql = (
                f"INSERT INTO {t} ({col_list}) VALUES ({placeholders}) "
                f"ON CONFLICT({conflict}) DO UPDATE SET {updates}"
            )
        params = [tuple(row.get(c) for c in write_columns) for row in rows]
        self._conn.executemany(sql, params)
        self._after_write()

    def supports_update_first(self) -> bool:
        return True

    def upsert_rows_update_first(
        self,
        *,
        table: str,
        key_fields: Sequence[str],
        write_columns: Sequence[str],
        rows: Sequence[SqlDbRow],
    ) -> None:
        if not rows:
            return
        missing = [f for f in key_fields if f not in write_columns]
        if missing:
            raise ValueError(f"key_fields missing from write_columns: {missing}")
        self._ensure_write_txn()
        t = _quote_ident(table)
        key_set = set(key_fields)
        set_cols = [c for c in write_columns if c not in key_set]
        assigns = ", ".join(f"{_quote_ident(c)}=?" for c in set_cols)
        where = " AND ".join(f"{_quote_ident(f)}=?" for f in key_fields)
        update_sql = f"UPDATE {t} SET {assigns} WHERE {where}"
        cols = ", ".join(_quote_ident(c) for c in write_columns)
        placeholders = ", ".join("?" for _ in write_columns)
        insert_sql = f"INSERT INTO {t} ({cols}) VALUES ({placeholders})"

        # Row at a time: executemany cannot report which rows matched.
        for row in rows:
            params = [row.get(c) for c in set_cols] + [row.get(f) for f in key_fields]
            if self._conn.execute(update_sql, params).rowcount:
                continue
            try:
                self._conn.execute(insert_sql, [row.get(c) for c in write_columns])
            except sqlite3.IntegrityError as exc:
                if "UNIQUE" not in str(exc):
                    # NOT NULL from a view that does not own the column: the
                    # row genuinely cannot be created from here. Surface it.
                    raise
                # Another writer inserted between the update and the insert.
                self._conn.execute(update_sql, params)
        self._after_write()

    def delete_by_keys(
        self,
        *,
        table: str,
        key_fields: Sequence[str],
        keys: Sequence[SqlDbRow],
    ) -> int:
        if not keys:
            return 0
        self._ensure_write_txn()
        t = _quote_ident(table)
        deleted = 0
        for key in keys:
            preds = " AND ".join(f"{_quote_ident(f)}=?" for f in key_fields)
            params = [key[f] for f in key_fields]
            cur = self._conn.execute(f"DELETE FROM {t} WHERE {preds}", params)
            deleted += int(cur.rowcount)
        self._after_write()
        return deleted

    def supports_compare_and_write(self) -> bool:
        return True

    def _matches(self, key_fields: Sequence[str], expected: SqlDbRow) -> tuple[str, list[Any]]:
        """``WHERE`` for the key and every expected column, ``NULL`` equal to ``NULL``."""
        compared = [c for c in expected if c not in key_fields]
        preds = [f"{_quote_ident(f)}=?" for f in key_fields] + [
            f"{_quote_ident(c)} IS ?" for c in compared
        ]
        return " AND ".join(preds), [expected[f] for f in key_fields] + [expected[c] for c in compared]

    def insert_each_if_absent(
        self,
        *,
        table: str,
        key_fields: Sequence[str],
        write_columns: Sequence[str],
        rows: Sequence[SqlDbRow],
    ) -> list[bool]:
        if not rows:
            return []
        missing = [f for f in key_fields if f not in write_columns]
        if missing:
            raise ValueError(f"key_fields missing from write_columns: {missing}")
        self._ensure_write_txn()
        cols = ", ".join(_quote_ident(c) for c in write_columns)
        placeholders = ", ".join("?" for _ in write_columns)
        conflict = ", ".join(_quote_ident(f) for f in key_fields)
        sql = (
            f"INSERT INTO {_quote_ident(table)} ({cols}) VALUES ({placeholders}) "
            f"ON CONFLICT({conflict}) DO NOTHING"
        )
        inserted = [
            self._conn.execute(sql, [row.get(c) for c in write_columns]).rowcount > 0
            for row in rows
        ]
        self._after_write()
        return inserted

    def replace_each_if(
        self,
        *,
        table: str,
        key_fields: Sequence[str],
        write_columns: Sequence[str],
        expected: Sequence[SqlDbRow],
        rows: Sequence[SqlDbRow],
    ) -> list[bool]:
        if not rows:
            return []
        self._ensure_write_txn()
        set_cols = [c for c in write_columns if c not in key_fields]
        assigns = ", ".join(f"{_quote_ident(c)}=?" for c in set_cols)
        replaced = []
        for want, row in zip(expected, rows, strict=True):
            where, params = self._matches(key_fields, want)
            cur = self._conn.execute(
                f"UPDATE {_quote_ident(table)} SET {assigns} WHERE {where}",
                [row.get(c) for c in set_cols] + params,
            )
            replaced.append(cur.rowcount > 0)
        self._after_write()
        return replaced

    def delete_each_if(
        self, *, table: str, key_fields: Sequence[str], expected: Sequence[SqlDbRow]
    ) -> list[bool]:
        if not expected:
            return []
        self._ensure_write_txn()
        deleted = []
        for want in expected:
            where, params = self._matches(key_fields, want)
            cur = self._conn.execute(f"DELETE FROM {_quote_ident(table)} WHERE {where}", params)
            deleted.append(cur.rowcount > 0)
        self._after_write()
        return deleted

    def supports_update_rows(self) -> bool:
        return True

    def update_rows(
        self,
        *,
        table: str,
        key_fields: Sequence[str],
        write_columns: Sequence[str],
        rows: Sequence[SqlDbRow],
    ) -> int:
        if not rows:
            return 0
        self._ensure_write_txn()
        t = _quote_ident(table)
        key_set = set(key_fields)
        set_cols = [c for c in write_columns if c not in key_set]
        assigns = ", ".join(f"{_quote_ident(c)}=?" for c in set_cols)
        where = " AND ".join(f"{_quote_ident(f)}=?" for f in key_fields)
        sql = f"UPDATE {t} SET {assigns} WHERE {where}"
        matched = 0
        for row in rows:
            params = [row.get(c) for c in set_cols] + [row.get(f) for f in key_fields]
            matched += int(self._conn.execute(sql, params).rowcount)
        self._after_write()
        return matched

    def supports_clear_columns(self) -> bool:
        return True

    def clear_columns(
        self,
        *,
        table: str,
        key_fields: Sequence[str],
        keys: Sequence[SqlDbRow],
        columns: Sequence[str],
    ) -> int:
        if not keys or not columns:
            return 0
        self._ensure_write_txn()
        t = _quote_ident(table)
        assigns = ", ".join(f"{_quote_ident(c)}=NULL" for c in columns)
        held = " OR ".join(f"{_quote_ident(c)} IS NOT NULL" for c in columns)
        where = " AND ".join(f"{_quote_ident(f)}=?" for f in key_fields)
        sql = f"UPDATE {t} SET {assigns} WHERE {where} AND ({held})"
        cleared = 0
        for key in keys:
            cleared += int(self._conn.execute(sql, [key[f] for f in key_fields]).rowcount)
        self._after_write()
        return cleared

    def _key_parts_filters(
        self,
        *,
        key_fields: Sequence[str],
        exact: Mapping[str, Any],
        prefixes: Mapping[str, str],
        after_key: SqlDbRow | None,
    ) -> tuple[list[str], list[Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        for field in key_fields:
            if field in exact:
                clauses.append(f"{_quote_ident(field)}=?")
                params.append(exact[field])
            elif field in prefixes:
                clauses.append(f"{_quote_ident(field)} LIKE ? ESCAPE '\\'")
                params.append(f"{_escape_like(prefixes[field])}%")
        if after_key is not None:
            missing = [f for f in key_fields if f not in after_key]
            if missing:
                raise ValueError(f"after_key missing key fields: {missing}")
            tuple_cols = ", ".join(_quote_ident(f) for f in key_fields)
            placeholders = ", ".join("?" for _ in key_fields)
            clauses.append(f"({tuple_cols}) > ({placeholders})")
            params.extend(after_key[f] for f in key_fields)
        return clauses, params

    def scan_by_key_parts(
        self,
        *,
        table: str,
        columns: Sequence[str],
        key_fields: Sequence[str],
        exact: Mapping[str, Any],
        prefixes: Mapping[str, str],
    ) -> Iterator[SqlDbRow]:
        cols = ", ".join(_quote_ident(c) for c in columns)
        t = _quote_ident(table)
        clauses, params = self._key_parts_filters(
            key_fields=key_fields,
            exact=exact,
            prefixes=prefixes,
            after_key=None,
        )
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        order = ", ".join(_quote_ident(f) for f in key_fields)
        sql = f"SELECT {cols} FROM {t}{where} ORDER BY {order}"
        cur = self._conn.execute(sql, params)
        for row in cur:
            yield _row_from_sqlite(row)

    def supports_paged_scan(self) -> bool:
        return True

    def scan_page_by_key_parts(
        self,
        *,
        table: str,
        columns: Sequence[str],
        key_fields: Sequence[str],
        exact: Mapping[str, Any],
        prefixes: Mapping[str, str],
        after_key: SqlDbRow | None,
        page_size: int,
    ) -> Sequence[SqlDbRow]:
        if page_size < 1:
            raise ValueError(f"page_size must be >= 1, got {page_size}")
        if not key_fields:
            raise ValueError("key_fields must be non-empty")
        cols = ", ".join(_quote_ident(c) for c in columns)
        t = _quote_ident(table)
        clauses, params = self._key_parts_filters(
            key_fields=key_fields,
            exact=exact,
            prefixes=prefixes,
            after_key=after_key,
        )
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        order = ", ".join(_quote_ident(f) for f in key_fields)
        sql = f"SELECT {cols} FROM {t}{where} ORDER BY {order} LIMIT ?"
        params.append(page_size)
        cur = self._conn.execute(sql, params)
        return [_row_from_sqlite(row) for row in cur]

    def supports_scan_ordering(self) -> bool:
        return True

    def supports_expiry_scan(self) -> bool:
        return True

    def scan_expired(
        self,
        *,
        table: str,
        columns: Sequence[str],
        key_fields: Sequence[str],
        exact: Mapping[str, Any],
        expires_at_field: str,
        updated_at_field: str,
        relative_ttl_seconds: int | None,
        now: float,
        limit: int,
    ) -> Sequence[SqlDbRow]:
        if limit < 1:
            raise ValueError(f"limit must be >= 1, got {limit}")
        cols = ", ".join(_quote_ident(c) for c in columns)
        t = _quote_ident(table)
        expires = _quote_ident(expires_at_field)
        updated = _quote_ident(updated_at_field)
        clauses: list[str] = []
        params: list[Any] = []
        for field, value in exact.items():
            clauses.append(f"{_quote_ident(field)}=?")
            params.append(value)
        if relative_ttl_seconds is None:
            clauses.append(f"({expires} IS NOT NULL AND {expires} <= ?)")
            params.append(now)
            order_expr = expires
        else:
            ttl = float(relative_ttl_seconds)
            clauses.append(
                f"(({expires} IS NOT NULL AND {expires} <= ?) OR "
                f"({expires} IS NULL AND {updated} IS NOT NULL "
                f"AND {updated} + ? <= ?))"
            )
            params.extend([now, ttl, now])
            order_expr = f"COALESCE({expires}, {updated} + ?)"
            # COALESCE bind comes after WHERE params, before LIMIT.
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"SELECT {cols} FROM {t}{where} ORDER BY {order_expr} ASC LIMIT ?"
        if relative_ttl_seconds is not None:
            params.append(float(relative_ttl_seconds))
        params.append(limit)
        cur = self._conn.execute(sql, params)
        return [_row_from_sqlite(row) for row in cur]

    def has_null_expires_at(
        self,
        *,
        table: str,
        exact: Mapping[str, Any],
        expires_at_field: str,
    ) -> bool:
        t = _quote_ident(table)
        expires = _quote_ident(expires_at_field)
        clauses = [f"{expires} IS NULL"]
        params: list[Any] = []
        for field, value in exact.items():
            clauses.append(f"{_quote_ident(field)}=?")
            params.append(value)
        where = " AND ".join(clauses)
        cur = self._conn.execute(
            f"SELECT 1 FROM {t} WHERE {where} LIMIT 1",
            params,
        )
        return cur.fetchone() is not None

    def scan_ordered(
        self,
        *,
        table: str,
        columns: Sequence[str],
        key_fields: Sequence[str],
        exact: Mapping[str, Any],
        prefixes: Mapping[str, str],
        order_by: Sequence[OrderBy],
        limit: int | None,
        live: SqlLiveness | None = None,
    ) -> Iterator[SqlDbRow]:
        cols = ", ".join(_quote_ident(c) for c in columns)
        t = _quote_ident(table)
        clauses: list[str] = []
        params: list[Any] = []
        for field in key_fields:
            if field in exact:
                clauses.append(f"{_quote_ident(field)}=?")
                params.append(exact[field])
            elif field in prefixes:
                clauses.append(f"{_quote_ident(field)} LIKE ? ESCAPE '\\'")
                params.append(f"{_escape_like(prefixes[field])}%")
        if live is not None:
            expires = _quote_ident(live.expires_at_field)
            updated = _quote_ident(live.updated_at_field)
            if live.ttl_seconds is None:
                clauses.append(f"({expires} IS NULL OR {expires} > ?)")
                params.append(live.now)
            else:
                clauses.append(
                    f"(CASE WHEN {expires} IS NOT NULL THEN {expires} > ? "
                    f"ELSE ({updated} IS NULL OR {updated} + ? > ?) END)"
                )
                params.extend([live.now, float(live.ttl_seconds), live.now])
            if live.held_columns:
                held = " OR ".join(
                    f"{_quote_ident(c)} IS NOT NULL" for c in live.held_columns
                )
                clauses.append(f"({held})")
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        order = ", ".join(
            f"{_quote_ident(item.column)}{' DESC' if item.descending else ''}"
            for item in order_by
        )
        sql = f"SELECT {cols} FROM {t}{where} ORDER BY {order}"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        cur = self._conn.execute(sql, params)
        for row in cur:
            yield _row_from_sqlite(row)
