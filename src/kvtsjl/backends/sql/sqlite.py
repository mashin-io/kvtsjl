"""SQLite ``SqlDbClientAdapter`` (stdlib ``sqlite3``)."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
import re
import sqlite3
from typing import Any

from kvtsjl.backends.sql.adapter import SqlDbClientAdapter
from kvtsjl.backends.sql.schema import SqlDbRow

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
    """First-party SQLite backend using a caller-owned ``sqlite3.Connection``."""

    def __init__(self, connection: sqlite3.Connection, *, db_name: str = "sqlite") -> None:
        self._conn = connection
        self._db_name = db_name
        self._conn.row_factory = sqlite3.Row

    @property
    def db_name(self) -> str:
        return self._db_name

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
        self._conn.commit()

    def delete_by_keys(
        self,
        *,
        table: str,
        key_fields: Sequence[str],
        keys: Sequence[SqlDbRow],
    ) -> int:
        if not keys:
            return 0
        t = _quote_ident(table)
        deleted = 0
        for key in keys:
            preds = " AND ".join(f"{_quote_ident(f)}=?" for f in key_fields)
            params = [key[f] for f in key_fields]
            cur = self._conn.execute(f"DELETE FROM {t} WHERE {preds}", params)
            deleted += int(cur.rowcount)
        self._conn.commit()
        return deleted

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
        clauses: list[str] = []
        params: list[Any] = []
        for field in key_fields:
            if field in exact:
                clauses.append(f"{_quote_ident(field)}=?")
                params.append(exact[field])
            elif field in prefixes:
                clauses.append(f"{_quote_ident(field)} LIKE ? ESCAPE '\\'")
                params.append(f"{_escape_like(prefixes[field])}%")
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        order = ", ".join(_quote_ident(f) for f in key_fields)
        sql = f"SELECT {cols} FROM {t}{where} ORDER BY {order}"
        cur = self._conn.execute(sql, params)
        for row in cur:
            yield _row_from_sqlite(row)
