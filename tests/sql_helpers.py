"""Shared SQL test helpers (real SqliteSqlDbClientAdapter, not a fake)."""

from __future__ import annotations

import sqlite3
from typing import Any

from kvtsjl import KeyLayout, KvSet, SerDe, TtlPolicy
from kvtsjl.backends.sql import SqlDbField, SqlDbKvSet, SqlDbRow


def str_sql_row_serde() -> SerDe[str, SqlDbRow]:
    return SerDe(
        serializer=lambda v: {"value": v},
        deserializer=lambda row: str(row["value"]),
        blob_type=dict,
    )


def make_sql_kvset(
    name: str,
    *,
    version: int = 1,
    ttl_policy: TtlPolicy | None = None,
    key_layout: KeyLayout = KeyLayout.LITERAL,
    leaf_key_fields: tuple[str, ...] = ("id",),
    scope_fields: tuple[str, ...] = (),
    scope_schema: tuple[str, ...] | None = None,
) -> SqlDbKvSet[str, str]:
    kvset = KvSet.with_str_keys(
        name,
        version=version,
        key_serde=SerDe.identity(str),
        value_serde=str_sql_row_serde(),
        ttl_policy=ttl_policy,
        key_layout=key_layout,
        scope_schema=scope_schema,
    )
    return SqlDbKvSet(
        kvset=kvset,
        leaf_key_fields=leaf_key_fields,
        scope_fields=scope_fields,
        fields=(SqlDbField("value"),),
    )


def make_composite_sql_kvset(
    name: str = "products",
    *,
    scope_fields: tuple[str, ...] = ("tenant",),
) -> SqlDbKvSet[tuple[str, str], str]:
    kvset: KvSet[tuple[str, str], str, str, SqlDbRow] = KvSet.with_str_keys(
        name,
        key_serde=SerDe(
            serializer=lambda k: f"{k[0]}\0{k[1]}",
            deserializer=lambda s: (s.split("\0", 1)[0], s.split("\0", 1)[1]),
            blob_type=str,
        ),
        value_serde=str_sql_row_serde(),
        scope_schema=scope_fields or None,
    )
    return SqlDbKvSet(
        kvset=kvset,
        leaf_key_fields=("category", "product"),
        scope_fields=scope_fields,
        fields=(SqlDbField("value"),),
    )


def ensure_sql_table(
    conn: sqlite3.Connection,
    sql_kvset: SqlDbKvSet[Any, Any],
) -> None:
    table = sql_kvset.table_name
    key_cols = ", ".join(f'"{f}" TEXT NOT NULL' for f in sql_kvset.key_fields)
    pk = ", ".join(f'"{f}"' for f in sql_kvset.key_fields)
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS "{table}" (
            {key_cols},
            "value" TEXT NOT NULL,
            "updated_at" REAL NOT NULL,
            "expires_at" REAL,
            PRIMARY KEY ({pk})
        )
        """
    )
    conn.commit()
