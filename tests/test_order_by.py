"""Scan ordering by declared SQL columns (``ScanSpec.order_by``).

``order_by`` is the one ``ScanSpec`` field that never falls back to the client
cascade — sorting would have to materialise the whole result, so unsupported
backends raise under both ``Pushdown`` modes rather than buffering.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
import sqlite3
from typing import Any

import pytest

from kvtsjl import KvSet, MemoryKvStore, SerDe
from kvtsjl.backends.sql import SqlDbField, SqlDbKvSet, SqlDbRow, SqliteSqlDbClientAdapter
from kvtsjl.backends.sql.adapter import SqlLiveness
from kvtsjl.backends.sql.store import SqlDbKvStore
from kvtsjl.exceptions import KvStoreScanUnsupported
from kvtsjl.store.schema.scan import OrderBy, Pushdown, ScanOrder, ScanSpec
from tests.sql_helpers import str_sql_row_serde


def _catalog_kvset(name: str = "catalog") -> SqlDbKvSet[str, str]:
    """A versioned catalog: declared ``created_at`` / ``filename`` columns."""
    kvset: KvSet[str, str, str, SqlDbRow] = KvSet.with_str_keys(
        name,
        key_serde=SerDe.identity(str),
        value_serde=str_sql_row_serde(),
    )
    return SqlDbKvSet(
        kvset=kvset,
        leaf_key_fields=("id",),
        fields=(
            SqlDbField("value"),
            SqlDbField("created_at"),
            SqlDbField("filename"),
        ),
    )


def _catalog(conn: sqlite3.Connection) -> SqlDbKvStore[str, str]:
    sql_kvset = _catalog_kvset()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS "catalog_1" (
            "id" TEXT NOT NULL PRIMARY KEY,
            "value" TEXT NOT NULL,
            "created_at" REAL NOT NULL,
            "filename" TEXT NOT NULL,
            "updated_at" REAL NOT NULL,
            "expires_at" REAL
        )
        """
    )
    conn.commit()
    return SqlDbKvStore(
        sql_kvset, adapter=SqliteSqlDbClientAdapter(conn, db_name="catalog")
    )


def _seed(conn: sqlite3.Connection, rows: Sequence[tuple[str, float, str]]) -> None:
    for key, created_at, filename in rows:
        conn.execute(
            'INSERT INTO "catalog_1" '
            '("id","value","created_at","filename","updated_at","expires_at") '
            "VALUES (?,?,?,?,?,NULL)",
            (key, f"v-{key}", created_at, filename, 0.0),
        )
    conn.commit()


@pytest.fixture
def conn() -> Iterator[sqlite3.Connection]:
    connection = sqlite3.connect(":memory:")
    try:
        yield connection
    finally:
        connection.close()


def test_order_by_returns_rows_newest_first(conn: sqlite3.Connection) -> None:
    store = _catalog(conn)
    _seed(conn, [("a", 1.0, "f1"), ("b", 3.0, "f2"), ("c", 2.0, "f3")])

    keys = list(store.scan(order_by=[OrderBy("created_at", descending=True)]))

    assert keys == ["b", "c", "a"]


def test_key_fields_are_appended_ascending_for_a_total_order(
    conn: sqlite3.Connection,
) -> None:
    """The tiebreak must be in the ORDER BY, not left to the engine.

    Asserting only on returned rows is not enough: SQLite may coincidentally
    emit tied rows in key order anyway, so such a test passes even when the
    tiebreak is missing. Capture what reaches the adapter instead.
    """
    sql_kvset = _catalog_kvset()
    _catalog(conn)
    seen: list[OrderBy] = []

    class CapturingAdapter(SqliteSqlDbClientAdapter):
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
            seen.extend(order_by)
            return super().scan_ordered(
                table=table,
                columns=columns,
                key_fields=key_fields,
                exact=exact,
                prefixes=prefixes,
                order_by=order_by,
                limit=limit,
                live=live,
            )

    store = SqlDbKvStore(
        sql_kvset, adapter=CapturingAdapter(conn, db_name="capturing")
    )
    _seed(conn, [("c", 5.0, "f"), ("a", 5.0, "f"), ("b", 5.0, "f")])
    rows = list(store.scan(order_by=[OrderBy("created_at", descending=True)]))

    assert seen == [
        OrderBy("created_at", descending=True),
        OrderBy("id", descending=False),
    ]
    assert rows == ["a", "b", "c"]


def test_order_by_already_ending_with_key_fields_is_not_duplicated(
    conn: sqlite3.Connection,
) -> None:
    sql_kvset = _catalog_kvset()
    _catalog(conn)
    seen: list[OrderBy] = []

    class CapturingAdapter(SqliteSqlDbClientAdapter):
        def scan_ordered(
            self, *, table: str, columns: Sequence[str], key_fields: Sequence[str],
            exact: Mapping[str, Any], prefixes: Mapping[str, str],
            order_by: Sequence[OrderBy], limit: int | None,
            live: SqlLiveness | None = None,
        ) -> Iterator[SqlDbRow]:
            seen.extend(order_by)
            return super().scan_ordered(
                table=table, columns=columns, key_fields=key_fields, exact=exact,
                prefixes=prefixes, order_by=order_by, limit=limit, live=live,
            )

    store = SqlDbKvStore(sql_kvset, adapter=CapturingAdapter(conn, db_name="dedupe"))
    _seed(conn, [("a", 1.0, "f")])
    list(store.scan(order_by=[OrderBy("created_at"), OrderBy("id")]))

    assert seen == [OrderBy("created_at"), OrderBy("id")]


def test_order_by_unknown_column_raises_naming_it(conn: sqlite3.Connection) -> None:
    store = _catalog(conn)
    with pytest.raises(KvStoreScanUnsupported, match="nope"):
        list(store.scan(order_by=[OrderBy("nope")]))


def test_limit_is_fetched_from_the_adapter_not_just_returned(
    conn: sqlite3.Connection,
) -> None:
    """Client-side truncation also returns N rows — assert N were *fetched*."""
    sql_kvset = _catalog_kvset()
    _catalog(conn)
    _seed(conn, [(f"k{i}", float(i), "f") for i in range(50)])

    fetched = 0

    class CountingAdapter(SqliteSqlDbClientAdapter):
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
            nonlocal fetched
            for row in super().scan_ordered(
                table=table,
                columns=columns,
                key_fields=key_fields,
                exact=exact,
                prefixes=prefixes,
                order_by=order_by,
                limit=limit,
                live=live,
            ):
                fetched += 1
                yield row

    store = SqlDbKvStore(sql_kvset, adapter=CountingAdapter(conn, db_name="counting"))
    keys = list(
        store.scan(order_by=[OrderBy("created_at", descending=True)], limit=5)
    )

    assert len(keys) == 5
    assert fetched == 5, "limit must reach SQL, not truncate a full scan"


def test_adapter_without_ordering_support_is_unsupported_not_type_error(
    conn: sqlite3.Connection,
) -> None:
    sql_kvset = _catalog_kvset()
    _catalog(conn)

    class PlainAdapter(SqliteSqlDbClientAdapter):
        def supports_scan_ordering(self) -> bool:
            return False

    store = SqlDbKvStore(sql_kvset, adapter=PlainAdapter(conn, db_name="plain"))
    with pytest.raises(KvStoreScanUnsupported):
        list(store.scan(order_by=[OrderBy("created_at")]))


def test_sql_reports_key_asc_and_orderable_columns(conn: sqlite3.Connection) -> None:
    store = _catalog(conn)
    caps = store.scan_capabilities()

    assert ScanOrder.KEY_ASC in caps.orders
    assert caps.orderable_columns is not None
    assert {"created_at", "filename", "id"} <= caps.orderable_columns


@pytest.mark.parametrize("mode", [Pushdown.CLIENT_SIDE, Pushdown.STRICT])
def test_non_sql_backend_raises_under_both_pushdown_modes(mode: Pushdown) -> None:
    store = MemoryKvStore(
        KvSet.with_str_keys(
            "mem", key_serde=SerDe.identity(str), value_serde=SerDe.utf8_bytes()
        )
    )
    store.set("a", "v")
    with pytest.raises(KvStoreScanUnsupported):
        list(store.scan(order_by=[OrderBy("created_at")], on_unsupported=mode))


def test_order_and_order_by_together_raise(conn: sqlite3.Connection) -> None:
    # Contradictory arguments are a usage error, not a backend limitation.
    with pytest.raises(ValueError):
        ScanSpec(order=ScanOrder.KEY_ASC, order_by=(OrderBy("created_at"),))


def test_order_by_with_default_order_is_allowed() -> None:
    spec = ScanSpec(order_by=(OrderBy("created_at"),))
    assert spec.order is ScanOrder.UNORDERED


def test_order_by_with_cursor_raises(conn: sqlite3.Connection) -> None:
    from kvtsjl.store.schema.scan import ScanCursor

    with pytest.raises(KvStoreScanUnsupported, match="cursor"):
        ScanSpec(order_by=(OrderBy("created_at"),), cursor=ScanCursor(token="x"))


def _ttl_catalog_kvset(name: str = "ttlcat") -> SqlDbKvSet[str, str]:
    from datetime import timedelta

    from kvtsjl import TtlPolicy

    kvset: KvSet[str, str, str, SqlDbRow] = KvSet.with_str_keys(
        name,
        key_serde=SerDe.identity(str),
        value_serde=str_sql_row_serde(),
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=60)),
    )
    return SqlDbKvSet(
        kvset=kvset,
        leaf_key_fields=("id",),
        fields=(SqlDbField("value"), SqlDbField("created_at")),
    )


def test_pushed_limit_returns_n_live_rows_when_newer_rows_expired(
    conn: sqlite3.Connection,
) -> None:
    """A pushed LIMIT must not be filled with rows the store then discards.

    Expired rows stay in the table until GC runs. With the newest rows expired,
    ``LIMIT n`` applied before the expiry filter returns rows that are all
    dropped client-side — the caller gets fewer than ``n``, or none at all.
    """
    import time

    sql_kvset = _ttl_catalog_kvset()
    conn.execute(
        'CREATE TABLE "ttlcat_1" ("id" TEXT PRIMARY KEY, "value" TEXT, '
        '"created_at" REAL, "updated_at" REAL, "expires_at" REAL)'
    )
    now = time.time()
    for i in range(10):  # newest, but already expired
        conn.execute(
            'INSERT INTO "ttlcat_1" VALUES (?,?,?,?,?)',
            (f"dead{i}", "v", now + 100 + i, now - 999, now - 1),
        )
    for i in range(10):  # older, live
        conn.execute(
            'INSERT INTO "ttlcat_1" VALUES (?,?,?,?,?)',
            (f"live{i}", "v", now + i, now, None),
        )
    conn.commit()

    seen_live: list[SqlLiveness | None] = []

    class CapturingAdapter(SqliteSqlDbClientAdapter):
        def scan_ordered(
            self, *, table: str, columns: Sequence[str], key_fields: Sequence[str],
            exact: Mapping[str, Any], prefixes: Mapping[str, str],
            order_by: Sequence[OrderBy], limit: int | None,
            live: SqlLiveness | None = None,
        ) -> Iterator[SqlDbRow]:
            seen_live.append(live)
            return super().scan_ordered(
                table=table, columns=columns, key_fields=key_fields, exact=exact,
                prefixes=prefixes, order_by=order_by, limit=limit, live=live,
            )

    store = SqlDbKvStore(sql_kvset, adapter=CapturingAdapter(conn, db_name="ttl"))
    keys = list(
        store.scan(order_by=[OrderBy("created_at", descending=True)], limit=5)
    )

    assert len(keys) == 5, "pushed LIMIT under-filled past expired rows"
    assert keys == ["live9", "live8", "live7", "live6", "live5"]
    # The adapter must receive the liveness condition, not just a LIMIT.
    assert seen_live and seen_live[0] is not None
    assert seen_live[0].ttl_seconds == 60
    assert seen_live[0].expires_at_field == "expires_at"
