"""SqlDbKvStore tests against SqliteSqlDbClientAdapter (stdlib sqlite3)."""

from __future__ import annotations

from datetime import timedelta
import sqlite3

from freezegun import freeze_time
import pytest

from kvtsjl import ExpiryGc, KeyLayout, TtlPolicy
from kvtsjl.backends.sql import SqlDbKvStore, SqliteSqlDbClientAdapter
from kvtsjl.exceptions import KvStoreScanUnsupported
from tests.conformance import assert_basic_crud, assert_batch_ops, assert_scan_and_scope
from tests.sql_helpers import ensure_sql_table, make_composite_sql_kvset, make_sql_kvset


@pytest.mark.sql
def test_sql_crud(sql_store: SqlDbKvStore[str, str]) -> None:
    assert_basic_crud(sql_store)


@pytest.mark.sql
def test_sql_batch(sql_store: SqlDbKvStore[str, str]) -> None:
    assert_batch_ops(sql_store)


@pytest.mark.sql
def test_sql_scan_and_scope(sql_store: SqlDbKvStore[str, str]) -> None:
    assert_scan_and_scope(sql_store)


@pytest.mark.sql
def test_sql_ttl_collection_updated_at(sqlite_conn: sqlite3.Connection) -> None:
    sql_kvset = make_sql_kvset(
        "ttl",
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=30)),
    )
    ensure_sql_table(sqlite_conn, sql_kvset)
    with freeze_time("2024-01-01 12:00:00") as frozen:
        store = SqlDbKvStore(
            sql_kvset,
            adapter=SqliteSqlDbClientAdapter(sqlite_conn),
        )
        store.set("a", "live")
        assert store.get("a") == "live"
        row = sqlite_conn.execute(
            f'SELECT expires_at FROM "{sql_kvset.table_name}" WHERE id = ?',
            ("a",),
        ).fetchone()
        assert row is not None
        assert row[0] is None
        frozen.move_to("2024-01-01 12:01:00")
        assert store.get("a") is None
        assert (
            sqlite_conn.execute(
                f'SELECT COUNT(*) FROM "{sql_kvset.table_name}"'
            ).fetchone()[0]
            == 0
        )


@pytest.mark.sql
def test_sql_ttl_per_write_expires_at(sqlite_conn: sqlite3.Connection) -> None:
    sql_kvset = make_sql_kvset("ttl", ttl_policy=TtlPolicy.hourly())
    ensure_sql_table(sqlite_conn, sql_kvset)
    with freeze_time("2024-01-01 12:00:00") as frozen:
        store = SqlDbKvStore(
            sql_kvset,
            adapter=SqliteSqlDbClientAdapter(sqlite_conn),
        )
        store.set("short", "b", ttl=TtlPolicy(ttl_duration=timedelta(seconds=30)))
        store.set("pinned", "c", ttl=TtlPolicy.none())
        frozen.move_to("2024-01-01 12:01:00")
        assert store.get("short") is None
        assert store.get("pinned") == "c"


@pytest.mark.sql
def test_sql_expiry_gc_hide(sqlite_conn: sqlite3.Connection) -> None:
    sql_kvset = make_sql_kvset(
        "ttl",
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=30)),
    )
    ensure_sql_table(sqlite_conn, sql_kvset)
    with freeze_time("2024-01-01 12:00:00") as frozen:
        store = SqlDbKvStore(
            sql_kvset,
            adapter=SqliteSqlDbClientAdapter(sqlite_conn),
            expiry_gc=ExpiryGc.HIDE,
        )
        store.set("a", "live")
        frozen.move_to("2024-01-01 12:01:00")
        assert store.get("a") is None
        assert (
            sqlite_conn.execute(
                f'SELECT COUNT(*) FROM "{sql_kvset.table_name}"'
            ).fetchone()[0]
            == 1
        )


@pytest.mark.sql
def test_sql_hashed_scan_unsupported(sqlite_conn: sqlite3.Connection) -> None:
    sql_kvset = make_sql_kvset("hashed", key_layout=KeyLayout.HASHED)
    ensure_sql_table(sqlite_conn, sql_kvset)
    store = SqlDbKvStore(
        sql_kvset,
        adapter=SqliteSqlDbClientAdapter(sqlite_conn),
    )
    store.set("k", "v")
    assert store.get("k") == "v"
    with pytest.raises(KvStoreScanUnsupported):
        list(store.scan())


@pytest.mark.sql
def test_sql_composite_key_and_scope_columns(sqlite_conn: sqlite3.Connection) -> None:
    sql_kvset = make_composite_sql_kvset()
    ensure_sql_table(sqlite_conn, sql_kvset)
    root = SqlDbKvStore(
        sql_kvset,
        adapter=SqliteSqlDbClientAdapter(sqlite_conn),
    )
    acme = root.scoped(tenant="acme")
    other = root.scoped(tenant="other")
    acme.set(("electronics", "phone"), "p1")
    acme.set(("electronics", "laptop"), "p2")
    acme.set(("home", "chair"), "p3")
    other.set(("electronics", "phone"), "x")

    assert acme.get(("electronics", "phone")) == "p1"
    assert other.get(("electronics", "phone")) == "x"
    assert acme.get(("electronics", "missing")) is None

    assert sorted(acme.list()) == [
        ("electronics", "laptop"),
        ("electronics", "phone"),
        ("home", "chair"),
    ]
    assert sorted(acme.scan(prefix=("electronics", ""))) == [
        ("electronics", "laptop"),
        ("electronics", "phone"),
    ]
    assert sorted(acme.scan(prefix=("electronics", "ph"))) == [
        ("electronics", "phone"),
    ]
    assert sorted(acme.scan(prefix=("", "ph"))) == [
        ("electronics", "phone"),
    ]
    assert sorted(acme.scan(prefix=("", ""))) == sorted(acme.list())
    assert dict(acme.scan(prefix=("home", ""), include_values=True)) == {
        ("home", "chair"): "p3",
    }


@pytest.mark.sql
def test_sql_scope_as_column_single_leaf(sqlite_conn: sqlite3.Connection) -> None:
    sql_kvset = make_sql_kvset(
        "scoped_docs",
        leaf_key_fields=("id",),
        scope_fields=("region",),
        scope_schema=("region",),
    )
    ensure_sql_table(sqlite_conn, sql_kvset)
    store = SqlDbKvStore(
        sql_kvset,
        adapter=SqliteSqlDbClientAdapter(sqlite_conn),
    )
    us = store.scoped(region="us")
    eu = store.scoped(region="eu")
    us.set("a", "us-a")
    eu.set("a", "eu-a")
    assert us.get("a") == "us-a"
    assert eu.get("a") == "eu-a"
    assert sorted(us.list()) == ["a"]
    assert sorted(eu.scan(include_values=True)) == [("a", "eu-a")]
