"""SqlDbKvStore tests against SqliteSqlDbClientAdapter (stdlib sqlite3)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import sqlite3

from freezegun import freeze_time
import pytest

from kvtsjl import ExpiryGc, KeyLayout, TtlPolicy
from kvtsjl.backends.sql import SqlDbKvStore, SqliteSqlDbClientAdapter
from kvtsjl.exceptions import KvStoreScanUnsupported
from kvtsjl.store.schema.ttl import TTL_NONE_EXPIRES_AT
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
def test_sql_scope_isolation(sql_store: SqlDbKvStore[str, str]) -> None:
    from tests.conformance import assert_scope_isolation

    assert_scope_isolation(sql_store)


@pytest.mark.sql
def test_sql_batch_return_counts(sql_store: SqlDbKvStore[str, str]) -> None:
    from tests.conformance import assert_batch_return_counts

    assert_batch_return_counts(sql_store)


@pytest.mark.sql
def test_sql_ttl_collection_binds_expires_at(sqlite_conn: sqlite3.Connection) -> None:
    """Collection relative TTL is stored as a concrete ``expires_at``."""
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
            f'SELECT expires_at, updated_at FROM "{sql_kvset.table_name}" WHERE id = ?',
            ("a",),
        ).fetchone()
        assert row is not None
        expires_at, updated_at = row
        assert expires_at is not None
        assert expires_at == updated_at + 30
        frozen.move_to("2024-01-01 12:01:00")
        assert store.get("a") is None
        assert (
            sqlite_conn.execute(
                f'SELECT COUNT(*) FROM "{sql_kvset.table_name}"'
            ).fetchone()[0]
            == 0
        )


@pytest.mark.sql
def test_sql_no_ttl_policy_writes_null_expires_at(
    sqlite_conn: sqlite3.Connection,
) -> None:
    """With no TTL policy, NULL ``expires_at`` means never expires."""
    sql_kvset = make_sql_kvset("no_ttl")
    ensure_sql_table(sqlite_conn, sql_kvset)
    store = SqlDbKvStore(
        sql_kvset,
        adapter=SqliteSqlDbClientAdapter(sqlite_conn),
    )
    store.set("a", "live")
    row = sqlite_conn.execute(
        f'SELECT expires_at FROM "{sql_kvset.table_name}" WHERE id = ?',
        ("a",),
    ).fetchone()
    assert row is not None
    assert row[0] is None
    assert store.get("a") == "live"


@pytest.mark.sql
def test_sql_null_expires_at_still_expires_via_updated_at(
    sqlite_conn: sqlite3.Connection,
) -> None:
    """Rows with NULL ``expires_at`` still expire from ``updated_at`` + policy."""
    sql_kvset = make_sql_kvset(
        "ttl_legacy",
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=30)),
    )
    ensure_sql_table(sqlite_conn, sql_kvset)
    with freeze_time("2024-01-01 12:00:00") as frozen:
        write_epoch = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC).timestamp()
        sqlite_conn.execute(
            f'INSERT INTO "{sql_kvset.table_name}" '
            f"(id, value, updated_at, expires_at) VALUES (?, ?, ?, NULL)",
            ("legacy", "v", write_epoch),
        )
        sqlite_conn.commit()
        store = SqlDbKvStore(
            sql_kvset,
            adapter=SqliteSqlDbClientAdapter(sqlite_conn),
        )
        assert store.get("legacy") == "v"
        frozen.move_to("2024-01-01 12:01:00")
        assert store.get("legacy") is None


@pytest.mark.sql
def test_sql_bound_expires_at_ignores_later_policy_shortening(
    sqlite_conn: sqlite3.Connection,
) -> None:
    """Shortening the kvset TTL must not silently kill already-written rows."""
    long_kvset = make_sql_kvset(
        "ttl_shorten",
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=120)),
    )
    ensure_sql_table(sqlite_conn, long_kvset)
    with freeze_time("2024-01-01 12:00:00") as frozen:
        writer = SqlDbKvStore(
            long_kvset,
            adapter=SqliteSqlDbClientAdapter(sqlite_conn),
        )
        writer.set("a", "live")
        # Same table, shorter collection policy — row already carries death time.
        short_kvset = make_sql_kvset(
            "ttl_shorten",
            ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=10)),
        )
        reader = SqlDbKvStore(
            short_kvset,
            adapter=SqliteSqlDbClientAdapter(sqlite_conn),
        )
        frozen.move_to("2024-01-01 12:00:30")
        assert reader.get("a") == "live"
        frozen.move_to("2024-01-01 12:03:00")
        assert reader.get("a") is None


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
        short_exp = sqlite_conn.execute(
            f'SELECT expires_at FROM "{sql_kvset.table_name}" WHERE id = ?',
            ("short",),
        ).fetchone()[0]
        pinned_exp = sqlite_conn.execute(
            f'SELECT expires_at FROM "{sql_kvset.table_name}" WHERE id = ?',
            ("pinned",),
        ).fetchone()[0]
        write_epoch = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC).timestamp()
        assert short_exp == write_epoch + 30
        assert pinned_exp == TTL_NONE_EXPIRES_AT.timestamp()
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


@pytest.mark.sql
def test_sql_gc_expired(sqlite_conn: sqlite3.Connection) -> None:
    sql_kvset = make_sql_kvset(
        "gc",
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=30)),
    )
    ensure_sql_table(sqlite_conn, sql_kvset)
    with freeze_time("2024-01-01 12:00:00") as frozen:
        store = SqlDbKvStore(
            sql_kvset,
            adapter=SqliteSqlDbClientAdapter(sqlite_conn),
            expiry_gc=ExpiryGc.HIDE,
        )
        store.set("a", "1")
        store.set("b", "2")
        store.set("live", "3", ttl=TtlPolicy.none())
        frozen.move_to("2024-01-01 12:01:00")
        assert store.get("a") is None
        assert (
            sqlite_conn.execute(
                f'SELECT COUNT(*) FROM "{sql_kvset.table_name}"'
            ).fetchone()[0]
            == 3
        )
        assert store.gc_expired(max_entries=1) == 1
        assert (
            sqlite_conn.execute(
                f'SELECT COUNT(*) FROM "{sql_kvset.table_name}"'
            ).fetchone()[0]
            == 2
        )
        assert store.gc_expired(max_entries=10) == 1
        assert store.get("live") == "3"
        assert (
            sqlite_conn.execute(
                f'SELECT COUNT(*) FROM "{sql_kvset.table_name}"'
            ).fetchone()[0]
            == 1
        )
