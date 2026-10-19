"""SqlDbKvStore tests against SqliteSqlDbClientAdapter (stdlib sqlite3)."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, datetime, timedelta
import sqlite3
from typing import Any

from freezegun import freeze_time
import pytest

from kvtsjl import ExpiryGc, KeyLayout, TtlPolicy
from kvtsjl.backends.sql import ExpiryBinding, SqlDbKvStore, SqliteSqlDbClientAdapter
from kvtsjl.backends.sql.schema import SqlDbRow
from kvtsjl.exceptions import KvStoreError, KvStoreReadOnlyError, KvStoreScanUnsupported
from kvtsjl.store.schema.scan import Projection, ScanCursor, ScanOrder, ScanSpec
from kvtsjl.store.schema.ttl import TTL_NONE_EXPIRES_AT
from tests.conformance import assert_basic_crud, assert_batch_ops, assert_scan_and_scope
from tests.sql_helpers import ensure_sql_table, make_composite_sql_kvset, make_sql_kvset


class _CountingExpiryAdapter(SqliteSqlDbClientAdapter):
    """Records ``scan_expired`` / ``scan_by_key_parts`` calls for pushdown gates."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        super().__init__(connection)
        self.scan_expired_calls: list[dict[str, Any]] = []
        self.scan_parts_calls = 0
        self._expiry_enabled = True

    def supports_expiry_scan(self) -> bool:
        return self._expiry_enabled

    def disable_expiry_scan(self) -> None:
        self._expiry_enabled = False

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
        self.scan_expired_calls.append(
            {
                "table": table,
                "relative_ttl_seconds": relative_ttl_seconds,
                "limit": limit,
            }
        )
        return super().scan_expired(
            table=table,
            columns=columns,
            key_fields=key_fields,
            exact=exact,
            expires_at_field=expires_at_field,
            updated_at_field=updated_at_field,
            relative_ttl_seconds=relative_ttl_seconds,
            now=now,
            limit=limit,
        )

    def scan_by_key_parts(
        self,
        *,
        table: str,
        columns: Sequence[str],
        key_fields: Sequence[str],
        exact: Mapping[str, Any],
        prefixes: Mapping[str, str],
    ) -> Iterator[SqlDbRow]:
        self.scan_parts_calls += 1
        return super().scan_by_key_parts(
            table=table,
            columns=columns,
            key_fields=key_fields,
            exact=exact,
            prefixes=prefixes,
        )


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


@pytest.mark.sql
def test_sql_expired_keys_pushdown_both_branches(
    sqlite_conn: sqlite3.Connection,
) -> None:
    """Pushdown finds concrete expires_at rows and legacy NULL relative-TTL rows."""
    sql_kvset = make_sql_kvset(
        "expiry_scan",
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=30)),
    )
    ensure_sql_table(sqlite_conn, sql_kvset)
    adapter = _CountingExpiryAdapter(sqlite_conn)
    with freeze_time("2024-01-01 12:00:00") as frozen:
        store = SqlDbKvStore(sql_kvset, adapter=adapter)
        store.set("bound", "1")  # expires_at = now + 30
        store.set("live", "2", ttl=TtlPolicy.none())
        # Legacy NULL relative-TTL row (written before death times were bound).
        write_epoch = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC).timestamp()
        sqlite_conn.execute(
            f'INSERT INTO "{sql_kvset.table_name}" '
            f"(id, value, updated_at, expires_at) VALUES (?, ?, ?, NULL)",
            ("legacy", "3", write_epoch),
        )
        sqlite_conn.commit()
        frozen.move_to("2024-01-01 12:01:00")
        now = datetime(2024, 1, 1, 12, 1, 0, tzinfo=UTC)
        keys = store.expired_keys(now=now, limit=10)
        assert sorted(keys) == ["bound", "legacy"]
        assert len(adapter.scan_expired_calls) == 1
        assert adapter.scan_expired_calls[0]["relative_ttl_seconds"] == 30
        assert adapter.scan_parts_calls == 0


@pytest.mark.sql
def test_sql_expired_keys_oldest_first_and_limit(
    sqlite_conn: sqlite3.Connection,
) -> None:
    sql_kvset = make_sql_kvset(
        "expiry_order",
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=60)),
    )
    ensure_sql_table(sqlite_conn, sql_kvset)
    with freeze_time("2024-01-01 12:00:00") as frozen:
        store = SqlDbKvStore(
            sql_kvset,
            adapter=SqliteSqlDbClientAdapter(sqlite_conn),
        )
        store.set(
            "older",
            "2",
            created_at=datetime(2024, 1, 1, 11, 58, 0, tzinfo=UTC),
        )
        store.set("newer", "1")
        frozen.move_to("2024-01-01 12:02:00")
        now = datetime(2024, 1, 1, 12, 2, 0, tzinfo=UTC)
        assert store.expired_keys(now=now, limit=1) == ["older"]
        assert store.expired_keys(now=now, limit=10) == ["older", "newer"]


@pytest.mark.sql
def test_sql_expired_keys_respects_scope(sqlite_conn: sqlite3.Connection) -> None:
    sql_kvset = make_sql_kvset(
        "expiry_scope",
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=30)),
        scope_fields=("tenant",),
        scope_schema=("tenant",),
    )
    ensure_sql_table(sqlite_conn, sql_kvset)
    with freeze_time("2024-01-01 12:00:00") as frozen:
        root = SqlDbKvStore(
            sql_kvset,
            adapter=SqliteSqlDbClientAdapter(sqlite_conn),
        )
        acme = root.scoped(tenant="acme")
        zeta = root.scoped(tenant="zeta")
        acme.set("a", "1")
        zeta.set("a", "2")
        frozen.move_to("2024-01-01 12:01:00")
        now = datetime(2024, 1, 1, 12, 1, 0, tzinfo=UTC)
        assert acme.expired_keys(now=now, limit=10) == ["a"]
        assert zeta.expired_keys(now=now, limit=10) == ["a"]
        # Unscoped root sees both (same leaf key in two scopes — two rows).
        assert sorted(root.expired_keys(now=now, limit=10)) == ["a", "a"]


@pytest.mark.sql
def test_sql_expired_keys_matches_python_row_expired(
    sqlite_conn: sqlite3.Connection,
) -> None:
    """Pushdown result equals the Python fallback scan on the same rows."""
    sql_kvset = make_sql_kvset(
        "expiry_parity",
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=30)),
    )
    ensure_sql_table(sqlite_conn, sql_kvset)
    pushed_adapter = _CountingExpiryAdapter(sqlite_conn)
    fallback_adapter = _CountingExpiryAdapter(sqlite_conn)
    fallback_adapter.disable_expiry_scan()
    with freeze_time("2024-01-01 12:00:00") as frozen:
        store = SqlDbKvStore(sql_kvset, adapter=pushed_adapter)
        store.set("a", "1")
        store.set("b", "2", ttl=TtlPolicy(ttl_duration=timedelta(seconds=120)))
        store.set("c", "3", ttl=TtlPolicy.none())
        write_epoch = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC).timestamp()
        sqlite_conn.execute(
            f'INSERT INTO "{sql_kvset.table_name}" '
            f"(id, value, updated_at, expires_at) VALUES (?, ?, ?, NULL)",
            ("legacy", "4", write_epoch),
        )
        sqlite_conn.commit()
        frozen.move_to("2024-01-01 12:01:00")
        now = datetime(2024, 1, 1, 12, 1, 0, tzinfo=UTC)
        pushed = store.expired_keys(now=now, limit=100)
        fallback = SqlDbKvStore(sql_kvset, adapter=fallback_adapter).expired_keys(
            now=now, limit=100
        )
        assert sorted(pushed) == sorted(fallback)
        assert pushed_adapter.scan_expired_calls
        assert fallback_adapter.scan_expired_calls == []


@pytest.mark.sql
def test_sql_expired_keys_fallback_without_capability(
    sqlite_conn: sqlite3.Connection,
) -> None:
    sql_kvset = make_sql_kvset(
        "expiry_fallback",
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=30)),
    )
    ensure_sql_table(sqlite_conn, sql_kvset)
    adapter = _CountingExpiryAdapter(sqlite_conn)
    adapter.disable_expiry_scan()
    with freeze_time("2024-01-01 12:00:00") as frozen:
        store = SqlDbKvStore(sql_kvset, adapter=adapter)
        store.set("a", "1")
        store.set("b", "2")
        frozen.move_to("2024-01-01 12:01:00")
        now = datetime(2024, 1, 1, 12, 1, 0, tzinfo=UTC)
        assert sorted(store.expired_keys(now=now, limit=10)) == ["a", "b"]
        assert adapter.scan_expired_calls == []
        assert store.gc_expired(max_entries=10) == 2


@pytest.mark.sql
def test_sql_early_binding_raises_when_null_rows_remain(
    sqlite_conn: sqlite3.Connection,
) -> None:
    sql_kvset = make_sql_kvset(
        "expiry_early",
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=30)),
        expiry_binding=ExpiryBinding.EARLY,
    )
    ensure_sql_table(sqlite_conn, sql_kvset)
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
    with pytest.raises(KvStoreError, match="un-backfilled"):
        store.expired_keys(limit=10)


@pytest.mark.sql
def test_sql_early_binding_single_branch_after_backfill(
    sqlite_conn: sqlite3.Connection,
) -> None:
    sql_kvset = make_sql_kvset(
        "expiry_early_ok",
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=30)),
        expiry_binding=ExpiryBinding.EARLY,
    )
    ensure_sql_table(sqlite_conn, sql_kvset)
    adapter = _CountingExpiryAdapter(sqlite_conn)
    with freeze_time("2024-01-01 12:00:00") as frozen:
        store = SqlDbKvStore(sql_kvset, adapter=adapter)
        store.set("a", "1")
        frozen.move_to("2024-01-01 12:01:00")
        now = datetime(2024, 1, 1, 12, 1, 0, tzinfo=UTC)
        assert store.expired_keys(now=now, limit=10) == ["a"]
        assert adapter.scan_expired_calls[0]["relative_ttl_seconds"] is None


def _insert_raw(
    conn: sqlite3.Connection, table: str, key: str, updated_at: float, expires_at: float | None
) -> None:
    conn.execute(
        f'INSERT INTO "{table}" (id, value, updated_at, expires_at) VALUES (?, ?, ?, ?)',
        (key, "v", updated_at, expires_at),
    )
    conn.commit()


def test_sql_expired_keys_orders_by_death_time_across_both_branches(
    sqlite_conn: sqlite3.Connection,
) -> None:
    """Oldest *death* first, mixing absolute and relative rows.

    The parity test compares sorted sets, so it cannot see order. Order is what
    ``limit`` spends: ordering by ``expires_at`` alone puts every relative row
    first (SQL sorts NULL first), so absolute rows that died long ago can be
    starved by newer relative ones for as long as the backlog lasts.
    """
    sql_kvset = make_sql_kvset(
        "expiry_order", ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=30))
    )
    ensure_sql_table(sqlite_conn, sql_kvset)
    with freeze_time("2024-01-01 12:00:00"):
        now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        t = now.timestamp()
        table = sql_kvset.table_name
        # deaths: abs_oldest t-100, rel_mid t-30 (t-60 + 30s ttl), abs_newest t-10
        _insert_raw(sqlite_conn, table, "abs_oldest", t, t - 100)
        _insert_raw(sqlite_conn, table, "rel_mid", t - 60, None)
        _insert_raw(sqlite_conn, table, "abs_newest", t, t - 10)
        store = SqlDbKvStore(sql_kvset, adapter=SqliteSqlDbClientAdapter(sqlite_conn))

        assert store.expired_keys(now=now, limit=10) == [
            "abs_oldest",
            "rel_mid",
            "abs_newest",
        ]
        # the strongest form: a budget of one must spend it on the oldest death
        assert store.expired_keys(now=now, limit=1) == ["abs_oldest"]


def test_sql_expired_keys_includes_a_row_dying_exactly_now(
    sqlite_conn: sqlite3.Connection,
) -> None:
    """The boundary is ``<=``, matching ``_row_expired``'s ``>=``.

    Randomised parity data never lands on exact equality, so the boundary needs
    its own case on both branches.
    """
    sql_kvset = make_sql_kvset(
        "expiry_boundary", ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=30))
    )
    ensure_sql_table(sqlite_conn, sql_kvset)
    with freeze_time("2024-01-01 12:00:00"):
        now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        t = now.timestamp()
        table = sql_kvset.table_name
        _insert_raw(sqlite_conn, table, "abs_exactly_now", t, t)
        _insert_raw(sqlite_conn, table, "rel_exactly_now", t - 30, None)
        store = SqlDbKvStore(sql_kvset, adapter=SqliteSqlDbClientAdapter(sqlite_conn))

        assert sorted(store.expired_keys(now=now, limit=10)) == [
            "abs_exactly_now",
            "rel_exactly_now",
        ]
        assert store.get("abs_exactly_now") is None
        assert store.get("rel_exactly_now") is None


def test_expired_keys_is_generic_and_gc_is_sugar_over_it(
    sqlite_conn: sqlite3.Connection,
) -> None:
    """``expired_keys`` is the primitive; every backend inherits the delete.

    Pins the hoist: a backend that implements only ``expired_keys`` still
    garbage-collects, and the two agree on which keys they mean.
    """
    from kvtsjl import KvSet, MemoryKvStore, SerDe

    ttl = TtlPolicy(ttl_duration=timedelta(seconds=30))
    sql_kvset = make_sql_kvset("generic_expiry", ttl_policy=ttl)
    ensure_sql_table(sqlite_conn, sql_kvset)
    # A dict walk and a SQL pushdown: two different implementations of the one
    # primitive, both inheriting the deletion from the base class.
    stores: list[Any] = [
        MemoryKvStore(
            KvSet.with_str_keys(
                "mem",
                key_serde=SerDe.identity(str),
                value_serde=SerDe.identity(bytes),
                ttl_policy=ttl,
            )
        ),
        SqlDbKvStore(sql_kvset, adapter=SqliteSqlDbClientAdapter(sqlite_conn)),
    ]
    for store in stores:
        label = type(store).__name__
        with freeze_time("2024-01-01 12:00:00") as frozen:
            one, two, three = (b"1", b"2", b"3") if "Memory" in label else ("1", "2", "3")
            store.set("live", one)
            store.set("dead", two)
            frozen.move_to("2024-01-01 12:00:10")
            store.set("fresh", three)
            # 'live'/'dead' die at 12:00:30; 'fresh' not until 12:00:40
            frozen.move_to("2024-01-01 12:00:35")

            found = set(store.expired_keys(limit=10))
            assert found == {"live", "dead"}, f"{label}: {found}"
            assert len(store.expired_keys(limit=1)) == 1, label
            # read-only: calling it twice must not have deleted anything
            assert set(store.expired_keys(limit=10)) == {"live", "dead"}, label

            collected = set(store.gc_expired_keys(max_entries=10))
            assert collected == {"live", "dead"}, f"{label}: {collected}"
            assert store.expired_keys(limit=10) == [], label
            assert store.get("fresh") == three, label
            assert store.get("dead") is None, label


def test_readonly_keeps_expired_keys_but_refuses_gc(
    sqlite_conn: sqlite3.Connection,
) -> None:
    """Listing what expired is a read; collecting it is a write."""
    sql_kvset = make_sql_kvset(
        "ro_expiry", ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=30))
    )
    ensure_sql_table(sqlite_conn, sql_kvset)
    with freeze_time("2024-01-01 12:00:00") as frozen:
        store = SqlDbKvStore(sql_kvset, adapter=SqliteSqlDbClientAdapter(sqlite_conn))
        store.set("a", "1")
        frozen.move_to("2024-01-01 12:01:00")
        view = store.readonly()

        assert view.expired_keys(limit=10) == ["a"]
        with pytest.raises(KvStoreReadOnlyError):
            view.gc_expired_keys(max_entries=10)


def _column_view(field: str) -> Any:
    """A SqlDbKvSet owning exactly one value column of a shared table."""
    from kvtsjl import SerDe
    from kvtsjl.backends.sql.schema import SqlDbField, SqlDbKvSet
    from kvtsjl.store.schema.kvset import KvSet

    kvset = KvSet.with_str_keys(
        "shared",
        key_serde=SerDe.identity(str),
        value_serde=SerDe[str, SqlDbRow](
            serializer=lambda v: {field: v},
            deserializer=lambda row: row[field],
            blob_type=dict,
        ),
    )
    return SqlDbKvSet(kvset=kvset, leaf_key_fields=("id",), fields=(SqlDbField(field),))


def test_set_is_already_insert_or_update_on_a_shared_table() -> None:
    """Two views of one row both write with ``set``; neither clobbers the other.

    This is why there is no ``patch``. ``upsert_rows`` lists only the view's own
    columns in both the INSERT and the ON CONFLICT DO UPDATE, so a sibling
    view's columns are never touched, and a view that writes second updates
    rather than replacing the row.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        'CREATE TABLE "shared_1" (id TEXT PRIMARY KEY, a TEXT, b TEXT, '
        "updated_at REAL NOT NULL, expires_at REAL)"
    )
    view_a = SqlDbKvStore(_column_view("a"), adapter=SqliteSqlDbClientAdapter(conn))
    view_b = SqlDbKvStore(_column_view("b"), adapter=SqliteSqlDbClientAdapter(conn))

    view_a.set("k", "from-A")   # first writer inserts the row
    view_b.set("k", "from-B")   # second writer updates only its own column

    assert view_a.get("k") == "from-A", "sibling view clobbered column a"
    assert view_b.get("k") == "from-B"
    assert len(conn.execute('SELECT id FROM "shared_1"').fetchall()) == 1

    view_a.set("k", "A-again")  # and back the other way
    assert (view_a.get("k"), view_b.get("k")) == ("A-again", "from-B")


def test_a_shared_table_needs_nullable_or_defaulted_sibling_columns() -> None:
    """The one real constraint on sharing a table, and it lives in the DDL.

    A view can only fill its own columns, so whichever view inserts the row
    first leaves the siblings to the schema. ``NOT NULL`` with no default makes
    that insert impossible — which is a DDL decision, and DDL is caller-owned.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        'CREATE TABLE "shared_1" (id TEXT PRIMARY KEY, a TEXT, b TEXT NOT NULL, '
        "updated_at REAL NOT NULL, expires_at REAL)"
    )
    view_a = SqlDbKvStore(_column_view("a"), adapter=SqliteSqlDbClientAdapter(conn))

    with pytest.raises(sqlite3.IntegrityError, match="NOT NULL"):
        view_a.set("k", "from-A")

    # the same schema with a default inserts cleanly
    conn2 = sqlite3.connect(":memory:")
    conn2.row_factory = sqlite3.Row
    conn2.execute(
        'CREATE TABLE "shared_1" (id TEXT PRIMARY KEY, a TEXT, '
        "b TEXT NOT NULL DEFAULT '', updated_at REAL NOT NULL, expires_at REAL)"
    )
    ok = SqlDbKvStore(_column_view("a"), adapter=SqliteSqlDbClientAdapter(conn2))
    ok.set("k", "from-A")
    assert ok.get("k") == "from-A"


@pytest.mark.sql
def test_sql_autocommit_false_rollback_drops_writes(
    sqlite_conn: sqlite3.Connection,
) -> None:
    sql_kvset = make_sql_kvset("txn_rollback")
    ensure_sql_table(sqlite_conn, sql_kvset)
    store = SqlDbKvStore(
        sql_kvset,
        adapter=SqliteSqlDbClientAdapter(sqlite_conn, autocommit=False),
    )
    sqlite_conn.execute("BEGIN")
    store.set("a", "1")
    store.set("b", "2")
    sqlite_conn.rollback()
    assert (
        sqlite_conn.execute(
            f'SELECT COUNT(*) FROM "{sql_kvset.table_name}"'
        ).fetchone()[0]
        == 0
    )


@pytest.mark.sql
def test_sql_autocommit_true_commits_each_write(
    sqlite_conn: sqlite3.Connection,
) -> None:
    sql_kvset = make_sql_kvset("txn_autocommit")
    ensure_sql_table(sqlite_conn, sql_kvset)
    store = SqlDbKvStore(
        sql_kvset, adapter=SqliteSqlDbClientAdapter(sqlite_conn)
    )
    store.set("a", "1")
    assert not sqlite_conn.in_transaction
    assert store.get("a") == "1"


@pytest.mark.sql
def test_sql_autocommit_false_raises_without_open_transaction(
    sqlite_conn: sqlite3.Connection,
) -> None:
    sql_kvset = make_sql_kvset("txn_no_begin")
    ensure_sql_table(sqlite_conn, sql_kvset)
    store = SqlDbKvStore(
        sql_kvset,
        adapter=SqliteSqlDbClientAdapter(sqlite_conn, autocommit=False),
    )
    with pytest.raises(KvStoreError, match="no open transaction"):
        store.set("a", "1")
    assert (
        sqlite_conn.execute(
            f'SELECT COUNT(*) FROM "{sql_kvset.table_name}"'
        ).fetchone()[0]
        == 0
    )


@pytest.mark.sql
def test_sql_rebind_keeps_store_identity_and_routes_reads(
    sqlite_conn: sqlite3.Connection,
) -> None:
    """``rebind`` is the same ``_clone`` path as ``scoped``; adapter covers reads."""

    class _FetchCountingAdapter(SqliteSqlDbClientAdapter):
        def __init__(self, connection: sqlite3.Connection) -> None:
            super().__init__(connection)
            self.fetch_calls = 0

        def fetch_by_keys(
            self,
            *,
            table: str,
            columns: Sequence[str],
            key_fields: Sequence[str],
            keys: Sequence[SqlDbRow],
        ) -> Sequence[SqlDbRow]:
            self.fetch_calls += 1
            return super().fetch_by_keys(
                table=table,
                columns=columns,
                key_fields=key_fields,
                keys=keys,
            )

    sql_kvset = make_sql_kvset(
        "rebind_docs",
        leaf_key_fields=("id",),
        scope_fields=("region",),
        scope_schema=("region",),
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=30)),
    )
    ensure_sql_table(sqlite_conn, sql_kvset)
    first = _FetchCountingAdapter(sqlite_conn)
    with freeze_time("2024-01-01 12:00:00") as frozen:
        root = SqlDbKvStore(
            sql_kvset,
            adapter=first,
            expiry_gc=ExpiryGc.HIDE,
            batch_size=17,
        )
        scoped = root.scoped(region="us")
        scoped.set("k", "v")

        second = _FetchCountingAdapter(sqlite_conn)
        rebound = scoped.rebind(second)
        assert rebound.scope == scoped.scope
        assert rebound.kvset == scoped.kvset
        assert rebound.batch_size == 17
        assert rebound.get("k") == "v"
        assert second.fetch_calls == 1
        assert first.fetch_calls == 0

        # ExpiryGc.HIDE survived _clone: expired get hides, does not delete.
        frozen.move_to("2024-01-01 12:01:00")
        assert rebound.get("k") is None
        assert (
            sqlite_conn.execute(
                f'SELECT COUNT(*) FROM "{sql_kvset.table_name}"'
            ).fetchone()[0]
            == 1
        )


class _PagingAdapter(SqliteSqlDbClientAdapter):
    """Records keyset page fetches (after_key + page_size)."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        super().__init__(connection)
        self.page_calls: list[dict[str, Any]] = []
        self._paged = True

    def supports_paged_scan(self) -> bool:
        return self._paged

    def disable_paged_scan(self) -> None:
        self._paged = False

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
        self.page_calls.append(
            {
                "after_key": None if after_key is None else dict(after_key),
                "page_size": page_size,
            }
        )
        return super().scan_page_by_key_parts(
            table=table,
            columns=columns,
            key_fields=key_fields,
            exact=exact,
            prefixes=prefixes,
            after_key=after_key,
            page_size=page_size,
        )


@pytest.mark.sql
def test_sql_scan_is_resumable_and_key_ordered(
    sqlite_conn: sqlite3.Connection,
) -> None:
    sql_kvset = make_sql_kvset("paged")
    ensure_sql_table(sqlite_conn, sql_kvset)
    adapter = _PagingAdapter(sqlite_conn)
    store = SqlDbKvStore(sql_kvset, adapter=adapter)
    assert store.scan_capabilities().resumable is True
    store.batch_set({k: k for k in ("a", "b", "c", "d", "e")})

    # KEY_ASC contract: exact sequence, not a sorted-set equality.
    assert list(store.scan()) == ["a", "b", "c", "d", "e"]
    assert len(adapter.page_calls) >= 1

    adapter.page_calls.clear()
    first_spec: ScanSpec[str] = ScanSpec(
        project=Projection.KEYS,
        limit=2,
        page_size=2,
        order=ScanOrder.UNORDERED,
    )
    first = [row.key for row in store.scan_rows(first_spec)]
    assert first == ["a", "b"]
    assert adapter.page_calls[0]["after_key"] is None
    assert adapter.page_calls[0]["page_size"] == 2

    adapter.page_calls.clear()
    rest_spec: ScanSpec[str] = ScanSpec(
        project=Projection.KEYS,
        page_size=2,
        cursor=ScanCursor(token=first[-1]),
    )
    rest = [row.key for row in store.scan_rows(rest_spec)]
    assert rest == ["c", "d", "e"]
    assert adapter.page_calls[0]["after_key"] == {"id": "b"}
    assert first[-1] not in rest
    assert first + rest == ["a", "b", "c", "d", "e"]


@pytest.mark.sql
def test_sql_scan_pages_bound_each_adapter_fetch(
    sqlite_conn: sqlite3.Connection,
) -> None:
    sql_kvset = make_sql_kvset("page_bound")
    ensure_sql_table(sqlite_conn, sql_kvset)
    adapter = _PagingAdapter(sqlite_conn)
    store = SqlDbKvStore(sql_kvset, adapter=adapter)
    store.batch_set({f"k{i}": str(i) for i in range(5)})

    assert list(store.scan(page_size=2)) == ["k0", "k1", "k2", "k3", "k4"]
    sizes = [c["page_size"] for c in adapter.page_calls]
    assert sizes == [2, 2, 2]
    afters = [c["after_key"] for c in adapter.page_calls]
    assert afters[0] is None
    assert afters[1] == {"id": "k1"}
    assert afters[2] == {"id": "k3"}


@pytest.mark.sql
def test_sql_scan_batches_uses_keyset_pages(
    sqlite_conn: sqlite3.Connection,
) -> None:
    sql_kvset = make_sql_kvset("scan_batches_pages")
    ensure_sql_table(sqlite_conn, sql_kvset)
    adapter = _PagingAdapter(sqlite_conn)
    store = SqlDbKvStore(sql_kvset, adapter=adapter, batch_size=2)
    store.batch_set({k: k for k in ("a", "b", "c")})

    batches = list(store.scan_batches(page_size=2))
    assert batches == [["a", "b"], ["c"]]
    assert adapter.page_calls[0]["page_size"] == 2


@pytest.mark.sql
def test_sql_composite_cursor_resumes_under_scope(
    sqlite_conn: sqlite3.Connection,
) -> None:
    import json

    sql_kvset = make_composite_sql_kvset()
    ensure_sql_table(sqlite_conn, sql_kvset)
    root = SqlDbKvStore(
        sql_kvset, adapter=SqliteSqlDbClientAdapter(sqlite_conn)
    )
    acme = root.scoped(tenant="acme")
    acme.set(("electronics", "laptop"), "p2")
    acme.set(("electronics", "phone"), "p1")
    acme.set(("home", "chair"), "p3")

    ordered = list(acme.scan())
    assert ordered == [
        ("electronics", "laptop"),
        ("electronics", "phone"),
        ("home", "chair"),
    ]
    first = ordered[0]
    resume: ScanSpec[tuple[str, str]] = ScanSpec(
        project=Projection.KEYS,
        cursor=ScanCursor(token=json.dumps(list(first))),
    )
    rest = [row.key for row in acme.scan_rows(resume)]
    assert rest == [
        ("electronics", "phone"),
        ("home", "chair"),
    ]


@pytest.mark.sql
def test_sql_without_paged_scan_falls_back_and_is_not_resumable(
    sqlite_conn: sqlite3.Connection,
) -> None:
    sql_kvset = make_sql_kvset("no_pages")
    ensure_sql_table(sqlite_conn, sql_kvset)
    adapter = _PagingAdapter(sqlite_conn)
    adapter.disable_paged_scan()
    store = SqlDbKvStore(sql_kvset, adapter=adapter)
    assert store.scan_capabilities().resumable is False
    store.batch_set({k: k for k in ("a", "b", "c")})
    assert list(store.scan()) == ["a", "b", "c"]
    assert adapter.page_calls == []


def test_a_page_that_does_not_advance_raises_instead_of_looping(
    sqlite_conn: sqlite3.Connection,
) -> None:
    """``SqlDbClientAdapter`` is implemented by third parties.

    An adapter that ignores ``after_key`` returns the same full page forever.
    Without a guard the scan spins rather than failing, so the loop names the
    adapter instead of hanging.
    """

    class IgnoresAfterKey(SqliteSqlDbClientAdapter):
        def scan_page_by_key_parts(
            self, *, after_key: SqlDbRow | None, **kw: Any
        ) -> Sequence[SqlDbRow]:
            return super().scan_page_by_key_parts(after_key=None, **kw)

    sql_kvset = make_sql_kvset("stuck")
    ensure_sql_table(sqlite_conn, sql_kvset)
    store = SqlDbKvStore(sql_kvset, adapter=SqliteSqlDbClientAdapter(sqlite_conn))
    for i in range(6):
        store.set(f"k{i}", "v")

    stuck = store.rebind(IgnoresAfterKey(sqlite_conn))
    with pytest.raises(KvStoreError, match="does not advance after_key"):
        list(stuck.scan(page_size=2))


def test_not_null_sibling_blocks_a_non_owning_view_even_on_an_existing_row() -> None:
    """The case the sharing docs got wrong, pinned so it cannot drift back.

    ``set`` is one INSERT ... ON CONFLICT DO UPDATE, and SQLite validates the
    INSERT's NOT NULL columns before resolving the conflict. So a NOT NULL
    sibling blocks a non-owning view's *update*, not just its insert — which
    is why NOT NULL cannot be used to say which view may create a row.

    The earlier sharing test passed only because its siblings were nullable.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        'CREATE TABLE "shared_1" (id TEXT PRIMARY KEY, a TEXT NOT NULL, b TEXT, '
        "updated_at REAL NOT NULL, expires_at REAL)"
    )
    owner = SqlDbKvStore(_column_view("a"), adapter=SqliteSqlDbClientAdapter(conn))
    other = SqlDbKvStore(_column_view("b"), adapter=SqliteSqlDbClientAdapter(conn))

    owner.set("k", "from-A")                      # the row exists and is complete
    assert owner.get("k") == "from-A"

    with pytest.raises(sqlite3.IntegrityError, match="NOT NULL"):
        other.set("k", "from-B")                  # still refused


def test_autocommit_is_readable_on_the_reference_adapter() -> None:
    conn = sqlite3.connect(":memory:")
    assert SqliteSqlDbClientAdapter(conn).autocommit is True
    assert SqliteSqlDbClientAdapter(conn, autocommit=False).autocommit is False


def _shared_view(field: str, *, shares_table: bool) -> Any:
    from dataclasses import replace

    return replace(_column_view(field), shares_table=shares_table)


def _shared_table(conn: sqlite3.Connection, *, not_null: bool) -> None:
    sib = "TEXT NOT NULL" if not_null else "TEXT"
    conn.row_factory = sqlite3.Row
    conn.execute(
        f'CREATE TABLE "shared_1" (id TEXT PRIMARY KEY, a {sib}, b TEXT, '
        "updated_at REAL NOT NULL, expires_at REAL)"
    )


def test_shares_table_lets_a_non_owning_view_update_a_not_null_row() -> None:
    """The case plain upsert cannot do: NOT NULL sibling, row already exists."""
    conn = sqlite3.connect(":memory:")
    _shared_table(conn, not_null=True)
    owner = SqlDbKvStore(
        _shared_view("a", shares_table=True), adapter=SqliteSqlDbClientAdapter(conn)
    )
    other = SqlDbKvStore(
        _shared_view("b", shares_table=True), adapter=SqliteSqlDbClientAdapter(conn)
    )

    owner.set("k", "from-A")
    other.set("k", "from-B")          # refused without shares_table

    assert (owner.get("k"), other.get("k")) == ("from-A", "from-B")
    assert len(conn.execute('SELECT id FROM "shared_1"').fetchall()) == 1


def test_shares_table_still_refuses_creation_by_a_non_owning_view() -> None:
    """Update-first must not quietly start creating hollow rows."""
    conn = sqlite3.connect(":memory:")
    _shared_table(conn, not_null=True)
    other = SqlDbKvStore(
        _shared_view("b", shares_table=True), adapter=SqliteSqlDbClientAdapter(conn)
    )
    with pytest.raises(sqlite3.IntegrityError, match="NOT NULL"):
        other.set("missing", "from-B")
    assert conn.execute('SELECT id FROM "shared_1"').fetchall() == []


def test_shares_table_batch_set_updates_and_inserts_in_one_call() -> None:
    conn = sqlite3.connect(":memory:")
    _shared_table(conn, not_null=False)
    view = SqlDbKvStore(
        _shared_view("a", shares_table=True), adapter=SqliteSqlDbClientAdapter(conn)
    )
    view.set("existing", "old")
    view.batch_set({"existing": "new", "fresh": "made"})
    assert (view.get("existing"), view.get("fresh")) == ("new", "made")


def test_shares_table_refuses_an_adapter_that_cannot_update_first() -> None:
    """Silently falling back is the write that cannot succeed."""

    class NoUpdateFirst(SqliteSqlDbClientAdapter):
        def supports_update_first(self) -> bool:
            return False

    conn = sqlite3.connect(":memory:")
    _shared_table(conn, not_null=False)
    view = SqlDbKvStore(_shared_view("a", shares_table=True), adapter=NoUpdateFirst(conn))
    with pytest.raises(KvStoreError, match="upsert_rows_update_first"):
        view.set("k", "v")


def test_without_shares_table_writes_stay_a_single_upsert() -> None:
    """The default path must not pay for a feature it does not use."""
    calls: list[str] = []

    class Counting(SqliteSqlDbClientAdapter):
        def upsert_rows(self, **kw: Any) -> None:
            calls.append("upsert")
            return super().upsert_rows(**kw)

        def upsert_rows_update_first(self, **kw: Any) -> None:
            calls.append("update_first")
            return super().upsert_rows_update_first(**kw)

    conn = sqlite3.connect(":memory:")
    _shared_table(conn, not_null=False)
    view = SqlDbKvStore(_shared_view("a", shares_table=False), adapter=Counting(conn))
    view.set("k", "v")
    view.batch_set({"x": "1", "y": "2"})
    assert calls == ["upsert", "upsert"]
