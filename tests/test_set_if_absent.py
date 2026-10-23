"""R15: insert without overwriting, as one operation.

A writer that must not replace an existing value had to ``get`` then ``set``,
leaving a window in which a concurrent writer's value is lost. This is the
medium's own conditional write instead, and no backend fakes it.
"""

from __future__ import annotations

from datetime import timedelta
import sqlite3
import sys
import time
from typing import Any

import pytest

from kvtsjl import KvSet, MemoryKvStore, SerDe
from kvtsjl.backends.sql.sqlite import SqliteSqlDbClientAdapter
from kvtsjl.backends.sql.store import SqlDbKvStore
from kvtsjl.exceptions import KvStoreError
from kvtsjl.store.schema.ttl import ExpiryGc, TtlPolicy

sys.path.insert(0, "tests")
from tests.sql_helpers import ensure_sql_table, make_sql_kvset


def _memory(ttl: TtlPolicy | None = None) -> Any:
    return MemoryKvStore(
        KvSet.with_str_keys(
            "m",
            key_serde=SerDe.identity(str),
            value_serde=SerDe.identity(str),
            ttl_policy=ttl,
        )
    )


def _sql(conn: sqlite3.Connection, ttl: TtlPolicy | None = None, **kw: Any) -> Any:
    sql_kvset = make_sql_kvset("sia", ttl_policy=ttl)
    ensure_sql_table(conn, sql_kvset)
    return SqlDbKvStore(sql_kvset, adapter=SqliteSqlDbClientAdapter(conn), **kw)


def test_memory_inserts_once_and_never_replaces() -> None:
    store = _memory()
    assert store.set_if_absent("k", "first") is True
    assert store.set_if_absent("k", "second") is False
    assert store.get("k") == "first", "an existing value was replaced"


def test_sql_inserts_once_and_never_replaces(sqlite_conn: sqlite3.Connection) -> None:
    store = _sql(sqlite_conn)
    assert store.set_if_absent("k", "first") is True
    assert store.set_if_absent("k", "second") is False
    assert store.get("k") == "first"


def test_only_one_of_two_racing_writers_wins(sqlite_conn: sqlite3.Connection) -> None:
    """Exactly one call reports the write, and its value is the one stored."""
    store = _sql(sqlite_conn)
    results = [store.set_if_absent("k", f"writer-{i}") for i in range(5)]

    assert results.count(True) == 1, f"{results.count(True)} writers each claimed the key"
    winner = results.index(True)
    assert store.get("k") == f"writer-{winner}"


@pytest.mark.parametrize("gc", [ExpiryGc.LAZY_DELETE, ExpiryGc.HIDE])
def test_an_expired_entry_still_blocks_the_insert(
    sqlite_conn: sqlite3.Connection, gc: ExpiryGc
) -> None:
    """"Absent" means absent to the medium, which has no view of kvtsjl's TTL.

    So this returns False for a key ``get`` reports as missing. Reconciling the
    two would need a read-modify-write — the race the method exists to remove.
    A caller that wants expired entries treated as absent runs
    ``gc_expired_keys`` first.
    """
    sql_kvset = make_sql_kvset("exp", ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=10)))
    ensure_sql_table(sqlite_conn, sql_kvset)
    store = SqlDbKvStore(
        sql_kvset, adapter=SqliteSqlDbClientAdapter(sqlite_conn), expiry_gc=gc
    )
    store.set("k", "old")
    sqlite_conn.execute(
        f'UPDATE "{sql_kvset.table_name}" SET "{sql_kvset.updated_at_field}"=?, '
        f'"{sql_kvset.expires_at_field}"=?',
        (time.time() - 999, time.time() - 900),
    )
    sqlite_conn.commit()

    assert store.set_if_absent("k", "new") is False
    # ...and the documented escape hatch works
    store.gc_expired_keys(max_entries=10)
    assert store.set_if_absent("k", "new") is True
    assert store.get("k") == "new"


def test_a_backend_without_the_capability_raises_rather_than_racing() -> None:
    """Never a get-then-set: that is the race, not an implementation of it."""

    class NoPrimitive(SqliteSqlDbClientAdapter):
        def supports_insert_if_absent(self) -> bool:
            return False

    conn = sqlite3.connect(":memory:")
    sql_kvset = make_sql_kvset("nope")
    ensure_sql_table(conn, sql_kvset)
    store = SqlDbKvStore(sql_kvset, adapter=NoPrimitive(conn))

    assert store.supports_set_if_absent() is False
    with pytest.raises(KvStoreError, match="set_if_absent"):
        store.set_if_absent("k", "v")
    assert store.get("k") is None, "the refused write still landed"


def test_the_default_is_unsupported() -> None:
    """A store that never opts in must raise, not silently race."""
    from kvtsjl.store.logical import KvStore

    assert KvStore.supports_set_if_absent(object()) is False  # type: ignore[arg-type]


def test_it_respects_the_bound_scope() -> None:
    store = MemoryKvStore(
        KvSet.with_str_keys(
            "s",
            key_serde=SerDe.identity(str),
            value_serde=SerDe.identity(str),
            scope_schema=("user",),
        )
    )
    assert store.scoped(user="a").set_if_absent("k", "A") is True
    assert store.scoped(user="b").set_if_absent("k", "B") is True, "scopes collided"
    assert store.scoped(user="a").get("k") == "A"
