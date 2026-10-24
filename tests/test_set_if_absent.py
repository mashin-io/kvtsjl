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

    with pytest.raises(KvStoreError, match="set_if_absent"):
        store.set_if_absent("k", "v")
    assert store.get("k") is None, "the refused write still landed"


def test_the_default_is_to_refuse() -> None:
    """A store that never implements it must raise, not silently race."""
    from kvtsjl.keymap import KeyMap

    with pytest.raises(KvStoreError, match="set_if_absent"):
        KeyMap.set_if_absent(object(), "k", "v")  # type: ignore[arg-type]


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


# --- the capability must survive composition -------------------------------


def _mem_pair() -> tuple[Any, Any]:
    return _memory(), _memory()


def test_it_survives_mirror_and_the_secondary_only_copies_a_real_write() -> None:
    """A losing writer must not plant its value in the secondary."""
    primary, secondary = _mem_pair()
    store = primary.mirror(secondary)

    assert store.set_if_absent("k", "first") is True
    assert secondary.get("k") == "first"

    assert store.set_if_absent("k", "second") is False
    assert primary.get("k") == "first"
    assert secondary.get("k") == "first", "the losing write reached the secondary"


def test_it_survives_fallback_read() -> None:
    primary, source = _mem_pair()
    store = primary.fallback_read(source)

    assert store.set_if_absent("k", "v") is True
    assert store.set_if_absent("k", "other") is False
    assert primary.get("k") == "v"


def test_it_survives_a_mapped_view() -> None:
    inner = _memory()
    view = inner.imap(lambda s: s.upper(), lambda s: s.lower())

    assert view.set_if_absent("k", "ABC") is True
    assert inner.get("k") == "abc", "the inverse was not applied"
    assert view.set_if_absent("k", "XYZ") is False
    assert view.get("k") == "ABC"


def test_readonly_refuses_it_as_a_write() -> None:
    from kvtsjl.exceptions import KvStoreReadOnlyError

    store = _memory()
    store.set_if_absent("k", "v")
    with pytest.raises(KvStoreReadOnlyError, match="set_if_absent"):
        store.readonly().set_if_absent("j", "v")


def test_an_indexed_store_indexes_only_a_write_that_happened() -> None:
    """A losing writer must not sync: the stored value is someone else's.

    Indexing our value would describe a record that is not there — search
    would return a hit whose hydrated value is the winner's, or nothing.
    """
    synced: list[tuple[str, str]] = []

    class SpyIndex:
        name = "spy"
        sync_on_write = True
        sync_policy = None

        def bind_store(self, store: Any) -> None:
            self._store = store

        def sync(self, key: str, value: str) -> None:
            synced.append((key, value))

        def batch_sync(self, items: Any) -> None: ...
        def delete(self, key: str) -> None: ...
        def batch_delete(self, keys: Any) -> None: ...
        def clone_with_scope(self, scope: Any) -> Any:
            return self

    store = _memory().indexed(SpyIndex())

    assert store.set_if_absent("k", "first") is True
    assert synced == [("k", "first")]

    assert store.set_if_absent("k", "second") is False
    assert synced == [("k", "first")], "indexed a write that never happened"
