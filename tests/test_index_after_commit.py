"""R14: an index bound to a store whose caller owns the commit.

Since R1 the client owns the SQL transaction. Syncing an index beside the
write then runs an embedding call — a network call — inside that transaction,
holding the tenant's write lock, and a rollback leaves the index entry behind.
"""

from __future__ import annotations

import sqlite3
import sys
from typing import Any

import pytest

from kvtsjl.backends.sql.sqlite import SqliteSqlDbClientAdapter
from kvtsjl.backends.sql.store import SqlDbKvStore
from kvtsjl.exceptions import PartialSyncError

sys.path.insert(0, "tests")
from tests.sql_helpers import ensure_sql_table, make_sql_kvset


class SpyIndex:
    """Records whether a write transaction was open when it was called."""

    name = "spy"
    sync_on_write = True
    sync_policy = None

    def __init__(self, conn: sqlite3.Connection, *, fail: bool = False) -> None:
        self._conn = conn
        self._fail = fail
        self.calls: list[tuple[str, bool]] = []

    def bind_store(self, store: object) -> None:
        self._store = store

    def _record(self, what: str) -> None:
        self.calls.append((what, self._conn.in_transaction))
        if self._fail:
            raise RuntimeError("index unavailable")

    def sync(self, key: Any, value: Any) -> None:
        self._record("sync")

    def batch_sync(self, items: Any) -> None:
        self._record("batch_sync")

    def delete(self, key: Any) -> None:
        self._record("delete")

    def batch_delete(self, keys: Any) -> None:
        self._record("batch_delete")

    def clone_with_scope(self, scope: Any) -> SpyIndex:
        return self


def _bound(conn: sqlite3.Connection, **kw: Any) -> tuple[Any, SpyIndex]:
    sql_kvset = make_sql_kvset("idx")
    ensure_sql_table(conn, sql_kvset)
    base = SqlDbKvStore(sql_kvset, adapter=SqliteSqlDbClientAdapter(conn))
    deferred = base.rebind(SqliteSqlDbClientAdapter(conn, autocommit=False))
    index = SpyIndex(conn, **kw)
    return deferred.indexed(index), index


def test_nothing_reaches_the_index_while_the_transaction_is_open(
    sqlite_conn: sqlite3.Connection,
) -> None:
    store, index = _bound(sqlite_conn)
    sqlite_conn.execute("BEGIN IMMEDIATE")
    store.set("k", "v")
    store.delete("k")

    assert index.calls == [], "an index call ran inside the caller's transaction"
    assert store.pending_index_syncs == 2


def test_the_queue_runs_after_the_commit(sqlite_conn: sqlite3.Connection) -> None:
    store, index = _bound(sqlite_conn)
    sqlite_conn.execute("BEGIN IMMEDIATE")
    store.set("k", "v")
    sqlite_conn.commit()
    store.flush_index_syncs()

    assert [what for what, _ in index.calls] == ["sync"]
    assert all(not in_txn for _, in_txn in index.calls), "ran inside a transaction"
    assert store.pending_index_syncs == 0


def test_a_rollback_leaves_no_index_entry(sqlite_conn: sqlite3.Connection) -> None:
    store, index = _bound(sqlite_conn)
    sqlite_conn.execute("BEGIN IMMEDIATE")
    store.set("k", "v")
    sqlite_conn.rollback()
    store.discard_index_syncs()

    assert index.calls == [], "a rolled-back write still reached the index"
    assert store.pending_index_syncs == 0


def test_autocommit_is_unchanged(sqlite_conn: sqlite3.Connection) -> None:
    """Without a caller transaction, syncing beside the write is still right."""
    sql_kvset = make_sql_kvset("plain")
    ensure_sql_table(sqlite_conn, sql_kvset)
    base = SqlDbKvStore(sql_kvset, adapter=SqliteSqlDbClientAdapter(sqlite_conn))
    index = SpyIndex(sqlite_conn)
    store = base.indexed(index)

    store.set("k", "v")
    assert [what for what, _ in index.calls] == ["sync"]
    assert store.pending_index_syncs == 0


def test_a_required_failure_surfaces_at_the_flush(
    sqlite_conn: sqlite3.Connection,
) -> None:
    """The write already succeeded, so the error has to arrive at the flush."""
    store, _index = _bound(sqlite_conn, fail=True)   # indexed() defaults to REQUIRED
    sqlite_conn.execute("BEGIN IMMEDIATE")
    store.set("k", "v")          # must not raise: the index has not run yet
    sqlite_conn.commit()

    with pytest.raises(PartialSyncError):
        store.flush_index_syncs()
    assert store.pending_index_syncs == 0, "a failed flush must not be retried silently"
