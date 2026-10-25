"""R26: a record written into the caller's transaction, its body outside it.

No ``transaction=`` parameter: kvtsjl never begins or commits a caller's
transaction. The order it already keeps does the work once the adapter lets
the first write open the transaction — the body goes first, before the
commit point and before ``BEGIN``; the row and every part on the same
connection join one transaction the caller commits; parts on other media
wait for ``flush_index_syncs``.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import sqlite3
from typing import Any

import pytest

from kvtsjl import KvSet, MemoryKvStore, SerDe
from kvtsjl.backends.sql.sqlite import SqliteSqlDbClientAdapter
from kvtsjl.backends.sql.store import SqlDbKvStore
from kvtsjl.exceptions import UncommittedWriteError
from kvtsjl.store.logical import KvStore
from tests.sql_helpers import ensure_sql_table, make_sql_kvset


@dataclass(frozen=True)
class Turn:
    core: str | None
    body: str | None
    mounts: Mapping[str, str] | None
    copy: str | None


class _Down(SqliteSqlDbClientAdapter):
    def upsert_rows(self, **_kw: Any) -> None:
        raise RuntimeError("receipts down")


def _sql(conn: sqlite3.Connection, name: str, leaf: str, scope: tuple[str, ...], adapter: type = SqliteSqlDbClientAdapter) -> Any:
    sql_kvset = make_sql_kvset(name, leaf_key_fields=(leaf,), scope_fields=scope, scope_schema=scope)
    ensure_sql_table(conn, sql_kvset)
    return SqlDbKvStore(sql_kvset, adapter=adapter(conn, autocommit=False, begin_on_write=True))


def _memory(name: str, writes: list[bool] | None = None, conn: sqlite3.Connection | None = None) -> Any:
    class Watched(MemoryKvStore[Any, Any, Any, Any]):
        def set(self, *a: Any, **kw: Any) -> None:
            if writes is not None and conn is not None:
                writes.append(conn.in_transaction)
            super().set(*a, **kw)

    return Watched(
        KvSet.with_str_keys(name, key_serde=SerDe.identity(str), value_serde=SerDe.identity(str))
    )


def _setup(*, receipts_down: bool = False) -> tuple[sqlite3.Connection, Any, list[bool], Any, Any]:
    conn = sqlite3.connect(":memory:", isolation_level=None)
    body_writes: list[bool] = []
    copies = _memory("copy")
    receipts = _sql(conn, "mount", "card", ("turn",), _Down if receipts_down else SqliteSqlDbClientAdapter)
    turns = KvStore.zip_with(
        Turn,
        commit_part="core",
        after_commit=("mounts", "copy"),
        core=_sql(conn, "turn", "id", ()),
        body=_memory("body", body_writes, conn),
        mounts=receipts.group_by("turn"),
        copy=copies,
    )
    return conn, turns, body_writes, copies, receipts


def _rows(conn: sqlite3.Connection, table: str) -> int:
    return conn.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0]


def test_the_body_is_written_before_the_transaction_opens() -> None:
    conn, turns, body_writes, _, _ = _setup()
    turns.set("t", Turn("C", "B", {"a": "1"}, "copied"))

    assert body_writes == [False], "the body uploaded inside the write lock"
    assert conn.in_transaction, "the row did not open the caller's transaction"
    conn.commit()
    turns.flush_index_syncs()
    assert turns.get("t") == Turn("C", "B", {"a": "1"}, "copied")


def test_the_row_and_its_sql_parts_commit_or_roll_back_together() -> None:
    conn, turns, _, _, _ = _setup()
    turns.set("t", Turn("C", "B", {"a": "1"}, None))
    conn.rollback()
    turns.discard_index_syncs()

    assert (_rows(conn, "turn_1"), _rows(conn, "mount_1")) == (0, 0)
    assert turns.get("t") is None


def test_a_failing_part_inside_the_transaction_is_before_the_commit_point() -> None:
    """Nothing is durable until the caller commits, so the error says roll back."""
    conn, turns, _, _, _ = _setup(receipts_down=True)
    with pytest.raises(UncommittedWriteError) as caught:
        turns.set("t", Turn("C", "B", {"a": "1"}, None))
    assert caught.value.committed is False
    conn.rollback()
    turns.discard_index_syncs()
    assert _rows(conn, "turn_1") == 0, "the row survived its part's failure"


def test_parts_on_other_media_wait_for_the_commit() -> None:
    conn, turns, _, copies, _ = _setup()
    turns.set("t", Turn("C", "B", None, "copied"))
    assert copies.get("t") is None, "a copy was written before the caller committed"
    assert turns.pending_index_syncs == 1

    conn.commit()
    turns.flush_index_syncs()
    assert copies.get("t") == "copied"


def test_begin_on_write_is_for_a_transaction_the_client_commits() -> None:
    with pytest.raises(ValueError, match="autocommit"):
        SqliteSqlDbClientAdapter(sqlite3.connect(":memory:"), begin_on_write=True)


# --- R25 without a hook: the caller's transaction already is one ---------------


def _cards(conn: sqlite3.Connection) -> None:
    conn.execute('CREATE TABLE cards (id TEXT PRIMARY KEY, folded INTEGER NOT NULL)')
    conn.execute("INSERT INTO cards VALUES ('a', 0)")


def _fold_and_forget(conn: sqlite3.Connection, turns: Any, key: str) -> None:
    """The client's domain work and the delete, under one lock, one commit."""
    conn.execute("BEGIN IMMEDIATE")
    turn = turns.get(key)
    for card in turn.mounts:
        conn.execute("UPDATE cards SET folded = folded + 1 WHERE id = ?", (card,))
    turns.delete(key)


def test_folding_then_deleting_commits_both_or_neither() -> None:
    conn, turns, _, _, _ = _setup()
    _cards(conn)
    turns.set("t", Turn("C", "B", {"a": "1"}, None))
    conn.commit()
    turns.flush_index_syncs()

    _fold_and_forget(conn, turns, "t")
    conn.rollback()
    turns.discard_index_syncs()
    assert conn.execute("SELECT folded FROM cards").fetchone()[0] == 0
    assert turns.get("t") == Turn("C", "B", {"a": "1"}, None), "a rollback lost the body"

    _fold_and_forget(conn, turns, "t")
    conn.commit()
    turns.flush_index_syncs()
    assert conn.execute("SELECT folded FROM cards").fetchone()[0] == 1, "folded twice or never"
    assert turns.get("t") is None


def test_expiry_is_folded_the_same_way() -> None:
    from datetime import timedelta

    from freezegun import freeze_time

    from kvtsjl import TtlPolicy

    conn = sqlite3.connect(":memory:", isolation_level=None)
    _cards(conn)
    core_kvset = make_sql_kvset("turn", ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=60)))
    ensure_sql_table(conn, core_kvset)
    core = SqlDbKvStore(core_kvset, adapter=SqliteSqlDbClientAdapter(conn, autocommit=False, begin_on_write=True))
    receipts = _sql(conn, "mount", "card", ("turn",)).group_by("turn")
    turns = KvStore.zip_with(
        Turn, commit_part="core", after_commit=("mounts",),
        core=core, body=_memory("body"), mounts=receipts, copy=_memory("copy"),
    )
    with freeze_time("2024-01-01 12:00:00") as frozen:
        turns.set("t", Turn("C", "B", {"a": "1"}, None))
        conn.commit()
        frozen.tick(timedelta(seconds=120))

        # An expired record no longer reads back — that is what expired means —
        # so the fold reads the part it folds, which is the client's own store.
        conn.execute("BEGIN IMMEDIATE")
        expired = turns.expired_keys(limit=10)
        assert turns.batch_get(expired) == {}
        for mounts in receipts.batch_get(expired).values():
            for card in mounts:
                conn.execute("UPDATE cards SET folded = folded + 1 WHERE id = ?", (card,))
        turns.batch_delete(expired)
        conn.commit()
        turns.flush_index_syncs()

    assert expired == ["t"]
    assert conn.execute("SELECT folded FROM cards").fetchone()[0] == 1
    assert _rows(conn, "turn_1") == 0
