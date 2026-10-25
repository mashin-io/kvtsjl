"""Only a conflict on the record's own key is a race; any other one raises.

Nexus keys a turn by ``(user, session, turn_id)`` and keeps ``turn_id``
unique across users. The update-first upsert read any ``UNIQUE`` failure of
its insert as another writer taking the key, retried the update, matched
nothing, and returned: a write reported done that wrote nothing. Every
write path must instead raise a violation of a constraint other than the key.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
import sqlite3
from typing import Any

import pytest

from kvtsjl import Put
from kvtsjl.backends.sql.sqlite import SqliteSqlDbClientAdapter
from kvtsjl.backends.sql.store import SqlDbKvStore
from kvtsjl.exceptions import KvStoreError
from tests.sql_helpers import ensure_sql_table, make_sql_kvset


def _turns(conn: sqlite3.Connection, **sharing: bool) -> Any:
    """Keyed by (user, id); ``id`` is also unique on its own, across users."""
    sql_kvset = make_sql_kvset(
        "turn", leaf_key_fields=("id",), scope_fields=("user",), scope_schema=("user",)
    )
    if sharing:
        sql_kvset = replace(sql_kvset, **sharing)
    ensure_sql_table(conn, sql_kvset)
    conn.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS "one_id" ON "{sql_kvset.table_name}" ("id")')
    return SqlDbKvStore(sql_kvset, adapter=SqliteSqlDbClientAdapter(conn))


WRITES: dict[str, Callable[[Any], object]] = {
    "set": lambda s: s.set("t1", "B's"),
    "batch_set": lambda s: s.batch_set({"t1": "B's"}),
    "set_if_absent": lambda s: s.set_if_absent("t1", "B's"),
    "update": lambda s: s.update("t1", lambda v: Put("B's")),
}


@pytest.mark.parametrize("sharing", ["own table", "shared, owner"])
@pytest.mark.parametrize("write", list(WRITES))
def test_a_write_that_breaks_another_unique_constraint_raises(sharing: str, write: str) -> None:
    conn = sqlite3.connect(":memory:")
    shares = {"shares_table": True, "owns_rows": True} if sharing == "shared, owner" else {}
    turns = _turns(conn, **shares)
    turns.scoped(user="A").set("t1", "A's")

    with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
        WRITES[write](turns.scoped(user="B"))
    assert turns.scoped(user="B").get("t1") is None
    assert turns.scoped(user="A").get("t1") == "A's"


def test_a_replace_that_breaks_another_unique_constraint_raises() -> None:
    """Moving a row onto another's unique value is refused, not passed over."""
    conn = sqlite3.connect(":memory:")
    turns = _turns(conn)
    conn.execute('CREATE UNIQUE INDEX "one_value" ON "turn_1" ("value")')
    turns.scoped(user="A").set("t1", "taken")
    turns.scoped(user="A").set("t2", "free")

    with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
        turns.scoped(user="A").update("t2", lambda v: Put("taken"))
    assert turns.scoped(user="A").get("t2") == "free"


def test_the_record_s_own_key_is_still_a_race_and_is_retaken() -> None:
    """The key conflict the update-first upsert exists for still resolves."""
    conn = sqlite3.connect(":memory:")
    turns = _turns(conn, shares_table=True, owns_rows=True)
    a = turns.scoped(user="A")
    a.set("t1", "first")
    a.set("t1", "second")
    assert a.get("t1") == "second"


class _Racing:
    """A connection on which another writer acts right after each matching statement."""

    def __init__(self, conn: sqlite3.Connection, after: str, act: Callable[[], object]) -> None:
        self._conn, self._after, self._act = conn, after, act

    def execute(self, sql: str, params: Any = ()) -> Any:
        cur = self._conn.execute(sql, params)
        if sql.startswith(self._after):
            self._act()
        return cur

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


def _racing(turns: Any, conn: sqlite3.Connection, after: str, act: Callable[[], object]) -> None:
    adapter = turns._adapter
    adapter._conn = _Racing(conn, after, act)


def test_a_key_taken_between_the_update_and_the_insert_is_updated() -> None:
    conn = sqlite3.connect(":memory:")
    turns = _turns(conn, shares_table=True, owns_rows=True)
    taken: list[bool] = []

    def another_writer_inserts() -> None:
        if not taken:
            taken.append(True)
            conn.execute(
                'INSERT INTO "turn_1" ("user", "id", "value", "updated_at") '
                "VALUES ('A', 't1', 'theirs', 0)"
            )

    _racing(turns, conn, "UPDATE", another_writer_inserts)
    turns.scoped(user="A").set("t1", "mine")
    assert turns.scoped(user="A").get("t1") == "mine", "the write was passed over"


def test_a_row_that_keeps_appearing_and_vanishing_raises() -> None:
    conn = sqlite3.connect(":memory:")
    turns = _turns(conn, shares_table=True, owns_rows=True)

    def flicker() -> None:
        conn.execute(
            'INSERT OR REPLACE INTO "turn_1" ("user", "id", "value", "updated_at") '
            "VALUES ('A', 't1', 'theirs', 0)"
        )

    def gone() -> None:
        conn.execute('DELETE FROM "turn_1"')

    class Both(_Racing):
        def execute(self, sql: str, params: Any = ()) -> Any:
            if sql.startswith("INSERT INTO"):
                flicker()
            cur = self._conn.execute(sql, params)
            if sql.startswith("INSERT INTO"):
                gone()
            return cur

    turns._adapter._conn = Both(conn, "", lambda: None)
    with pytest.raises(KvStoreError, match="neither updated nor inserted"):
        turns.scoped(user="A").set("t1", "mine")
