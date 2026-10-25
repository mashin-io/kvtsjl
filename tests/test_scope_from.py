"""R22: a part keyed more coarsely than its record takes only some of its scope.

A turn is scoped ``(user, session)``; its receipts live in a table scoped
``(user, consumer_turn_id)``. ``scope_from("user")`` hands the receipts part
the ``user`` segment of whatever scope the record is given — with the public
API alone, no subclass reaching into a store's fields.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import sqlite3
from typing import Any

import pytest

from kvtsjl import KvSet, MemoryKvStore, Scope, SerDe
from kvtsjl.backends.sql.sqlite import SqliteSqlDbClientAdapter
from kvtsjl.backends.sql.store import SqlDbKvStore
from kvtsjl.exceptions import KvStoreScopeError
from kvtsjl.store.logical import KvStore
from tests.sql_helpers import ensure_sql_table, make_sql_kvset


@dataclass(frozen=True)
class Turn:
    core: str | None
    mounts: Mapping[str, str] | None


def _sql(conn: sqlite3.Connection, name: str, leaf: str, scope: tuple[str, ...]) -> Any:
    sql_kvset = make_sql_kvset(
        name, leaf_key_fields=(leaf,), scope_fields=scope, scope_schema=scope
    )
    ensure_sql_table(conn, sql_kvset)
    return SqlDbKvStore(sql_kvset, adapter=SqliteSqlDbClientAdapter(conn))


def _turns(conn: sqlite3.Connection, *kinds: str) -> Any:
    receipts = _sql(conn, "mount", "card", ("user", "consumer_turn_id"))
    return KvStore.zip_with(
        Turn,
        commit_part="core",
        after_commit=("mounts",),
        core=_sql(conn, "turn", "id", ("user", "session")),
        mounts=receipts.group_by("consumer_turn_id").scope_from(*kinds),
    )


def _receipts(conn: sqlite3.Connection) -> list[tuple[str, str, str]]:
    return sorted(
        tuple(r) for r in conn.execute('SELECT "user", consumer_turn_id, card FROM "mount_1"')
    )


def test_a_part_takes_only_the_kinds_it_names() -> None:
    conn = sqlite3.connect(":memory:")
    turns = _turns(conn, "user").scoped(user="u", session="s")

    turns.set("t1", Turn(core="C", mounts={"a": "1"}))

    assert turns.get("t1") == Turn(core="C", mounts={"a": "1"})
    assert _receipts(conn) == [("u", "t1", "a")], "the session reached the receipts"


def test_two_sessions_keep_their_own_turns_receipts() -> None:
    conn = sqlite3.connect(":memory:")
    turns = _turns(conn, "user")
    turns.scoped(user="u", session="s1").set("t1", Turn(core="C", mounts={"a": "1"}))
    turns.scoped(user="u", session="s2").set("t2", Turn(core="C", mounts={"b": "2"}))

    assert turns.scoped(user="u", session="s1").get("t1") == Turn("C", {"a": "1"})
    assert turns.scoped(user="u", session="s2").get("t2") == Turn("C", {"b": "2"})
    assert turns.scoped(user="v", session="s1").get("t1") is None


def test_scoping_in_steps_reaches_the_same_place() -> None:
    conn = sqlite3.connect(":memory:")
    _turns(conn, "user").scoped(user="u").scoped(session="s").set(
        "t", Turn(core="C", mounts={"a": "1"})
    )
    assert _receipts(conn) == [("u", "t", "a")]


def test_a_kind_the_scope_lacks_raises_rather_than_leaving_the_part_unscoped() -> None:
    """A typo would otherwise share one user's receipts with every other's."""
    conn = sqlite3.connect(":memory:")
    with pytest.raises(KvStoreScopeError, match="usr"):
        _turns(conn, "usr").scoped(user="u", session="s")


def test_it_narrows_any_store_not_only_a_zip_part() -> None:
    store = MemoryKvStore(
        KvSet.with_str_keys(
            "m", key_serde=SerDe.identity(str), value_serde=SerDe.identity(str),
            scope_schema=("user",),
        )
    )
    narrowed = store.scope_from("user").scoped(user="u", session="s")
    narrowed.set("k", "v")

    assert store.scoped(user="u").get("k") == "v"
    assert narrowed.scope == Scope.of(user="u", session="s"), "the scope given was lost"
    assert narrowed.scoped(tag="x").scope == Scope.of(user="u", session="s", tag="x")


def test_the_narrowing_is_one_rule_on_scope() -> None:
    scope = Scope.of(user="u", session="s", turn="t")
    assert scope.only("user", "turn") == Scope.of(user="u", turn="t")
    assert scope.only("turn", "user") == Scope.of(user="u", turn="t"), "the prefix order changed"
    assert scope.only() == Scope.empty()
    with pytest.raises(KvStoreScopeError):
        scope.only("tenant")
