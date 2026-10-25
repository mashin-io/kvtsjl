"""R19: a parent's child rows, read and written as one part of its record.

``rows.group_by(segment)`` makes the rows under one scope segment a single
value. In a zip it refers to the record's key, so it is declared
``after_commit``: written only once the commit part has, removed after it,
and never deciding existence.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import sqlite3
from typing import Any

from freezegun import freeze_time
import pytest

from kvtsjl import KvSet, MemoryKvStore, SerDe, TtlPolicy
from kvtsjl.backends.sql.sqlite import SqliteSqlDbClientAdapter
from kvtsjl.backends.sql.store import SqlDbKvStore
from kvtsjl.exceptions import (
    KvStoreScanUnsupported,
    KvStoreUnsupported,
    PartialSyncError,
    UncommittedWriteError,
)
from kvtsjl.store.logical import KvStore
from tests.sql_helpers import ensure_sql_table, make_sql_kvset

CLOCK = datetime(2024, 1, 1, tzinfo=UTC)


@dataclass(frozen=True)
class Turn:
    core: str | None
    body: str | None
    mounts: Mapping[str, str] | None


class _Down(SqliteSqlDbClientAdapter):
    """Refuses every write; the adapter outlives the clones scoping makes."""

    def upsert_rows(self, **_kw: Any) -> None:
        raise RuntimeError("medium down")


def _sql(
    conn: sqlite3.Connection,
    name: str,
    leaf: str,
    scope: tuple[str, ...],
    ttl: TtlPolicy | None = None,
    *,
    down: bool = False,
) -> Any:
    sql_kvset = make_sql_kvset(
        name,
        leaf_key_fields=(leaf,),
        scope_fields=scope,
        scope_schema=scope,
        ttl_policy=ttl,
    )
    ensure_sql_table(conn, sql_kvset)
    adapter = (_Down if down else SqliteSqlDbClientAdapter)(conn)
    return SqlDbKvStore(sql_kvset, adapter=adapter)


def _mounts(conn: sqlite3.Connection, *, down: bool = False) -> Any:
    """Receipts keyed (user, consumer_turn_id, card_turn_id)."""
    return _sql(conn, "mount", "card_turn_id", ("user", "consumer_turn_id"), down=down)


def _bodies() -> Any:
    return MemoryKvStore(
        KvSet.with_str_keys(
            "body",
            key_serde=SerDe.identity(str),
            value_serde=SerDe.identity(str),
            scope_schema=("user",),
        )
    )


def _turns(conn: sqlite3.Connection, *, core: Any = None, mounts: Any = None) -> Any:
    return KvStore.zip_with(
        Turn,
        commit_part="core",
        after_commit=("mounts",),
        core=core if core is not None else _sql(conn, "turn", "id", ("user",)),
        body=_bodies(),
        mounts=(mounts if mounts is not None else _mounts(conn)).group_by("consumer_turn_id"),
    ).scoped(user="u")


def _rows(conn: sqlite3.Connection) -> list[tuple[str, str, str]]:
    return sorted(
        tuple(r)
        for r in conn.execute('SELECT "user", consumer_turn_id, card_turn_id FROM "mount_1"')
    )


# --- the grouped store ------------------------------------------------------


def test_setting_a_group_makes_its_members_equal_the_given_ones() -> None:
    conn = sqlite3.connect(":memory:")
    groups = _mounts(conn).scoped(user="u").group_by("consumer_turn_id")
    groups.set("t", {"a": "1", "b": "2"})
    groups.set("t", {"b": "2b", "c": "3"})

    assert groups.get("t") == {"b": "2b", "c": "3"}, "a member no longer given survived"


def test_an_empty_group_clears_every_member_and_reads_back_empty() -> None:
    conn = sqlite3.connect(":memory:")
    groups = _mounts(conn).scoped(user="u").group_by("consumer_turn_id")
    groups.set("t", {"a": "1"})
    groups.set("t", {})

    assert groups.get("t") == {}
    assert groups.get("never-written") == {}, "a group is empty, not absent"
    assert groups.exists("never-written") is True, "exists disagreed with get"
    assert groups.batch_exists(["x", "y"]) == {"x", "y"}
    assert _rows(conn) == []


def test_deleting_a_group_removes_its_members_and_no_one_else_s() -> None:
    conn = sqlite3.connect(":memory:")
    groups = _mounts(conn).scoped(user="u").group_by("consumer_turn_id")
    groups.set("t", {"a": "1", "b": "2"})
    groups.set("other", {"a": "x"})

    assert groups.delete("t") is True
    assert groups.delete("t") is False
    assert _rows(conn) == [("u", "other", "a")]


def test_groups_are_scoped_like_any_store() -> None:
    conn = sqlite3.connect(":memory:")
    rows = _mounts(conn)
    rows.scoped(user="u").group_by("consumer_turn_id").set("t", {"a": "1"})
    rows.scoped(user="v").group_by("consumer_turn_id").set("t", {"b": "2"})

    assert rows.scoped(user="u").group_by("consumer_turn_id").get("t") == {"a": "1"}
    assert rows.scoped(user="v").group_by("consumer_turn_id").get("t") == {"b": "2"}


def test_a_group_takes_no_conditional_write_and_lists_no_groups() -> None:
    conn = sqlite3.connect(":memory:")
    groups = _mounts(conn).scoped(user="u").group_by("consumer_turn_id")
    with pytest.raises(KvStoreUnsupported, match="after_commit"):
        groups.set_if_absent("t", {"a": "1"})
    with pytest.raises(KvStoreScanUnsupported):
        list(groups.list())
    with pytest.raises(KvStoreUnsupported, match="ask the rows"):
        list(groups.missing_keys())


# --- as a part of a record --------------------------------------------------


def test_a_record_reads_and_writes_its_children_whole() -> None:
    conn = sqlite3.connect(":memory:")
    turns = _turns(conn)
    turns.set("t", Turn("C", "B", {"a": "1", "b": "2"}))
    assert turns.get("t") == Turn("C", "B", {"a": "1", "b": "2"})

    turns.set("t", Turn("C", "B", {"b": "2"}))
    assert turns.get("t") == Turn("C", "B", {"b": "2"})
    assert _rows(conn) == [("u", "t", "b")]

    turns.set("t", Turn("C", "B", None))
    assert turns.get("t") == Turn("C", "B", {}), "no children read as a missing part"


def test_children_never_decide_existence() -> None:
    conn = sqlite3.connect(":memory:")
    mounts = _mounts(conn)
    turns = _turns(conn, mounts=mounts)
    mounts.scoped(user="u", consumer_turn_id="orphan").set("a", "1")

    assert turns.exists("orphan") is False
    assert turns.get("orphan") is None
    turns.set("t", Turn("C", None, {}))
    assert sorted(turns.list()) == ["t"]


def test_deleting_the_record_takes_its_children() -> None:
    conn = sqlite3.connect(":memory:")
    turns = _turns(conn)
    turns.set("t", Turn("C", "B", {"a": "1"}))
    turns.set("keep", Turn("K", None, {"a": "x"}))

    assert turns.delete("t") is True
    assert turns.get("t") is None
    assert _rows(conn) == [("u", "keep", "a")]


def test_no_child_is_written_when_the_record_loses_the_race() -> None:
    conn = sqlite3.connect(":memory:")
    turns = _turns(conn)
    assert turns.set_if_absent("t", Turn("C", "B", {"a": "1"})) is True

    assert turns.set_if_absent("t", Turn("C2", "B2", {"z": "9"})) is False
    assert _rows(conn) == [("u", "t", "a")], "a losing writer's children were written"


def test_children_wait_for_the_commit_part() -> None:
    """Written after it, so a failed commit leaves no receipts behind."""
    conn = sqlite3.connect(":memory:")
    turns = _turns(conn, core=_sql(conn, "turn", "id", ("user",), down=True))

    with pytest.raises(UncommittedWriteError):
        turns.set("t", Turn("C", "B", {"a": "1"}))
    assert _rows(conn) == []


def test_a_failed_child_write_reports_the_record_committed() -> None:
    conn = sqlite3.connect(":memory:")
    turns = _turns(conn, mounts=_mounts(conn, down=True))
    with pytest.raises(PartialSyncError) as caught:
        turns.set("t", Turn("C", "B", {"a": "1"}))
    assert caught.value.committed is True
    assert turns.exists("t") is True


def test_children_take_the_record_s_write_clock() -> None:
    conn = sqlite3.connect(":memory:")
    mounts = _mounts(conn)
    turns = _turns(conn, mounts=mounts)
    turns.set("t", Turn("C", None, {"a": "1"}), created_at=CLOCK)

    meta = mounts.scoped(user="u", consumer_turn_id="t").stat("a")
    assert meta is not None and meta.created_at == CLOCK


def test_a_record_that_expires_takes_its_children_at_gc() -> None:
    conn = sqlite3.connect(":memory:")
    with freeze_time("2024-01-01 12:00:00") as frozen:
        core = _sql(conn, "turn", "id", ("user",), ttl=TtlPolicy(ttl_duration=timedelta(seconds=60)))
        turns = _turns(conn, core=core)
        turns.set("t", Turn("C", None, {"a": "1"}))
        frozen.tick(timedelta(seconds=120))
        collected = turns.gc_expired_keys(max_entries=10)

    assert _rows(conn) == [], "the expired record's children were left behind"
    assert collected == ["t"], "a child's key was reported as a record's"


def test_children_that_expire_on_their_own_are_collected_and_the_record_stays() -> None:
    conn = sqlite3.connect(":memory:")
    with freeze_time("2024-01-01 12:00:00") as frozen:
        mounts = _sql(
            conn,
            "mount",
            "card_turn_id",
            ("user", "consumer_turn_id"),
            ttl=TtlPolicy(ttl_duration=timedelta(seconds=60)),
        )
        turns = _turns(conn, mounts=mounts)
        turns.set("t", Turn("C", None, {"a": "1"}))
        frozen.tick(timedelta(seconds=120))
        collected = turns.gc_expired_keys(max_entries=10)

    assert conn.execute('SELECT count(*) FROM "mount_1"').fetchone()[0] == 0
    assert turns.get("t") == Turn("C", None, {})
    assert collected == [], "a child's key was reported as an expired record"


def test_after_commit_names_parts_that_come_after_a_commit_part() -> None:
    conn = sqlite3.connect(":memory:")
    core, groups = _sql(conn, "turn", "id", ("user",)), _mounts(conn).group_by("consumer_turn_id")
    with pytest.raises(ValueError, match="needs a commit_part"):
        KvStore.zip_with(Turn, after_commit=("mounts",), core=core, body=_bodies(), mounts=groups)
    with pytest.raises(ValueError, match="unknown parts"):
        KvStore.zip_with(
            Turn, commit_part="core", after_commit=("nope",), core=core, body=_bodies(), mounts=groups
        )
    with pytest.raises(ValueError, match="after itself"):
        KvStore.zip_with(
            Turn, commit_part="core", after_commit=("core",), core=core, body=_bodies(), mounts=groups
        )
