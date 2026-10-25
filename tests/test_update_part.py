"""R23: update one part of an existing record, atomically.

No ``update(key, feedback=…)``: a record's ``update`` already decides from the
record and writes only if its commit part is unchanged. It writes only the
parts that changed, so changing one part is ``Put(replace(record, part=…))``
— typed, and the same call for any part. An absent record, or one deleted
concurrently, is written nowhere.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
import sqlite3
from typing import Any

import pytest

from kvtsjl import Keep, KvSet, MemoryKvStore, Put, SerDe
from kvtsjl.exceptions import KvStoreUnsupported
from kvtsjl.store.logical import KvStore
from tests.test_caller_commit import _sql
from tests.test_clear_part import _column, _doc, _wide_table


@dataclass(frozen=True)
class Turn:
    core: str | None
    body: str | None
    feedback: str | None
    mounts: Mapping[str, str] | None
    tag: str | None = None


class _Counted(MemoryKvStore[Any, Any, Any, Any]):
    """Counts every write that reaches it."""

    writes: int = 0

    def set(self, *a: Any, **kw: Any) -> None:
        self.writes += 1
        super().set(*a, **kw)

    def batch_set(self, *a: Any, **kw: Any) -> None:
        self.writes += 1
        super().batch_set(*a, **kw)

    def batch_write_if_unchanged(self, *a: Any, **kw: Any) -> Any:
        self.writes += 1
        return super().batch_write_if_unchanged(*a, **kw)


def _leaf(name: str) -> _Counted:
    return _Counted(
        KvSet.with_str_keys(name, key_serde=SerDe.identity(str), value_serde=SerDe.identity(str))
    )


def _turns() -> tuple[Any, dict[str, Any]]:
    conn = sqlite3.connect(":memory:", isolation_level=None)
    parts = {
        "core": _leaf("core"),
        "body": _leaf("body"),
        "feedback": _leaf("feedback"),
        "mounts": _sql(conn, "mount", "card", ("turn",)).group_by("turn"),
        "tag": _leaf("tag"),
    }
    turns = KvStore.zip_with(
        Turn, commit_part="core", after_commit=("feedback", "mounts", "tag"), **parts
    )
    return turns, parts


def _feedback(fb: str | None) -> Any:
    return lambda t: Put(replace(t, feedback=fb)) if t is not None else Keep()


def test_changing_one_part_writes_that_part_and_the_commit_part_only() -> None:
    turns, parts = _turns()
    turns.set("t", Turn("C", "B", None, {"a": "1"}, "x"))
    before = {name: getattr(p, "writes", None) for name, p in parts.items()}

    assert turns.update("t", _feedback("up")) == Turn("C", "B", "up", {"a": "1"}, "x")

    after = {name: getattr(p, "writes", None) for name, p in parts.items()}
    assert after["body"] == before["body"], "an unchanged part before the commit point was written"
    assert after["tag"] == before["tag"], "an unchanged part after the commit point was written"
    assert after["feedback"] == before["feedback"] + 1
    assert after["core"] == before["core"] + 1, "the commit part is the condition"
    assert turns.get("t") == Turn("C", "B", "up", {"a": "1"}, "x")


def test_an_absent_record_is_written_nowhere() -> None:
    turns, parts = _turns()
    assert turns.update("t", _feedback("up")) is None
    assert parts["feedback"].get("t") is None
    assert parts["core"].writes == parts["feedback"].writes == 0


def test_a_record_deleted_concurrently_gets_no_orphan_part() -> None:
    turns, parts = _turns()
    turns.set("t", Turn("C", "B", None, None))
    read = turns.batch_read_versioned

    def deleted_after_the_read(keys: Any) -> Any:
        seen = read(keys)
        if seen["t"].value is not None:
            turns.delete("t")
        return seen

    turns.batch_read_versioned = deleted_after_the_read
    assert turns.update("t", _feedback("up")) is None
    assert parts["feedback"].get("t") is None, "the part outlived its record"
    assert parts["core"].get("t") is None


def test_a_child_collection_part_is_replaced() -> None:
    turns, parts = _turns()
    turns.set("t", Turn("C", "B", None, {"a": "1", "b": "2"}))
    turns.update("t", lambda t: Put(replace(t, mounts={"c": "3"})) if t else Keep())
    assert parts["mounts"].get("t") == {"c": "3"}


def test_clearing_a_part_unsets_only_it() -> None:
    turns, parts = _turns()
    turns.set("t", Turn("C", "B", "up", {"a": "1"}))
    assert turns.update("t", _feedback(None)) == Turn("C", "B", None, {"a": "1"})
    assert parts["feedback"].get("t") is None
    assert parts["body"].get("t") == "B"


def test_changing_a_part_before_the_commit_point_still_refuses() -> None:
    turns, parts = _turns()
    turns.set("t", Turn("C", "B", None, None))
    with pytest.raises(KvStoreUnsupported, match="after_commit"):
        turns.update("t", lambda t: Put(replace(t, body="B2")))
    assert parts["body"].get("t") == "B"


def test_a_record_seen_through_a_fallback_is_written_in_full() -> None:
    """Nothing is stored yet, so no part can be compared with the fallback's copy."""
    turns, parts = _turns()
    cold = KvStore.zip_with(Turn, commit_part="core", **{n: _leaf(f"cold-{n}") for n in ("core", "body", "feedback", "mounts", "tag")})
    cold.set("t", Turn("C", "B", None, None, "x"))
    tiered = turns.fallback_read(cold, promote=False)

    tiered.update("t", _feedback("up"))

    assert (parts["body"].get("t"), parts["tag"].get("t")) == ("B", "x")


def test_an_expired_record_is_replaced_in_full() -> None:
    from datetime import timedelta

    from freezegun import freeze_time

    from kvtsjl import TtlPolicy

    with freeze_time("2024-01-01 12:00:00") as frozen:
        core = _Counted(
            KvSet.with_str_keys(
                "core", key_serde=SerDe.identity(str), value_serde=SerDe.identity(str),
                ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=60)),
            )
        )
        tag = _leaf("tag")
        records = KvStore.zip_with(Doc, commit_part="core", after_commit=("impact",), core=core, impact=tag)
        records.set("d", Doc("C", "x"))
        frozen.tick(timedelta(seconds=120))
        tag.delete("d")

        made = records.update("d", lambda d: Put(Doc("C", "x")) if d is None else Keep())

    assert made == Doc("C", "x")
    assert tag.get("d") == "x", "a part of the replaced record was left out"


# --- a part on the commit part's own row ------------------------------------


@dataclass(frozen=True)
class Doc:
    core: str | None
    impact: str | None


def _docs(conn: sqlite3.Connection, **adapter: Any) -> Any:
    _wide_table(conn)
    return KvStore.zip_with(
        Doc,
        commit_part="core",
        after_commit=("impact",),
        core=_column(conn, "core", owns_rows=True, **adapter),
        impact=_column(conn, "impact", **adapter),
    )


def _impact(value: str) -> Any:
    return lambda d: Put(replace(d, impact=value)) if d is not None else Keep()


def test_a_column_group_part_is_written_only_where_its_row_is() -> None:
    """The part's columns live on the commit part's row, and only its owner creates one."""
    conn = sqlite3.connect(":memory:")
    docs = _docs(conn)
    docs.set("d", Doc("C", None))

    assert docs.update("d", _impact("I")) == Doc("C", "I")
    assert _doc(conn, "d") == {"core": "C", "text": None, "score": None, "impact": "I"}
    assert docs.update("gone", _impact("I")) is None
    assert _doc(conn, "gone") == {}


def test_the_update_is_one_with_the_caller_s_transaction() -> None:
    conn = sqlite3.connect(":memory:", isolation_level=None)
    docs = _docs(conn, autocommit=False, begin_on_write=True)
    docs.set("d", Doc("C", "I"))
    conn.commit()

    docs.update("d", _impact("I2"))
    conn.rollback()
    assert docs.get("d") == Doc("C", "I"), "a rolled-back update left its part"

    docs.update("d", _impact("I2"))
    conn.commit()
    assert docs.get("d") == Doc("C", "I2")
