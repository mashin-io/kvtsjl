"""R16: a zip where one part decides whether the record exists.

A record made of a body and a row must write the body first, so a reader that
finds the row always finds the body. The row is the commit part: it is written
last, and it alone answers ``exists`` and ``scan``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from kvtsjl import KvSet, MemoryKvStore, SerDe
from kvtsjl.exceptions import KvStoreError, UncommittedWriteError
from kvtsjl.keymap import KeyMap


@dataclass(frozen=True)
class Record:
    """The commit part is declared *first* on purpose.

    If it were declared last, plain declaration order would already write it
    last and the reordering would be untestable — the tests here would pass
    whether or not the commit part is moved to the end.
    """

    row: str | None
    body: str | None


def _part(name: str) -> Any:
    return MemoryKvStore(
        KvSet.with_str_keys(
            name, key_serde=SerDe.identity(str), value_serde=SerDe.identity(str)
        )
    )


def _zip(body: Any = None, row: Any = None, *, commit: str | None = "row") -> Any:
    return KeyMap.zip_with(
        Record, commit_part=commit, body=body or _part("b"), row=row or _part("r")
    )


def _failing(part: Any) -> Any:
    """Make a real part's write raise; it must stay a KvStore to be zipped."""

    def boom(*_a: Any, **_kw: Any) -> None:
        raise RuntimeError("part unavailable")

    part.set = boom
    return part


def _recording(part: Any, label: str, log: list[str]) -> Any:
    original = part.set

    def record(key: Any, value: Any, **kw: Any) -> None:
        log.append(label)
        original(key, value, **kw)

    part.set = record
    return part


def test_the_body_is_written_before_the_row() -> None:
    order: list[str] = []
    body = _recording(_part("b"), "body", order)
    row = _recording(_part("r"), "row", order)
    _zip(body, row).set("k", Record(body="B", row="R"))

    assert order == ["body", "row"], "the commit part must be written last"


def test_a_body_failure_leaves_no_row() -> None:
    body, row = _failing(_part("b")), _part("r")
    store = _zip(body, row)

    with pytest.raises(UncommittedWriteError) as caught:
        store.set("k", Record(body="B", row="R"))
    assert caught.value.committed is False
    assert isinstance(caught.value.failures["body"], RuntimeError)

    assert row.get("k") is None, "the record was committed despite a failed part"
    assert store.exists("k") is False


def test_a_row_failure_leaves_the_record_absent() -> None:
    body, row = _part("b"), _failing(_part("r"))
    store = _zip(body, row)

    with pytest.raises(UncommittedWriteError) as caught:
        store.set("k", Record(body="B", row="R"))
    assert caught.value.committed is False, "the commit point itself failed"

    assert body.get("k") == "B", "the body should already be written"
    assert store.exists("k") is False, "a written body must not mean present"


def test_exists_and_scan_follow_the_commit_part_only() -> None:
    body, row = _part("b"), _part("r")
    store = _zip(body, row)
    body.set("orphan", "B")                      # a body with no row
    store.set("whole", Record(body="B", row="R"))

    assert store.exists("orphan") is False
    assert store.exists("whole") is True
    assert sorted(store.batch_exists(["orphan", "whole"])) == ["whole"]
    assert sorted(store.list()) == ["whole"], "scan saw an uncommitted record"


def test_set_if_absent_commits_once() -> None:
    store = _zip()
    assert store.set_if_absent("k", Record(body="B1", row="R1")) is True
    assert store.set_if_absent("k", Record(body="B2", row="R2")) is False
    assert store.get("k").row == "R1", "the second writer replaced the record"


def test_set_if_absent_needs_a_commit_part() -> None:
    store = _zip(commit=None)
    with pytest.raises(KvStoreError, match="commit_part"):
        store.set_if_absent("k", Record(body="B", row="R"))


def test_a_commit_part_that_cannot_do_it_makes_the_zip_refuse() -> None:
    """The zip has no opinion of its own: the commit part decides."""
    plain = _part("r")

    def refuse(*_a: Any, **_kw: Any) -> bool:
        raise KvStoreError("row store does not support set_if_absent")

    plain.set_if_absent = refuse
    with pytest.raises(UncommittedWriteError) as caught:
        _zip(row=plain).set_if_absent("k", Record(row="R", body="B"))
    assert "set_if_absent" in str(caught.value.failures["row"])


def test_an_unknown_commit_part_is_rejected() -> None:
    with pytest.raises(ValueError, match="commit_part"):
        _zip(commit="nope")


def test_the_keymap_layer_orders_writes_the_same_way() -> None:
    """One write order, wherever the zip lands.

    ``zip_with`` builds a store when every part is a ``KvStore`` and a plain
    keymap otherwise. The ordering lives in the keymap layer so both get it;
    implementing it only in the store would make the behaviour depend on what
    the parts happened to be.
    """
    from kvtsjl.keymap import KeyMap
    from kvtsjl.keymap_algebra.dict_map import DictKeyMap

    order: list[str] = []

    def recording(part: Any, label: str) -> Any:
        original = part.set

        def record(key: Any, value: Any, **kw: Any) -> None:
            order.append(label)
            original(key, value, **kw)

        part.set = record
        return part

    zipped = KeyMap.zip_with(
        Record,
        commit_part="row",
        row=recording(DictKeyMap({}), "row"),
        body=recording(DictKeyMap({}), "body"),
    )
    zipped.set("k", Record(row="R", body="B"))

    assert order == ["body", "row"], "the keymap layer ignored the commit part"
