"""R24: an assembled record's ordered and paged scans follow its commit part.

The commit part decides which records exist, so its scan is the record's
scan: the order and the pages come from it, and each record is assembled and
confirmed by ``get``. Asking for an order no one part can give refuses.
"""

from __future__ import annotations

from dataclasses import dataclass
import sqlite3
from typing import Any

import pytest

from kvtsjl import KvSet, MemoryKvStore, SerDe
from kvtsjl.backends.sql.sqlite import SqliteSqlDbClientAdapter
from kvtsjl.backends.sql.store import SqlDbKvStore
from kvtsjl.exceptions import KvStoreScanUnsupported
from kvtsjl.store.logical import KvStore
from kvtsjl.store.schema.scan import OrderBy, Projection, ScanCursor, ScanSpec
from tests.sql_helpers import ensure_sql_table, make_sql_kvset


@dataclass(frozen=True)
class Turn:
    core: str | None
    body: str | None


def _core(conn: sqlite3.Connection) -> Any:
    sql_kvset = make_sql_kvset("turn")
    ensure_sql_table(conn, sql_kvset)
    return SqlDbKvStore(sql_kvset, adapter=SqliteSqlDbClientAdapter(conn))


def _bodies() -> Any:
    return MemoryKvStore(
        KvSet.with_str_keys("body", key_serde=SerDe.identity(str), value_serde=SerDe.identity(str))
    )


def _turns(conn: sqlite3.Connection, *, bodies: Any = None, **kw: Any) -> Any:
    body = bodies if bodies is not None else _bodies()
    turns = KvStore.zip_with(Turn, commit_part="core", core=_core(conn), body=body, **kw)
    for key, seq in (("a", "3"), ("b", "1"), ("c", "2"), ("d", "4")):
        turns.set(key, Turn(core=seq, body=f"body-{key}"))
    return turns


def test_an_ordered_scan_follows_the_commit_part_s_columns() -> None:
    turns = _turns(sqlite3.connect(":memory:"))
    newest = list(turns.scan(order_by=[OrderBy("value", descending=True)]))
    assert newest == ["d", "a", "c", "b"], "the order asked for was ignored"


def test_ordered_records_are_assembled_exactly_as_get_assembles_them() -> None:
    turns = _turns(sqlite3.connect(":memory:"))
    rows = list(turns.scan(order_by=[OrderBy("value")], include_values=True, limit=3))
    assert rows == [(k, turns.get(k)) for k in ("b", "c", "a")]


def test_a_limit_counts_records_not_commit_rows() -> None:
    """A row whose record does not assemble must not use up the limit."""
    conn = sqlite3.connect(":memory:")
    bodies = _bodies()
    turns = _turns(conn, bodies=bodies, mandatory=[True, True])
    turns.set("e", Turn(core="5", body="body-e"))
    bodies.delete("e")   # e's row stays, its record no longer assembles

    newest = list(turns.scan(order_by=[OrderBy("value", descending=True)], limit=2))
    assert newest == ["d", "a"]


def test_pages_resume_where_the_last_one_stopped() -> None:
    turns = _turns(sqlite3.connect(":memory:"))
    first = [r.key for r in turns.scan_rows(ScanSpec(project=Projection.KEYS, limit=2, page_size=2))]
    rest = [
        r.key
        for r in turns.scan_rows(
            ScanSpec(project=Projection.KEYS, page_size=2, cursor=ScanCursor(token=first[-1]))
        )
    ]
    assert first + rest == ["a", "b", "c", "d"]


def test_an_order_the_commit_part_cannot_give_refuses() -> None:
    turns = _turns(sqlite3.connect(":memory:"))
    with pytest.raises(KvStoreScanUnsupported):
        list(turns.scan(order_by=[OrderBy("not_a_column")]))


def test_a_record_without_a_commit_part_refuses_an_order() -> None:
    """Its keys come from every part, so no one part's order is the record's."""
    conn = sqlite3.connect(":memory:")
    turns = KvStore.zip_with(Turn, core=_core(conn), body=_bodies())
    turns.set("a", Turn(core="1", body="x"))
    with pytest.raises(KvStoreScanUnsupported, match="commit part"):
        list(turns.scan(order_by=[OrderBy("value")]))


def test_a_view_listing_another_store_s_keys_keeps_its_order() -> None:
    """The same fix reaches every view that lists another store's keys."""
    conn = sqlite3.connect(":memory:")
    orders = _core(conn)
    for key, seq in (("a", "3"), ("b", "1"), ("c", "2")):
        orders.set(key, seq)
    labels = MemoryKvStore(
        KvSet.with_str_keys("labels", key_serde=SerDe.identity(str), value_serde=SerDe.identity(str))
    )
    for seq in ("1", "2", "3"):
        labels.set(seq, f"label-{seq}")

    view = orders.then(labels)
    assert list(view.scan(order_by=[OrderBy("value")])) == ["b", "c", "a"]
