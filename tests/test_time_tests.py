"""``ModifiedBefore`` / ``ModifiedAfter`` mean the same on every store; SQL runs them.

A scan that brings rows without their stat reads the stat to test them,
as it reads a value to test one; a medium that keeps no write time (Redis)
has none to test, so a time test holds there for nothing. SQL tests its
clock column, where the store keeps that time — one query for a GC page.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
import sqlite3
from typing import Any

from freezegun import freeze_time
import pytest

from kvtsjl.backends.sql.adapter import SqlSelect
from kvtsjl.backends.sql.sqlite import SqliteSqlDbClientAdapter
from kvtsjl.backends.sql.store import SqlDbKvStore
from kvtsjl.exceptions import KvStoreScanUnsupported
from kvtsjl.store.schema.scan import (
    Field,
    KeyIn,
    ModifiedAfter,
    ModifiedBefore,
    Predicate,
    Pushdown,
    ScanSpec,
    ValueWhere,
)
from tests.media import MEDIA, make_medium
from tests.sql_helpers import ensure_sql_table, make_sql_kvset

T0 = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
MID = T0 + timedelta(minutes=5)

# The filesystem's clock is the OS's, which a frozen clock does not move;
# Redis keeps no write time at all.
CLOCKED = [m for m in MEDIA if m not in ("filesystem", "redis", "redis_hash")]

CASES: list[tuple[str, Predicate[str], str]] = [
    ("before", ModifiedBefore[str](MID), "a"),
    ("after", ModifiedAfter[str](MID), "bc"),
    ("not before", ~ModifiedBefore[str](MID), "bc"),
    ("between", ModifiedAfter[str](T0 - timedelta(minutes=1)) & ModifiedBefore[str](MID), "a"),
    ("or a key", ModifiedBefore[str](MID) | KeyIn[str](frozenset({"c"})), "ac"),
    ("or a value", ModifiedBefore[str](MID) | ValueWhere[str](lambda v: v == "3"), "ac"),
    ("before is strict", ModifiedBefore[str](T0), ""),
    ("after is strict", ModifiedAfter[str](T0 + timedelta(minutes=10)), ""),
]
IDS = [c[0] for c in CASES]


def _written(store: Any, frozen: Any) -> Any:
    store.set("a", "1")
    frozen.tick(timedelta(minutes=10))
    store.batch_set({"b": "2", "c": "3"})
    return store


@pytest.mark.parametrize(("case", "where", "expected"), CASES, ids=IDS)
@pytest.mark.parametrize("name", CLOCKED)
def test_a_time_test_means_the_same_on_every_store(
    name: str, case: str, where: Predicate[str], expected: str, request: pytest.FixtureRequest
) -> None:
    with freeze_time(T0) as frozen:
        store = _written(make_medium(name, request), frozen)
        assert "".join(sorted(store.scan(where=where))) == expected, case


def test_redis_keeps_no_write_time_to_test(request: pytest.FixtureRequest) -> None:
    store = make_medium("redis", request)
    store.set("a", "1")
    far = datetime.now(UTC) + timedelta(days=1)
    assert list(store.scan(where=ModifiedBefore[str](far))) == []
    assert list(store.scan(where=~ModifiedBefore[str](far))) == ["a"]


class _Counting(SqliteSqlDbClientAdapter):
    def __init__(self, conn: sqlite3.Connection, *, selects: bool = True) -> None:
        super().__init__(conn)
        self._selects = selects
        self.queries: list[SqlSelect] = []
        self.returned = 0

    def supports_select(self) -> bool:
        return self._selects

    def select(self, query: SqlSelect) -> Iterator[Any]:
        self.queries.append(query)
        for row in super().select(query):
            self.returned += 1
            yield row


def _sql(frozen: Any, *, selects: bool = True) -> tuple[SqlDbKvStore[str, str], _Counting]:
    conn = sqlite3.connect(":memory:")
    sql_kvset = make_sql_kvset("timed")
    ensure_sql_table(conn, sql_kvset)
    adapter = _Counting(conn, selects=selects)
    store = SqlDbKvStore(sql_kvset, adapter=adapter)
    _written(store, frozen)
    adapter.queries.clear()
    adapter.returned = 0
    return store, adapter


PUSHED = [c for c in CASES if "or" not in c[0]]


@pytest.mark.parametrize(("case", "where", "expected"), PUSHED, ids=[c[0] for c in PUSHED])
def test_sql_runs_a_time_test_on_its_clock_column(
    case: str, where: Predicate[str], expected: str
) -> None:
    with freeze_time(T0) as frozen:
        store, adapter = _sql(frozen)
        rows = store.scan(where=where, on_unsupported=Pushdown.STRICT)
        assert "".join(sorted(rows)) == expected, case
        assert adapter.returned == len(expected), "SQL returned rows Python then dropped"
        assert "FieldTest" in store.explain(where=where).root.pushed


def test_a_gc_page_is_one_query_that_stops_at_the_page() -> None:
    with freeze_time(T0) as frozen:
        store, adapter = _sql(frozen)
        page = ScanSpec[str](
            where=ModifiedBefore(T0 + timedelta(hours=1)),
            limit=2,
            on_unsupported=Pushdown.STRICT,
        )
        assert [r.key for r in store.scan_rows(page)] == ["a", "b"]
        assert adapter.returned == 2
        assert len(adapter.queries) == 1


def test_sql_leaves_a_time_test_it_cannot_take_to_python_with_the_stat() -> None:
    """In an ``Or`` with a value test, the time test runs on the row's stat."""
    with freeze_time(T0) as frozen:
        store, _ = _sql(frozen)
        where = ModifiedBefore[str](MID) | ValueWhere[str](lambda v: v == "3")
        assert "".join(sorted(store.scan(where=where))) == "ac"
        with pytest.raises(KvStoreScanUnsupported):
            list(store.scan(where=where, on_unsupported=Pushdown.STRICT))


def test_sql_without_select_tests_the_time_in_python() -> None:
    with freeze_time(T0) as frozen:
        store, adapter = _sql(frozen, selects=False)
        assert "".join(sorted(store.scan(where=ModifiedBefore[str](MID)))) == "a"
        assert adapter.queries == []


def test_a_field_compares_only_with_a_value() -> None:
    n = Field[str, int]("n", int)
    for compare in (n.lt, n.le, n.gt, n.ge):
        with pytest.raises(ValueError, match="is_null"):
            compare(None)  # type: ignore[arg-type]


def test_a_key_ordered_limit_under_a_python_filter_counts_what_it_keeps() -> None:
    """A limit spent in SQL on rows Python then drops returned none at all."""
    with freeze_time(T0) as frozen:
        store, adapter = _sql(frozen)
        last = ValueWhere[str](lambda v: v == "3")
        assert list(store.scan(where=last, limit=1)) == ["c"]
        assert "limit" not in store.explain(where=last, limit=1).root.pushed
        adapter.returned = 0
        assert list(store.scan(limit=2)) == ["a", "b"]
        assert adapter.returned == 2
        assert "limit" in store.explain(limit=2).root.pushed


def test_a_row_gone_before_its_stat_is_read_is_not_listed(
    request: pytest.FixtureRequest,
) -> None:
    store = make_medium("memory", request)
    store.batch_set({"a": "1", "b": "2"})
    original = store.batch_stat
    def vanishing(keys: Any) -> dict[str, Any]:
        return {k: v for k, v in original(keys).items() if k != "b"}

    store.batch_stat = vanishing
    far = datetime.now(UTC) + timedelta(days=1)
    assert list(store.scan(where=ModifiedBefore[str](far))) == ["a"]
