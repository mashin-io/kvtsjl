"""``tally()`` counts what a scan lists, and its latest write, on every store.

A change token for a scope: it moves when an entry is added, removed or
expires, and on a write that moves the time. SQL answers in one statement;
any other store lists keys and stats, never values, and ``Pushdown.STRICT``
refuses there. Views whose rows are their store's tally on that store.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
import sqlite3
from typing import Any

from freezegun import freeze_time
import pytest

from kvtsjl.backends.sql.adapter import SqlSelect, SqlTally
from kvtsjl.backends.sql.sqlite import SqliteSqlDbClientAdapter
from kvtsjl.backends.sql.store import SqlDbKvStore
from kvtsjl.exceptions import KvStoreScanUnsupported
from kvtsjl.store.schema.scan import KeyIn, KeyPrefix, Pushdown, ScanRow, Tally, ValueWhere
from kvtsjl.store.schema.stat import RecordStat
from tests.doc_fixtures import DOCS, KIND, columns_store
from tests.media import MEDIA, TTL, make_medium
from tests.sql_helpers import ensure_sql_table, make_sql_kvset

T0 = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)

# The filesystem's clock is the OS's, which a frozen clock does not move;
# Redis keeps no write time at all.
CLOCKED = [m for m in MEDIA if m not in ("filesystem", "redis", "redis_hash")]


@pytest.mark.parametrize("name", CLOCKED)
def test_a_tally_moves_on_every_change_and_only_then(
    name: str, request: pytest.FixtureRequest
) -> None:
    with freeze_time(T0) as frozen:
        store = make_medium(name, request, TTL)
        assert store.tally() == Tally(0, None)
        store.set("a", "1")
        frozen.tick(timedelta(minutes=1))
        store.set("b", "2")
        first = store.tally()
        assert first.count == 2
        assert first.newest is not None and first.newest >= T0 + timedelta(minutes=1)
        assert store.tally() == first, "a tally moved with nothing written"

        frozen.tick(timedelta(minutes=1))
        store.set("a", "3")
        rewritten = store.tally()
        assert rewritten.count == 2 and rewritten != first, "a rewrite did not move it"

        store.delete("b")
        assert store.tally().count == 1

        frozen.tick(timedelta(hours=2))
        assert store.tally() == Tally(0, None), "an expired entry was counted"


def test_redis_keeps_no_write_time_so_its_tally_has_none(
    request: pytest.FixtureRequest,
) -> None:
    store = make_medium("redis", request)
    store.set("a", "1")
    assert store.tally() == Tally(1, None)


@pytest.mark.parametrize("name", MEDIA)
def test_a_tally_counts_what_a_scan_lists(name: str, request: pytest.FixtureRequest) -> None:
    store = make_medium(name, request)
    store.batch_set({"a1": "x", "a2": "y", "b1": "x"})
    assert store.tally().count == 3
    assert store.tally(KeyPrefix[str]("a")).count == 2
    assert store.tally(KeyIn[str](frozenset({"a1", "b1", "zz"}))).count == 2
    assert store.tally(ValueWhere[str](lambda v: v == "x")).count == 2


def test_a_tally_reads_no_value_where_nothing_tests_one(request: pytest.FixtureRequest) -> None:
    store = make_medium("memory", request)
    store.batch_set({"a": "1", "b": "2"})
    read: list[object] = []
    original = store.batch_get

    def counting(keys: Any) -> Any:
        read.extend(keys)
        return original(keys)

    store.batch_get = counting
    def reading(key: str) -> None:
        read.append(key)

    store.get = reading
    assert store.tally().count == 2
    assert read == []


class _Counting(SqliteSqlDbClientAdapter):
    def __init__(self, conn: sqlite3.Connection, *, tallies: bool = True) -> None:
        super().__init__(conn)
        self._tallies = tallies
        self.tallied: list[SqlSelect] = []
        self.selected = 0

    def supports_tally(self) -> bool:
        return self._tallies

    def tally(self, query: SqlSelect, *, clock: str) -> SqlTally:
        self.tallied.append(query)
        return super().tally(query, clock=clock)

    def select(self, query: SqlSelect) -> Iterator[Any]:
        self.selected += 1
        return super().select(query)

    def scan_by_key_parts(self, **kw: Any) -> Any:
        self.selected += 1
        return super().scan_by_key_parts(**kw)


def _sql(*, tallies: bool = True, ttl: Any = None) -> tuple[SqlDbKvStore[str, str], _Counting]:
    conn = sqlite3.connect(":memory:")
    sql_kvset = make_sql_kvset("tallied", ttl_policy=ttl)
    ensure_sql_table(conn, sql_kvset)
    adapter = _Counting(conn, tallies=tallies)
    return SqlDbKvStore(sql_kvset, adapter=adapter), adapter


def test_sql_tallies_in_one_statement() -> None:
    with freeze_time(T0) as frozen:
        store, adapter = _sql(ttl=TTL)
        store.set("a1", "x")
        frozen.tick(timedelta(minutes=5))
        store.set("a2", "y")
        store.set("b1", "z")
        frozen.tick(timedelta(minutes=50))
        assert store.tally(KeyPrefix[str]("a")) == Tally(2, T0 + timedelta(minutes=5))
        frozen.tick(timedelta(minutes=6))  # a1 expires
        tally = store.tally(KeyPrefix[str]("a"), on_unsupported=Pushdown.STRICT)
        assert tally == Tally(1, T0 + timedelta(minutes=5))
        assert len(adapter.tallied) == 2
        assert adapter.selected == 0, "the tally read rows"


def test_sql_runs_field_tests_in_its_tally() -> None:
    store, adapter = columns_store()
    expected = sum(1 for d in DOCS.values() if d.kind == "chat")
    assert store.tally(KIND.eq("chat"), on_unsupported=Pushdown.STRICT).count == expected
    assert adapter.returned == 0, "the tally listed rows"


def test_sql_lists_what_it_cannot_count_and_strict_refuses() -> None:
    store, adapter = _sql()
    store.batch_set({"a": "x", "b": "y"})
    by_value = ValueWhere[str](lambda v: v == "x")
    assert store.tally(by_value).count == 1
    assert adapter.tallied == []
    with pytest.raises(KvStoreScanUnsupported, match="tally"):
        store.tally(by_value, on_unsupported=Pushdown.STRICT)

    plain, plain_adapter = _sql(tallies=False)
    plain.batch_set({"a": "x"})
    assert plain.tally().count == 1
    with pytest.raises(KvStoreScanUnsupported, match="tally"):
        plain.tally(on_unsupported=Pushdown.STRICT)
    assert plain_adapter.tallied == []


def test_strict_refuses_where_a_store_would_list(request: pytest.FixtureRequest) -> None:
    store = make_medium("memory", request)
    with pytest.raises(KvStoreScanUnsupported, match="tally"):
        store.tally(on_unsupported=Pushdown.STRICT)


def _views(store: SqlDbKvStore[str, str]) -> dict[str, Any]:
    return {
        "readonly": store.readonly(),
        "map": store.map(lambda v: f"v{v}"),
        "imap": store.imap(lambda v: f"v{v}", lambda u: u[1:]),
        "imap_keys": store.imap_keys(lambda k: k.removeprefix("doc:"), lambda sk: f"doc:{sk}"),
    }


@pytest.mark.parametrize("view", ["readonly", "map", "imap", "imap_keys"])
def test_a_view_of_one_store_tallies_on_it(view: str) -> None:
    store, adapter = _sql()
    store.batch_set({"a": "1", "b": "2"})
    assert _views(store)[view].tally(on_unsupported=Pushdown.STRICT).count == 2
    assert adapter.selected == 0


@pytest.mark.parametrize("view", ["map", "imap"])
def test_a_value_view_tallies_a_key_test_on_its_store(view: str) -> None:
    store, adapter = _sql()
    store.batch_set({"a1": "1", "a2": "2", "b": "3"})
    tally = _views(store)[view].tally(KeyPrefix[str]("a"), on_unsupported=Pushdown.STRICT)
    assert tally.count == 2
    assert adapter.selected == 0


@pytest.mark.parametrize("view", ["map", "imap", "imap_keys"])
def test_a_view_tallies_its_own_values_by_listing(view: str) -> None:
    store, _ = _sql()
    store.batch_set({"a": "1", "b": "2"})
    views = _views(store)
    one = ValueWhere[str](lambda v: v.endswith("1"))
    assert views[view].tally(one).count == 1
    with pytest.raises(KvStoreScanUnsupported, match="tally"):
        views[view].tally(one, on_unsupported=Pushdown.STRICT)


def test_imap_keys_lists_to_test_a_key() -> None:
    store, _ = _sql()
    store.batch_set({"a": "1", "b": "2"})
    view = _views(store)["imap_keys"]
    assert view.tally(KeyIn[str](frozenset({"doc:a"}))).count == 1
    with pytest.raises(KvStoreScanUnsupported, match="tally"):
        view.tally(KeyIn[str](frozenset({"doc:a"})), on_unsupported=Pushdown.STRICT)


def test_beneath_tallies_every_scope_on_its_store() -> None:
    from tests.sql_helpers import make_composite_sql_kvset

    conn = sqlite3.connect(":memory:")
    sql_kvset = make_composite_sql_kvset()
    ensure_sql_table(conn, sql_kvset)
    adapter = _Counting(conn)
    store = SqlDbKvStore(sql_kvset, adapter=adapter)
    store.scoped(tenant="t1").batch_set({("c", "p"): "v", ("c", "q"): "v"})
    store.scoped(tenant="t2").set(("d", "p"), "v")
    assert store.beneath().tally(on_unsupported=Pushdown.STRICT).count == 3
    assert adapter.selected == 0


def test_a_record_held_whole_by_one_store_tallies_there(request: pytest.FixtureRequest) -> None:
    store, adapter = _sql()
    store.batch_set({"a": "1", "b": "2"})
    cache = make_medium("memory", request)
    cache.set("c", "3")
    assert store.mirror(cache).tally(on_unsupported=Pushdown.STRICT).count == 2
    read_through = cache.fallback_read(store)
    assert read_through.tally().count == len(list(read_through.scan())), "not what it lists"
    assert adapter.selected <= 1


def test_a_listed_key_gone_before_its_stat_is_not_counted(
    request: pytest.FixtureRequest,
) -> None:
    store = make_medium("memory", request)
    store.batch_set({"a": "1", "b": "2"})
    original = store.batch_stat

    def vanishing(keys: Any) -> dict[str, RecordStat]:
        return {k: v for k, v in original(keys).items() if k != "b"}

    store.batch_stat = vanishing
    assert store.tally().count == 1


@pytest.mark.parametrize("view", ["store", "readonly", "map", "imap", "imap_keys"])
def test_a_tally_honours_a_limit_and_a_refine(view: str) -> None:
    store, _ = _sql()
    store.batch_set({"a": "1", "b": "2", "c": "3"})
    target = store if view == "store" else _views(store)[view]
    assert target.tally(limit=2).count == 2
    def only_a(row: ScanRow[str, Any]) -> bool:
        return row.key.endswith("a")

    assert target.tally(refine=only_a).count == 1


def test_a_listing_that_brings_stats_is_not_stat_again(request: pytest.FixtureRequest) -> None:
    store = make_medium("s3", request)
    store.batch_set({"a": "1", "b": "2"})

    def refused(keys: Any) -> dict[str, RecordStat]:
        raise AssertionError(f"stat again for {list(keys)}")

    store.batch_stat = refused
    tally = store.tally()
    assert tally.count == 2 and tally.newest is not None
