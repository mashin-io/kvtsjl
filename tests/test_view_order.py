"""A scan's order travels through a view of one store.

``readonly()``, ``map``, ``imap`` and ``imap_keys`` list one store's rows, so
the order — ``order_by`` names that store's columns — travels to it, and the
view returns the store's rows in that order. A store that cannot give the
order refuses it through the view as it would directly. ``limit`` travels
only where nothing is left to filter.
"""

from __future__ import annotations

from datetime import timedelta
import sqlite3
from typing import Any

from freezegun import freeze_time
import pytest

from kvtsjl import KvSet, MemoryKvStore, SerDe
from kvtsjl.backends.sql.sqlite import SqliteSqlDbClientAdapter
from kvtsjl.backends.sql.store import SqlDbKvStore
from kvtsjl.exceptions import KvStoreScanUnsupported
from kvtsjl.keymap_algebra.dict_map import DictKeyMap
from kvtsjl.store.schema.scan import (
    OrderBy,
    ScanOrder,
    ScanRow,
    ScanSpec,
    ValueWhere,
    apply_cascade,
)
from kvtsjl.store.schema.ttl import ExpiryGc
from tests.media import TTL, make_medium
from tests.sql_helpers import ensure_sql_table, make_sql_kvset

NEWEST = [OrderBy("value", descending=True)]


class _Counting(SqliteSqlDbClientAdapter):
    fetches = 0

    def fetch_by_keys(self, **kw: Any) -> Any:
        self.fetches += 1
        return super().fetch_by_keys(**kw)


def _catalog() -> tuple[Any, _Counting]:
    """``a``…``e`` whose values sort in another order than their keys."""
    sql_kvset = make_sql_kvset("catalog")
    conn = sqlite3.connect(":memory:")
    ensure_sql_table(conn, sql_kvset)
    adapter = _Counting(conn)
    store = SqlDbKvStore(sql_kvset, adapter=adapter)
    store.batch_set({"a": "3", "b": "5", "c": "1", "d": "4", "e": "2"})
    return store, adapter


def _views(store: Any) -> dict[str, Any]:
    return {
        "readonly": store.readonly(),
        "map": store.map(lambda v: f"v{v}"),
        "imap": store.imap(lambda v: f"v{v}", lambda u: u[1:]),
    }


@pytest.mark.parametrize("view", ["readonly", "map", "imap"])
def test_a_view_lists_its_store_s_rows_in_the_order_asked(view: str) -> None:
    store, _ = _catalog()
    listed = list(_views(store)[view].scan(order_by=NEWEST))
    assert listed == ["b", "d", "a", "e", "c"]
    assert listed == list(store.scan(order_by=NEWEST))


@pytest.mark.parametrize("view", ["map", "imap"])
def test_a_value_view_maps_the_values_it_lists(view: str) -> None:
    store, _ = _catalog()
    rows = list(_views(store)[view].scan(order_by=NEWEST, include_values=True, limit=2))
    assert rows == [("b", "v5"), ("d", "v4")]


@pytest.mark.parametrize("view", ["readonly", "map", "imap"])
def test_a_key_prefix_travels_with_the_order(view: str) -> None:
    store, _ = _catalog()
    store.batch_set({"x1": "9", "x2": "0"})
    assert list(_views(store)[view].scan("x", order_by=NEWEST)) == ["x1", "x2"]


@pytest.mark.parametrize("view", ["map", "imap"])
def test_a_limit_under_a_filter_counts_what_the_filter_keeps(view: str) -> None:
    """Pushed down, the limit would count rows the filter then drops."""
    store, _ = _catalog()
    odd = ValueWhere[str](lambda v: int(v[1:]) % 2 == 1)
    rows = list(_views(store)[view].scan_rows(ScanSpec(where=odd, order_by=NEWEST, limit=2)))
    assert [r.key for r in rows] == ["b", "a"]


@pytest.mark.parametrize("view", ["map", "imap"])
def test_a_value_filter_reads_the_values_the_scan_brought(view: str) -> None:
    store, adapter = _catalog()
    odd = ValueWhere[str](lambda v: int(v[1:]) % 2 == 1)
    before = adapter.fetches
    list(_views(store)[view].scan_rows(ScanSpec(where=odd, order_by=NEWEST)))
    assert adapter.fetches == before, "values were fetched again, key by key"


@pytest.mark.parametrize("view", ["readonly", "map", "imap"])
def test_a_store_that_cannot_order_refuses_through_the_view(view: str) -> None:
    plain = MemoryKvStore(
        KvSet.with_str_keys("plain", key_serde=SerDe.identity(str), value_serde=SerDe.identity(str))
    )
    plain.set("a", "1")
    with pytest.raises(KvStoreScanUnsupported, match="order_by"):
        list(_views(plain)[view].scan(order_by=NEWEST))


# --- imap_keys: the store's columns travel, the caller's keys do not --------


def _renamed(store: Any) -> Any:
    return store.imap_keys(lambda k: k.removeprefix("doc:"), lambda sk: f"doc:{sk}")


def test_imap_keys_lists_the_store_s_rows_under_the_caller_s_keys() -> None:
    store, _ = _catalog()
    renamed = _renamed(store)
    assert list(renamed.scan(order_by=NEWEST, include_values=True, limit=2)) == [
        ("doc:b", "5"),
        ("doc:d", "4"),
    ]
    odd = ValueWhere[str](lambda v: int(v) % 2 == 1)
    rows = list(renamed.scan_rows(ScanSpec(where=odd, order_by=NEWEST, limit=2)))
    assert [r.key for r in rows] == ["doc:b", "doc:a"]


def test_imap_keys_refuses_a_key_order_it_cannot_give_in_the_caller_s_keys() -> None:
    store, _ = _catalog()
    with pytest.raises(KvStoreScanUnsupported, match="key order"):
        list(_renamed(store).scan_rows(ScanSpec(order=ScanOrder.KEY_ASC)))
    with pytest.raises(KvStoreScanUnsupported, match="prefix"):
        list(_renamed(store).scan("doc:a", order_by=NEWEST))


# --- a value filter reads the value a row carries ---------------------------


# The filesystem's clock is the OS's, which a frozen clock does not move.
@pytest.mark.parametrize("name", ["memory", "sql", "s3", "gcs", "azure"])
@pytest.mark.parametrize("gc", [ExpiryGc.HIDE, ExpiryGc.LAZY_DELETE])
def test_a_value_filter_never_lists_an_expired_entry(
    name: str, gc: ExpiryGc, request: pytest.FixtureRequest
) -> None:
    """The value a scan brings is a live one: a scan lists only what get returns."""
    with freeze_time("2024-01-01 12:00:00") as frozen:
        store = make_medium(name, request, TTL, expiry_gc=gc)
        store.set("old", "v")
        frozen.tick(timedelta(hours=2))
        store.set("new", "v")
        every = ValueWhere[str](lambda v: True)
        assert [r.key for r in store.scan_rows(ScanSpec(where=every))] == ["new"]


def test_rows_with_and_without_their_value_keep_their_order() -> None:
    held = DictKeyMap({"a": "1", "c": "3"})
    rows = iter([ScanRow(key="a"), ScanRow(key="b", value="2"), ScanRow(key="c")])
    every = ValueWhere[str](lambda v: True)
    listed = [r.key for r in apply_cascade(rows, residual=every, store=held)]
    assert listed == ["a", "b", "c"]
