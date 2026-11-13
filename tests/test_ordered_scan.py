"""An ordered scan means the same on every store; SQL sorts in the database.

``order_by`` orders by fields of the value — a missing one first ascending,
last descending — then by key, ascending. A SQL view that declares every
field as a column sorts there; any other store sorts in process. Both must
give the same rows in the same order, and ``after`` must resume either one
just past the place it names, so each case runs on every medium — through a
view, in process — and on a SQL table with real columns, pushed down.
"""

from __future__ import annotations

from dataclasses import replace
import sqlite3
from typing import Any

import pytest

from kvtsjl import KvSet, MemoryKvStore, SerDe
from kvtsjl.backends.sql.sqlite import SqliteSqlDbClientAdapter
from kvtsjl.backends.sql.store import SqlDbKvStore
from kvtsjl.exceptions import KvStoreScanUnsupported
from kvtsjl.store.schema.scan import (
    After,
    Field,
    OrderBy,
    Projection,
    Pushdown,
    ScanRow,
    ScanSpec,
    sort_rows,
)
from tests.doc_fixtures import (
    AGENT,
    DOCS,
    KIND,
    MIME,
    NAME,
    Doc,
    columns_store,
    decode_doc,
    encode_doc,
)
from tests.media import make_medium
from tests.sql_helpers import ensure_sql_table, make_composite_sql_kvset

# Keyed by what they show; each expects DOCS' keys in order.
ORDERS: list[tuple[str, list[OrderBy[Any]], str]] = [
    ("ties fall in key order", [OrderBy(KIND)], "dacfbe"),
    ("descending ties still ascend by key", [OrderBy(KIND, descending=True)], "beacfd"),
    ("unset first ascending", [OrderBy(AGENT)], "beadcf"),
    ("unset last descending", [OrderBy(AGENT, descending=True)], "fcadbe"),
    ("each field in turn", [OrderBy(KIND), OrderBy(AGENT, descending=True)], "dfcabe"),
    ("unset last on another field", [OrderBy(MIME, descending=True)], "dfbaec"),
    ("text by code point", [OrderBy(NAME)], "faedbc"),
]
IDS = [o[0] for o in ORDERS]


@pytest.fixture(params=["memory", "sql", "redis", "s3", "filesystem", "sql_columns"])
def docs(request: pytest.FixtureRequest) -> Any:
    """``DOCS`` on a medium, through a view; or in a table whose fields are columns."""
    if request.param == "sql_columns":
        return columns_store()[0]
    store = make_medium(request.param, request).imap(decode_doc, encode_doc)
    store.batch_set(DOCS)
    return store


@pytest.mark.parametrize(("case", "order_by", "expected"), ORDERS, ids=IDS)
def test_an_order_means_the_same_on_every_store(
    docs: Any, case: str, order_by: list[OrderBy[Any]], expected: str
) -> None:
    assert "".join(docs.scan(order_by=order_by)) == expected, case
    assert "".join(docs.scan(order_by=order_by, limit=2)) == expected[:2], case


@pytest.mark.parametrize("page", [1, 2, 4])
@pytest.mark.parametrize(("case", "order_by", "expected"), ORDERS, ids=IDS)
def test_after_the_last_row_resumes_every_order_on_every_store(
    docs: Any, case: str, order_by: list[OrderBy[Any]], expected: str, page: int
) -> None:
    seen: list[str] = []
    after: After[str] | None = None
    for _ in range(len(DOCS) + 1):  # bounded: a page that does not advance fails
        rows = list(docs.scan(order_by=order_by, include_values=True, limit=page, after=after))
        seen.extend(key for key, _ in rows)
        if len(rows) < page:
            break
        key, value = rows[-1]
        after = After.of(order_by, key, value)
    assert "".join(seen) == expected, case


def test_after_values_alone_resume_past_every_row_with_them(docs: Any) -> None:
    by_kind = [OrderBy(KIND)]
    assert list(docs.scan(order_by=by_kind, after=After(("chat",)))) == ["b", "e"]
    assert list(docs.scan(order_by=by_kind, after=After(("chat",), "c"))) == ["f", "b", "e"]
    assert list(docs.scan(order_by=[OrderBy(AGENT)], after=After((None,)))) == list("adcf")


def test_sql_sorts_and_resumes_in_the_database() -> None:
    store, adapter = columns_store()
    by_kind = [OrderBy(KIND, descending=True)]
    spec = ScanSpec[str](order_by=by_kind, after=After(("job",), "b"), limit=2)

    assert [r.key for r in store.scan_rows(spec)] == ["e", "a"]
    assert adapter.returned == 2, "SQL returned rows Python then dropped"
    assert adapter.queries[-1].after == {"kind": "job", "id": "b"}
    assert {"order_by", "after", "limit"} <= store.explain(spec=spec).root.pushed


def test_a_store_without_columns_sorts_in_process_and_says_so(
    request: pytest.FixtureRequest,
) -> None:
    store = make_medium("memory", request).imap(decode_doc, encode_doc)
    store.batch_set(DOCS)
    plan = store.explain(order_by=[OrderBy(KIND), OrderBy(NAME, descending=True)])
    assert plan.root.name == "Sort"
    assert plan.root.residual == "order_by(kind, name desc)"
    assert plan.root.children
    with pytest.raises(KvStoreScanUnsupported, match="kind"):
        list(store.scan(order_by=[OrderBy(KIND)], on_unsupported=Pushdown.STRICT))
    pushed = columns_store()[0].scan(order_by=[OrderBy(KIND)], on_unsupported=Pushdown.STRICT)
    assert "".join(pushed) == "dacfbe"


def test_sql_breaks_ties_by_the_whole_key_beneath_a_scope() -> None:
    """A scoped, composite key: ``after.key`` becomes the scope's and the leaf's columns."""
    conn = sqlite3.connect(":memory:")
    sql_kvset = make_composite_sql_kvset()
    ensure_sql_table(conn, sql_kvset)
    store = SqlDbKvStore(sql_kvset, adapter=SqliteSqlDbClientAdapter(conn)).scoped(tenant="t")
    keys = [(c, p) for c in "ba" for p in "zyx"]
    store.batch_set({k: "same" for k in keys})
    by_value = [OrderBy(Field[str, str]("value", lambda v: v))]
    seen: list[tuple[str, str]] = []
    after: After[tuple[str, str]] | None = None
    for _ in range(len(keys) + 1):
        page = list(store.scan(order_by=by_value, include_values=True, limit=1, after=after))
        if not page:
            break
        key, value = page[0]
        seen.append(key)
        after = After.of(by_value, key, value)
    assert seen == sorted(keys)


def test_after_resumes_an_order_and_names_each_of_its_fields() -> None:
    with pytest.raises(ValueError, match="pass order_by too"):
        ScanSpec[str](after=After(("chat",)))
    with pytest.raises(ValueError, match="1 values for 2"):
        ScanSpec[str](order_by=[OrderBy(KIND), OrderBy(NAME)], after=After(("chat",)))


def test_after_of_reads_each_order_field_from_the_value() -> None:
    order_by = [OrderBy(KIND), OrderBy(AGENT, descending=True)]
    assert After.of(order_by, "b", DOCS["b"]) == After(("job", None), "b")


def test_an_order_partly_on_columns_sorts_in_process() -> None:
    """SQL takes an order only if it has every field: a column then a getter."""
    store, adapter = columns_store()
    title_length = Field[Doc, int]("title_length", lambda d: len(d.name))
    order_by = [OrderBy(KIND), OrderBy(title_length)]
    assert "".join(store.scan(order_by=order_by)) == "dfacbe"
    assert all(not q.order_by or q.order_by[0].column != "kind" for q in adapter.queries)
    assert store.explain(order_by=order_by).root.name == "Sort"


def test_a_sorted_scan_projects_what_was_asked() -> None:
    store, _ = columns_store()
    spec = ScanSpec[str](order_by=[OrderBy(Field[Doc, int]("n", lambda d: len(d.name)))])
    rows = list(store.scan_rows(spec))
    assert [r.key for r in rows] == list("dfbace")
    assert all(r.value is None for r in rows), "a key scan carried values"


def test_after_keeps_its_values_as_a_tuple() -> None:
    assert After(["chat"], "c") == After(("chat",), "c")
    assert hash(After[str](["chat"])) == hash(After[str](("chat",)))


def test_the_adapter_takes_after_only_as_a_leading_run_of_the_order() -> None:
    from kvtsjl.backends.sql.adapter import SqlOrder, SqlSelect

    conn = sqlite3.connect(":memory:")
    conn.execute('CREATE TABLE "t" (id TEXT PRIMARY KEY, kind TEXT)')
    conn.executemany('INSERT INTO "t" VALUES (?, ?)', [(k, d.kind) for k, d in DOCS.items()])
    adapter = SqliteSqlDbClientAdapter(conn)
    order = (SqlOrder("kind"), SqlOrder("id"))
    query = SqlSelect(table="t", columns=("id",), key_fields=("id",), order_by=order)
    assert [r["id"] for r in adapter.select(replace(query, after={"kind": "chat"}))] == ["b", "e"]
    for bad in ({"id": "c"}, {"kind": "chat", "id": "c", "name": "x"}, {}):
        with pytest.raises(ValueError, match="leading run"):
            list(adapter.select(replace(query, after=bad)))


def test_a_sort_passes_over_a_row_that_came_without_its_value() -> None:
    """A value can vanish between listing and reading it; the scan skips that row."""
    rows = [ScanRow[str, Any](key="gone"), ScanRow[str, Any](key="b", value=DOCS["b"])]
    spec = ScanSpec[str](order_by=[OrderBy(KIND)], project=Projection.ENTRIES)
    assert [r.key for r in sort_rows(rows, spec)] == ["b"]


def test_a_store_that_lists_beneath_its_scopes_sorts_too() -> None:
    """``beneath()`` scans with the store's default scan, which sorts as any other."""
    kvset = KvSet.with_str_keys(
        "tenants",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.identity(str),
        scope_schema=("tenant",),
    )
    store = MemoryKvStore(kvset)
    for tenant, key, value in (("t1", "a", "3"), ("t2", "b", "1"), ("t1", "c", "2")):
        store.scoped(tenant=tenant).set(key, value)
    by_value = [OrderBy(Field[str, str]("value", lambda v: v))]
    assert [s.key for s in store.beneath().scan(order_by=by_value)] == ["b", "c", "a"]
