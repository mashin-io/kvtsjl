"""Field filters mean the same on every store; SQL runs them in the database.

A ``Field`` names a field of the value and says how to read it. Every store
runs the test on its values; a SQL view that declares the field as a column
runs it in the ``WHERE`` instead. Both must return the same rows, so each
case here runs on every medium — through a view, in-process — and on a SQL
table with real columns, pushed down.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
import json
import sqlite3
from typing import Any

import pytest

from kvtsjl import KvSet, SerDe
from kvtsjl.backends.sql import SqlDbField, SqlDbKvSet, SqlDbRow, SqliteSqlDbClientAdapter
from kvtsjl.backends.sql.adapter import SqlSelect
from kvtsjl.backends.sql.store import SqlDbKvStore
from kvtsjl.exceptions import KvStoreScanUnsupported
from kvtsjl.store.schema.scan import (
    Field,
    FieldContains,
    FieldEq,
    FieldIn,
    FieldIsNull,
    FieldStartsWith,
    KeyIn,
    OrderBy,
    Predicate,
    Pushdown,
    ScanCursor,
    ScanRow,
    ScanSpec,
)
from tests.media import MEDIA, make_medium
from tests.sql_helpers import ensure_sql_table, make_composite_sql_kvset


@dataclass(frozen=True)
class Doc:
    name: str
    kind: str
    mime: str | None
    agent: str | None


NAME = Field[Doc, str]("name", lambda d: d.name)
KIND = Field[Doc, str]("kind", lambda d: d.kind)
MIME = Field[Doc, str | None]("mime", lambda d: d.mime)
AGENT = Field[Doc, str | None]("agent", lambda d: d.agent)

DOCS = {
    "a": Doc("Report.xlsx", "chat", "application/vnd.ms-excel", "alice"),
    "b": Doc("photo.png", "job", "image/png", None),
    "c": Doc("Ünïcode.txt", "chat", None, "bob"),
    "d": Doc("notes.md", "asset", "text/markdown", "alice"),
    "e": Doc("image_dump.bin", "job", "application/octet-stream", None),
    "f": Doc("100%.csv", "chat", "text/csv", "carol"),
}

CASES: list[tuple[str, Predicate[str], str]] = [
    ("eq", KIND.eq("chat"), "acf"),
    ("is_in", KIND.is_in(["job", "asset"]), "bde"),
    ("is_in nothing", KIND.is_in([]), ""),
    ("startswith", MIME.startswith("image/"), "b"),
    ("startswith empty holds for text, not unset", MIME.startswith(""), "abdef"),
    ("contains a LIKE wildcard _", NAME.contains("_"), "e"),
    ("contains a LIKE wildcard %", NAME.contains("%"), "f"),
    ("contains is case-sensitive", NAME.contains("report"), ""),
    ("contains ignore_case", NAME.contains("REPORT", ignore_case=True), "a"),
    ("ignore_case folds ASCII only", NAME.contains("ü", ignore_case=True), ""),
    ("contains non-ASCII", NAME.contains("Ü"), "c"),
    ("is_null", AGENT.is_null(), "be"),
    ("not eq holds where unset", ~AGENT.eq("alice"), "bcef"),
    ("not startswith holds where unset", ~MIME.startswith("text/"), "abce"),
    ("not is_in holds where unset", ~AGENT.is_in(["alice"]), "bcef"),
    ("not contains holds where unset", ~MIME.contains("text"), "abce"),
    ("in or unset", AGENT.is_in(["alice"]) | AGENT.is_null(), "abde"),
    ("and", KIND.eq("chat") & ~AGENT.is_null(), "acf"),
    ("with a key test", KeyIn[str](frozenset("abc")) & KIND.eq("chat"), "ac"),
    ("or with a key test", KeyIn[str](frozenset("b")) | KIND.eq("asset"), "bd"),
]


def _encode(doc: Doc) -> str:
    return json.dumps(doc.__dict__)


def _decode(raw: str) -> Doc:
    return Doc(**json.loads(raw))


def _doc_row_serde() -> SerDe[Doc, SqlDbRow]:
    return SerDe(
        serializer=lambda d: {"name": d.name, "kind": d.kind, "mime": d.mime, "agent": d.agent},
        deserializer=lambda r: Doc(r["name"], r["kind"], r["mime"], r["agent"]),
        blob_type=dict,
    )


class _Recording(SqliteSqlDbClientAdapter):
    """Counts the rows the database returns, and keeps each query."""

    def __init__(self, conn: sqlite3.Connection, *, selects: bool = True) -> None:
        super().__init__(conn)
        self._selects = selects
        self.queries: list[SqlSelect] = []
        self.returned = 0

    def supports_select(self) -> bool:
        return self._selects

    def select(self, query: SqlSelect) -> Iterator[SqlDbRow]:
        self.queries.append(query)
        for row in super().select(query):
            self.returned += 1
            yield row


def _columns_store(*, selects: bool = True) -> tuple[SqlDbKvStore[str, Doc], _Recording]:
    """``DOCS`` in a SQL table whose fields are its columns."""
    conn = sqlite3.connect(":memory:")
    conn.execute(
        'CREATE TABLE "docs_1" (id TEXT PRIMARY KEY, name TEXT, kind TEXT, mime TEXT, '
        "agent TEXT, updated_at REAL NOT NULL, expires_at REAL)"
    )
    kvset: KvSet[str, Doc, str, SqlDbRow] = KvSet.with_str_keys(
        "docs", version=1, key_serde=SerDe.identity(str), value_serde=_doc_row_serde()
    )
    sql_kvset = SqlDbKvSet[str, Doc].create(
        kvset,
        key_field="id",
        fields=[SqlDbField("name"), SqlDbField("kind"), SqlDbField("mime"), SqlDbField("agent")],
    )
    adapter = _Recording(conn, selects=selects)
    store = SqlDbKvStore(sql_kvset, adapter=adapter)
    store.batch_set(DOCS)
    adapter.returned = 0
    adapter.queries.clear()
    return store, adapter


@pytest.fixture(params=[*MEDIA, "sql_columns"])
def docs(request: pytest.FixtureRequest) -> Any:
    if request.param == "sql_columns":
        return _columns_store()[0]
    store = make_medium(request.param, request).imap(_decode, _encode)
    store.batch_set(DOCS)
    return store


@pytest.mark.parametrize(("case", "where", "expected"), CASES, ids=[c[0] for c in CASES])
def test_a_field_filter_means_the_same_on_every_store(
    docs: Any, case: str, where: Predicate[str], expected: str
) -> None:
    assert sorted(docs.scan(where=where)) == list(expected), case


@pytest.mark.parametrize(("case", "where", "expected"), CASES, ids=[c[0] for c in CASES])
def test_sql_runs_a_field_filter_in_the_database(
    case: str, where: Predicate[str], expected: str
) -> None:
    store, adapter = _columns_store()
    assert sorted(store.scan(where=where)) == list(expected), case
    # Only key tests are left for Python: the rows SQL returned are the
    # field filter's own matches, plus whatever a key test then drops.
    plan = store.explain(where=where)
    assert "FieldTest" in plan.root.pushed or "Key" in plan.root.residual
    if "Key" not in plan.root.residual:
        assert adapter.returned == len(expected)
        assert plan.root.residual == "AlwaysTrue"


def test_an_ordered_scan_pushes_the_limit_past_a_field_filter() -> None:
    """Nothing left to filter in Python, so SQL stops after the rows asked for."""
    store, adapter = _columns_store()
    by_name = [OrderBy("name")]
    assert list(store.scan(order_by=by_name, limit=2)) == ["f", "a"]
    adapter.returned = 0
    adapter.queries.clear()
    assert list(store.scan(where=KIND.eq("job"), order_by=by_name, limit=2)) == ["e", "b"]
    assert adapter.returned == 2
    assert adapter.queries[0].limit == 2
    plan = store.explain(spec=ScanSpec[str](where=KIND.eq("job"), order_by=by_name, limit=2))
    assert {"FieldTest", "order_by", "limit"} <= plan.root.pushed


def test_keyset_pages_carry_the_field_filter_to_each_page() -> None:
    store, adapter = _columns_store()
    assert list(store.scan(where=KIND.eq("chat"), page_size=1)) == ["a", "c", "f"]
    assert all(q.where is not None for q in adapter.queries)
    assert adapter.returned == 3


def test_a_cursor_resumes_a_field_filtered_scan() -> None:
    """The cursor is the last key returned; the filter skips what lies between."""
    store, _ = _columns_store()
    chat = KIND.eq("chat")
    first = [r.key for r in store.scan_rows(ScanSpec[str](where=chat, limit=2, page_size=2))]
    assert first == ["a", "c"]
    spec = ScanSpec[str](where=chat, page_size=2, cursor=ScanCursor(token=first[-1]))
    assert [r.key for r in store.scan_rows(spec)] == ["f"]


def test_a_field_the_view_does_not_declare_runs_in_python() -> None:
    """``title`` is not one of the view's columns, so SQL is not asked to test it."""
    store, adapter = _columns_store()
    title = Field[Doc, str]("title", lambda d: d.name)
    assert list(store.scan(where=title.startswith("photo"))) == ["b"]
    assert all(q.where is None for q in adapter.queries)
    assert store.explain(where=title.startswith("photo")).root.residual == "FieldStartsWith"


def test_an_adapter_without_select_runs_the_filter_in_python() -> None:
    store, adapter = _columns_store(selects=False)
    assert list(store.scan(where=KIND.eq("asset"))) == ["d"]
    assert adapter.queries == []


def test_strict_takes_a_pushed_field_filter_and_refuses_one_that_is_not() -> None:
    store, _ = _columns_store()
    strict = Pushdown.STRICT
    pushed = ScanSpec[str](where=KIND.eq("asset"), on_unsupported=strict)
    assert [r.key for r in store.scan_rows(pushed)] == ["d"]
    title = Field[Doc, str]("title", lambda d: d.name)
    with pytest.raises(KvStoreScanUnsupported, match="FieldEq"):
        list(store.scan_rows(ScanSpec[str](where=title.eq("notes.md"), on_unsupported=strict)))


def test_eq_and_is_in_refuse_none() -> None:
    with pytest.raises(ValueError, match="is_null"):
        AGENT.eq(None)
    with pytest.raises(ValueError, match="is_null"):
        AGENT.is_in(["alice", None])


# --- select's keyset ``after`` -------------------------------------------------

_ROWS = [("k1", 3), ("k2", None), ("k3", 1), ("k4", 3), ("k5", None), ("k6", 2)]


@pytest.mark.parametrize("descending", [False, True])
@pytest.mark.parametrize("page", [1, 2, 4])
def test_select_pages_through_an_order_with_nulls(descending: bool, page: int) -> None:
    """Paging by ``after`` returns each row once, in order, a NULL being the smallest."""
    conn = sqlite3.connect(":memory:")
    conn.execute('CREATE TABLE "t" (id TEXT PRIMARY KEY, n INTEGER)')
    conn.executemany('INSERT INTO "t" VALUES (?, ?)', _ROWS)
    adapter = SqliteSqlDbClientAdapter(conn)
    order = (OrderBy("n", descending=descending), OrderBy("id"))
    if descending:  # NULLs last; ties by id, ascending
        expected = sorted(_ROWS, key=lambda r: (r[1] is None, -(r[1] or 0), r[0]))
    else:  # NULLs first
        expected = sorted(_ROWS, key=lambda r: (r[1] is not None, r[1] or 0, r[0]))

    seen: list[tuple[str, int | None]] = []
    after: SqlDbRow | None = None
    for _ in range(len(_ROWS) + 1):  # a page that does not advance must fail, not spin
        rows = list(
            adapter.select(
                SqlSelect(table="t", columns=("id", "n"), key_fields=("id",), order_by=order,
                          after=after, limit=page)
            )
        )
        seen.extend((r["id"], r["n"]) for r in rows)
        if len(rows) < page:
            break
        after = {"n": rows[-1]["n"], "id": rows[-1]["id"]}
    assert seen == expected


def test_a_field_test_is_false_on_a_missing_value() -> None:
    """Two-valued even without a value: ``is_null`` too, since no field is there."""
    row = ScanRow[str, Any](key="a")
    assert not KIND.eq("chat").evaluate(row)
    assert not AGENT.is_null().evaluate(row)


def test_sql_reports_the_field_tests_it_takes_only_with_select() -> None:
    tests = {FieldEq, FieldIn, FieldStartsWith, FieldContains, FieldIsNull}
    assert tests <= _columns_store()[0].scan_capabilities().pushdown
    assert not tests & _columns_store(selects=False)[0].scan_capabilities().pushdown


def test_a_key_ordered_page_reads_the_index_without_sorting() -> None:
    """The row-value ``after``: the expanded form sorts every page in a temp B-tree.

    A key of several columns, none fixed by a scope: over one column both
    forms are one comparison, and a scope's equality already walks the index.
    """
    conn = sqlite3.connect(":memory:")
    sql_kvset = make_composite_sql_kvset(scope_fields=())
    ensure_sql_table(conn, sql_kvset)
    store = SqlDbKvStore(sql_kvset, adapter=SqliteSqlDbClientAdapter(conn))
    keys = [(c, p) for c in "ab" for p in "xyz"]
    store.batch_set({k: "v" for k in keys})
    run: list[str] = []
    conn.set_trace_callback(run.append)
    assert list(store.scan(page_size=2)) == keys
    pages = [sql for sql in run if sql.startswith("SELECT") and "LIMIT" in sql]
    assert len(pages) == 4
    for sql in pages:
        plan = conn.execute(f"EXPLAIN QUERY PLAN {sql}").fetchall()
        assert not any("TEMP B-TREE" in step[3] for step in plan), plan
