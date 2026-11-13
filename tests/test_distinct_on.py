"""``distinct_on`` keeps each group's first row on every store; SQL ranks in the database.

The newest version of each file, then those that match: ranking sees every
live row, and ``where``, ``refine``, ``after`` and ``limit`` apply to the
rows it keeps — so an older version that matches a filter its newest does
not is never returned. A test of the group's own fields means the same on
either side of the ranking. Each case runs on every medium, ranked in
process, and on a SQL table with real columns, ranked by a window function.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import timedelta
import json
import sqlite3
from typing import Any

from freezegun import freeze_time
import pytest

from kvtsjl import KvSet, SerDe, TtlPolicy
from kvtsjl.backends.sql import SqlDbField, SqlDbKvSet, SqlDbRow, SqliteSqlDbClientAdapter
from kvtsjl.backends.sql.adapter import SqlSelect
from kvtsjl.backends.sql.store import SqlDbKvStore
from kvtsjl.exceptions import KvStoreScanUnsupported
from kvtsjl.store.schema.scan import (
    After,
    AlwaysTrue,
    Field,
    KeyIn,
    KeyPrefix,
    Not,
    OrderBy,
    Predicate,
    Projection,
    Pushdown,
    ScanRow,
    ScanSpec,
)
from tests.media import TTL, make_medium


@dataclass(frozen=True)
class Version:
    file: str | None
    at: int
    mime: str


FILE = Field[Version, str | None]("file", lambda v: v.file)
AT = Field[Version, int]("at", lambda v: v.at)
MIME = Field[Version, str]("mime", lambda v: v.mime)

NEWEST = [OrderBy(AT, descending=True)]

VERSIONS = {
    "r1": Version("report", 1, "text/csv"),
    "r2": Version("report", 3, "image/png"),  # the report's newest
    "r3": Version("report", 2, "text/csv"),  # older, and a csv
    "n2": Version("notes", 5, "text/plain"),  # written, and listed, before n1
    "n1": Version("notes", 5, "text/md"),  # ties n2; the smaller key is first
    "u1": Version(None, 4, "text/csv"),  # unset files group together
    "u2": Version(None, 2, "image/png"),
    "x9": Version("report", 0, "text/csv"),  # the report's oldest, under another prefix
}

# (case, where, expected keys), each with distinct_on=[FILE] and NEWEST.
CASES: list[tuple[str, Predicate[str], str]] = [
    ("each file's newest", AlwaysTrue(), "n1 u1 r2"),
    ("a filter tests the newest, not an older match", MIME.startswith("text/"), "n1 u1"),
    ("a test of the group alone", FILE.eq("report"), "r2"),
    ("a group test that holds for no group", FILE.eq("nothing"), ""),
    ("unset is a group of its own", FILE.is_null(), "u1"),
    ("a filter of group and other fields", MIME.eq("text/csv") | FILE.eq("notes"), "n1 u1"),
    ("a group test and another", FILE.is_in(["report", "notes"]) & ~MIME.eq("text/md"), "r2"),
    ("a key test tests the rows kept", KeyIn[str](frozenset({"r1", "r3", "n2", "u1"})), "u1"),
    ("a key prefix tests the rows kept", KeyPrefix[str]("x"), ""),
]
IDS = [c[0] for c in CASES]


def _encode(v: Version) -> str:
    return json.dumps(v.__dict__)


def _decode(raw: str) -> Version:
    return Version(**json.loads(raw))


class _Recording(SqliteSqlDbClientAdapter):
    def __init__(self, conn: sqlite3.Connection, *, distinct: bool = True) -> None:
        super().__init__(conn)
        self._distinct = distinct
        self.queries: list[SqlSelect] = []
        self.returned = 0

    def supports_distinct_on(self) -> bool:
        return self._distinct

    def select(self, query: SqlSelect) -> Iterator[SqlDbRow]:
        self.queries.append(query)
        for row in super().select(query):
            self.returned += 1
            yield row


def _columns_store(
    *, distinct: bool = True, ttl: TtlPolicy | None = None
) -> tuple[SqlDbKvStore[str, Version], _Recording]:
    """A table whose fields are its columns, empty."""
    conn = sqlite3.connect(":memory:")
    conn.execute(
        'CREATE TABLE "versions_1" (id TEXT PRIMARY KEY, file TEXT, at INTEGER, mime TEXT, '
        "updated_at REAL NOT NULL, expires_at REAL)"
    )
    kvset: KvSet[str, Version, str, SqlDbRow] = KvSet.with_str_keys(
        "versions",
        version=1,
        key_serde=SerDe.identity(str),
        value_serde=SerDe(
            serializer=lambda v: {"file": v.file, "at": v.at, "mime": v.mime},
            deserializer=lambda r: Version(r["file"], r["at"], r["mime"]),
            blob_type=dict,
        ),
        ttl_policy=ttl,
    )
    sql_kvset = SqlDbKvSet[str, Version].create(
        kvset, key_field="id", fields=[SqlDbField("file"), SqlDbField("at"), SqlDbField("mime")]
    )
    adapter = _Recording(conn, distinct=distinct)
    return SqlDbKvStore(sql_kvset, adapter=adapter), adapter


def _filled(store: Any, adapter: _Recording | None = None) -> Any:
    store.batch_set(VERSIONS)
    if adapter is not None:
        adapter.returned = 0
        adapter.queries.clear()
    return store


@pytest.fixture(params=["memory", "sql", "redis", "s3", "filesystem", "sql_columns"])
def versions(request: pytest.FixtureRequest) -> Any:
    if request.param == "sql_columns":
        return _filled(_columns_store()[0])
    return _filled(make_medium(request.param, request).imap(_decode, _encode))


def _keys(rows: Any) -> str:
    return " ".join(rows)


@pytest.mark.parametrize(("case", "where", "expected"), CASES, ids=IDS)
def test_distinct_on_means_the_same_on_every_store(
    versions: Any, case: str, where: Predicate[str], expected: str
) -> None:
    rows = versions.scan(where=where, order_by=NEWEST, distinct_on=[FILE])
    assert _keys(rows) == expected, case


def test_limit_after_and_refine_apply_to_the_rows_kept(versions: Any) -> None:
    assert _keys(versions.scan(order_by=NEWEST, distinct_on=[FILE], limit=2)) == "n1 u1"
    after = After[str]((4,), "u1")
    assert _keys(versions.scan(order_by=NEWEST, distinct_on=[FILE], after=after)) == "r2"
    csv = ScanSpec[str](
        order_by=NEWEST,
        distinct_on=[FILE],
        refine=lambda row: row.value is not None and row.value.mime == "text/csv",
        project=Projection.ENTRIES,
    )
    assert [r.key for r in versions.scan_rows(csv)] == ["u1"], "an older csv was returned"


def test_each_group_of_several_fields(versions: Any) -> None:
    rows = versions.scan(order_by=NEWEST, distinct_on=[FILE, MIME])
    assert _keys(rows) == "n1 n2 u1 r2 r3 u2"


# The filesystem's clock is the OS's, which a frozen clock does not move.
@pytest.mark.parametrize("name", ["memory", "sql", "sql_columns"])
def test_a_group_s_first_row_is_its_first_live_one(
    name: str, request: pytest.FixtureRequest
) -> None:
    """Rank first and filter after, and the report would vanish with its newest version."""
    with freeze_time("2024-01-01 12:00:00") as frozen:
        if name == "sql_columns":
            store: Any = _columns_store(ttl=TTL)[0]
        else:
            store = make_medium(name, request, TTL).imap(_decode, _encode)
        store.set("r2", VERSIONS["r2"])
        frozen.tick(timedelta(hours=2))
        store.batch_set({k: v for k, v in VERSIONS.items() if k != "r2"})
        assert _keys(store.scan(order_by=NEWEST, distinct_on=[FILE])) == "n1 u1 r3"


@pytest.mark.parametrize(("case", "where", "expected"), CASES, ids=IDS)
def test_sql_ranks_in_the_database(case: str, where: Predicate[str], expected: str) -> None:
    store, adapter = _columns_store()
    _filled(store, adapter)
    spec = ScanSpec[str](where=where, order_by=NEWEST, distinct_on=[FILE])
    assert _keys(r.key for r in store.scan_rows(spec)) == expected, case
    query = adapter.queries[-1]
    assert list(query.distinct_on) == ["file"]
    pushed = store.explain(spec=spec).root.pushed
    assert {"order_by", "distinct_on"} <= pushed
    if "key" not in case:
        assert adapter.returned == len(expected.split()), "SQL returned rows Python dropped"


def test_sql_runs_a_test_of_the_group_before_ranking_and_the_rest_after() -> None:
    store, adapter = _columns_store()
    _filled(store, adapter)
    where = FILE.is_in(["report", "notes"]) & ~MIME.eq("text/md")
    list(store.scan(where=where, order_by=NEWEST, distinct_on=[FILE]))
    query = adapter.queries[-1]
    assert query.where == FILE.is_in(["report", "notes"])
    assert isinstance(query.kept_where, Not)
    assert query.kept_where.inner == MIME.eq("text/md")


def test_sql_pushes_the_limit_and_after_to_the_rows_kept() -> None:
    store, adapter = _columns_store()
    _filled(store, adapter)
    spec = ScanSpec[str](
        where=MIME.startswith("text/"),
        order_by=NEWEST,
        distinct_on=[FILE],
        after=After((5,), "n1"),
        limit=1,
    )
    assert [r.key for r in store.scan_rows(spec)] == ["u1"]
    assert adapter.returned == 1
    assert {"limit", "after", "FieldTest"} <= store.explain(spec=spec).root.pushed


def test_a_key_test_keeps_the_limit_in_python() -> None:
    store, adapter = _columns_store()
    _filled(store, adapter)
    spec = ScanSpec[str](
        where=KeyIn[str](frozenset({"u1", "r2"})), order_by=NEWEST, distinct_on=[FILE], limit=1
    )
    assert [r.key for r in store.scan_rows(spec)] == ["u1"]
    assert "limit" not in store.explain(spec=spec).root.pushed


def test_an_adapter_without_distinct_on_ranks_in_process() -> None:
    store, adapter = _columns_store(distinct=False)
    _filled(store, adapter)
    assert _keys(store.scan(order_by=NEWEST, distinct_on=[FILE])) == "n1 u1 r2"
    assert all(not q.distinct_on for q in adapter.queries)
    plan = store.explain(where=MIME.eq("x"), order_by=NEWEST, distinct_on=[FILE])
    assert plan.root.name == "Sort"
    assert plan.root.residual == "order_by(at desc) distinct_on(file) then FieldEq"
    with pytest.raises(KvStoreScanUnsupported, match="distinct_on"):
        list(store.scan(order_by=NEWEST, distinct_on=[FILE], on_unsupported=Pushdown.STRICT))


def test_strict_ranks_only_where_the_medium_does(request: pytest.FixtureRequest) -> None:
    pushed = _filled(_columns_store()[0])
    rows = pushed.scan(order_by=NEWEST, distinct_on=[FILE], on_unsupported=Pushdown.STRICT)
    assert _keys(rows) == "n1 u1 r2"
    memory = _filled(make_medium("memory", request).imap(_decode, _encode))
    with pytest.raises(KvStoreScanUnsupported, match="sort in process"):
        list(memory.scan(order_by=NEWEST, distinct_on=[FILE], on_unsupported=Pushdown.STRICT))


def test_a_value_view_hands_the_ranking_to_its_store() -> None:
    store: SqlDbKvStore[str, Version] = _filled(_columns_store()[0])
    view = store.map(lambda v: v)
    rows = view.scan(
        where=MIME.startswith("text/"),
        order_by=NEWEST,
        distinct_on=[FILE],
        on_unsupported=Pushdown.STRICT,
    )
    assert _keys(rows) == "n1 u1"


def test_distinct_on_needs_an_order() -> None:
    with pytest.raises(ValueError, match="order_by"):
        ScanSpec[str](distinct_on=[FILE])
    assert ScanSpec[str](order_by=NEWEST, distinct_on=[FILE]).distinct_on == (FILE,)


def test_a_group_s_first_row_reads_only_rows_with_their_value() -> None:
    from kvtsjl.store.schema.scan import first_of_each_group

    rows = [ScanRow[str, Any](key="gone"), ScanRow[str, Any](key="r1", value=VERSIONS["r1"])]
    spec = ScanSpec[str](order_by=NEWEST, distinct_on=[FILE])
    assert [r.key for r in first_of_each_group(rows, spec)] == ["r1"]


def test_the_reference_adapter_ranks_in_the_database() -> None:
    conn = sqlite3.connect(":memory:")
    assert SqliteSqlDbClientAdapter(conn).supports_distinct_on()
