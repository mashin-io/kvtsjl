"""R20: clearing one part of a record must clear only that part.

A zip writes a part whose slot is ``None`` by clearing it. When the part is a
column-group view over a table it shares with its siblings, the row is not
the view's to remove: clearing must touch the view's own columns and nothing
else.
"""

from __future__ import annotations

from dataclasses import dataclass
import sqlite3
from typing import Any

import pytest

from kvtsjl import SerDe
from kvtsjl.backends.sql.schema import SqlDbField, SqlDbKvSet, SqlDbRow
from kvtsjl.backends.sql.sqlite import SqliteSqlDbClientAdapter
from kvtsjl.backends.sql.store import SqlDbKvStore
from kvtsjl.exceptions import KvStoreUnsupported
from kvtsjl.keymap import KeyMap
from kvtsjl.store.schema.kvset import KvSet


@dataclass(frozen=True)
class Turn:
    core: str | None
    digest: str | None


def _table(conn: sqlite3.Connection) -> None:
    conn.row_factory = sqlite3.Row
    conn.execute(
        'CREATE TABLE "turn_1" (id TEXT PRIMARY KEY, core TEXT, digest TEXT, '
        "impact TEXT, updated_at REAL NOT NULL, expires_at REAL)"
    )


def _view(conn: sqlite3.Connection, column: str) -> Any:
    """A view owning one value column of the shared ``turn`` table.

    The ``core`` view owns the rows: only it creates and deletes them.
    """
    kvset = KvSet.with_str_keys(
        "turn",
        key_serde=SerDe.identity(str),
        value_serde=SerDe[str, SqlDbRow](
            serializer=lambda v: {column: v},
            deserializer=lambda row: row[column],
            blob_type=dict,
        ),
    )
    sql_kvset = SqlDbKvSet.create(
        kvset,
        fields=(SqlDbField(column),),
        key_field="id",
        shares_table=True,
        owns_rows=column == "core",
    )
    return SqlDbKvStore(sql_kvset, adapter=SqliteSqlDbClientAdapter(conn))


def _rows(conn: sqlite3.Connection) -> list[tuple[Any, ...]]:
    return [tuple(r) for r in conn.execute('SELECT id, core, digest FROM "turn_1"')]


def test_writing_a_record_with_one_part_empty_keeps_the_row() -> None:
    """The hazard Nexus reported: "no digest" must not delete the turn."""
    conn = sqlite3.connect(":memory:")
    _table(conn)
    turns = KeyMap.zip_with(
        Turn,
        commit_part="core",
        after_commit=("digest",),
        core=_view(conn, "core"),
        digest=_view(conn, "digest"),
    )
    turns.set("t", Turn(core="C", digest="D"))
    assert _rows(conn) == [("t", "C", "D")]

    turns.set("t", Turn(core="C2", digest=None))

    assert _rows(conn) == [("t", "C2", None)], "clearing one view removed the row"
    assert turns.get("t") == Turn(core="C2", digest=None)


def test_clearing_a_part_keeps_columns_no_part_of_the_record_owns() -> None:
    """The row was deleted and re-created: a sibling outside the zip was lost."""
    conn = sqlite3.connect(":memory:")
    _table(conn)
    impact = _view(conn, "impact")
    turns = KeyMap.zip_with(
        Turn,
        commit_part="core",
        after_commit=("digest",),
        core=_view(conn, "core"),
        digest=_view(conn, "digest"),
    )
    turns.set("t", Turn(core="C", digest="D"))
    impact.set("t", "I")

    turns.set("t", Turn(core="C", digest=None))

    assert impact.get("t") == "I", "clearing the digest wiped a sibling view"


def test_a_record_cannot_be_created_through_a_view_that_only_updates() -> None:
    """The hazard's second form is refused before any write.

    Without a commit part the parts go in field order, so a view that only
    updates rows would be written beside the one creating them — and on a
    new record find no row. A view of a row it does not own belongs after
    the commit point.
    """
    conn = sqlite3.connect(":memory:")
    _table(conn)
    with pytest.raises(ValueError, match="after the commit point"):
        KeyMap.zip_with(Turn, core=_view(conn, "core"), digest=_view(conn, "digest"))
    with pytest.raises(ValueError, match="after_commit"):
        KeyMap.zip_with(
            Turn, commit_part="core", core=_view(conn, "core"), digest=_view(conn, "digest")
        )


# --- the rule, one fact at a time -------------------------------------------


@dataclass(frozen=True)
class Digest:
    text: str | None
    score: str | None


def _wide_table(conn: sqlite3.Connection) -> None:
    conn.row_factory = sqlite3.Row
    conn.execute(
        'CREATE TABLE "doc_1" (id TEXT PRIMARY KEY, core TEXT, text TEXT, score TEXT, '
        "impact TEXT, updated_at REAL NOT NULL, expires_at REAL)"
    )


def _digest_kvset(*, reads_core: bool = False) -> Any:
    """Owns ``text`` and ``score``; may read ``core`` without owning it."""
    kvset = KvSet.with_str_keys(
        "doc",
        key_serde=SerDe.identity(str),
        value_serde=SerDe[Digest, SqlDbRow](
            serializer=lambda d: {"text": d.text, "score": d.score},
            deserializer=lambda row: Digest(row["text"], row["score"]),
            blob_type=dict,
        ),
    )
    fields = [SqlDbField("text"), SqlDbField("score")]
    if reads_core:
        fields.append(SqlDbField("core", read_only=True))
    return SqlDbKvSet.create(kvset, fields=fields, key_field="id", shares_table=True)


def _digest_view(conn: sqlite3.Connection, *, reads_core: bool = False) -> Any:
    return SqlDbKvStore(
        _digest_kvset(reads_core=reads_core), adapter=SqliteSqlDbClientAdapter(conn)
    )


def _column(
    conn: sqlite3.Connection, name: str, *, owns_rows: bool = False, **adapter: Any
) -> Any:
    kvset = KvSet.with_str_keys(
        "doc",
        key_serde=SerDe.identity(str),
        value_serde=SerDe[str, SqlDbRow](
            serializer=lambda v: {name: v},
            deserializer=lambda row: row[name],
            blob_type=dict,
        ),
    )
    sql_kvset = SqlDbKvSet.create(
        kvset,
        fields=(SqlDbField(name),),
        key_field="id",
        shares_table=True,
        owns_rows=owns_rows,
    )
    return SqlDbKvStore(sql_kvset, adapter=SqliteSqlDbClientAdapter(conn, **adapter))


def _doc(conn: sqlite3.Connection, key: str) -> dict[str, Any]:
    row = conn.execute('SELECT * FROM "doc_1" WHERE id=?', (key,)).fetchone()
    return {} if row is None else {k: row[k] for k in ("core", "text", "score", "impact")}


def test_unsetting_nulls_exactly_the_view_s_own_columns() -> None:
    """Its read-only column is a sibling's, so it stays."""
    conn = sqlite3.connect(":memory:")
    _wide_table(conn)
    core, impact = _column(conn, "core", owns_rows=True), _column(conn, "impact")
    digest = _digest_view(conn, reads_core=True)
    core.set("d", "C")
    impact.set("d", "I")
    digest.set("d", Digest("T", "S"))

    assert digest.unset("d") is True
    assert _doc(conn, "d") == {"core": "C", "text": None, "score": None, "impact": "I"}
    assert digest.unset("d") is False, "a view already clear reported holding a value"


def test_a_cleared_view_no_longer_holds_the_key() -> None:
    """Otherwise it reads back as a value made of NULLs, not as absent."""
    conn = sqlite3.connect(":memory:")
    _wide_table(conn)
    core, digest = _column(conn, "core", owns_rows=True), _digest_view(conn)
    core.set("d", "C")
    digest.set("d", Digest("T", "S"))
    assert digest.exists("d") is True, "a view holding its columns reported absent"
    assert digest.batch_exists(["d"]) == {"d"}
    assert digest.stat("d") is not None
    digest.unset("d")

    assert digest.get("d") is None
    assert digest.exists("d") is False
    assert digest.batch_get(["d"]) == {}
    assert digest.stat("d") is None
    assert list(digest.list()) == []
    assert core.get("d") == "C", "the sibling lost the row"


def test_an_ordered_scan_with_a_limit_skips_cleared_rows_in_sql() -> None:
    """A pushed LIMIT must not be used up by rows this view no longer holds."""
    from kvtsjl.store.schema.scan import OrderBy

    conn = sqlite3.connect(":memory:")
    _wide_table(conn)
    core, digest = _column(conn, "core", owns_rows=True), _digest_view(conn, reads_core=True)
    for key, order in (("a", "1"), ("b", "2"), ("c", "3")):
        core.set(key, order)
        digest.set(key, Digest(f"t{key}", None))
    digest.unset("c")

    newest = list(digest.scan(order_by=[OrderBy("core", descending=True)], limit=2))
    assert newest == ["b", "a"], "the cleared row took a place in the limit"


def test_a_view_with_no_columns_of_its_own_refuses_to_clear() -> None:
    """Its only way to clear would be deleting a row its siblings share."""
    conn = sqlite3.connect(":memory:")
    _wide_table(conn)
    core = _column(conn, "core", owns_rows=True)
    reader = SqlDbKvStore(
        SqlDbKvSet.create(
            core.kvset,
            fields=(SqlDbField("core", read_only=True),),
            key_field="id",
            shares_table=True,
        ),
        adapter=SqliteSqlDbClientAdapter(conn),
    )
    core.set("d", "C")

    with pytest.raises(KvStoreUnsupported, match="no columns of its own"):
        reader.unset("d")
    assert core.get("d") == "C"


def test_an_adapter_that_cannot_clear_columns_refuses_rather_than_deletes() -> None:
    class NoClear(SqliteSqlDbClientAdapter):
        def supports_clear_columns(self) -> bool:
            return False

    conn = sqlite3.connect(":memory:")
    _wide_table(conn)
    core = _column(conn, "core", owns_rows=True)
    digest = _digest_view(conn)
    core.set("d", "C")
    digest.set("d", Digest("T", "S"))
    narrow = SqlDbKvStore(_digest_kvset(), adapter=NoClear(conn))

    with pytest.raises(KvStoreUnsupported, match="clear_columns"):
        narrow.unset("d")
    assert core.get("d") == "C"


def test_a_view_that_owns_its_table_unsets_by_deleting() -> None:
    conn = sqlite3.connect(":memory:")
    _wide_table(conn)
    kvset = _column(conn, "core", owns_rows=True).kvset
    owner = SqlDbKvStore(
        SqlDbKvSet.create(kvset, fields=(SqlDbField("core"),), key_field="id"),
        adapter=SqliteSqlDbClientAdapter(conn),
    )
    owner.set("d", "C")
    assert owner.unset("d") is True
    assert _doc(conn, "d") == {}


@dataclass(frozen=True)
class Doc:
    core: str | None
    digest: Digest | None


def _docs(conn: sqlite3.Connection, **kw: Any) -> Any:
    return KeyMap.zip_with(
        Doc,
        commit_part="core",
        after_commit=("digest",),
        core=_column(conn, "core", owns_rows=True),
        digest=_digest_view(conn),
        **kw,
    )


def test_a_record_written_with_a_part_absent_reads_back_absent() -> None:
    conn = sqlite3.connect(":memory:")
    _wide_table(conn)
    docs = _docs(conn)
    docs.set("d", Doc("C", Digest("T", "S")))
    docs.set("d", Doc("C", None))
    assert docs.get("d") == Doc("C", None), "the cleared part read back as NULLs"


def test_a_batch_write_clears_parts_the_same_way() -> None:
    conn = sqlite3.connect(":memory:")
    _wide_table(conn)
    impact = _column(conn, "impact")
    docs = _docs(conn)
    docs.batch_set({"d": Doc("C", Digest("T", "S")), "e": Doc("E", Digest("U", "V"))})
    impact.set("d", "I")

    docs.batch_set({"d": Doc("C", None), "e": Doc("E2", None)})

    assert _doc(conn, "d") == {"core": "C", "text": None, "score": None, "impact": "I"}
    assert docs.batch_get(["d", "e"]) == {"d": Doc("C", None), "e": Doc("E2", None)}


def test_deleting_the_record_still_removes_the_row() -> None:
    conn = sqlite3.connect(":memory:")
    _wide_table(conn)
    docs = _docs(conn)
    docs.set("d", Doc("C", Digest("T", "S")))
    assert docs.delete("d") is True
    assert _doc(conn, "d") == {}, "deleting the record left its row"


def test_a_wrapped_view_is_unset_through_its_wrapper() -> None:
    """An index around the view forgets the key; the row stays."""
    conn = sqlite3.connect(":memory:")
    _wide_table(conn)
    forgotten: list[str] = []

    class SpyIndex:
        name = "spy"
        sync_on_write = True
        sync_policy = None

        def bind_store(self, store: Any) -> None: ...
        def sync(self, key: str, value: Any) -> None: ...
        def batch_sync(self, items: Any) -> None: ...
        def delete(self, key: str) -> None:
            forgotten.append(key)

        def batch_delete(self, keys: Any) -> None:
            forgotten.extend(keys)

        def clone_with_scope(self, scope: Any) -> Any:
            return self

    impact = _column(conn, "impact")
    digest = _digest_view(conn).indexed(SpyIndex())
    docs = KeyMap.zip_with(Doc, commit_part="core", after_commit=("digest",), core=_column(conn, "core", owns_rows=True), digest=digest)
    docs.set("d", Doc("C", Digest("T", "S")))
    impact.set("d", "I")

    docs.set("d", Doc("C", None))

    assert forgotten == ["d"], "the index kept an entry for a cleared part"
    assert _doc(conn, "d")["impact"] == "I"

    mapped = _digest_view(conn).imap(lambda d: d, lambda d: d)
    mapped.set("d", Digest("T", "S"))
    assert mapped.unset("d") is True
    assert _doc(conn, "d")["impact"] == "I", "the value view deleted the row"
    keyed = _digest_view(conn).imap_keys(lambda k: f"k:{k}", lambda sk: sk[2:])
    core = _column(conn, "core", owns_rows=True)
    core.set("k:e", "C")
    keyed.set("e", Digest("T", "S"))
    assert keyed.unset("e") is True
    assert core.get("k:e") == "C", "the key view deleted the row"
    assert keyed.batch_unset(["e"]) == 0


def test_a_scan_without_keyset_paging_skips_cleared_rows_too() -> None:
    class Streaming(SqliteSqlDbClientAdapter):
        def supports_paged_scan(self) -> bool:
            return False

    conn = sqlite3.connect(":memory:")
    _wide_table(conn)
    core = _column(conn, "core", owns_rows=True)
    digest = SqlDbKvStore(_digest_kvset(), adapter=Streaming(conn))
    for key in ("a", "b"):
        core.set(key, "C")
        digest.set(key, Digest("T", None))
    digest.unset("b")

    assert list(digest.list()) == ["a"]


def test_a_batch_write_unsets_a_wrapped_part_through_its_wrapper() -> None:
    conn = sqlite3.connect(":memory:")
    _wide_table(conn)
    impact = _column(conn, "impact")
    mirrored = _digest_view(conn).mirror(_digest_view(conn))
    docs = KeyMap.zip_with(Doc, commit_part="core", after_commit=("digest",), core=_column(conn, "core", owns_rows=True), digest=mirrored)
    docs.batch_set({"d": Doc("C", Digest("T", "S"))})
    impact.set("d", "I")

    docs.batch_set({"d": Doc("C", None)})

    assert _doc(conn, "d") == {"core": "C", "text": None, "score": None, "impact": "I"}


# --- one rule for every view of a shared entry ------------------------------


def test_deleting_through_a_view_that_does_not_own_rows_keeps_the_record() -> None:
    """Only the row owner deletes; a sibling's delete removes what it wrote."""
    conn = sqlite3.connect(":memory:")
    _wide_table(conn)
    core, digest = _column(conn, "core", owns_rows=True), _digest_view(conn)
    core.set("d", "C")
    digest.set("d", Digest("T", "S"))

    assert digest.delete("d") is True
    assert _doc(conn, "d") == {"core": "C", "text": None, "score": None, "impact": None}
    assert digest.batch_delete(["d"]) == 0, "a view already clear reported holding a value"

    assert core.delete("d") is True
    assert _doc(conn, "d") == {}, "the row owner left its row"


def test_owns_rows_describes_a_view_of_a_shared_table() -> None:
    kvset = _digest_kvset().kvset
    with pytest.raises(ValueError, match="shared table"):
        SqlDbKvSet.create(kvset, fields=(SqlDbField("text"),), key_field="id", owns_rows=True)


def test_an_index_view_refuses_the_same_way_a_table_view_does() -> None:
    """Both families answer to KvStoreUnsupported; the index one is still an index error."""
    from kvtsjl.exceptions import KvStoreIndexError, KvStoreIndexUnsupported

    assert issubclass(KvStoreIndexUnsupported, KvStoreUnsupported)
    assert issubclass(KvStoreIndexUnsupported, KvStoreIndexError)


# --- only the owner creates -------------------------------------------------


def test_a_composition_of_a_view_that_only_updates_is_such_a_view_too() -> None:
    """Wrapping a sibling view keeps it usable after the commit point, and only there."""
    conn = sqlite3.connect(":memory:")
    _wide_table(conn)
    indexed_digest = _digest_view(conn).mirror(_digest_view(conn))
    assert indexed_digest.creates_entries is False
    assert _digest_view(conn).imap(lambda d: d, lambda d: d).creates_entries is False
    assert _digest_view(conn).group_by("id").creates_entries is False

    docs = KeyMap.zip_with(
        Doc,
        commit_part="core",
        after_commit=("digest",),
        core=_column(conn, "core", owns_rows=True),
        digest=indexed_digest,
    )
    docs.set("d", Doc("C", Digest("T", "S")))
    assert docs.get("d") == Doc("C", Digest("T", "S"))
    assert docs.creates_entries is True

    with pytest.raises(ValueError, match="after_commit"):
        KeyMap.zip_with(
            Doc, commit_part="core", core=_column(conn, "core", owns_rows=True), digest=indexed_digest
        )
