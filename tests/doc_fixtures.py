"""Documents with fields, on every medium and in a table whose fields are columns.

Shared by the tests of field filters and ordered scans: the same values, read
the same way, wherever they are kept.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
import json
import sqlite3

from kvtsjl import KvSet, SerDe
from kvtsjl.backends.sql import SqlDbField, SqlDbKvSet, SqlDbRow, SqliteSqlDbClientAdapter
from kvtsjl.backends.sql.adapter import SqlSelect
from kvtsjl.backends.sql.store import SqlDbKvStore
from kvtsjl.store.schema.scan import Field


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

def encode_doc(doc: Doc) -> str:
    return json.dumps(doc.__dict__)


def decode_doc(raw: str) -> Doc:
    return Doc(**json.loads(raw))


def doc_row_serde() -> SerDe[Doc, SqlDbRow]:
    return SerDe(
        serializer=lambda d: {"name": d.name, "kind": d.kind, "mime": d.mime, "agent": d.agent},
        deserializer=lambda r: Doc(r["name"], r["kind"], r["mime"], r["agent"]),
        blob_type=dict,
    )


class RecordingAdapter(SqliteSqlDbClientAdapter):
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


def columns_store(*, selects: bool = True) -> tuple[SqlDbKvStore[str, Doc], RecordingAdapter]:
    """``DOCS`` in a SQL table whose fields are its columns."""
    conn = sqlite3.connect(":memory:")
    conn.execute(
        'CREATE TABLE "docs_1" (id TEXT PRIMARY KEY, name TEXT, kind TEXT, mime TEXT, '
        "agent TEXT, updated_at REAL NOT NULL, expires_at REAL)"
    )
    kvset: KvSet[str, Doc, str, SqlDbRow] = KvSet.with_str_keys(
        "docs", version=1, key_serde=SerDe.identity(str), value_serde=doc_row_serde()
    )
    sql_kvset = SqlDbKvSet[str, Doc].create(
        kvset,
        key_field="id",
        fields=[SqlDbField("name"), SqlDbField("kind"), SqlDbField("mime"), SqlDbField("agent")],
    )
    adapter = RecordingAdapter(conn, selects=selects)
    store = SqlDbKvStore(sql_kvset, adapter=adapter)
    store.batch_set(DOCS)
    adapter.returned = 0
    adapter.queries.clear()
    return store, adapter
