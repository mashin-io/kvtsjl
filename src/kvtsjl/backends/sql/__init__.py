"""SQL DB-backed KvStore (row-oriented).

Optional import (stdlib SQLite adapter included)::

    from kvtsjl.backends.sql import (
        SqlDbKvStore,
        SqlDbKvSet,
        SqlDbField,
        SqlDbRow,
        SqlDbClientAdapter,
        SqliteSqlDbClientAdapter,
    )

``SqlDbKvStore`` never builds SQL strings. Subclass ``SqlDbClientAdapter`` for
other dialects; ``SqliteSqlDbClientAdapter`` is the shipped SQLite backend.

Physical PK = ``scope_fields`` (from ``scoped(...)``) + ``leaf_key_fields``
(from leaf ``K``, including composite tuples). Scan uses per-part prefixes.
"""

from __future__ import annotations

from kvtsjl.backends.sql.adapter import SqlDbClientAdapter
from kvtsjl.backends.sql.schema import SqlDbField, SqlDbKvSet, SqlDbRow, sql_table_name
from kvtsjl.backends.sql.sqlite import SqliteSqlDbClientAdapter
from kvtsjl.backends.sql.store import SqlDbKvStore

__all__ = [
    "SqlDbClientAdapter",
    "SqlDbField",
    "SqlDbKvSet",
    "SqlDbKvStore",
    "SqlDbRow",
    "SqliteSqlDbClientAdapter",
    "sql_table_name",
]
