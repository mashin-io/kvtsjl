"""Dialect-free SQL client ABC for ``SqlDbKvStore``."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

from kvtsjl.backends.sql.schema import SqlDbRow


class SqlDbClientAdapter(ABC):
    """Caller-owned DB access. The store never passes SQL text."""

    @property
    @abstractmethod
    def db_name(self) -> str:
        """Short label for repr / debugging."""

    @abstractmethod
    def fetch_by_keys(
        self,
        *,
        table: str,
        columns: Sequence[str],
        key_fields: Sequence[str],
        keys: Sequence[SqlDbRow],
    ) -> Sequence[SqlDbRow]:
        """Return rows matching any of the full key rows (equality on all key fields)."""

    @abstractmethod
    def upsert_rows(
        self,
        *,
        table: str,
        key_fields: Sequence[str],
        write_columns: Sequence[str],
        rows: Sequence[SqlDbRow],
    ) -> None:
        """Insert-or-update rows (conflict target = ``key_fields``)."""

    @abstractmethod
    def delete_by_keys(
        self,
        *,
        table: str,
        key_fields: Sequence[str],
        keys: Sequence[SqlDbRow],
    ) -> int:
        """Delete rows by full key equality; return deleted count."""

    @abstractmethod
    def scan_by_key_parts(
        self,
        *,
        table: str,
        columns: Sequence[str],
        key_fields: Sequence[str],
        exact: Mapping[str, Any],
        prefixes: Mapping[str, str],
    ) -> Iterator[SqlDbRow]:
        """Yield rows with equality on ``exact`` and ``LIKE prefix%`` on ``prefixes``.

        Key fields omitted from both maps are unconstrained. Prefix semantics
        match other kvtsjl stores (per-part string prefix).
        """
