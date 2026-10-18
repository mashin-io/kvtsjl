"""Dialect-free SQL client ABC for ``SqlDbKvStore``."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from kvtsjl.backends.sql.schema import SqlDbRow
from kvtsjl.store.schema.scan import OrderBy


@dataclass(frozen=True, slots=True)
class SqlLiveness:
    """Expiry predicate for ``scan_ordered`` to apply in SQL.

    A row is live when ``expires_at`` is in the future, or ``expires_at`` is
    NULL and (no collection TTL, or ``updated_at + ttl_seconds`` is in the
    future). Mirrors ``SqlDbKvStore._row_expired``.

    This must be pushed whenever ``LIMIT`` is pushed: filtering expired rows in
    Python after a pushed ``LIMIT n`` returns fewer than ``n`` rows — and
    returns none at all when the newest ``n`` rows have expired.

    **Contract: the TTL columns must hold numeric epoch seconds.**
    ``SqlDbKvStore`` writes floats and reads them back through ``_as_timestamp``,
    which also accepts ISO-8601 strings — but SQL cannot compare those the same
    way. SQLite orders TEXT above every number, so ``expires_at > now`` keeps an
    expired ISO-string row; the Python check then drops it, reintroducing the
    short-result bug this class exists to prevent. Declare the columns ``REAL``
    (or the dialect equivalent) and store epoch seconds.
    """

    now: float
    ttl_seconds: int | None
    updated_at_field: str
    expires_at_field: str


class SqlDbClientAdapter(ABC):
    """Caller-owned DB access. The store never passes SQL text.

    Writes run on the caller's connection and transaction. kvtsjl does not
    begin, commit, or roll back — the client owns that. The reference SQLite
    adapter defaults to ``autocommit=True`` (commit after each write) for
    backward compatibility; pass ``autocommit=False`` when the client opens
    the transaction.
    """

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
    def exists_by_keys(
        self,
        *,
        table: str,
        key_fields: Sequence[str],
        keys: Sequence[SqlDbRow],
        columns: Sequence[str] | None = None,
    ) -> Sequence[SqlDbRow]:
        """Return rows that exist (no value payload).

        Selects ``columns`` when provided, otherwise ``key_fields``. Callers that
        must honour TTL should pass key + clock columns (e.g. updated/expires).
        """

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

    def supports_scan_ordering(self) -> bool:
        """Whether this adapter implements ``scan_ordered``.

        Default ``False`` so existing adapters keep working untouched: the store
        checks this before routing, and reports ``order_by`` as unsupported
        otherwise. Adding ``order_by`` / ``limit`` as parameters of
        ``scan_by_key_parts`` instead would narrow every existing override's
        signature — a strict-mode type error for anyone wrapping this ABC.
        """
        return False

    def supports_paged_scan(self) -> bool:
        """Whether this adapter implements ``scan_page_by_key_parts``.

        Default ``False``: the store streams ``scan_by_key_parts`` (one query,
        connection held for the iterator). Opt in for keyset pages so each
        page is a bounded fetch — no connection held across pages, and
        ``ScanCursor`` resumes with ``after_key`` instead of a client-side seek.
        """
        return False

    def scan_page_by_key_parts(
        self,
        *,
        table: str,
        columns: Sequence[str],
        key_fields: Sequence[str],
        exact: Mapping[str, Any],
        prefixes: Mapping[str, str],
        after_key: SqlDbRow | None,
        page_size: int,
    ) -> Sequence[SqlDbRow]:
        """One keyset page: rows with ``key_fields > after_key``, ordered, limited.

        Same equality/prefix filters as ``scan_by_key_parts``. When
        ``after_key`` is set it must contain every ``key_fields`` column; the
        page starts strictly after that full key (SQL row-value compare).
        Returns a materialized sequence so the caller can close the underlying
        cursor/connection before the next page.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not implement scan_page_by_key_parts"
        )

    def scan_ordered(
        self,
        *,
        table: str,
        columns: Sequence[str],
        key_fields: Sequence[str],
        exact: Mapping[str, Any],
        prefixes: Mapping[str, str],
        order_by: Sequence[OrderBy],
        limit: int | None,
        live: SqlLiveness | None = None,
    ) -> Iterator[SqlDbRow]:
        """Like ``scan_by_key_parts`` but with ``ORDER BY``, ``LIMIT`` and liveness.

        ``order_by`` is already total (the store appends key fields ascending),
        so ties are deterministic. ``limit`` is only passed when nothing remains
        to filter client-side, so pushing it cannot drop rows the caller asked
        for. Implement together with ``supports_scan_ordering``.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not implement scan_ordered"
        )

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

        Rows must be yielded in ascending ``key_fields`` order. ``SqlDbKvStore``
        reports ``ScanOrder.KEY_ASC`` on the strength of this; an adapter that
        cannot guarantee it must say so rather than relying on a query plan.
        """

    def supports_expiry_scan(self) -> bool:
        """Whether this adapter implements ``scan_expired`` / ``has_null_expires_at``.

        Default ``False``: the store falls back to a full ``scan_by_key_parts``
        and Python ``_row_expired``.
        """
        return False

    def scan_expired(
        self,
        *,
        table: str,
        columns: Sequence[str],
        key_fields: Sequence[str],
        exact: Mapping[str, Any],
        expires_at_field: str,
        updated_at_field: str,
        relative_ttl_seconds: int | None,
        now: float,
        limit: int,
    ) -> Sequence[SqlDbRow]:
        """Return up to ``limit`` expired rows, oldest death time first.

        Predicate must match ``SqlDbKvStore._row_expired``:

        - ``expires_at IS NOT NULL AND expires_at <= now``, or
        - when ``relative_ttl_seconds`` is set: ``expires_at IS NULL AND
          updated_at + relative_ttl_seconds <= now``.

        Pass ``relative_ttl_seconds=None`` for early-bound tables (and for
        kvsets with no collection TTL): only the concrete ``expires_at``
        branch runs.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not implement scan_expired"
        )

    def has_null_expires_at(
        self,
        *,
        table: str,
        exact: Mapping[str, Any],
        expires_at_field: str,
    ) -> bool:
        """True if any row under ``exact`` has NULL ``expires_at``.

        Used once when ``expiry_binding`` is early and the kvset has a relative
        TTL, to refuse a premature single-branch scan that would skip those rows.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not implement has_null_expires_at"
        )
