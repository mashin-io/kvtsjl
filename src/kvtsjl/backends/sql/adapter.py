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
    held_columns: Sequence[str] = ()
    """A row is live only if one of these is not NULL; empty means no condition.

    A view over a shared table holds a row only while one of its own columns
    is set. Like expiry, this must be pushed with ``LIMIT``: a row a sibling
    still holds but this view has cleared would otherwise use up the limit.
    """


class SqlDbClientAdapter(ABC):
    """Caller-owned DB access. The store never passes SQL text.

    Writes run on the caller's connection and transaction. kvtsjl does not
    begin, commit, or roll back — the client owns that. The reference SQLite
    adapter defaults to ``autocommit=True`` (commit after each write) for
    backward compatibility; pass ``autocommit=False`` when the client opens
    the transaction.

    **Wrapping the reference adapter.** Only five methods are abstract, so a
    wrapper that forwards those and nothing else type-checks, passes its tests
    and runs correctly — on the slow path for everything. The optional
    capabilities default to "unsupported", and a wrapper inherits that default
    rather than the adapter it wraps:

    ===========================  =========================================
    Forward this                 ...or lose
    ===========================  =========================================
    ``supports_scan_ordering``   ``order_by`` entirely — it raises
    ``scan_ordered``             (forward ``live`` too, or a pushed ``LIMIT``
                                 counts rows that have expired or that a
                                 shared-table view has cleared)
    ``supports_expiry_scan``     expiry pushdown; GC reads every row
    ``scan_expired``
    ``has_null_expires_at``      ``ExpiryBinding.EARLY``, which raises
    ``supports_clear_columns``   ``unset`` on a shared-table view, which
    ``clear_columns``            raises rather than deleting the row
    ``supports_paged_scan``      keyset paging; scans materialise in full
    ``scan_page_by_key_parts``
    ===========================  =========================================

    Forwarding a ``supports_*`` flag without its method is worse than
    forwarding neither: the store takes the fast path and the method raises.
    Subclassing ``SqliteSqlDbClientAdapter`` instead of delegating to it
    inherits all of these, which is the simpler option when the wrapper only
    needs to intercept a few calls.
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

    def supports_insert_if_absent(self) -> bool:
        """Whether this adapter implements ``insert_rows_if_absent``."""
        return False

    def insert_rows_if_absent(
        self,
        *,
        table: str,
        key_fields: Sequence[str],
        write_columns: Sequence[str],
        rows: Sequence[SqlDbRow],
    ) -> int:
        """Insert rows that do not exist; return how many were written.

        One statement, so a concurrent writer cannot land between a check and
        the insert. An existing row is left exactly as it is, expired or not:
        the database has no view of kvtsjl's TTL.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not implement insert_rows_if_absent"
        )

    def defers_commit(self) -> bool:
        """Whether writes wait for a commit the caller owns.

        ``True`` means a write is not durable when the call returns, so work
        that depends on it — syncing an index, for one — must wait for the
        commit rather than run beside it.
        """
        return False

    def supports_update_first(self) -> bool:
        """Whether this adapter implements ``upsert_rows_update_first``.

        Needed only by a kvset declaring ``shares_table``; the store refuses
        rather than silently falling back, because the fallback is the write
        that cannot succeed.
        """
        return False

    def upsert_rows_update_first(
        self,
        *,
        table: str,
        key_fields: Sequence[str],
        write_columns: Sequence[str],
        rows: Sequence[SqlDbRow],
    ) -> None:
        """Update by key, inserting only where no row matched.

        ``INSERT ... ON CONFLICT DO UPDATE`` validates the insert's
        ``NOT NULL`` columns before resolving the conflict, so on a shared
        table it refuses a view that does not own one — even when the row
        already exists. Updating first avoids that, while still letting the
        insert fail loudly when a non-owning view would have to create a row.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not implement upsert_rows_update_first"
        )

    def supports_clear_columns(self) -> bool:
        """Whether this adapter implements ``clear_columns``.

        Needed only to ``unset`` a view of a shared table; the store refuses
        rather than fall back to deleting the row its siblings still hold.
        """
        return False

    def clear_columns(
        self,
        *,
        table: str,
        key_fields: Sequence[str],
        keys: Sequence[SqlDbRow],
        columns: Sequence[str],
    ) -> int:
        """Set ``columns`` to NULL on the keyed rows; never insert or delete.

        Returns how many rows had one of ``columns`` set — the rows this view
        held — so a row already clear is not counted.
        """
        raise NotImplementedError(f"{type(self).__name__} does not implement clear_columns")

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
