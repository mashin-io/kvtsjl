"""KV-store framework errors."""

from __future__ import annotations

from collections.abc import Mapping


class KvStoreError(Exception):
    """Base error for the kvstore framework."""


class KvStoreSerDeError(KvStoreError):
    """Key or value serialization / deserialization failed."""


class KvStoreReadOnlyError(KvStoreError):
    """Mutation attempted on a read-only store view."""


class KvStoreUnsupported(KvStoreError):
    """The store cannot do this as asked, and will not emulate it.

    A request its medium has no primitive for — a conditional write with no
    atomic one, a scan the key layout cannot answer, a per-write TTL it cannot
    keep. Unlike a failure, retrying cannot help: the composition asked its
    media for something they do not do. So a best-effort sync, which tolerates
    a medium *failing*, never hides one of these; it is raised where the
    composition is put together, not logged where it runs.
    """


class KvStoreScanUnsupported(KvStoreUnsupported):
    """Prefix/layout cannot honor the requested scan (e.g. Hashed + prefix)."""


class KvStoreScopeError(KvStoreError):
    """Scope segments do not match the KvSet scope_schema or composition rules."""


class KvStoreComposeError(KvStoreError):
    """Composition requires matching KvSet / Scope identity."""


class KvStoreIndexError(KvStoreError):
    """Unknown or incompatible index for search."""


class KvStoreTtlUnsupported(KvStoreUnsupported):
    """Backend cannot honor a per-write TTL override."""


class RecordWriteError(KvStoreError):
    """A record spread over several targets was not written everywhere.

    ``committed`` is the one thing a caller needs to know: whether the target
    that decides the record exists — the commit point — was written.

    - ``True`` (:class:`PartialSyncError`): the record exists. Something
      written *after* the commit point failed, so a copy or index may be stale.
    - ``False`` (:class:`UncommittedWriteError`): the record does not exist.
      Something written *before* the commit point may now be orphaned, for GC.

    The flag holds at every level of a composed tree: each composition
    reclassifies a failure by where it happened relative to its own commit
    point, so the caller never needs to know the tree's shape.

    ``failures`` maps target labels to the exceptions they raised.
    """

    committed: bool

    def __init__(
        self,
        message: str = "record write failed",
        *,
        failures: Mapping[str, Exception],
    ) -> None:
        super().__init__(message)
        self.failures: dict[str, Exception] = dict(failures)


class UncommittedWriteError(RecordWriteError):
    """The commit point was not written, so the record does not exist.

    Targets written before it may hold orphaned data, which is unreferenced
    and left to the GC policy.
    """

    committed = False


class PartialSyncError(RecordWriteError):
    """One or more fan-out targets failed after the primary write landed.

    ``failures`` maps target labels to the exceptions they raised — that map is
    the complete picture. ``__cause__`` (when set via ``raise ... from``) holds
    only the *first* failure for traceback purposes; do not treat it as the
    full set.

    The primary / earlier write is **not** rolled back. This is the committed
    side of :class:`RecordWriteError`: the record exists, and what failed was
    written after its commit point.
    """

    committed = True

    def __init__(
        self,
        message: str = "fan-out sync failed",
        *,
        failures: Mapping[str, Exception],
    ) -> None:
        super().__init__(message, failures=failures)
