"""KV-store framework errors."""

from __future__ import annotations

from collections.abc import Mapping


class KvStoreError(Exception):
    """Base error for the kvstore framework."""


class KvStoreSerDeError(KvStoreError):
    """Key or value serialization / deserialization failed."""


class KvStoreReadOnlyError(KvStoreError):
    """Mutation attempted on a read-only store view."""


class KvStoreScanUnsupported(KvStoreError):
    """Prefix/layout cannot honor the requested scan (e.g. Hashed + prefix)."""


class KvStoreScopeError(KvStoreError):
    """Scope segments do not match the KvSet scope_schema or composition rules."""


class KvStoreComposeError(KvStoreError):
    """Composition requires matching KvSet / Scope identity."""


class KvStoreIndexError(KvStoreError):
    """Unknown or incompatible index for search."""


class KvStoreTtlUnsupported(KvStoreError):
    """Backend cannot honor a per-write TTL override."""


class PartialSyncError(KvStoreError):
    """One or more fan-out targets failed after the primary write landed.

    ``failures`` maps target labels to the exceptions they raised — that map is
    the complete picture. ``__cause__`` (when set via ``raise ... from``) holds
    only the *first* failure for traceback purposes; do not treat it as the
    full set.

    The primary / earlier write is **not** rolled back.
    """

    def __init__(
        self,
        message: str = "fan-out sync failed",
        *,
        failures: Mapping[str, Exception],
    ) -> None:
        super().__init__(message)
        self.failures: dict[str, Exception] = dict(failures)
