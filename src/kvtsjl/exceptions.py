"""KV-store framework errors."""

from __future__ import annotations


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
