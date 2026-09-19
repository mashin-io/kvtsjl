"""Object-store leaf template package."""

from __future__ import annotations

from kvtsjl.backends.object.binding import (
    normalize_key_prefix,
    object_collection_binder,
    safe_collection_name,
)
from kvtsjl.backends.object.driver import BlobDriver, BlobStat
from kvtsjl.backends.object.store import ObjectStoreKvBackend
from kvtsjl.backends.object.ttl import ObjectTimeTtl, S3ExpiresTtl, TtlSource

__all__ = [
    "BlobDriver",
    "BlobStat",
    "ObjectStoreKvBackend",
    "ObjectTimeTtl",
    "S3ExpiresTtl",
    "TtlSource",
    "normalize_key_prefix",
    "object_collection_binder",
    "safe_collection_name",
]
