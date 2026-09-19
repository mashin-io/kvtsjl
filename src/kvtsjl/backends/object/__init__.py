"""Object-store leaf template package."""

from __future__ import annotations

from kvtsjl.backends.object.binding import (
    filesystem_collection_binder,
    normalize_key_prefix,
    object_collection_binder,
    safe_collection_name,
)
from kvtsjl.backends.object.driver import BlobDriver, BlobStat
from kvtsjl.backends.object.store import ObjectStoreKvBackend
from kvtsjl.backends.object.ttl import (
    AzureMetadataTtl,
    FilesystemMtimeTtl,
    FilesystemSidecarTtl,
    GcsCustomTimeTtl,
    ObjectTimeTtl,
    S3ExpiresTtl,
    TtlSource,
)

__all__ = [
    "AzureMetadataTtl",
    "BlobDriver",
    "BlobStat",
    "FilesystemMtimeTtl",
    "FilesystemSidecarTtl",
    "GcsCustomTimeTtl",
    "ObjectStoreKvBackend",
    "ObjectTimeTtl",
    "S3ExpiresTtl",
    "TtlSource",
    "filesystem_collection_binder",
    "normalize_key_prefix",
    "object_collection_binder",
    "safe_collection_name",
]
