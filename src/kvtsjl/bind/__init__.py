"""Namespace binding: wire identity → collection or key prefix."""

from __future__ import annotations

from kvtsjl.bind.namespace import (
    CollectionBinding,
    KeyPrefixBinder,
    NamespaceBinder,
    NativeCollectionBinder,
    NativeStrCollectionBinder,
    PhysicalRef,
    resolve_collection_binding,
)

__all__ = [
    "CollectionBinding",
    "KeyPrefixBinder",
    "NamespaceBinder",
    "NativeCollectionBinder",
    "NativeStrCollectionBinder",
    "PhysicalRef",
    "resolve_collection_binding",
]
