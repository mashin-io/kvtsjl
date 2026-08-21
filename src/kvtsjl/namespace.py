"""Namespace binding: name/version → collection vs key prefix (store-owned)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass

from kvtsjl.blob_ops import BlobOps
from kvtsjl.kvset import KvSet
from kvtsjl.kvset_ref import KvSetRef
from kvtsjl.scope import Scope
from kvtsjl.serde import SerDe


@dataclass(frozen=True, slots=True)
class PhysicalRef[KBLOB, COLL]:
    """Backend-facing address of one item (or a scan prefix if key is partial)."""

    collection: COLL | None
    key: KBLOB


@dataclass(frozen=True, slots=True)
class CollectionBinding[KBLOB, COLL]:
    """Resolved binding for one KvSet under a NamespaceBinder policy."""

    collection: COLL | None
    _name_version_prefix: tuple[KBLOB, ...] = ()

    def scope_parts(
        self,
        scope: Scope,
        *,
        str_serde: SerDe[str, KBLOB],
    ) -> list[KBLOB]:
        parts: list[KBLOB] = []
        for seg in scope.segments:
            parts.append(str_serde.serialize(seg.kind))
            parts.append(str_serde.serialize(seg.id))
        return parts

    def item_key(
        self,
        scope: Scope,
        key_blob: KBLOB,
        *,
        str_serde: SerDe[str, KBLOB],
        blob_ops: BlobOps[KBLOB],
    ) -> KBLOB:
        parts = list(self._name_version_prefix)
        parts.extend(self.scope_parts(scope, str_serde=str_serde))
        parts.append(key_blob)
        return blob_ops.join(parts)

    def scope_prefix(
        self,
        scope: Scope,
        *,
        str_serde: SerDe[str, KBLOB],
        blob_ops: BlobOps[KBLOB],
        key_prefix_blob: KBLOB | None = None,
    ) -> KBLOB:
        parts = list(self._name_version_prefix)
        parts.extend(self.scope_parts(scope, str_serde=str_serde))
        if key_prefix_blob is not None:
            parts.append(key_prefix_blob)
        if not parts:
            return blob_ops.concat()
        return blob_ops.join(parts)

    def physical(
        self,
        scope: Scope,
        key_blob: KBLOB,
        *,
        str_serde: SerDe[str, KBLOB],
        blob_ops: BlobOps[KBLOB],
    ) -> PhysicalRef[KBLOB, COLL]:
        return PhysicalRef(
            collection=self.collection,
            key=self.item_key(scope, key_blob, str_serde=str_serde, blob_ops=blob_ops),
        )


class NamespaceBinder[KBLOB, COLL](ABC):
    """Maps logical KvSet identity → collection vs key prefix. Owned by the store."""

    @abstractmethod
    def bind[K, V, VBLOB](
        self, kvset: KvSet[K, V, KBLOB, VBLOB]
    ) -> CollectionBinding[KBLOB, COLL]: ...


class NativeCollectionBinder[KBLOB, COLL](NamespaceBinder[KBLOB, COLL]):
    """Name/version become a collection handle; in-key is scope + leaf only."""

    def __init__(
        self,
        *,
        collection_formatter: Callable[[KvSetRef[KBLOB]], COLL],
    ) -> None:
        self._collection_formatter = collection_formatter

    def bind[K, V, VBLOB](
        self, kvset: KvSet[K, V, KBLOB, VBLOB]
    ) -> CollectionBinding[KBLOB, COLL]:
        ref = KvSetRef.from_kvset(kvset)
        return CollectionBinding(
            collection=self._collection_formatter(ref),
            _name_version_prefix=(),
        )


def default_str_collection_name[KBLOB](ref: KvSetRef[KBLOB]) -> str:
    return f"{ref.name}:{ref.version_label()}"


class NativeStrCollectionBinder[KBLOB](NativeCollectionBinder[KBLOB, str]):
    """Native collections identified by ``\"{name}:v{version}\"`` strings."""

    def __init__(
        self,
        *,
        collection_formatter: Callable[[KvSetRef[KBLOB]], str] | None = None,
    ) -> None:
        super().__init__(
            collection_formatter=collection_formatter or default_str_collection_name
        )


class KeyPrefixBinder[KBLOB](NamespaceBinder[KBLOB, None]):
    """Flat keyspace: name + version are prepended into every in-key."""

    def bind[K, V, VBLOB](
        self, kvset: KvSet[K, V, KBLOB, VBLOB]
    ) -> CollectionBinding[KBLOB, None]:
        prefix = (
            kvset.str_serde.serialize(kvset.name),
            kvset.str_serde.serialize(kvset.version_label()),
        )
        return CollectionBinding(collection=None, _name_version_prefix=prefix)
