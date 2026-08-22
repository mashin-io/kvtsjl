"""Namespace binding: wire identity → collection vs key prefix."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass

from kvtsjl.schema.blob_ops import BlobOps
from kvtsjl.schema.ref import WireRef
from kvtsjl.scope import Scope
from kvtsjl.serde import SerDe


@dataclass(frozen=True, slots=True)
class PhysicalRef[KBLOB, COLL]:
    """Physical address of one item (or a scan prefix if key is partial)."""

    collection: COLL | None
    key: KBLOB


@dataclass(frozen=True, slots=True)
class CollectionBinding[KBLOB, COLL]:
    """Resolved binding for one wire schema under a NamespaceBinder policy."""

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


def resolve_collection_binding[KBLOB, COLL](
    wire: WireRef[KBLOB],
    *,
    binder: NamespaceBinder[KBLOB, COLL] | None = None,
    binding: CollectionBinding[KBLOB, COLL] | None = None,
) -> CollectionBinding[KBLOB, COLL]:
    if binding is not None:
        return binding
    if binder is not None:
        return binder.bind_wire(wire)
    raise TypeError("physical backend requires binder= or binding=")


class NamespaceBinder[KBLOB, COLL](ABC):
    """Maps wire identity → collection vs key prefix."""

    @abstractmethod
    def bind_wire(self, wire: WireRef[KBLOB]) -> CollectionBinding[KBLOB, COLL]: ...


class NativeCollectionBinder[KBLOB, COLL](NamespaceBinder[KBLOB, COLL]):
    """Name/version become a collection handle; in-key is scope + leaf only."""

    def __init__(
        self,
        *,
        collection_formatter: Callable[[WireRef[KBLOB]], COLL],
    ) -> None:
        self._collection_formatter = collection_formatter

    def bind_wire(self, wire: WireRef[KBLOB]) -> CollectionBinding[KBLOB, COLL]:
        return CollectionBinding(
            collection=self._collection_formatter(wire),
            _name_version_prefix=(),
        )


def default_str_collection_name[KBLOB](wire: WireRef[KBLOB]) -> str:
    return f"{wire.name}:{wire.version_label()}"


class NativeStrCollectionBinder[KBLOB](NativeCollectionBinder[KBLOB, str]):
    """Native collections identified by ``\"{name}:v{version}\"`` strings."""

    def __init__(
        self,
        *,
        collection_formatter: Callable[[WireRef[KBLOB]], str] | None = None,
    ) -> None:
        super().__init__(
            collection_formatter=collection_formatter or default_str_collection_name
        )


class KeyPrefixBinder[KBLOB](NamespaceBinder[KBLOB, None]):
    """Flat keyspace: name + version are prepended into every in-key."""

    def bind_wire(self, wire: WireRef[KBLOB]) -> CollectionBinding[KBLOB, None]:
        prefix = (
            wire.str_serde.serialize(wire.name),
            wire.str_serde.serialize(wire.version_label()),
        )
        return CollectionBinding(collection=None, _name_version_prefix=prefix)
