"""End-to-end physical key codec for object-store leaves."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, cast

from kvtsjl.bind.namespace import CollectionBinding
from kvtsjl.scope import Scope
from kvtsjl.store.schema.kvset import KvSet
from kvtsjl.store.schema.layout import KeyLayout


class KeyCodec[K](ABC):
    """Owns the full round trip: logical key ↔ medium object key / path.

    ``decode_physical(encode_physical(k)) == k`` is the invariant; KBLOB join and
    ``KeyLayout`` path encoding are internal steps, not a second public seam.
    Bound to one scope + collection prefix — rebuild on ``scoped()``.
    """

    @abstractmethod
    def encode_physical(self, key: K) -> str:
        """Full object key / relative path under the collection."""

    @abstractmethod
    def decode_physical(self, physical: str) -> K | None:
        """Invert ``encode_physical``, or ``None`` if outside this scope."""

    @abstractmethod
    def matches_prefix(self, physical: str, key_prefix: K | None) -> bool:
        """Whether ``physical`` falls under ``key_prefix`` within this scope."""

    def listing_prefix(self, key_prefix: K | None) -> str:
        """Object-store list Prefix: collection + encoded scope/key prefix.

        Subclasses that own the medium path must override. The default raises.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not implement listing_prefix"
        )

    def decode_if_prefix(self, physical: str, key_prefix: K | None) -> K | None:
        """Decode when under ``key_prefix``; one round trip for the default codec."""
        if not self.matches_prefix(physical, key_prefix):
            return None
        return self.decode_physical(physical)


class BindingLayoutKeyCodec[K, KBLOB: str | bytes](KeyCodec[K]):
    """Default codec: collection binding join + ``KeyLayout`` path physicalization."""

    def __init__(
        self,
        kvset: KvSet[K, Any, KBLOB, Any],
        binding: CollectionBinding[KBLOB, Any],
        *,
        scope: Scope,
        collection_prefix: str,
    ) -> None:
        self._kvset = kvset
        self._binding = binding
        self._scope = scope
        self._collection_prefix = collection_prefix

    def _in_key_blob(self, key: K) -> KBLOB:
        leaf = self._kvset.key_serde.serialize(key)
        return self._binding.item_key(
            self._scope,
            leaf,
            str_serde=self._kvset.str_serde,
            blob_ops=self._kvset.blob_ops,
        )

    def _scan_prefix_blob(self, key_prefix: K | None) -> KBLOB:
        leaf: KBLOB | None = None
        if key_prefix is not None:
            leaf = self._kvset.key_serde.serialize(key_prefix)
        return self._binding.scope_prefix(
            self._scope,
            str_serde=self._kvset.str_serde,
            blob_ops=self._kvset.blob_ops,
            key_prefix_blob=leaf,
        )

    def _blob_from_physical(self, physical: str) -> KBLOB | None:
        if not physical.startswith(self._collection_prefix):
            return None
        rel = physical[len(self._collection_prefix) :]
        blob_type = cast(type[str] | type[bytes], self._kvset.str_serde.blob_type)
        try:
            return cast(
                KBLOB,
                self._kvset.key_layout.decode_path(rel, blob_type=blob_type),
            )
        except ValueError:
            return None

    def encode_physical(self, key: K) -> str:
        rel = self._kvset.key_layout.encode_path(self._in_key_blob(key))
        return f"{self._collection_prefix}{rel}"

    def decode_physical(self, physical: str) -> K | None:
        pk = self._blob_from_physical(physical)
        if pk is None:
            return None
        return self._decode_in_key(pk)

    def matches_prefix(self, physical: str, key_prefix: K | None) -> bool:
        pk = self._blob_from_physical(physical)
        if pk is None:
            return False
        prefix = self._scan_prefix_blob(key_prefix)
        return self._kvset.blob_ops.startswith(pk, prefix)

    def listing_prefix(self, key_prefix: K | None) -> str:
        """Three layers: collection binding, scope, encoded user key prefix."""
        blob = self._scan_prefix_blob(key_prefix)
        rel = self._kvset.key_layout.encode_path(blob)
        return f"{self._collection_prefix}{rel}"

    def decode_if_prefix(self, physical: str, key_prefix: K | None) -> K | None:
        pk = self._blob_from_physical(physical)
        if pk is None:
            return None
        prefix = self._scan_prefix_blob(key_prefix)
        if not self._kvset.blob_ops.startswith(pk, prefix):
            return None
        return self._decode_in_key(pk)

    def _decode_in_key(self, physical_key: KBLOB) -> K | None:
        ops = self._kvset.blob_ops
        binding = self._binding
        name_version = list(binding.name_version_prefix)
        rest: KBLOB = physical_key
        if name_version:
            nv = ops.join(name_version)
            if not ops.startswith(rest, nv):
                return None
            stripped = ops.strip_prefix(rest, nv)
            if stripped is None:
                return None
            if ops.len(stripped) >= ops.len(ops.separator) and ops.startswith(
                stripped, ops.separator
            ):
                stripped = ops.strip_prefix(stripped, ops.separator)
            if stripped is None:
                return None
            rest = stripped
        return self._kvset.decode_leaf_from_in_key(self._scope, rest)

    @property
    def key_layout(self) -> KeyLayout:
        return self._kvset.key_layout
