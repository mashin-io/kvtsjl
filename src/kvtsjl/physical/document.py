"""Leaf document ``KvBackend``: KvSet plus physical key helpers."""

from __future__ import annotations

from abc import ABC

from kvtsjl.batching import DEFAULT_BATCH_SIZE
from kvtsjl.bind.namespace import CollectionBinding, NamespaceBinder, resolve_collection_binding
from kvtsjl.physical.base import PhysicalBackend
from kvtsjl.scope import Scope
from kvtsjl.serde import SerDe
from kvtsjl.store.logical import KvStore
from kvtsjl.wire.blob_ops import BlobOps
from kvtsjl.wire.kvset import KvSet
from kvtsjl.wire.layout import KeyLayout
from kvtsjl.wire.ref import WireRef


class KvBackend[K, V, KBLOB, VBLOB, COLL](
    PhysicalBackend[K, V, KBLOB, VBLOB, COLL],
    KvStore[K, V],
    ABC,
):
    """Leaf document backend: ``KvSet``, collection binding, and medium I/O."""

    kvset: KvSet[K, V, KBLOB, VBLOB]

    def key_layout(self) -> KeyLayout:
        return self.kvset.key_layout

    @property
    def physical(self) -> KvSet[K, V, KBLOB, VBLOB]:
        return self.kvset

    def _str_serde(self) -> SerDe[str, KBLOB]:
        return self.kvset.str_serde

    def _blob_ops(self) -> BlobOps[KBLOB]:
        return self.kvset.blob_ops

    def __init__(
        self,
        kvset: KvSet[K, V, KBLOB, VBLOB],
        *,
        scope: Scope | None = None,
        binder: NamespaceBinder[KBLOB, COLL] | None = None,
        binding: CollectionBinding[KBLOB, COLL] | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        self.kvset = kvset
        self.scope = scope or Scope.empty()
        kvset.validate_scope(self.scope)
        self.batch_size = batch_size
        self._binding = resolve_collection_binding(
            WireRef.from_kvset(kvset),
            binder=binder,
            binding=binding,
        )

    def _physical_key_blob(self, key: K) -> KBLOB:
        leaf = self.kvset.key_serde.serialize(key)
        return self._item_key_blob(leaf)

    def _scan_prefix_blob(self, key_prefix: K | None) -> KBLOB:
        leaf: KBLOB | None = None
        if key_prefix is not None:
            leaf = self.kvset.key_serde.serialize(key_prefix)
        return super()._scope_prefix_blob(leaf)

    def _decode_key_from_physical(self, physical_key: KBLOB) -> K | None:
        """Decode leaf K from a full in-key physical key under this scope."""
        ops = self.kvset.blob_ops
        binding = self._binding
        name_version = list(binding._name_version_prefix)
        rest = physical_key
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
        return self.kvset.decode_leaf_from_in_key(self.scope, rest)

    def ttl_seconds(self) -> int | None:
        return self.kvset.ttl_policy.ttl_seconds()
