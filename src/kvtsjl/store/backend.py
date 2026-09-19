"""Leaf document ``KvBackend``: KvSet plus physical key helpers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Self

from kvtsjl.batching import DEFAULT_BATCH_SIZE
from kvtsjl.bind.namespace import CollectionBinding, NamespaceBinder, resolve_collection_binding
from kvtsjl.leaf.base import PhysicalBackend
from kvtsjl.schema.blob_ops import BlobOps
from kvtsjl.schema.ref import WireRef
from kvtsjl.scope import Scope
from kvtsjl.serde import SerDe
from kvtsjl.store.schema.kvset import KvSet
from kvtsjl.store.schema.layout import KeyLayout, ScanQuery
from kvtsjl.store.schema.ttl import TtlPolicy


class KvBackend[K, V, KBLOB, VBLOB, COLL](
    PhysicalBackend[K, V, KBLOB, VBLOB, COLL],
    ABC,
):
    """Leaf document I/O: ``KvSet``, collection binding, and medium access.

    Not a ``KvStore`` — public leaves wrap a backend in ``LeafKvStore``.
    """

    kvset: KvSet[K, V, KBLOB, VBLOB]
    batch_size: int

    def key_layout(self) -> KeyLayout:
        return self.kvset.key_layout

    @property
    def physical(self) -> KvSet[K, V, KBLOB, VBLOB]:
        return self.kvset

    @property
    def supports_write_clock(self) -> bool:
        return False

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
        name_version = list(binding.name_version_prefix)
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

    def resolve_ttl_seconds(self, ttl: TtlPolicy | None = None) -> int | None:
        policy = self.kvset.ttl_policy if ttl is None else ttl
        return policy.ttl_seconds()

    @abstractmethod
    def scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        """Yield decoded keys (and values if requested) under scope + prefix."""

    @abstractmethod
    def clone_with_scope(self, scope: Scope) -> Self:
        """Return a backend view narrowed to ``scope`` (same physical medium)."""

    def gc_expired_keys(self, *, max_entries: int) -> list[K]:
        """Delete up to ``max_entries`` expired entries; return deleted keys."""
        if max_entries < 1:
            raise ValueError(f"max_entries must be >= 1, got {max_entries}")
        return []

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import backend_repr

        return backend_repr(self)
