"""Leaf document ``KvBackend``: KvSet plus physical key helpers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator, Sequence
from datetime import datetime
from typing import Self

from kvtsjl.batching import DEFAULT_BATCH_SIZE
from kvtsjl.bind.namespace import CollectionBinding, NamespaceBinder, resolve_collection_binding
from kvtsjl.keymap import ConditionalWrite, Versioned
from kvtsjl.leaf.base import PhysicalBackend
from kvtsjl.schema.blob_ops import BlobOps
from kvtsjl.schema.ref import WireRef
from kvtsjl.scope import Scope
from kvtsjl.serde import SerDe
from kvtsjl.store.schema.kvset import KvSet
from kvtsjl.store.schema.layout import KeyLayout, ScanQuery
from kvtsjl.store.schema.stat import RecordStat
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

    def batch_unset(self, keys: Sequence[K]) -> int:
        """A leaf owns its entries, so unsetting is its native bulk delete.

        A backend whose entries are shared overrides ``unset`` and this together.
        """
        return self.batch_delete(keys)

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
        # Guards the path that actually writes: a leaf is only unambiguous
        # when the number of scope segments is fixed, or the leaf is escaped.
        escape_leaf = self.kvset.leaf_needs_escaping(self.scope)
        leaf = self.kvset.key_serde.serialize(key)
        if escape_leaf:
            leaf = self.kvset.blob_ops.escape(leaf)
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

    def scan_scoped_entries(
        self, query: ScanQuery[K]
    ) -> Iterator[tuple[Scope, K, V | None]]:
        """``scan_entries``, each key with its whole scope.

        The default decodes the scope from the key, which a layout keeping the
        scope in the key allows; a key that does not decode under the schema
        is not this collection's, and is passed over.
        """
        for key, value in self.scan_entries(query):
            found = self.kvset.rescope(self.scope, key)
            if found is not None:
                yield found[0], found[1], value

    def expired_scoped_keys(
        self, *, now: datetime | None = None, limit: int
    ) -> list[tuple[Scope, K]]:
        """``expired_keys``, each with its whole scope; read-only."""
        found = (self.kvset.rescope(self.scope, k) for k in self.expired_keys(now=now, limit=limit))
        return [f for f in found if f is not None]

    # --- across scopes ------------------------------------------------------
    #
    # Each pair is a key and this backend's clone bound to that key's scope.
    # The clone addresses the key, so every rule of a scope holds; a backend
    # whose medium can do the I/O for many scopes at once overrides these to
    # make one batch of all of them. The defaults ask each clone for its own
    # batch. Results are in the order of the pairs.

    def _own(self, clone: KvBackend[K, V, KBLOB, VBLOB, COLL]) -> Self:
        """``clone`` as this backend's own kind: the pairs carry clones of it."""
        if not isinstance(clone, type(self)):
            raise TypeError(
                f"{type(self).__name__} cannot batch with a {type(clone).__name__}"
            )
        return clone

    def batch_get_across(self, pairs: Sequence[tuple[KvBackend[K, V, KBLOB, VBLOB, COLL], K]]) -> list[V | None]:
        """``batch_get`` of keys in several scopes."""
        return per_clone(pairs, lambda b, ks: b.batch_get(ks), lambda got, k: got.get(k))

    def batch_exists_across(self, pairs: Sequence[tuple[KvBackend[K, V, KBLOB, VBLOB, COLL], K]]) -> list[bool]:
        """``batch_exists`` of keys in several scopes."""
        return per_clone(pairs, lambda b, ks: b.batch_exists(ks), lambda held, k: k in held)

    def batch_stat_across(self, pairs: Sequence[tuple[KvBackend[K, V, KBLOB, VBLOB, COLL], K]]) -> list[RecordStat | None]:
        """``batch_stat`` of keys in several scopes."""
        return per_clone(pairs, lambda b, ks: b.batch_stat(ks), lambda got, k: got.get(k))

    def batch_read_versioned_across(
        self, pairs: Sequence[tuple[KvBackend[K, V, KBLOB, VBLOB, COLL], K]]
    ) -> list[Versioned[V]]:
        """``batch_read_versioned`` of keys in several scopes."""
        return per_clone(
            pairs, lambda b, ks: b.batch_read_versioned(ks), lambda got, k: got[k]
        )

    def batch_write_if_unchanged_across(
        self,
        writes: Sequence[tuple[KvBackend[K, V, KBLOB, VBLOB, COLL], K, ConditionalWrite[V]]],
        *,
        ttl: TtlPolicy | None = None,
    ) -> list[bool]:
        """``batch_write_if_unchanged`` of keys in several scopes; whether each was written."""
        return per_clone(
            [(b, (k, w)) for b, k, w in writes],
            lambda b, kws: b.batch_write_if_unchanged(dict(kws), ttl=ttl),
            lambda written, kw: kw[0] in written,
        )

    def batch_set_across(
        self,
        items: Sequence[tuple[KvBackend[K, V, KBLOB, VBLOB, COLL], K, V]],
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        """``batch_set`` of keys in several scopes."""
        per_clone(
            [(b, (k, v)) for b, k, v in items],
            lambda b, kvs: b.batch_set(dict(kvs), ttl=ttl, created_at=created_at),
            lambda _done, _kv: None,
        )

    def batch_delete_across(self, pairs: Sequence[tuple[KvBackend[K, V, KBLOB, VBLOB, COLL], K]]) -> int:
        """``batch_delete`` of keys in several scopes; how many were removed."""
        return sum(b.batch_delete(ks) for b, ks in by_clone(pairs))

    def batch_unset_across(self, pairs: Sequence[tuple[KvBackend[K, V, KBLOB, VBLOB, COLL], K]]) -> int:
        """``batch_unset`` of keys in several scopes; how many were removed."""
        return sum(b.batch_unset(ks) for b, ks in by_clone(pairs))

    def resolve_ttl_seconds(self, ttl: TtlPolicy | None = None) -> int | None:
        policy = self.kvset.ttl_policy if ttl is None else ttl
        return policy.ttl_seconds()

    @abstractmethod
    def scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        """Yield decoded keys (and values if requested) under scope + prefix."""

    @abstractmethod
    def clone_with_scope(self, scope: Scope) -> Self:
        """Return a backend view narrowed to ``scope`` (same physical medium)."""

    def expired_keys(self, *, now: datetime | None = None, limit: int) -> list[K]:
        """Up to ``limit`` expired keys under the bound scope; read-only.

        The primitive: ``gc_expired_keys`` is "find these, then delete them".
        Backends implement this one and inherit the deletion.

        The default is empty, which is right for a medium that expires entries
        itself (Redis) — there is nothing left to collect.
        """
        if limit < 1:
            raise ValueError(f"limit must be >= 1, got {limit}")
        return []

    def gc_expired_keys(self, *, max_entries: int) -> list[K]:
        """Delete up to ``max_entries`` expired entries; return deleted keys."""
        if max_entries < 1:
            raise ValueError(f"max_entries must be >= 1, got {max_entries}")
        keys = self.expired_keys(limit=max_entries)
        if keys:
            self.batch_delete(keys)
        return keys

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import backend_repr

        return backend_repr(self)


def by_clone[B, X](pairs: Sequence[tuple[B, X]]) -> list[tuple[B, list[X]]]:
    """The pairs grouped by the clone each is addressed by, in first-seen order."""
    groups: dict[int, tuple[B, list[X]]] = {}
    for clone, item in pairs:
        groups.setdefault(id(clone), (clone, []))[1].append(item)
    return list(groups.values())


def per_clone[B, X, R, T](
    pairs: Sequence[tuple[B, X]],
    batch: Callable[[B, list[X]], R],
    pick: Callable[[R, X], T],
) -> list[T]:
    """Run ``batch`` once per clone, then ``pick`` each pair's result, in order."""
    results = {id(clone): batch(clone, items) for clone, items in by_clone(pairs)}
    return [pick(results[id(clone)], item) for clone, item in pairs]
