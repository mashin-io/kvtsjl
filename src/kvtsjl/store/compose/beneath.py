"""``beneath()``: a store's entries in every scope beneath its own, keyed with their scope."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from datetime import datetime
from typing import Any, cast

from kvtsjl.exceptions import KvStoreScopeError, KvStoreUnsupported
from kvtsjl.keymap import ConditionalWrite, Versioned
from kvtsjl.keymap_algebra.topology import MissingPage
from kvtsjl.scope import Scope, Scoped
from kvtsjl.store.backend import KvBackend, by_clone, per_clone
from kvtsjl.store.leaf import LeafKvStore
from kvtsjl.store.logical import KvStore
from kvtsjl.store.schema.layout import KeyLayout, ScanQuery
from kvtsjl.store.schema.scan import Needs, Predicate, ScanRow
from kvtsjl.store.schema.stat import RecordStat
from kvtsjl.store.schema.ttl import TtlPolicy
from kvtsjl.store.streaming import StreamSource


class BeneathKvStore[K, V](KvStore[Scoped[K], V]):
    """``store.beneath()``: its entries in every scope beneath its own.

    A key's ``scope`` names kinds ``store`` has not fixed, so it can only
    reach entries beneath ``store``: containment holds by construction, not
    by a check at read time. Which kinds a key must name is the store's own
    rule, as it is for ``scoped()`` — with a declared schema, all of them. Each operation is ``store``'s, cloned to the key's whole scope, and
    a batch is one batch per scope. ``beneath()`` and ``scoped()`` commute.

    A leaf batches across scopes in one round of I/O instead
    (:class:`LeafBeneathKvStore`), and a composition is rebuilt over its
    children's ``beneath()``; this is the view any other store gets.
    """

    def __init__(self, store: KvStore[K, V]) -> None:
        self._store = store
        self.scope = store.scope
        self.batch_size = store.batch_size

    # --- addressing -----------------------------------------------------------

    def _at(self, key: Scoped[K]) -> KvStore[K, V]:
        return self._store.clone_with_scope(self.scope / key.scope)

    def _clones(self, keys: Sequence[Scoped[K]]) -> list[tuple[KvStore[K, V], K]]:
        """Each key with the store cloned to its scope: one clone per scope."""
        made: dict[Scope, KvStore[K, V]] = {}
        out: list[tuple[KvStore[K, V], K]] = []
        for key in keys:
            clone = made.get(key.scope)
            if clone is None:
                clone = made[key.scope] = self._at(key)
            out.append((clone, key.key))
        return out

    def _beneath(self, whole: Scope) -> Scope:
        """``whole`` without the segments this view fixes."""
        return Scope(segments=whole.segments[len(self.scope.segments) :])

    def _store_name(self) -> str:
        return type(self._store).__name__

    # --- one key ----------------------------------------------------------------

    def key_layout(self) -> KeyLayout:
        return self._store.key_layout()

    @property
    def supports_write_clock(self) -> bool:
        return self._store.supports_write_clock

    @property
    def writes_are_deferred(self) -> bool:
        return self._store.writes_are_deferred

    @property
    def creates_entries(self) -> bool:
        return self._store.creates_entries

    def get(self, key: Scoped[K]) -> V | None:
        return self._at(key).get(key.key)

    def exists(self, key: Scoped[K]) -> bool:
        return self._at(key).exists(key.key)

    def stat(self, key: Scoped[K]) -> RecordStat | None:
        return self._at(key).stat(key.key)

    def set(
        self,
        key: Scoped[K],
        value: V,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        self._at(key).set(key.key, value, ttl=ttl, created_at=created_at)

    def delete(self, key: Scoped[K]) -> bool:
        return self._at(key).delete(key.key)

    def unset(self, key: Scoped[K]) -> bool:
        return self._at(key).unset(key.key)

    def set_stream[C: (bytes, str)](
        self: BeneathKvStore[K, C],
        key: Scoped[K],
        source: StreamSource,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
        size: int | None = None,
    ) -> None:
        self._at(key).set_stream(key.key, source, ttl=ttl, created_at=created_at, size=size)

    def stream_chunk_type(self) -> type[Any]:
        return self._store.stream_chunk_type()

    def read_stream[C: (bytes, str)](
        self: BeneathKvStore[K, C], key: Scoped[K]
    ) -> Iterator[C] | None:
        return self._at(key).read_stream(key.key)

    # --- many keys: one batch per scope here, one across them in a leaf ----------

    def batch_get(self, keys: Sequence[Scoped[K]]) -> dict[Scoped[K], V]:
        keys = list(dict.fromkeys(keys))
        got = self._get_across(self._clones(keys))
        return {k: v for k, v in zip(keys, got, strict=True) if v is not None}

    def batch_exists(self, keys: Sequence[Scoped[K]]) -> set[Scoped[K]]:
        keys = list(dict.fromkeys(keys))
        held = self._exists_across(self._clones(keys))
        return {k for k, h in zip(keys, held, strict=True) if h}

    def batch_stat(self, keys: Sequence[Scoped[K]]) -> dict[Scoped[K], RecordStat]:
        keys = list(dict.fromkeys(keys))
        got = self._stat_across(self._clones(keys))
        return {k: m for k, m in zip(keys, got, strict=True) if m is not None}

    def batch_read_versioned(
        self, keys: Sequence[Scoped[K]]
    ) -> dict[Scoped[K], Versioned[V]]:
        keys = list(dict.fromkeys(keys))
        return dict(zip(keys, self._read_versioned_across(self._clones(keys)), strict=True))

    def batch_write_if_unchanged(
        self,
        writes: Mapping[Scoped[K], ConditionalWrite[V]],
        *,
        ttl: TtlPolicy | None = None,
    ) -> set[Scoped[K]]:
        keys = list(writes)
        addressed = [(c, k, writes[s]) for (c, k), s in zip(self._clones(keys), keys, strict=True)]
        won = self._write_if_unchanged_across(addressed, ttl)
        return {k for k, w in zip(keys, won, strict=True) if w}

    def batch_set(
        self,
        items: Mapping[Scoped[K], V],
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        keys = list(items)
        addressed = [(c, k, items[s]) for (c, k), s in zip(self._clones(keys), keys, strict=True)]
        self._set_across(addressed, ttl, created_at)

    def batch_delete(self, keys: Sequence[Scoped[K]]) -> int:
        return self._delete_across(self._clones(list(dict.fromkeys(keys))))

    def batch_unset(self, keys: Sequence[Scoped[K]]) -> int:
        return self._unset_across(self._clones(list(dict.fromkeys(keys))))

    def _get_across(self, pairs: list[tuple[KvStore[K, V], K]]) -> list[V | None]:
        return per_clone(pairs, lambda s, ks: s.batch_get(ks), lambda got, k: got.get(k))

    def _exists_across(self, pairs: list[tuple[KvStore[K, V], K]]) -> list[bool]:
        return per_clone(pairs, lambda s, ks: s.batch_exists(ks), lambda held, k: k in held)

    def _stat_across(self, pairs: list[tuple[KvStore[K, V], K]]) -> list[RecordStat | None]:
        return per_clone(pairs, lambda s, ks: s.batch_stat(ks), lambda got, k: got.get(k))

    def _read_versioned_across(
        self, pairs: list[tuple[KvStore[K, V], K]]
    ) -> list[Versioned[V]]:
        return per_clone(pairs, lambda s, ks: s.batch_read_versioned(ks), lambda got, k: got[k])

    def _write_if_unchanged_across(
        self,
        writes: list[tuple[KvStore[K, V], K, ConditionalWrite[V]]],
        ttl: TtlPolicy | None,
    ) -> list[bool]:
        return per_clone(
            [(s, (k, w)) for s, k, w in writes],
            lambda s, kws: s.batch_write_if_unchanged(dict(kws), ttl=ttl),
            lambda won, kw: kw[0] in won,
        )

    def _set_across(
        self,
        items: list[tuple[KvStore[K, V], K, V]],
        ttl: TtlPolicy | None,
        created_at: datetime | None,
    ) -> None:
        for store, kvs in by_clone([(s, (k, v)) for s, k, v in items]):
            store.batch_set(dict(kvs), ttl=ttl, created_at=created_at)

    def _delete_across(self, pairs: list[tuple[KvStore[K, V], K]]) -> int:
        return sum(store.batch_delete(ks) for store, ks in by_clone(pairs))

    def _unset_across(self, pairs: list[tuple[KvStore[K, V], K]]) -> int:
        return sum(store.batch_unset(ks) for store, ks in by_clone(pairs))

    # --- listing: the scope comes back with each key ------------------------------

    def _require_scoped_scan(self, op: str) -> None:
        if not self._store.supports_scoped_scan():
            raise KvStoreUnsupported(
                f"{op} through beneath(): {self._store_name()} cannot recover scopes "
                "from the keys it lists — that needs a declared scope_schema "
                "and a layout that keeps the scope in the key. Keyed operations "
                "still work, since each key carries its scope."
            )

    def scan_entries(self, query: ScanQuery[Scoped[K]]) -> Iterator[tuple[Scoped[K], V | None]]:
        """The entries beneath, each key with its scope; a prefix names one scope."""
        self._require_scoped_scan("a scan")
        for whole, leaf, value in self._scoped_entries(query):
            yield Scoped(self._beneath(whole), leaf), value

    def _scoped_entries(
        self, query: ScanQuery[Scoped[K]]
    ) -> Iterator[tuple[Scope, K, V | None]]:
        store, prefix = self._store, None
        if query.prefix is not None:
            store, prefix = self._at(query.prefix), query.prefix.key
        listed = ScanQuery(prefix=prefix, include_values=query.include_values, page_size=query.page_size)
        for key, value in store.scan_entries(listed):
            found = store.rescope(key)
            if found is not None:
                yield found[0], found[1], value

    def supports_scoped_scan(self) -> bool:
        return self._store.supports_scoped_scan()

    def expired_keys(self, *, now: datetime | None = None, limit: int) -> list[Scoped[K]]:
        self._require_scoped_scan("listing expired keys")
        return [Scoped(self._beneath(w), k) for w, k in self._expired(now, limit)]

    def gc_expired_keys(self, *, max_entries: int) -> list[Scoped[K]]:
        """Find the expired entries beneath, then delete them by their whole keys."""
        found = self.expired_keys(limit=max_entries)
        if found:
            self.batch_delete(found)
        return found

    def _expired(self, now: datetime | None, limit: int) -> list[tuple[Scope, K]]:
        found = (self._store.rescope(k) for k in self._store.expired_keys(now=now, limit=limit))
        return [f for f in found if f is not None]

    def missing_pages(self, page_size: int) -> Iterator[MissingPage[Scoped[K], V]]:
        raise KvStoreUnsupported(
            f"missing_keys through beneath() of {self._store_name()}: its fallbacks "
            "list keys without their scope; fill the store it views instead"
        )

    # --- views of the view --------------------------------------------------------

    def clone_with_scope(self, scope: Scope) -> KvStore[Scoped[K], V]:
        """``beneath().scoped(...)`` is ``scoped(...).beneath()``."""
        if scope.segments[: len(self.scope.segments)] != self.scope.segments:
            raise KvStoreScopeError(
                f"{scope!r} is not beneath {self.scope!r}: a view only narrows"
            )
        return self._rebuilt(self._store.clone_with_scope(scope))

    def _rebuilt(self, store: KvStore[K, V]) -> KvStore[Scoped[K], V]:
        return BeneathKvStore(store)

    def child_stores(self) -> tuple[object, ...]:
        return (self._store,)

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import compose_repr

        return compose_repr(type(self).__name__, store=self._store)


class LeafBeneathKvStore[K, V](BeneathKvStore[K, V]):
    """``beneath()`` of a leaf: one batch across scopes, as the medium allows.

    Each key is addressed by the backend cloned to its scope, so every rule of
    a scope — escaping, scope columns, key codecs — holds as it does there;
    the backend then makes one batch of all of them (``batch_*_across``).
    """

    def __init__(self, leaf: LeafKvStore[K, V, Any, Any, Any]) -> None:
        super().__init__(leaf)
        self._leaf = leaf

    def _rebuilt(self, store: KvStore[K, V]) -> KvStore[Scoped[K], V]:
        return LeafBeneathKvStore(cast(LeafKvStore[K, V, Any, Any, Any], store))

    def _backends(
        self, pairs: list[tuple[KvStore[K, V], K]]
    ) -> list[tuple[KvBackend[K, V, Any, Any, Any], K]]:
        return [(cast(LeafKvStore[K, V, Any, Any, Any], s).backend, k) for s, k in pairs]

    def _get_across(self, pairs: list[tuple[KvStore[K, V], K]]) -> list[V | None]:
        return self._leaf.backend.batch_get_across(self._backends(pairs))

    def _exists_across(self, pairs: list[tuple[KvStore[K, V], K]]) -> list[bool]:
        return self._leaf.backend.batch_exists_across(self._backends(pairs))

    def _stat_across(self, pairs: list[tuple[KvStore[K, V], K]]) -> list[RecordStat | None]:
        return self._leaf.backend.batch_stat_across(self._backends(pairs))

    def _read_versioned_across(
        self, pairs: list[tuple[KvStore[K, V], K]]
    ) -> list[Versioned[V]]:
        return self._leaf.backend.batch_read_versioned_across(self._backends(pairs))

    def _write_if_unchanged_across(
        self,
        writes: list[tuple[KvStore[K, V], K, ConditionalWrite[V]]],
        ttl: TtlPolicy | None,
    ) -> list[bool]:
        backends = self._backends([(s, k) for s, k, _ in writes])
        return self._leaf.backend.batch_write_if_unchanged_across(
            [(b, k, w) for (b, k), (_, _, w) in zip(backends, writes, strict=True)], ttl=ttl
        )

    def _set_across(
        self,
        items: list[tuple[KvStore[K, V], K, V]],
        ttl: TtlPolicy | None,
        created_at: datetime | None,
    ) -> None:
        backends = self._backends([(s, k) for s, k, _ in items])
        self._leaf.backend.batch_set_across(
            [(b, k, v) for (b, k), (_, _, v) in zip(backends, items, strict=True)],
            ttl=ttl,
            created_at=created_at,
        )

    def _delete_across(self, pairs: list[tuple[KvStore[K, V], K]]) -> int:
        return self._leaf.backend.batch_delete_across(self._backends(pairs))

    def _unset_across(self, pairs: list[tuple[KvStore[K, V], K]]) -> int:
        return self._leaf.backend.batch_unset_across(self._backends(pairs))

    def missing_pages(self, page_size: int) -> Iterator[MissingPage[Scoped[K], V]]:
        """A leaf has no fallback, so it lacks nothing a read through it finds."""
        return iter(())

    def _scoped_entries(
        self, query: ScanQuery[Scoped[K]]
    ) -> Iterator[tuple[Scope, K, V | None]]:
        backend, prefix = self._leaf.backend, None
        if query.prefix is not None:
            at = cast(LeafKvStore[K, V, Any, Any, Any], self._at(query.prefix))
            backend, prefix = at.backend, query.prefix.key
        return backend.scan_scoped_entries(
            ScanQuery(prefix=prefix, include_values=query.include_values, page_size=query.page_size)
        )

    def _expired(self, now: datetime | None, limit: int) -> list[tuple[Scope, K]]:
        return self._leaf.backend.expired_scoped_keys(now=now, limit=limit)


class OnLeaf[K](Predicate[Scoped[K]]):
    """A predicate on leaf keys, applied to the leaf of a scoped key."""

    def __init__(self, inner: Predicate[K]) -> None:
        self._inner = inner

    def needs(self) -> Needs:
        return self._inner.needs()

    def evaluate(self, row: ScanRow[Scoped[K], Any]) -> bool:
        return self._inner.evaluate(ScanRow(key=row.key.key, value=row.value, stat=row.stat))
