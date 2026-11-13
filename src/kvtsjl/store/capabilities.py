"""Capability mixins for store composition (not part of the public ``__all__``)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator, Mapping, Sequence
from datetime import datetime
from typing import Any, Literal, Self, cast, overload

from kvtsjl.exceptions import (
    KvStoreError,
    KvStoreScanUnsupported,
    KvStoreScopeError,
)
from kvtsjl.scope import Scope
from kvtsjl.store.schema.layout import KeyLayout, ScanQuery, supports_prefix_scan
from kvtsjl.store.schema.policy import SyncPolicy
from kvtsjl.store.schema.scan import (
    After,
    CompiledScan,
    Expired,
    OrderBy,
    Predicate,
    Projection,
    Pushdown,
    ScanCursor,
    ScanOrder,
    ScanPlan,
    ScanPlanNode,
    ScanRow,
    ScanSpec,
    explain_sorted,
    is_gc_predicate,
    peel_key_prefix,
    predicate_label,
    scan_sorted,
    sugar_to_spec,
    unordered,
)
from kvtsjl.store.schema.ttl import TtlPolicy, require_write_clock_supported


class ScopedMixin(ABC):
    """Logical key-prefix scoping.

    ``clone_with_scope`` has a concrete raising default (P1 fail-closed). Leaves
    and scopable indexes override; vector indexes may inherit the default.
    """

    scope: Scope

    def scoped(self, scope: Scope | None = None, **kinds_to_ids: str) -> Self:
        """Narrow to a longer logical key prefix (appends scope segments)."""
        if scope is not None and kinds_to_ids:
            raise KvStoreScopeError("pass scope= or kwargs, not both")
        if scope is None:
            scope = self.scope.child(**kinds_to_ids) if kinds_to_ids else self.scope
        else:
            scope = Scope(segments=self.scope.segments + scope.segments)
        return self.clone_with_scope(scope)

    def prefixed(self, scope: Scope | None = None, **kinds_to_ids: str) -> Self:
        """Alias of ``scoped`` — scope is a logical key prefix."""
        return self.scoped(scope, **kinds_to_ids)

    def clone_with_scope(self, scope: Scope) -> Self:
        raise KvStoreScopeError(
            f"{type(self).__name__} cannot be scoped with the store"
        )


class ScannableMixin[K, V](ABC):
    """Prefix scan / list over a keyed document space."""

    batch_size: int

    @abstractmethod
    def key_layout(self) -> KeyLayout:
        """Key physicalization policy (from the leaf ``KvSet``)."""

    @abstractmethod
    def scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        """Yield decoded keys (and values if requested) under scope + prefix.

        Leaves may override ``scan_rows`` for full ``ScanSpec`` support; the
        default adapts ``ScanSpec`` → ``ScanQuery``.
        """

    def supports_scoped_scan(self) -> bool:
        """Whether ``scan_scoped`` can recover the scope from a key.

        Needs a declared ``scope_schema`` to know the arity, and a layout that
        keeps the scope readable in the key. ``KeyLayout.HASHED`` discards it,
        so there is nothing to recover.
        """
        kvset = getattr(self, "kvset", None)
        if kvset is None or getattr(kvset, "scope_schema", None) is None:
            return False
        return self.key_layout() is not KeyLayout.HASHED

    def scan_scoped(
        self,
        prefix: K | None = None,
        *,
        include_values: bool = False,
        page_size: int | None = None,
        where: Predicate[K] | None = None,
        limit: int | None = None,
    ) -> Iterator[tuple[Scope, K, V | None]]:
        """Scan yielding each entry's **full** scope, not just the leaf.

        ``scan`` decodes the leaf against the scope the store is bound to, so
        on an unscoped store the scope is simply part of the key it returns.
        This recovers it: on a store bound to a prefix of the schema, the
        entries are still only that prefix's, but each one comes back with its
        whole scope.

        Raises ``KvStoreError`` where the scope is not recoverable rather than
        guessing — see ``supports_scoped_scan``.
        """
        if not self.supports_scoped_scan():
            raise KvStoreError(
                f"{type(self).__name__} cannot recover scopes from keys: "
                "scan_scoped needs a declared scope_schema and a layout that "
                "keeps the scope in the key"
            )
        me = cast(Any, self)
        rows: Iterator[Any] = me.scan(
            prefix,
            include_values=include_values,
            page_size=page_size,
            where=where,
            limit=limit,
        )
        for row in rows:
            leaf, value = row if include_values else (row, None)
            decoded = self.rescope(leaf)
            if decoded is None:
                continue
            scope, real_leaf = decoded
            yield scope, real_leaf, value

    def rescope(self, key: K) -> tuple[Scope, K] | None:
        """The whole scope and leaf of a key this store returned under its own scope.

        On a store bound to a prefix of the schema, a key it lists — by scan,
        or as expired — still carries the scope segments beneath that prefix.
        ``None`` for a key that does not decode under the schema.
        """
        me = cast(Any, self)
        return cast("tuple[Scope, K] | None", me.kvset.rescope(me.scope, key))

    def pushes_order(self, order_by: Sequence[OrderBy[Any]]) -> bool:
        """Whether this store's own scan gives ``order_by``, so it never sorts here.

        False by default: the store sorts its rows in process. A medium that
        keeps every field as a column answers True; a view that lists one
        store's rows under the same keys answers for that store, and hands
        it the order.
        """
        return False

    def sorts_in_process(self, spec: ScanSpec[K]) -> bool:
        """Whether a scan of ``spec`` here reads every row and sorts it."""
        return bool(spec.order_by) and not self.pushes_order(spec.order_by)

    def scan_in_process_order(self, spec: ScanSpec[K]) -> Iterator[ScanRow[K, V]]:
        """``spec`` by sorting every row this store's own scan keeps."""
        return scan_sorted(self.scan_rows, spec, backend=type(self).__name__)

    def explain_in_process_order(self, spec: ScanSpec[K]) -> ScanPlanNode:
        return explain_sorted(self.explain_node(unordered(spec)), spec)

    def scan_rows(self, spec: ScanSpec[K]) -> Iterator[ScanRow[K, V]]:
        """Yield ``ScanRow``s for ``spec`` (default: legacy ``scan_entries``)."""
        from kvtsjl.store.schema.scan import apply_cascade, spec_to_scan_query

        if self.sorts_in_process(spec):
            yield from self.scan_in_process_order(spec)
            return
        query = spec_to_scan_query(spec)
        raw = (
            ScanRow(key=key, value=value)
            for key, value in self.scan_entries(query)
        )
        store = cast(Any, self) if hasattr(self, "batch_get") else None
        yield from apply_cascade(
            raw,
            residual=spec.where,
            store=store,
            refine=spec.refine,
            limit=spec.limit,
            project=spec.project,
        )

    @overload
    def scan(
        self,
        prefix: K | None = None,
        *,
        include_values: Literal[False] = False,
        page_size: int = 100,
        where: Predicate[K] | None = None,
        limit: int | None = None,
        order_by: Sequence[OrderBy[V]] = (),
        after: After[K] | None = None,
        on_unsupported: Pushdown = Pushdown.CLIENT_SIDE,
    ) -> Iterator[K]: ...

    @overload
    def scan(
        self,
        prefix: K | None = None,
        *,
        include_values: Literal[True],
        page_size: int = 100,
        where: Predicate[K] | None = None,
        limit: int | None = None,
        order_by: Sequence[OrderBy[V]] = (),
        after: After[K] | None = None,
        on_unsupported: Pushdown = Pushdown.CLIENT_SIDE,
    ) -> Iterator[tuple[K, V]]: ...

    def scan(
        self,
        prefix: K | None = None,
        *,
        include_values: bool = False,
        page_size: int = 100,
        where: Predicate[K] | None = None,
        limit: int | None = None,
        project: Projection | None = None,
        refine: Callable[[ScanRow[K, Any]], bool] | None = None,
        order: ScanOrder = ScanOrder.UNORDERED,
        cursor: ScanCursor | None = None,
        on_unsupported: Pushdown = Pushdown.CLIENT_SIDE,
        spec: ScanSpec[K] | None = None,
        order_by: Sequence[OrderBy[V]] = (),
        after: After[K] | None = None,
    ) -> Iterator[K] | Iterator[tuple[K, V]]:
        if prefix is not None and not supports_prefix_scan(self.key_layout()):
            raise KvStoreScanUnsupported(
                f"prefix scan unsupported for layout {self.key_layout()!r}"
            )
        if spec is None:
            spec = sugar_to_spec(
                prefix=prefix,
                include_values=include_values,
                page_size=page_size,
                where=where,
                project=project,
                limit=limit,
                refine=refine,
                order=order,
                cursor=cursor,
                on_unsupported=on_unsupported,
                order_by=order_by,
                after=after,
            )
        if include_values or spec.project is Projection.ENTRIES:
            for row in self.scan_rows(spec):
                if row.value is None:
                    continue
                yield row.key, row.value
        else:
            for row in self.scan_rows(spec):
                yield row.key

    def list(self, prefix: K | None = None, *, where: Predicate[K] | None = None) -> list[K]:
        return list(self.scan(prefix=prefix, include_values=False, where=where))

    @overload
    def scan_batches(
        self,
        prefix: K | None = None,
        *,
        batch_size: int | None = None,
        include_values: Literal[False] = False,
        page_size: int = 100,
    ) -> Iterator[list[K]]: ...

    @overload
    def scan_batches(
        self,
        prefix: K | None = None,
        *,
        batch_size: int | None = None,
        include_values: Literal[True],
        page_size: int = 100,
    ) -> Iterator[list[tuple[K, V]]]: ...

    def scan_batches(
        self,
        prefix: K | None = None,
        *,
        batch_size: int | None = None,
        include_values: bool = False,
        page_size: int = 100,
        where: Predicate[K] | None = None,
    ) -> Iterator[list[K]] | Iterator[list[tuple[K, V]]]:
        size = batch_size or self.batch_size
        if include_values:
            batch_v: list[tuple[K, V]] = []
            for item in self.scan(
                prefix=prefix,
                include_values=True,
                page_size=page_size,
                where=where,
            ):
                batch_v.append(item)
                if len(batch_v) >= size:
                    yield batch_v
                    batch_v = []
            if batch_v:
                yield batch_v
        else:
            batch_k: list[K] = []
            for item in self.scan(
                prefix=prefix,
                include_values=False,
                page_size=page_size,
                where=where,
            ):
                batch_k.append(item)
                if len(batch_k) >= size:
                    yield batch_k
                    batch_k = []
            if batch_k:
                yield batch_k

    def count(self, where: Predicate[K] | None = None, **kwargs: Any) -> int:
        """Count keys matching ``where`` (KEYS projection + residual)."""
        spec = sugar_to_spec(where=where, project=Projection.KEYS, **kwargs)
        return sum(1 for _ in self.scan_rows(spec))

    def explain(
        self,
        where: Predicate[K] | None = None,
        *,
        spec: ScanSpec[K] | None = None,
        **kwargs: Any,
    ) -> ScanPlan:
        """Return a nested ``ScanPlan`` describing pushdown vs residual."""
        if spec is None:
            spec = sugar_to_spec(where=where, **kwargs)
        return ScanPlan(root=self.explain_node(spec))

    def explain_node(self, spec: ScanSpec[K]) -> ScanPlanNode:
        if self.sorts_in_process(spec):
            return self.explain_in_process_order(spec)
        compile_fn = getattr(self, "compile_scan", None)
        if callable(compile_fn):
            compiled = cast(CompiledScan[K], compile_fn(spec))
            return ScanPlanNode(
                name=type(self).__name__,
                pushed=compiled.pushed,
                residual=predicate_label(compiled.residual),
            )
        prefix, residual = peel_key_prefix(spec.where)
        pushed = frozenset({"KeyPrefix"} if prefix is not None else ())
        return ScanPlanNode(
            name=type(self).__name__,
            pushed=pushed,
            residual=predicate_label(residual),
        )


class ExpiringMixin[K](ABC):
    """Explicit GC of expired entries via ``delete_where(Expired())``."""

    def expired_keys(self, *, now: datetime | None = None, limit: int) -> list[K]:
        """Up to ``limit`` expired keys, oldest expiry first; read-only.

        The primitive behind ``gc_expired_keys``, exposed so a caller can fold
        or clean up related rows before deleting. Empty by default, which is
        correct for a medium that expires entries itself.

        Cost follows the medium: SQL pushes the predicate down, memory walks
        its dict, and an object store lists — there is no query for "which
        objects expired", so it is the same scan the deletion always did.
        """
        if limit < 1:
            raise ValueError(f"limit must be >= 1, got {limit}")
        return []

    def gc_expired_keys(self, *, max_entries: int) -> list[K]:
        """Backend/fanout hook: delete up to ``max_entries`` expired keys.

        Prefer public ``gc_expired`` / ``delete_where(Expired(), …)``. Kept for
        budgeted fan-out and leaf implementations.
        """
        if max_entries < 1:
            raise ValueError(f"max_entries must be >= 1, got {max_entries}")
        return []

    def delete_where(
        self,
        where: Predicate[K],
        *,
        limit: int | None = None,
        sync_policy: SyncPolicy | None = None,
    ) -> int:
        """Delete keys matching ``where``.

        When ``where`` is GC (``Expired`` or And/Or solely of ``Expired``),
        fan-out sync stays ``BEST_EFFORT`` unless ``sync_policy`` overrides.
        Non-Expired predicates honor the store's configured policy (or
        ``sync_policy`` when set).
        """
        del sync_policy  # leaves ignore; indexed/mirror override
        if is_gc_predicate(where):
            max_entries = limit if limit is not None else 2_147_483_647
            if max_entries < 1:
                raise ValueError(f"limit must be >= 1 for GC, got {limit}")
            return len(self.gc_expired_keys(max_entries=max_entries))
        scan_self = cast(ScannableMixin[K, Any], self)
        writable = cast(Any, self)
        keys = [
            row.key
            for row in scan_self.scan_rows(
                ScanSpec(where=where, project=Projection.KEYS, limit=limit)
            )
        ]
        if not keys:
            return 0
        return int(writable.batch_delete(keys))

    def gc_expired(self, *, max_entries: int) -> int:
        """Delete expired entries under this scope, up to ``max_entries``."""
        if max_entries < 1:
            raise ValueError(f"max_entries must be >= 1, got {max_entries}")
        return self.delete_where(Expired(), limit=max_entries)


class TtlWritesMixin[K, V](ABC):
    """Writes that may carry per-write ``ttl=`` / ``created_at=``."""

    @property
    def supports_write_clock(self) -> bool:
        return False

    @abstractmethod
    def set(
        self,
        key: K,
        value: V,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        """Insert or replace ``key``. ``ttl=None`` uses the collection policy."""

    def batch_set(
        self,
        items: Mapping[K, V],
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        require_write_clock_supported(
            created_at,
            allowed=self.supports_write_clock,
            backend=type(self).__name__,
        )
        for key, value in items.items():
            self.set(key, value, ttl=ttl, created_at=created_at)
