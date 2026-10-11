"""Capability mixins for store composition (not part of the public ``__all__``)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator, Mapping
from datetime import datetime
from typing import Any, Literal, Self, cast, overload

from kvtsjl.exceptions import KvStoreScanUnsupported, KvStoreScopeError
from kvtsjl.scope import Scope
from kvtsjl.store.schema.layout import KeyLayout, ScanQuery, supports_prefix_scan
from kvtsjl.store.schema.policy import SyncPolicy
from kvtsjl.store.schema.scan import (
    CompiledScan,
    Expired,
    Predicate,
    Projection,
    Pushdown,
    ScanCursor,
    ScanOrder,
    ScanPlan,
    ScanPlanNode,
    ScanRow,
    ScanSpec,
    is_gc_predicate,
    peel_key_prefix,
    predicate_label,
    sugar_to_spec,
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

    def scan_rows(self, spec: ScanSpec[K]) -> Iterator[ScanRow[K, V]]:
        """Yield ``ScanRow``s for ``spec`` (default: legacy ``scan_entries``)."""
        from kvtsjl.store.schema.scan import apply_cascade, spec_to_scan_query

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
