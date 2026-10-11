"""Fan-out mixins for composed KvStores (scope / scan / GC).

Not exported in package ``__all__`` — same rule as capability mixins.

Helpers are typed against capability ABCs already in every store's MRO
(``ReadableKeyMap``, ``ScannableMixin``, ``ExpiringMixin``, ``ScopedMixin``),
not structural ``Protocol``s — owned types should appear in the implementor's
inheritance graph so a missing method is a type error, not a silent mismatch.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator, Sequence
from typing import Any, Self

from kvtsjl.exceptions import KvStoreScanUnsupported
from kvtsjl.keymap import ReadableKeyMap
from kvtsjl.scope import Scope
from kvtsjl.store.capabilities import ExpiringMixin, ScannableMixin, ScopedMixin
from kvtsjl.store.readable import ReadableKvStore
from kvtsjl.store.schema.layout import ScanQuery
from kvtsjl.store.schema.scan import (
    Needs,
    Projection,
    ScanOrder,
    ScanRow,
    ScanSpec,
    apply_cascade,
    split_key_predicates,
)


def iter_paged_batch_get[K, V](
    store: ReadableKeyMap[K, V],
    keys: Iterator[K],
    *,
    page_size: int,
    include_values: bool,
) -> Iterator[tuple[K, V | None]]:
    """Yield ``(key, value|None)``; values come from paged ``batch_get``."""
    if not include_values:
        for key in keys:
            yield key, None
        return
    page: list[K] = []
    for key in keys:
        page.append(key)
        if len(page) >= page_size:
            found = store.batch_get(page)
            for k in page:
                yield k, found.get(k)
            page = []
    if page:
        found = store.batch_get(page)
        for k in page:
            yield k, found.get(k)


def iter_union_keys[K](
    parts: Sequence[ScannableMixin[K, Any]],
    query: ScanQuery[K],
) -> Iterator[K]:
    """Key-union across parts (order: first-seen walking parts in order).

    Holds every distinct key in ``seen`` for the lifetime of the scan — O(total
    unique keys) memory. Same as the pre-P6 per-operator ``_union_keys``; not a
    regression, but **not streaming-safe at scale**.

    P7 keeps this ``seen`` set for unordered zip: memory and FS scans are
    unordered (insertion / readdir), so merge-dedupe cannot stream without
    sorted ``KEY_ASC`` leaves. When every part's ``ScanCapabilities.orders``
    contains ``KEY_ASC`` *and* the zip is explicitly allowed that order, a
    heap-merge without full ``seen`` becomes viable — out of the default P7 path.
    """
    seen: set[K] = set()
    key_only = ScanQuery(
        prefix=query.prefix,
        include_values=False,
        page_size=query.page_size,
    )
    for part in parts:
        for key, _ in part.scan_entries(key_only):
            if key not in seen:
                seen.add(key)
                yield key


def iter_union_keys_spec[K](
    parts: Sequence[ScannableMixin[K, Any]],
    key_spec: ScanSpec[K],
) -> Iterator[K]:
    """Key-union via part ``scan_rows`` (forwards key predicates into each part)."""
    seen: set[K] = set()
    for part in parts:
        for row in part.scan_rows(key_spec):
            if row.key not in seen:
                seen.add(row.key)
                yield row.key


def gc_across_parts[K](
    parts: Sequence[ExpiringMixin[K]],
    *,
    max_entries: int,
) -> list[K]:
    """Budget-capped GC across parts; dedupe keys (order-preserving)."""
    if max_entries < 1:
        raise ValueError(f"max_entries must be >= 1, got {max_entries}")
    deleted: list[K] = []
    remaining = max_entries
    for part in parts:
        if remaining < 1:
            break
        deleted.extend(part.gc_expired_keys(max_entries=remaining))
        remaining = max_entries - len(deleted)
    seen: set[K] = set()
    unique: list[K] = []
    for key in deleted:
        if key not in seen:
            seen.add(key)
            unique.append(key)
    return unique[:max_entries]


def _part_key_spec[K](spec: ScanSpec[K], key_where: Any) -> ScanSpec[K]:
    return ScanSpec(
        where=key_where,
        project=Projection.KEYS,
        order=spec.order,
        page_size=spec.page_size,
        cursor=spec.cursor,
        on_unsupported=spec.on_unsupported,
    )


def _explain_parts[K](
    parts: Sequence[ScannableMixin[K, Any]],
    spec: ScanSpec[K],
    *,
    name: str,
    residual_label: str,
) -> Any:
    from kvtsjl.store.schema.scan import ScanPlanNode

    children = tuple(part.explain_node(spec) for part in parts)
    return ScanPlanNode(
        name=name,
        pushed=frozenset({"key→parts"}),
        residual=residual_label,
        children=children,
    )


def _needs_values[K](spec: ScanSpec[K], residual: Any) -> bool:
    if spec.project is Projection.ENTRIES:
        return True
    if residual.needs() & Needs.VALUE:
        return True
    return False


def _require_unordered_zip[K](spec: ScanSpec[K], op: str) -> None:
    if spec.order is not ScanOrder.UNORDERED:
        raise KvStoreScanUnsupported(
            f"{op} scan supports ScanOrder.UNORDERED only "
            f"(got ScanOrder.{spec.order.name})"
        )


class ScanPassthroughMixin[K, V](ReadableKeyMap[K, V], ABC):
    """Scan keys from a single underlying; values via ``self.batch_get`` pages.

    Assembled onto a store that is already a ``ReadableKeyMap`` (provides
    ``batch_get``) with a ``ScannableMixin`` ``_scan_src``.
    """

    _scan_src: ScannableMixin[K, Any]

    def scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        keys = (
            key
            for key, _ in self._scan_src.scan_entries(
                ScanQuery(
                    prefix=query.prefix,
                    include_values=False,
                    page_size=query.page_size,
                )
            )
        )
        yield from iter_paged_batch_get(
            self,
            keys,
            page_size=query.page_size,
            include_values=query.include_values,
        )

    def scan_rows(self, spec: ScanSpec[K]) -> Iterator[ScanRow[K, V]]:
        key_where, residual = split_key_predicates(spec.where)
        part_spec = _part_key_spec(spec, key_where)
        keys = (row.key for row in self._scan_src.scan_rows(part_spec))
        need_values = _needs_values(spec, residual)
        raw = (
            ScanRow(key=k, value=v)
            for k, v in iter_paged_batch_get(
                self,
                keys,
                page_size=spec.page_size,
                include_values=need_values,
            )
        )
        yield from apply_cascade(
            raw,
            residual=residual,
            store=self,
            refine=spec.refine,
            limit=spec.limit,
            project=spec.project,
        )

    def explain_node(self, spec: ScanSpec[K]) -> Any:
        from kvtsjl.store.schema.scan import predicate_label

        key_where, residual = split_key_predicates(spec.where)
        part_spec = _part_key_spec(spec, key_where)
        return _explain_parts(
            [self._scan_src],
            part_spec,
            name=type(self).__name__,
            residual_label=predicate_label(residual),
        )


class ScanUnionMixin[K, V](ReadableKeyMap[K, V], ABC):
    """Key-union scan across ``_scan_parts``; values via ``self.batch_get`` pages."""

    _scan_parts: Sequence[ScannableMixin[K, Any]]

    def scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        yield from iter_paged_batch_get(
            self,
            iter_union_keys(self._scan_parts, query),
            page_size=query.page_size,
            include_values=query.include_values,
        )

    def scan_rows(self, spec: ScanSpec[K]) -> Iterator[ScanRow[K, V]]:
        _require_unordered_zip(spec, "zip")
        key_where, residual = split_key_predicates(spec.where)
        part_spec = _part_key_spec(spec, key_where)
        keys = iter_union_keys_spec(self._scan_parts, part_spec)
        need_values = _needs_values(spec, residual)
        raw = (
            ScanRow(key=k, value=v)
            for k, v in iter_paged_batch_get(
                self,
                keys,
                page_size=spec.page_size,
                include_values=need_values,
            )
        )
        yield from apply_cascade(
            raw,
            residual=residual,
            store=self,
            refine=spec.refine,
            limit=spec.limit,
            project=spec.project,
        )

    def explain_node(self, spec: ScanSpec[K]) -> Any:
        from kvtsjl.store.schema.scan import predicate_label

        _require_unordered_zip(spec, "zip")
        key_where, residual = split_key_predicates(spec.where)
        part_spec = _part_key_spec(spec, key_where)
        return _explain_parts(
            list(self._scan_parts),
            part_spec,
            name=type(self).__name__,
            residual_label=predicate_label(residual),
        )


class ScanPrimaryMixin[K, V]:
    """Scan keys from primary; values via ``primary.batch_get`` only (no promote).

    ``_scan_primary`` is a ``ReadableKvStore`` (scannable + ``batch_get``).
    Must **not** call ``self.batch_get`` — coalesce/fallback promote on that path.
    """

    _scan_primary: ReadableKvStore[K, V]

    def scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        keys = (
            key
            for key, _ in self._scan_primary.scan_entries(
                ScanQuery(
                    prefix=query.prefix,
                    include_values=False,
                    page_size=query.page_size,
                )
            )
        )
        yield from iter_paged_batch_get(
            self._scan_primary,
            keys,
            page_size=query.page_size,
            include_values=query.include_values,
        )

    def scan_rows(self, spec: ScanSpec[K]) -> Iterator[ScanRow[K, V]]:
        key_where, residual = split_key_predicates(spec.where)
        part_spec = _part_key_spec(spec, key_where)
        keys = (row.key for row in self._scan_primary.scan_rows(part_spec))
        need_values = _needs_values(spec, residual)
        raw = (
            ScanRow(key=k, value=v)
            for k, v in iter_paged_batch_get(
                self._scan_primary,
                keys,
                page_size=spec.page_size,
                include_values=need_values,
            )
        )
        yield from apply_cascade(
            raw,
            residual=residual,
            store=self._scan_primary,
            refine=spec.refine,
            limit=spec.limit,
            project=spec.project,
        )

    def explain_node(self, spec: ScanSpec[K]) -> Any:
        from kvtsjl.store.schema.scan import predicate_label

        key_where, residual = split_key_predicates(spec.where)
        part_spec = _part_key_spec(spec, key_where)
        return _explain_parts(
            [self._scan_primary],
            part_spec,
            name=type(self).__name__,
            residual_label=predicate_label(residual),
        )


class GcPassthroughMixin[K]:
    """GC delegates to a single underlying (``_gc_src``)."""

    _gc_src: ExpiringMixin[K]

    def gc_expired_keys(self, *, max_entries: int) -> list[K]:
        return self._gc_src.gc_expired_keys(max_entries=max_entries)


class GcFanoutMixin[K]:
    """Budget-capped GC across ``_gc_parts``."""

    _gc_parts: Sequence[ExpiringMixin[K]]

    def gc_expired_keys(self, *, max_entries: int) -> list[K]:
        return gc_across_parts(self._gc_parts, max_entries=max_entries)


class ScopeFanoutMixin(ABC):
    """``clone_with_scope`` via ``_clone_fanout(scope)`` implemented by the class.

    Both hooks return ``Self`` so a composed operator's clone is checked as the
    same type (P1: ``scoped`` / ``clone_with_scope`` dropping state was the
    fragile path — ``Any`` would hide a wrong return type the same way a
    dropped kwarg hid missing config).
    """

    def clone_with_scope(self, scope: Scope) -> Self:
        return self._clone_fanout(scope)

    @abstractmethod
    def _clone_fanout(self, scope: Scope) -> Self:
        """Rebuild this operator from parts / src cloned under ``scope``."""


def clone_parts[T: ScopedMixin](parts: Sequence[T], scope: Scope) -> list[T]:
    return [p.clone_with_scope(scope) for p in parts]


def clone_parts_mapping[T: ScopedMixin](
    parts: dict[str, T], scope: Scope
) -> dict[str, T]:
    return {name: store.clone_with_scope(scope) for name, store in parts.items()}


__all__: list[str] = []  # intentionally empty — internal module
