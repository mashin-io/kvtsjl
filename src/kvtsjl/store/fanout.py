"""Fan-out helpers for composed KvStores (scope / scan / GC passthrough).

Not exported in package ``__all__`` — same rule as capability mixins.

Helpers are typed against capability ABCs already in every store's MRO
(``ReadableKeyMap``, ``ScannableMixin``, ``ExpiringMixin``, ``ScopedMixin``),
not structural ``Protocol``s — owned types should appear in the implementor's
inheritance graph so a missing method is a type error, not a silent mismatch.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator, Sequence
from typing import Any, Self

from kvtsjl.exceptions import KvStoreScanUnsupported
from kvtsjl.keymap import ReadableKeyMap
from kvtsjl.scope import Scope
from kvtsjl.store.capabilities import ExpiringMixin, ScannableMixin, ScopedMixin
from kvtsjl.store.schema.layout import ScanQuery
from kvtsjl.store.schema.scan import (
    AlwaysTrue,
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


def iter_present[K, V](
    store: ReadableKeyMap[K, V],
    keys: Iterator[K],
    *,
    page_size: int,
    include_values: bool,
) -> Iterator[tuple[K, V | None]]:
    """Yield the keys that ``store`` confirms, a page at a time.

    For a composition whose keys come from somewhere other than the record
    itself — the parts of an assembled record — a key found is a candidate,
    not a record: it is yielded only if ``batch_get`` (or ``batch_exists``,
    when no values are wanted) agrees, so a scan never lists what ``get``
    would not return.
    """
    page: list[K] = []

    def confirm(batch: list[K]) -> Iterator[tuple[K, V | None]]:
        if include_values:
            found = store.batch_get(batch)
            return ((k, found[k]) for k in batch if k in found)
        present = store.batch_exists(batch)
        return ((k, None) for k in batch if k in present)

    for key in keys:
        page.append(key)
        if len(page) >= page_size:
            yield from confirm(page)
            page = []
    if page:
        yield from confirm(page)


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


def part_key_spec[K](spec: ScanSpec[K], key_where: Any) -> ScanSpec[K]:
    """What the store whose keys are listed is asked: its keys, in the order asked.

    The order — ``order`` or ``order_by`` — travels with the keys, so a view
    that lists another store's keys returns them in that store's order, and a
    store that cannot honour it refuses. Dropping it silently returned key
    order to a caller who asked for something else.
    """
    return ScanSpec(
        where=key_where,
        project=Projection.KEYS,
        order=spec.order,
        order_by=spec.order_by,
        page_size=spec.page_size,
        cursor=spec.cursor,
        on_unsupported=spec.on_unsupported,
    )


def explain_parts[K](
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


def needs_values[K](spec: ScanSpec[K], residual: Any) -> bool:
    if spec.project is Projection.ENTRIES:
        return True
    if residual.needs() & Needs.VALUE:
        return True
    return False


def scan_values_through[K, V, U](
    view: ReadableKeyMap[K, U],
    src: ScannableMixin[K, V],
    spec: ScanSpec[K],
    forward: Callable[[V], U],
) -> Iterator[ScanRow[K, U]]:
    """A view of ``src`` with its values mapped: ``src``'s rows, in the order asked.

    What concerns keys travels to ``src`` — key predicates, the order, the
    page size, the cursor — and ``src`` refuses an order it cannot give;
    ``order_by`` names ``src``'s own columns. What reads a value runs on the
    view's values. ``limit`` travels only when nothing is left to filter:
    pushed under a filter, it would count rows the filter then drops.
    """
    key_where, residual = split_key_predicates(spec.where)
    unfiltered = isinstance(residual, AlwaysTrue) and spec.refine is None
    down = ScanSpec[K](
        where=key_where,
        project=Projection.ENTRIES if needs_values(spec, residual) else spec.project,
        order=spec.order,
        order_by=spec.order_by,
        limit=spec.limit if unfiltered else None,
        page_size=spec.page_size,
        cursor=spec.cursor,
        on_unsupported=spec.on_unsupported,
    )
    rows = (
        ScanRow(key=r.key, value=None if r.value is None else forward(r.value), stat=r.stat)
        for r in src.scan_rows(down)
    )
    yield from apply_cascade(
        rows,
        residual=residual,
        store=view,
        refine=spec.refine,
        limit=spec.limit,
        project=spec.project,
    )


def require_unordered[K](spec: ScanSpec[K], op: str) -> None:
    """Refuse an order over keys merged from several stores: no one order holds."""
    if spec.order is not ScanOrder.UNORDERED:
        raise KvStoreScanUnsupported(
            f"{op} scan supports ScanOrder.UNORDERED only "
            f"(got ScanOrder.{spec.order.name})"
        )
    if spec.order_by:
        raise KvStoreScanUnsupported(
            f"{op} scan merges keys from several stores, so no one store's "
            "columns order it; give the record a commit part to order by"
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
        part_spec = part_key_spec(spec, key_where)
        keys = (row.key for row in self._scan_src.scan_rows(part_spec))
        need_values = needs_values(spec, residual)
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
        part_spec = part_key_spec(spec, key_where)
        return explain_parts(
            [self._scan_src],
            part_spec,
            name=type(self).__name__,
            residual_label=predicate_label(residual),
        )


class GcPassthroughMixin[K]:
    """GC delegates to a single underlying (``_gc_src``)."""

    _gc_src: ExpiringMixin[K]

    def gc_expired_keys(self, *, max_entries: int) -> list[K]:
        return self._gc_src.gc_expired_keys(max_entries=max_entries)


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
