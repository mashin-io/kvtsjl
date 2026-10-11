"""ScanSpec, Predicate ADT, cascade evaluation, and compile contract."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, IntFlag, auto
from fnmatch import fnmatch
from typing import TYPE_CHECKING, Any

from kvtsjl.exceptions import KvStoreScanUnsupported
from kvtsjl.store.schema.layout import ScanQuery

if TYPE_CHECKING:  # annotation only — kvtsjl.keymap imports this package
    from kvtsjl.keymap import ReadableKeyMap


class Needs(IntFlag):
    """What a predicate needs from a row to evaluate."""

    KEY = auto()
    STAT = auto()
    VALUE = auto()


class Projection(str, Enum):
    KEYS = "keys"
    STATS = "stats"
    ENTRIES = "entries"


class ScanOrder(str, Enum):
    UNORDERED = "unordered"
    KEY_ASC = "key_asc"
    KEY_DESC = "key_desc"


@dataclass(frozen=True, slots=True)
class OrderBy:
    """Order by one declared column (see ``ScanSpec.order_by``)."""

    column: str
    descending: bool = False


class Pushdown(str, Enum):
    CLIENT_SIDE = "client_side"
    STRICT = "strict"


@dataclass(frozen=True, slots=True)
class ScanStat:
    size: int | None = None
    modified_at: datetime | None = None
    expires_at: datetime | None = None
    etag: str | None = None


@dataclass(frozen=True, slots=True)
class ScanRow[K, V]:
    key: K
    value: V | None = None
    stat: ScanStat | None = None


@dataclass(frozen=True, slots=True)
class ScanCursor:
    """Opaque leaf resume token (Redis cursor, S3 ContinuationToken, etc.)."""

    token: str
    leaf: str = ""


@dataclass(frozen=True, slots=True)
class ScanCapabilities:
    pushdown: frozenset[type]
    orders: frozenset[ScanOrder]
    resumable: bool
    stats: bool
    orderable_columns: frozenset[str] | None = None
    """Columns usable in ``ScanSpec.order_by``; ``None`` means unsupported."""


@dataclass(frozen=True, slots=True)
class CompiledScan[K]:
    rows: Iterator[ScanRow[K, Any]]
    residual: Predicate[K]
    pushed: frozenset[str] = field(default_factory=lambda: frozenset())


# --- Predicate ADT -----------------------------------------------------------


class Predicate[K](ABC):
    """Declarative filter; backends may push down a subset and return a residual."""

    @abstractmethod
    def needs(self) -> Needs: ...

    @abstractmethod
    def evaluate(self, row: ScanRow[K, Any]) -> bool: ...

    def and_(self, other: Predicate[K]) -> Predicate[K]:
        return And(self, other)

    def or_(self, other: Predicate[K]) -> Predicate[K]:
        return Or(self, other)

    def invert(self) -> Predicate[K]:
        return Not(self)

    def __and__(self, other: Predicate[K]) -> Predicate[K]:
        return self.and_(other)

    def __or__(self, other: Predicate[K]) -> Predicate[K]:
        return self.or_(other)

    def __invert__(self) -> Predicate[K]:
        return self.invert()


class AlwaysTrue[K](Predicate[K]):
    def needs(self) -> Needs:
        return Needs(0)

    def evaluate(self, row: ScanRow[K, Any]) -> bool:
        return True


class And[K](Predicate[K]):
    def __init__(self, left: Predicate[K], right: Predicate[K]) -> None:
        self.left = left
        self.right = right

    def needs(self) -> Needs:
        return self.left.needs() | self.right.needs()

    def evaluate(self, row: ScanRow[K, Any]) -> bool:
        return self.left.evaluate(row) and self.right.evaluate(row)


class Or[K](Predicate[K]):
    def __init__(self, left: Predicate[K], right: Predicate[K]) -> None:
        self.left = left
        self.right = right

    def needs(self) -> Needs:
        return self.left.needs() | self.right.needs()

    def evaluate(self, row: ScanRow[K, Any]) -> bool:
        return self.left.evaluate(row) or self.right.evaluate(row)


class Not[K](Predicate[K]):
    def __init__(self, inner: Predicate[K]) -> None:
        self.inner = inner

    def needs(self) -> Needs:
        return self.inner.needs()

    def evaluate(self, row: ScanRow[K, Any]) -> bool:
        return not self.inner.evaluate(row)


class KeyPredicate[K](Predicate[K], ABC):
    def needs(self) -> Needs:
        return Needs.KEY


class StatPredicate[K](Predicate[K], ABC):
    def needs(self) -> Needs:
        return Needs.STAT


class ValuePredicate[K](Predicate[K], ABC):
    def needs(self) -> Needs:
        return Needs.VALUE


@dataclass(frozen=True, slots=True)
class KeyPrefix[K](KeyPredicate[K]):
    prefix: K

    def evaluate(self, row: ScanRow[K, Any]) -> bool:
        key, prefix = row.key, self.prefix
        if isinstance(key, str) and isinstance(prefix, str):
            return key.startswith(prefix)
        if isinstance(key, bytes) and isinstance(prefix, bytes):
            return key.startswith(prefix)
        return str(key).startswith(str(prefix))


def _cmp_ordered_keys(a: object, b: object) -> int:
    """Total order for KeyRange without Protocols or ``type: ignore``.

    Supports the common leaf key types (``str`` / ``bytes`` / ``int`` / ``float``).
    Other types need an explicit ``KeyRange(..., compare=...)``.
    """
    if isinstance(a, str) and isinstance(b, str):
        return (a > b) - (a < b)
    if isinstance(a, bytes) and isinstance(b, bytes):
        return (a > b) - (a < b)
    if isinstance(a, int) and isinstance(b, int) and not isinstance(a, bool) and not isinstance(
        b, bool
    ):
        return (a > b) - (a < b)
    if isinstance(a, float) and isinstance(b, float):
        return (a > b) - (a < b)
    raise TypeError(
        f"KeyRange requires ordered keys (str/bytes/int/float) or compare=; "
        f"got {type(a).__name__} vs {type(b).__name__}"
    )


@dataclass(frozen=True, slots=True)
class KeyRange[K](KeyPredicate[K]):
    start: K | None = None
    end: K | None = None
    include_start: bool = True
    include_end: bool = False
    compare: Callable[[K, K], int] | None = field(
        default=None, compare=False, hash=False
    )

    def _cmp(self, left: K, right: K) -> int:
        if self.compare is not None:
            return self.compare(left, right)
        return _cmp_ordered_keys(left, right)

    def evaluate(self, row: ScanRow[K, Any]) -> bool:
        key = row.key
        if self.start is not None:
            c = self._cmp(key, self.start)
            if self.include_start:
                if c < 0:
                    return False
            elif c <= 0:
                return False
        if self.end is not None:
            c = self._cmp(key, self.end)
            if self.include_end:
                if c > 0:
                    return False
            elif c >= 0:
                return False
        return True


@dataclass(frozen=True, slots=True)
class KeyIn[K](KeyPredicate[K]):
    keys: frozenset[K]

    def evaluate(self, row: ScanRow[K, Any]) -> bool:
        return row.key in self.keys


@dataclass(frozen=True, slots=True)
class KeyMatches[K](KeyPredicate[K]):
    """Glob pattern (not regex) — maps to Redis MATCH / SQL LIKE."""

    pattern: str

    def evaluate(self, row: ScanRow[K, Any]) -> bool:
        return fnmatch(str(row.key), self.pattern)


@dataclass(frozen=True, slots=True)
class ModifiedBefore[K](StatPredicate[K]):
    when: datetime

    def evaluate(self, row: ScanRow[K, Any]) -> bool:
        if row.stat is None or row.stat.modified_at is None:
            return False
        return row.stat.modified_at < self.when


@dataclass(frozen=True, slots=True)
class ModifiedAfter[K](StatPredicate[K]):
    when: datetime

    def evaluate(self, row: ScanRow[K, Any]) -> bool:
        if row.stat is None or row.stat.modified_at is None:
            return False
        return row.stat.modified_at > self.when


@dataclass(frozen=True, slots=True)
class SizeBetween[K](StatPredicate[K]):
    min_size: int | None = None
    max_size: int | None = None

    def evaluate(self, row: ScanRow[K, Any]) -> bool:
        if row.stat is None or row.stat.size is None:
            return False
        size = row.stat.size
        if self.min_size is not None and size < self.min_size:
            return False
        if self.max_size is not None and size > self.max_size:
            return False
        return True


@dataclass(frozen=True, slots=True)
class Expired[K](StatPredicate[K]):
    """Row is past expiry (or marked expired by the leaf). Used by GC / delete_where."""

    def evaluate(self, row: ScanRow[K, Any]) -> bool:
        if row.stat is None or row.stat.expires_at is None:
            return False
        return row.stat.expires_at <= datetime.now(tz=row.stat.expires_at.tzinfo)


@dataclass(frozen=True, slots=True)
class ValueWhere[K](ValuePredicate[K]):
    predicate: Callable[[Any], bool]

    def evaluate(self, row: ScanRow[K, Any]) -> bool:
        if row.value is None:
            return False
        return self.predicate(row.value)


@dataclass(frozen=True, slots=True)
class ScanSpec[K]:
    where: Predicate[K] = field(default_factory=lambda: AlwaysTrue())
    refine: Callable[[ScanRow[K, Any]], bool] | None = None
    project: Projection = Projection.KEYS
    order: ScanOrder = ScanOrder.UNORDERED
    limit: int | None = None
    page_size: int = 100
    cursor: ScanCursor | None = None
    on_unsupported: Pushdown = Pushdown.CLIENT_SIDE
    order_by: Sequence[OrderBy] = ()

    def __post_init__(self) -> None:
        if not self.order_by:
            return
        # Keep the frozen spec hashable when built directly with a list.
        if not isinstance(self.order_by, tuple):
            object.__setattr__(self, "order_by", tuple(self.order_by))
        # ``order`` always has a value (defaults to UNORDERED), so a literal
        # "both set" check would reject every order_by call.
        if self.order is not ScanOrder.UNORDERED:
            raise ValueError(
                "pass order= (key order) or order_by= (column order), not both"
            )
        if self.cursor is not None:
            raise KvStoreScanUnsupported(
                "order_by with cursor is unsupported: the cursor is key-shaped "
                "and would reorder pages. Scan without a cursor, or drop order_by."
            )


def sugar_to_spec[K](
    *,
    prefix: K | None = None,
    include_values: bool = False,
    page_size: int = 100,
    where: Predicate[K] | None = None,
    project: Projection | None = None,
    limit: int | None = None,
    cursor: ScanCursor | None = None,
    order: ScanOrder = ScanOrder.UNORDERED,
    refine: Callable[[ScanRow[K, Any]], bool] | None = None,
    on_unsupported: Pushdown = Pushdown.CLIENT_SIDE,
    order_by: Sequence[OrderBy] = (),
) -> ScanSpec[K]:
    """Build a ``ScanSpec`` from sugar kwargs and/or an explicit ``where``."""
    pred: Predicate[K] = AlwaysTrue()
    if where is not None:
        pred = where
    if prefix is not None:
        pred = pred & KeyPrefix(prefix) if not isinstance(pred, AlwaysTrue) else KeyPrefix(prefix)
    proj = project
    if proj is None:
        proj = Projection.ENTRIES if include_values else Projection.KEYS
    return ScanSpec(
        where=pred,
        refine=refine,
        project=proj,
        order=order,
        limit=limit,
        page_size=page_size,
        cursor=cursor,
        on_unsupported=on_unsupported,
        order_by=tuple(order_by),
    )


def enforce_order_by_supported[K](
    spec: ScanSpec[K],
    capabilities: ScanCapabilities | None,
    *,
    backend: str,
) -> None:
    """Reject ``order_by`` where it cannot be pushed down.

    Unlike predicates, ``order_by`` does **not** fall back to the client
    cascade, and it raises even under ``Pushdown.CLIENT_SIDE``. Filtering a
    stream costs nothing extra; sorting has to materialise the whole result
    first, which would quietly turn a streaming scan into an unbounded buffer.
    Do not add a client-side sort fallback.
    """
    if not spec.order_by:
        return
    orderable = capabilities.orderable_columns if capabilities is not None else None
    if orderable is None:
        raise KvStoreScanUnsupported(
            f"{backend} does not support order_by (no orderable columns); "
            "order_by is never applied client-side"
        )
    unknown = [item.column for item in spec.order_by if item.column not in orderable]
    if unknown:
        raise KvStoreScanUnsupported(
            f"{backend} order_by column(s) {unknown!r} are not declared "
            f"(have {sorted(orderable)!r})"
        )


def compile_from_scan_entries[K, V](
    scan_entries: Callable[[ScanQuery[K]], Iterator[tuple[K, V | None]]],
    spec: ScanSpec[K],
    *,
    backend: str,
) -> CompiledScan[K]:
    """Default ``compile_scan``: peel a KeyPrefix, stream legacy ``scan_entries``.

    Shared by ``LeafKvStore`` and by leaves that only specialise part of the
    spec (e.g. SQL handles ``order_by`` natively and defers the rest here).
    """
    prefix, residual = peel_key_prefix(spec.where)
    query = ScanQuery(
        prefix=prefix,
        include_values=spec.project is Projection.ENTRIES,
        page_size=spec.page_size,
    )
    rows_iter = (ScanRow(key=key, value=value) for key, value in scan_entries(query))
    if spec.cursor is not None:
        token = spec.cursor.token

        def after_cursor() -> Iterator[ScanRow[K, V]]:
            started = False
            for row in rows_iter:
                if not started:
                    if str(row.key) == token:
                        started = True
                    continue
                yield row

        rows: Iterator[ScanRow[K, V]] = after_cursor()
    else:
        rows = rows_iter
    enforce_strict(spec, residual, backend=backend)
    return CompiledScan(
        rows=rows,
        residual=residual,
        pushed=frozenset({"KeyPrefix"} if prefix is not None else ()),
    )


def spec_to_scan_query[K](spec: ScanSpec[K]) -> ScanQuery[K]:
    """Temporary adapter: extract KeyPrefix + ENTRIES for legacy leaf scans."""
    prefix = extract_key_prefix(spec.where)
    return ScanQuery(
        prefix=prefix,
        include_values=spec.project is Projection.ENTRIES,
        page_size=spec.page_size,
    )


def extract_key_prefix[K](where: Predicate[K]) -> K | None:
    """Best-effort single ``KeyPrefix`` for legacy adapters."""
    if isinstance(where, KeyPrefix):
        return where.prefix
    if isinstance(where, And):
        left = extract_key_prefix(where.left)
        right = extract_key_prefix(where.right)
        return left if left is not None else right
    return None


def peel_key_prefix[K](
    where: Predicate[K],
) -> tuple[K | None, Predicate[K]]:
    """Extract one ``KeyPrefix`` for listing pushdown; return residual without it."""
    if isinstance(where, KeyPrefix):
        return where.prefix, AlwaysTrue()
    if isinstance(where, And):
        lp, lr = peel_key_prefix(where.left)
        if lp is not None:
            if isinstance(lr, AlwaysTrue):
                return lp, where.right
            return lp, And(lr, where.right)
        rp, rr = peel_key_prefix(where.right)
        if rp is not None:
            if isinstance(rr, AlwaysTrue):
                return rp, where.left
            return rp, And(where.left, rr)
        return None, where
    return None, where


def is_key_only_predicate[K](pred: Predicate[K]) -> bool:
    if isinstance(pred, AlwaysTrue):
        return True
    if isinstance(pred, KeyPredicate):
        return True
    if isinstance(pred, And | Or):
        return is_key_only_predicate(pred.left) and is_key_only_predicate(pred.right)
    if isinstance(pred, Not):
        return is_key_only_predicate(pred.inner)
    return False


def split_key_predicates[K](
    where: Predicate[K],
) -> tuple[Predicate[K], Predicate[K]]:
    """Split ``where`` into part-forwardable key preds and residual.

    Key family (and key-only And/Or/Not of those) can be forwarded into part
    ``ScanSpec``s. Stat/value predicates stay residual for the post-assembly
    cascade.
    """
    if isinstance(where, AlwaysTrue):
        return AlwaysTrue(), AlwaysTrue()
    if isinstance(where, KeyPredicate):
        return where, AlwaysTrue()
    if isinstance(where, StatPredicate | ValuePredicate):
        return AlwaysTrue(), where
    if isinstance(where, Not):
        key_w, resid = split_key_predicates(where.inner)
        if isinstance(resid, AlwaysTrue) and not isinstance(key_w, AlwaysTrue):
            return Not(key_w), AlwaysTrue()
        if isinstance(key_w, AlwaysTrue) and not isinstance(resid, AlwaysTrue):
            return AlwaysTrue(), Not(resid)
        # Mixed — keep whole Not as residual (cannot push partial invert safely).
        return AlwaysTrue(), where
    if isinstance(where, And):
        lk, lr = split_key_predicates(where.left)
        rk, rr = split_key_predicates(where.right)
        key_w: Predicate[K] = AlwaysTrue()
        if not isinstance(lk, AlwaysTrue) and not isinstance(rk, AlwaysTrue):
            key_w = And(lk, rk)
        elif not isinstance(lk, AlwaysTrue):
            key_w = lk
        elif not isinstance(rk, AlwaysTrue):
            key_w = rk
        resid: Predicate[K] = AlwaysTrue()
        if not isinstance(lr, AlwaysTrue) and not isinstance(rr, AlwaysTrue):
            resid = And(lr, rr)
        elif not isinstance(lr, AlwaysTrue):
            resid = lr
        elif not isinstance(rr, AlwaysTrue):
            resid = rr
        return key_w, resid
    if isinstance(where, Or):
        if is_key_only_predicate(where):
            return where, AlwaysTrue()
        return AlwaysTrue(), where
    return AlwaysTrue(), where


def is_gc_predicate[K](where: Predicate[K]) -> bool:
    """True when ``where`` is Expired (or And/Or solely of Expired) — GC path."""
    if isinstance(where, Expired):
        return True
    if isinstance(where, And | Or):
        return is_gc_predicate(where.left) and is_gc_predicate(where.right)
    if isinstance(where, Not):
        return False
    return False


def predicate_label(pred: Predicate[Any]) -> str:
    if isinstance(pred, AlwaysTrue):
        return "AlwaysTrue"
    if isinstance(pred, And):
        return f"And({predicate_label(pred.left)}, {predicate_label(pred.right)})"
    if isinstance(pred, Or):
        return f"Or({predicate_label(pred.left)}, {predicate_label(pred.right)})"
    if isinstance(pred, Not):
        return f"Not({predicate_label(pred.inner)})"
    return type(pred).__name__


def enforce_strict[K](spec: ScanSpec[K], residual: Predicate[K], *, backend: str) -> None:
    if spec.on_unsupported is Pushdown.STRICT and not isinstance(residual, AlwaysTrue):
        raise KvStoreScanUnsupported(
            f"{backend} cannot push residual: {predicate_label(residual)}"
        )


def flatten_and[K](pred: Predicate[K]) -> list[Predicate[K]]:
    if isinstance(pred, And):
        return flatten_and(pred.left) + flatten_and(pred.right)
    if isinstance(pred, AlwaysTrue):
        return []
    return [pred]


def partition_by_needs[K](
    residual: Predicate[K],
) -> tuple[list[Predicate[K]], list[Predicate[K]], list[Predicate[K]]]:
    """Partition residual conjuncts into KEY / STAT / VALUE lists."""
    key_p: list[Predicate[K]] = []
    stat_p: list[Predicate[K]] = []
    value_p: list[Predicate[K]] = []
    for part in flatten_and(residual):
        needs = part.needs()
        if needs & Needs.VALUE:
            value_p.append(part)
        elif needs & Needs.STAT:
            stat_p.append(part)
        else:
            key_p.append(part)
    return key_p, stat_p, value_p


def _all_pass[K](preds: Sequence[Predicate[K]], row: ScanRow[K, Any]) -> bool:
    return all(p.evaluate(row) for p in preds)


def apply_cascade[K, V](
    rows: Iterator[ScanRow[K, V]],
    *,
    residual: Predicate[K],
    store: ReadableKeyMap[K, V] | None = None,
    refine: Callable[[ScanRow[K, Any]], bool] | None = None,
    limit: int | None = None,
    project: Projection = Projection.KEYS,
) -> Iterator[ScanRow[K, V]]:
    """Client-side cascade: KEY → STAT → VALUE (batch_get survivors) → refine → limit."""
    key_p, stat_p, value_p = partition_by_needs(residual)
    yielded = 0
    pending_keys: list[K] = []
    pending_rows: dict[K, ScanRow[K, V]] = {}

    def flush() -> Iterator[ScanRow[K, V]]:
        nonlocal yielded
        if not pending_keys:
            return
        assert store is not None
        keys = list(pending_keys)
        pending_keys.clear()
        pending_rows_snap = {k: pending_rows.pop(k) for k in keys}
        found = store.batch_get(keys)
        for key in keys:
            if key not in found:
                continue
            base = pending_rows_snap[key]
            row = ScanRow(key=key, value=found[key], stat=base.stat)
            if not _all_pass(value_p, row):
                continue
            if refine is not None and not refine(row):
                continue
            out = _project_row(row, project)
            yield out
            yielded += 1
            if limit is not None and yielded >= limit:
                return

    for row in rows:
        if limit is not None and yielded >= limit:
            break
        if not _all_pass(key_p, row):
            continue
        if not _all_pass(stat_p, row):
            continue
        if value_p:
            if store is None:
                raise KvStoreScanUnsupported(
                    "VALUE residual requires a store for batch_get cascade"
                )
            pending_keys.append(row.key)
            pending_rows[row.key] = row
            if len(pending_keys) >= 100:
                yield from flush()
                if limit is not None and yielded >= limit:
                    break
            continue
        if refine is not None and not refine(row):
            continue
        out = _project_row(row, project)
        yield out
        yielded += 1

    if value_p and pending_keys and (limit is None or yielded < limit):
        yield from flush()


def _project_row[K, V](row: ScanRow[K, V], project: Projection) -> ScanRow[K, V]:
    if project is Projection.KEYS:
        return ScanRow(key=row.key)
    if project is Projection.STATS:
        return ScanRow(key=row.key, stat=row.stat)
    return row


class ScanCompiler[K](ABC):
    """Leaf-only: push what you can, return residual for the cascade."""

    @abstractmethod
    def scan_capabilities(self) -> ScanCapabilities: ...

    @abstractmethod
    def compile_scan(self, spec: ScanSpec[K]) -> CompiledScan[K]: ...


@dataclass(frozen=True, slots=True)
class ScanPlanNode:
    name: str
    pushed: frozenset[str] = field(default_factory=lambda: frozenset())
    residual: str = "AlwaysTrue"
    children: tuple[ScanPlanNode, ...] = ()


@dataclass(frozen=True, slots=True)
class ScanPlan:
    root: ScanPlanNode

    def __str__(self) -> str:
        return self._fmt(self.root, 0)

    def _fmt(self, node: ScanPlanNode, depth: int) -> str:
        pad = "  " * depth
        pushed = ", ".join(sorted(node.pushed)) or "(none)"
        lines = [f"{pad}{node.name}: pushed={{{pushed}}} residual={node.residual}"]
        for child in node.children:
            lines.append(self._fmt(child, depth + 1))
        return "\n".join(lines)
