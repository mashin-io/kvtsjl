"""ScanSpec, Predicate ADT, cascade evaluation, and compile contract."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime
from enum import Enum, IntFlag, auto
from fnmatch import fnmatch
from functools import cmp_to_key
import heapq
import string
from typing import TYPE_CHECKING, Any, cast

from kvtsjl.exceptions import KvStoreScanUnsupported
from kvtsjl.scope import Scoped
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
    """Columns the medium sorts by, for ``ScanSpec.order_by``; ``None`` for none.

    An order on other fields is sorted in process (see ``pushes_order``).
    """
    distinct_columns: frozenset[str] | None = None
    """Columns the medium groups by, for ``ScanSpec.distinct_on``; ``None`` for none."""


@dataclass(frozen=True, slots=True)
class Tally:
    """How many entries a scan lists, and the latest write among them.

    ``newest`` is the latest ``modified_at`` — the medium's write time, or
    its clock where it keeps no other — or ``None`` for no entries, or on a
    medium that keeps no write time (Redis). Together they change whenever
    an entry is added, removed or expires, and on a write that moves the
    time: a cheap token for "has this scope changed".
    """

    count: int
    newest: datetime | None = None


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
        if isinstance(key, Scoped) and isinstance(prefix, Scoped):
            return _under_scoped_prefix(cast(Scoped[object], key), cast(Scoped[object], prefix))
        return str(row.key).startswith(str(self.prefix))


def _under_scoped_prefix(key: Scoped[object], prefix: Scoped[object]) -> bool:
    """In the prefix's scope, with a leaf that starts with its leaf."""
    return key.scope == prefix.scope and KeyPrefix(prefix.key).evaluate(ScanRow(key=key.key))


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
class Field[V, T]:
    """One field of a value ``V``: the name a medium stores it under, and how to read it.

    Declare it once, beside the value type, and filter any store of ``V``
    with it::

        MIME = Field[Artifact, str | None]("mime", lambda a: a.mime)
        store.scan(where=MIME.startswith("image/"))

    Every store runs the test on its values. A medium that keeps the field
    as a column of that ``name`` runs it there instead — SQL, for a field
    its view declares — so ``get`` and the column must say the same thing.

    Every test is two-valued and false on a missing field, so ``~`` keeps
    its meaning wherever it runs: ``~MIME.eq("x")`` holds where ``mime`` is
    unset. Test for that with ``is_null()``.
    """

    name: str
    get: Callable[[V], T] = field(compare=False, hash=False)

    def eq(self, value: T) -> FieldEq[V, T]:
        """The field equals ``value``."""
        if value is None:
            raise ValueError(f"{self.name}.eq(None) never holds; use is_null()")
        return FieldEq(self, value)

    def is_in(self, values: Iterable[T]) -> FieldIn[V, T]:
        """The field equals one of ``values``; none at all holds for no row."""
        kept = tuple(dict.fromkeys(values))
        if any(v is None for v in kept):
            raise ValueError(f"{self.name}.is_in(...) cannot match None; or it with is_null()")
        return FieldIn(self, kept)

    def startswith(self: Field[V, str] | Field[V, str | None], prefix: str) -> FieldStartsWith[V]:
        """The field is text that starts with ``prefix``, case and all."""
        return FieldStartsWith(self, prefix)

    def contains(
        self: Field[V, str] | Field[V, str | None], part: str, *, ignore_case: bool = False
    ) -> FieldContains[V]:
        """The field is text with ``part`` in it.

        ``ignore_case`` folds ASCII letters only, as SQL's ``lower()`` does on
        every engine, so the test means the same wherever it runs.
        """
        return FieldContains(self, part, ignore_case)

    def is_null(self) -> FieldIsNull[V, T]:
        """The field is unset."""
        return FieldIsNull(self)


@dataclass(frozen=True, slots=True)
class OrderBy[V]:
    """Order a scan by one field of the value (see ``ScanSpec.order_by``).

    Every store gives the same order: by each ``OrderBy`` in turn — a
    missing field (``None``) first ascending, last descending — then by key,
    ascending. A store that keeps every field as a column of its name sorts
    there (SQL, for the columns its view declares); any other sorts in
    process, reading the whole scan, and ``explain()`` shows a ``Sort``.
    """

    field: Field[V, Any]
    descending: bool = False

    @property
    def column(self) -> str:
        return self.field.name


@dataclass(frozen=True, slots=True)
class After[K]:
    """Where an ordered scan resumes: just past one place in its ``order_by``.

    ``values`` are the ``order_by`` fields' values at that place, in order.
    With ``key`` the place is that row's, so rows with the same values and a
    larger key come next. Without it the scan resumes past every row with
    those values — the same place whenever the order tells every row apart.
    """

    values: Sequence[Any]
    key: K | None = None

    def __post_init__(self) -> None:
        # A tuple, as ScanSpec.order_by is, so the spec stays hashable.
        if not isinstance(self.values, tuple):
            object.__setattr__(self, "values", tuple(self.values))

    @staticmethod
    def of[RK, RV](order_by: Sequence[OrderBy[RV]], key: RK, value: RV) -> After[RK]:
        """The place of the row ``key`` → ``value``: resume a scan just past it."""
        return After(tuple(item.field.get(value) for item in order_by), key)


class FieldTest[V, T](ValuePredicate[Any], ABC):
    """A test of one field of the value; ``Predicate[Any]``, since it reads no key.

    The node types a medium that pushes field tests down receives, under
    ``And`` / ``Or`` / ``Not``: ``FieldEq``, ``FieldIn``, ``FieldStartsWith``,
    ``FieldContains`` and ``FieldIsNull``.
    """

    @property
    @abstractmethod
    def of(self) -> Field[V, T]:
        """The field tested."""

    @abstractmethod
    def holds(self, value: T) -> bool:
        """Whether the test holds for the field's value."""

    def evaluate(self, row: ScanRow[Any, Any]) -> bool:
        if row.value is None:
            return False
        return self.holds(self.of.get(row.value))


@dataclass(frozen=True, slots=True)
class FieldEq[V, T](FieldTest[V, T]):
    field: Field[V, T]
    value: T

    @property
    def of(self) -> Field[V, T]:
        return self.field

    def holds(self, value: T) -> bool:
        return value == self.value


@dataclass(frozen=True, slots=True)
class FieldIn[V, T](FieldTest[V, T]):
    field: Field[V, T]
    values: tuple[T, ...]

    @property
    def of(self) -> Field[V, T]:
        return self.field

    def holds(self, value: T) -> bool:
        return value in self.values


@dataclass(frozen=True, slots=True)
class FieldStartsWith[V](FieldTest[V, str | None]):
    field: Field[V, str] | Field[V, str | None]
    prefix: str

    @property
    def of(self) -> Field[V, str | None]:
        return cast(Field[V, str | None], self.field)

    def holds(self, value: str | None) -> bool:
        return isinstance(value, str) and value.startswith(self.prefix)


_ASCII_FOLD = str.maketrans(string.ascii_uppercase, string.ascii_lowercase)


def fold_ascii(text: str) -> str:
    """Lower-case ASCII letters only — what ``FieldContains(ignore_case=True)`` folds."""
    return text.translate(_ASCII_FOLD)


@dataclass(frozen=True, slots=True)
class FieldContains[V](FieldTest[V, str | None]):
    field: Field[V, str] | Field[V, str | None]
    part: str
    ignore_case: bool = False

    @property
    def of(self) -> Field[V, str | None]:
        return cast(Field[V, str | None], self.field)

    def holds(self, value: str | None) -> bool:
        if not isinstance(value, str):
            return False
        if self.ignore_case:
            return fold_ascii(self.part) in fold_ascii(value)
        return self.part in value


@dataclass(frozen=True, slots=True)
class FieldIsNull[V, T](FieldTest[V, T]):
    field: Field[V, T]

    @property
    def of(self) -> Field[V, T]:
        return self.field

    def holds(self, value: T) -> bool:
        return value is None


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
    order_by: Sequence[OrderBy[Any]] = ()
    after: After[K] | None = None
    distinct_on: Sequence[Field[Any, Any]] = ()
    """Keep only each group's first row under ``order_by``, then apply ``where``.

    Rows group by these fields' values, an unset field being a value of its
    own. Ranking sees every live row of the store, so ``where``, ``refine``,
    ``after`` and ``limit`` all apply to the rows it keeps: the newest
    version of each file, then those that match — never an older version
    that happens to match. A test of the group's fields alone means the same
    either side of the ranking, so a medium may run it first.
    """

    def __post_init__(self) -> None:
        if self.distinct_on:
            if not isinstance(self.distinct_on, tuple):
                object.__setattr__(self, "distinct_on", tuple(self.distinct_on))
            if not self.order_by:
                raise ValueError(
                    "distinct_on keeps each group's first row; pass order_by to say which"
                )
        if self.after is not None:
            if not self.order_by:
                raise ValueError("after resumes an order_by; pass order_by too")
            if len(self.after.values) != len(self.order_by):
                raise ValueError(
                    f"after has {len(self.after.values)} values for "
                    f"{len(self.order_by)} order_by fields"
                )
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
                "and would reorder pages. Resume an ordered scan with after=."
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
    order_by: Sequence[OrderBy[Any]] = (),
    after: After[K] | None = None,
    distinct_on: Sequence[Field[Any, Any]] = (),
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
        after=after,
        distinct_on=tuple(distinct_on),
    )


def _compare(a: Any, b: Any) -> int:
    """``a`` against ``b`` as -1, 0 or 1, ``None`` being the smallest value."""
    if a is None or b is None:
        return (a is not None) - (b is not None)
    return (a > b) - (a < b)


type _Place[K] = tuple[Sequence[Any], K | None]


def _compare_places[K](
    order_by: Sequence[OrderBy[Any]], a: _Place[K], b: _Place[K], *, by_key: bool
) -> int:
    for item, x, y in zip(order_by, a[0], b[0], strict=True):
        if c := _compare(x, y):
            return -c if item.descending else c
    return _compare(a[1], b[1]) if by_key else 0


def lists_rows_alone[K](spec: ScanSpec[K], where: Callable[[Predicate[K]], bool]) -> bool:
    """Whether ``spec`` asks only which rows there are, filtered as ``where`` allows.

    No ``refine``, order or limit: what a tally can take wherever the rows
    are. A view whose rows are its store's tallies such a spec there.
    """
    return (
        spec.refine is None
        and not spec.order_by
        and not spec.distinct_on
        and spec.order is ScanOrder.UNORDERED
        and spec.limit is None
        and where(spec.where)
    )


def is_unfiltered[K](where: Predicate[K]) -> bool:
    return isinstance(where, AlwaysTrue)


def unordered[K](spec: ScanSpec[K]) -> ScanSpec[K]:
    """What a store that sorts in process asks of its own scan.

    Every row ``spec`` keeps; with ``distinct_on``, every row the ranking
    sees — only a test of the group's fields filters before it.
    """
    if not spec.distinct_on:
        return replace(spec, order_by=(), after=None, limit=None, project=Projection.ENTRIES)
    before, _ = split_by_groups(spec)
    return replace(
        spec,
        where=before,
        refine=None,
        order_by=(),
        after=None,
        distinct_on=(),
        limit=None,
        project=Projection.ENTRIES,
    )


def split_by_groups[K](spec: ScanSpec[K]) -> tuple[Predicate[K], Predicate[K]]:
    """``spec.where`` as what may run before ranking — tests of group fields — and the rest."""
    groups = {f.name for f in spec.distinct_on}

    def of_a_group_field(p: Predicate[K]) -> bool:
        return isinstance(p, FieldTest) and cast(FieldTest[Any, Any], p).of.name in groups

    return split_where(spec.where, of_a_group_field)


def first_of_each_group[K](
    rows: Iterable[ScanRow[K, Any]], spec: ScanSpec[K]
) -> Iterator[ScanRow[K, Any]]:
    """Each ``spec.distinct_on`` group's first row under ``spec.order_by``; holds one per group."""
    firsts: dict[tuple[Any, ...], tuple[_Place[K], ScanRow[K, Any]]] = {}
    for row in rows:
        if row.value is None:
            continue
        group = tuple(f.get(row.value) for f in spec.distinct_on)
        place: _Place[K] = (tuple(item.field.get(row.value) for item in spec.order_by), row.key)
        held = firsts.get(group)
        if held is None or _compare_places(spec.order_by, place, held[0], by_key=True) < 0:
            firsts[group] = (place, row)
    return (row for _, row in firsts.values())


def sort_rows[K](
    rows: Iterable[ScanRow[K, Any]], spec: ScanSpec[K]
) -> Iterator[ScanRow[K, Any]]:
    """``rows`` — what ``unordered(spec)`` returned — in ``spec``'s order, past its ``after``.

    Holds every row to sort them, or only ``spec.limit`` of them with one.
    """
    order_by = spec.order_by
    after = spec.after
    placed = (
        ((tuple(item.field.get(row.value) for item in order_by), row.key), row)
        for row in rows
        if row.value is not None
    )
    if after is not None:
        at: _Place[K] = (after.values, after.key)
        by_key = after.key is not None
        placed = (
            p for p in placed if _compare_places(order_by, p[0], at, by_key=by_key) > 0
        )

    def compare(a: tuple[_Place[K], ScanRow[K, Any]], b: tuple[_Place[K], ScanRow[K, Any]]) -> int:
        return _compare_places(order_by, a[0], b[0], by_key=True)

    sort_key = cmp_to_key(compare)
    ordered = (
        sorted(placed, key=sort_key)
        if spec.limit is None
        else heapq.nsmallest(spec.limit, placed, key=sort_key)
    )
    for _, row in ordered:
        yield _project_row(row, spec.project)


def order_label[K](spec: ScanSpec[K]) -> str:
    """``order_by(…)``, and ``distinct_on(…)`` with it, as ``explain()`` shows them."""
    label = "order_by(" + ", ".join(
        f"{item.column} desc" if item.descending else item.column for item in spec.order_by
    ) + ")"
    if spec.distinct_on:
        label += " distinct_on(" + ", ".join(f.name for f in spec.distinct_on) + ")"
    return label


def scan_sorted[K](
    scan: Callable[[ScanSpec[K]], Iterator[ScanRow[K, Any]]],
    spec: ScanSpec[K],
    *,
    backend: str,
    store: ReadableKeyMap[K, Any] | None = None,
) -> Iterator[ScanRow[K, Any]]:
    """``spec`` from a store that cannot give its order: ``scan`` every row, then sort.

    With ``distinct_on``, keep each group's first row, then filter those by
    ``where`` and ``refine`` — reading ``store`` for a row without its value.
    ``Pushdown.STRICT`` refuses, as it does a filter that would run here.
    """
    if spec.on_unsupported is Pushdown.STRICT:
        raise KvStoreScanUnsupported(
            f"{backend} cannot push {order_label(spec)}; it would sort in process"
        )
    rows = scan(unordered(spec))
    if spec.distinct_on:
        _, kept = split_by_groups(spec)
        rows = apply_cascade(
            first_of_each_group(rows, spec),
            residual=kept,
            store=store,
            refine=spec.refine,
            project=Projection.ENTRIES,
        )
    return sort_rows(rows, spec)


def explain_sorted[K](child: ScanPlanNode, spec: ScanSpec[K]) -> ScanPlanNode:
    """The plan of ``scan_sorted``: ``child`` lists every row, then a ``Sort`` here."""
    residual = order_label(spec)
    if spec.distinct_on:
        residual += f" then {predicate_label(split_by_groups(spec)[1])}"
    return ScanPlanNode(name="Sort", residual=residual, children=(child,))


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


def made_of[K](pred: Predicate[K], leaf: Callable[[Predicate[K]], bool]) -> bool:
    """Whether ``pred`` is ``leaf`` tests alone, under ``And`` / ``Or`` / ``Not``."""
    if isinstance(pred, AlwaysTrue):
        return True
    if isinstance(pred, And | Or):
        return made_of(pred.left, leaf) and made_of(pred.right, leaf)
    if isinstance(pred, Not):
        return made_of(pred.inner, leaf)
    return leaf(pred)


def split_where[K](
    where: Predicate[K], leaf: Callable[[Predicate[K]], bool]
) -> tuple[Predicate[K], Predicate[K]]:
    """Split ``where`` into the conjuncts made of ``leaf`` tests alone, and the rest.

    How a medium takes the part of a filter it can run: the first half goes
    to it, the second runs on the rows it returns, and the two together mean
    ``where``. Conjuncts split; an ``Or`` or a ``Not`` goes whole to one side,
    since part of one would mean something else.
    """
    if isinstance(where, And):
        left_taken, left_rest = split_where(where.left, leaf)
        right_taken, right_rest = split_where(where.right, leaf)
        return conjoin(left_taken, right_taken), conjoin(left_rest, right_rest)
    if made_of(where, leaf):
        return where, AlwaysTrue()
    return AlwaysTrue(), where


def conjoin[K](left: Predicate[K], right: Predicate[K]) -> Predicate[K]:
    """``left`` and ``right``, without an ``AlwaysTrue`` side."""
    if isinstance(left, AlwaysTrue):
        return right
    if isinstance(right, AlwaysTrue):
        return left
    return And(left, right)


def _is_key_test[K](pred: Predicate[K]) -> bool:
    return isinstance(pred, KeyPredicate)


def is_key_only_predicate[K](pred: Predicate[K]) -> bool:
    return made_of(pred, _is_key_test)


def split_key_predicates[K](
    where: Predicate[K],
) -> tuple[Predicate[K], Predicate[K]]:
    """Split ``where`` into part-forwardable key preds and residual.

    Key family (and key-only And/Or/Not of those) can be forwarded into part
    ``ScanSpec``s. Stat/value predicates stay residual for the post-assembly
    cascade.
    """
    return split_where(where, _is_key_test)


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
    """Client-side cascade: KEY → STAT → VALUE → refine → limit.

    A value predicate reads the value a row carries; only rows the scan
    brought without one are read, a page at a time, with ``batch_get``.
    """
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
        if value_p and row.value is None:
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
        if value_p:
            # The scan brought the value: filter on it, after the rows still
            # waiting for theirs, so the order holds.
            yield from flush()
            if limit is not None and yielded >= limit:
                break
            if not _all_pass(value_p, row):
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
