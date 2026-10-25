"""The meta-algebra: the rules every composition of stores obeys.

kvtsjl has two algebras. The **composition algebra** builds stores from
stores — ``mirror``, ``fallback_read``, ``indexed``, ``zip_with`` and whatever
comes next. This module is the **meta-algebra** that governs it: one set of
rules for how *any* composition that puts a record in more than one place
carries out *any* operation. A composition declares what each place is to
the record; every operation over those places follows from the declaration,
so no composition states an order of its own and no two operations can
disagree about one.

The meta-algebra has three layers — a declaration, the traversals derived
from it, and operations built on those traversals — and a set of laws that
hold for every composition built this way.


1. The declaration
==================

Each place a record lives is a :class:`Target`. A composition declares, per
target, only what that target *is* to the record:

**Role** — what it is to the record, and the only thing that orders writes,
deletes and failures:

==============  =====================  =====================  ================
Role            Writes                 Deletes, GC            Reads
==============  =====================  =====================  ================
``COMPONENT``   before the authority   after the authority    assembled
``AUTHORITY``   the commit point       first                  the anchor
``DEPENDENT``   after the authority    after the authority    per ``lookup``
``FALLBACK``    never                  never                  after a miss
==============  =====================  =====================  ================

To choose one, ask what the target is to the record:

- *Part of* it, referred to by the authority? ``COMPONENT``. Written first,
  or a reader could follow the authority to something never written.
- *Decides* whether it exists? ``AUTHORITY``. At most one per record.
- *Derived from* it — a copy, an index, a cache — or *referring to* it, like
  a parent's child rows keyed by the parent? ``DEPENDENT``. Written after the
  commit point, so a failure there leaves the record present and the
  dependent stale, and nothing is written for a record that never commits.
- Consulted only when the authority does not have the key? ``FALLBACK``.

**Lookup** — when a read consults a target that is neither the authority nor
a component, independently of when it is written: before the authority (a
cache), after it (a fallback, a failover replica) or never (an index).

**Fill** — whether a target consulted and missed by a read is given the value
found later. A fallback hit promoted into the authority and a cache filled on
a miss are the same rule. A fill is a conditional write, so it never replaces
a value written since the miss, and it runs downstream only: a fallback feeds
the authority, the authority feeds its dependents, never the reverse.

**Policy** — for a dependent, whether a failure after the commit point is
raised (``REQUIRED``) or logged (``BEST_EFFORT``).

**Shape** — targets holding the whole record go in ``targets``; those holding
a piece of it go in ``parts``, with an ``assemble`` that builds the record
from :class:`Parts`. Parts are indexed by the part itself, so each piece keeps
its own static type and assembling needs no casts.

**Role and shape are independent.** A target has exactly one role, and the
two cannot merge: a component is written *before* the commit point and the
authority *is* the commit point. But the authority may have either shape:

=================  ========================  ================================
                   Holds the whole record    Holds a piece of it
=================  ========================  ================================
Decides existence  ``AUTHORITY`` in          ``AUTHORITY`` in ``parts`` —
                   ``targets`` — ``mirror``  a zip's ``commit_part``
                   's primary
Before it          —                         ``COMPONENT`` — a zip's other
                                             fields
After it           ``DEPENDENT`` — a copy,   ``DEPENDENT`` in ``parts`` — a
                   an index; or              zip's ``after_commit`` fields,
                   ``FALLBACK``, never       such as child rows from
                   written                   ``group_by``
=================  ========================  ================================

So "a piece that decides existence" is the authority declared in ``parts``:
its read feeds ``assemble`` like a component's, but it is written last,
removed first and consulted first. When several pieces must *all* be present
for the record to exist, there is no authority: every piece is a component
and ``assemble`` decides — ``zip_with(..., mandatory=[True, True])``. No
single piece can then refuse an insert atomically, so ``set_if_absent`` is
not available.

A dependent in ``parts`` is a piece written after the commit point because it
refers to the record — a turn's receipts, keyed by the turn. It is read into
the record like any part, never decides its existence, is written only once
the record commits (so not at all by a ``set_if_absent`` that loses) and is
removed after it.


2. The traversals
=================

From the declaration the meta-algebra derives four traversals. These are the
only orders in which any operation visits targets.

============  =====================================  =========================
Traversal     Order                                  Code
============  =====================================  =========================
Commit        components → authority → dependents    ``committing``,
                                                     ``dependents``
Removal       authority → components → dependents    ``_by_role``
Lookup        before → authority → after             ``lookups()``
Assembly      authority, then the other parts        ``parts``
============  =====================================  =========================


3. The operations
=================

An operation is a traversal plus four choices: when to **stop**, how to
**combine** what the targets return, what a **failure** means, and what
**side effect** a step has.

================  ==========  ===============  ======================  ==========
Operation         Traversal   Stops            Combines                Side effect
================  ==========  ===============  ======================  ==========
``set``           commit      never            —                       —
``set_if_absent`` commit      if the authority whether the authority   —
                              refuses          wrote
``update``        authority,  when every       what the entry holds    as ``set``
                  then commit key is           afterwards              or
                  or removal  written, or                              ``delete``
                              ``attempts``
                              run out
``delete``        removal     never            the authority's answer  —
``unset``         removal     never            the authority's answer  —
``gc``            removal     never            union of keys           dependents
                                                                       forget the
                                                                       authority's
``get``           lookup /    at the first     the hit / ``assemble``  fill
                  assembly    hit
``exists``        lookup /    at the first     whether ``get`` would   —
                  assembly    hit              return a record
``stat``          lookup      at the first     the hit's metadata      —
                              hit
``scan``          —           at ``limit``     the authority's keys    —
                                               in the order asked, or
                                               the parts' keys, each
                                               confirmed by ``get``
``missing_keys``  lookup,     never            the authority's own     —
                  over keys                    gaps, then each
                                               fallback's keys the
                                               authority and earlier
                                               fallbacks lack
``fill_missing``  lookup,     at ``limit``     ``FillStats``           a
                  then commit                                          conditional
                                                                       write of
                                                                       each record
================  ==========  ===============  ======================  ==========

Batch forms take the same traversal, one call per target, and a batch read
asks each target only for the keys still missing. ``update`` is a
``batch_update`` of one key; each round reads the records as the authority
holds them — a cache may be stale, so it is not asked; a fallback is, where
the authority misses, and a ``Put`` then writes into the authority — then writes every
decision in one conditional batch through the commit traversal (the
removal traversal for ``Remove``), and decides again only the keys the
authority refused. ``set_if_absent`` and the fills are that conditional
batch against nothing stored. An ordered or paged scan
of an assembled record asks its authority for the keys in that order, then
confirms each record, so a ``limit`` counts records; keys merged from
several parts have no one order, and asking for one refuses. ``unset`` is ``delete``
with each target removing only its own part — the same traversal, a
narrower step. ``delete_where`` removes
the records a scan selects, along the removal traversal. ``delete`` without
an authority answers whether any target held the key. Streams take the same
traversals as their point forms: the authority takes the stream and each
dependent is fed by re-reading it, since a stream is consumed once.

A new operation is defined the same way — ``missing_keys`` and
``fill_missing`` were: pick a traversal, then the stop, combine, failure and
side-effect rules. It never introduces an order.

``missing_keys`` follows the chain of authorities down, so a fallback nested
inside ``indexed`` or ``mirror`` still counts: every store answers
``missing_pages``, and a view in front of one translates the answer.
``fill_missing`` then writes each copy with the store's own
``set_if_absent``, at the level it was asked for, so the indexes and copies
on the way down receive it. It differs from a read's fill in two rules: it
writes the whole record rather than one target, and as an explicit write a
failure raises instead of being logged.


4. The laws
===========

These hold for every composition built from a declaration, at every level of
nesting. ``tests/test_record_topology.py`` and ``tests/test_fill_missing.py``
check each one, against the compositions kvtsjl ships and ones it does not.

1. **Commit before derive.** No dependent is written before the commit point
   succeeds; a refused conditional write writes no dependent.
2. **Referent before reference.** Components are written before the authority
   that refers to them and removed after it, so a reader following the
   authority never finds a part missing.
3. **A part is unset; a record is deleted.** A write that leaves a part
   absent — a zip's ``None`` slot — *unsets* that part: each target removes
   only what is its own, so a view of a row other views share nulls its own
   columns and leaves the row. Deleting the record deletes, authority first,
   so the commit point takes the row with it. Every view of a shared entry
   — a table row, an index entry — follows one rule: ``unset`` removes its
   own part, and ``delete`` removes the entry only from the view that owns
   entries (``owns_rows``, an index view with a document); from any other
   view it removes that view's part too. Creating is the owner's alone as
   well: a view that does not own entries writes only into ones that exist
   (``creates_entries`` is ``False``), so it can only come after the commit
   point, and a record whose committing parts disagree is refused.
4. **Existence is a read.** ``exists(k)`` holds exactly when ``get(k)``
   returns a record. For an assembled record the authority is necessary, not
   sufficient: ``assemble`` has the last word.
5. **Scans list only records.** Every key a scan yields is one ``get``
   returns. A scan enumerates what decides existence — the authority — so a
   key held only by a fallback or a cache is readable but not listed.
6. **Reads write only fills, and a fill never replaces.** A read changes a
   target only to fill one marked ``fill``, with a conditional write: a value
   written since the miss is kept, not overwritten by the copy. A fill is
   best-effort — the read already succeeded — and carries the write clock, so
   a filled value keeps its remaining life.
7. **Values flow downstream only.** Fallback → authority → dependents. A copy
   never feeds what it was copied from, so neither a read nor
   ``fill_missing`` can write a stale cache or a lagging replica back into
   the authority.
8. **Failures carry their side of the commit point.**
   :class:`UncommittedWriteError` (``committed=False``) before it,
   :class:`PartialSyncError` (``committed=True``) after it. A target that is
   itself a composition reports its own outcome, and the parent maps it
   through the role that child plays — so the answer is right at every level.
   When the caller owns the commit, the commit point is the caller's commit:
   a part that joins it and fails is before it, ``UncommittedWriteError`` —
   roll back and the record is as it was.
9. **Unsupported is never best-effort.** Best-effort tolerates a medium
   *failing*. A :class:`~kvtsjl.exceptions.KvStoreUnsupported` — a
   composition asking a medium for something it cannot do — is raised under
   every policy, by a fill as much as by a sync.
10. **The clock is all or nothing where it counts.** A write clock the
    authority or a component cannot keep is refused before any target is
    written; a dependent that cannot keep one receives the write without it.
11. **Budgets do not couple targets.** GC gives every target it collects the
    full budget; targets expire independently, and a shared budget starved the
    later ones. The union may therefore exceed the budget.
12. **After-commit work waits for a deferred commit.** When the caller owns
    the commit, what comes after the commit point — a dependent's write, and
    every removal after the authority's — waits for ``flush_index_syncs`` if
    it is durable at once, and ``discard_index_syncs`` drops it; one whose
    writes are deferred too joins the caller's commit and runs at once. A
    rollback therefore never leaves a record whose body is already gone.
13. **Composition is closed.** A composition is a target to its parent. The
    laws hold for the parent whatever the child is, and the tree walk —
    flushing, discarding, counting deferred syncs — reaches every node.


5. Extending
============

- **A new composition** declares its targets and mixes in
  :class:`RecordTopologyMixin` (keymaps) or ``StoreTopologyMixin`` (stores).
  It writes no ordering code. If it needs one, the meta-algebra is missing a
  rule — add it here, for every composition, rather than in the composition.
- **A new kind of target** — not a keymap, like an index — subclasses
  :class:`Target` and implements what its role needs; every operation has a
  neutral default.
- **A new operation** is a traversal plus its four choices (section 3).


6. Limits
=========

- *One authority per record.* A commit point of "k of n replicas
  acknowledged" — quorum replication — is not a single target and cannot be
  expressed with these roles.
- *Targets independent of the key.* A composition that routes each key to a
  different target, such as a sharded store, would need ``topology`` to take
  the key. That is the extension point; it is not built until something needs
  it.
- *Assembled records are read whole.* A record made of parts has no lookup
  order of its own; put a cache or a fallback around the composition rather
  than among its parts.
- *A read's fill writes one target.* Promoting a value into the authority
  does not re-run the authority's dependents. ``fill_missing``, an explicit
  write, writes the whole record and does.
- *A streamed fill replaces.* Media share no conditional streaming write —
  S3's managed upload takes no precondition — so a fill during
  ``read_stream`` overwrites; law 6 holds for ``get`` and ``batch_get``.
- *An assembled record is filled at its parts.* No fallback holds a whole
  record made of parts, so a zip lists nothing missing; a part that is
  itself a ``fallback_read`` is filled by asking that part.
- *Conditional writes ask the medium.* ``set_if_absent`` asks the authority
  whether the key is absent to the medium, which can differ from law 4 — an
  expired entry, or a committed row whose mandatory part is gone, still
  refuses the insert. Reconciling them would need a read before the write,
  the race the method exists to avoid.

For instance, ``mirror`` makes its primary the authority and its secondary a
dependent; ``fallback_read`` makes its secondary a fallback and fills the
primary when promoting; ``indexed`` makes its indexes a dependent; a zip with
a ``commit_part`` makes that part the authority and the rest components.
These are examples, not the definition.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from enum import Enum
import logging
from typing import cast

from kvtsjl.batching import iter_chunks
from kvtsjl.exceptions import (
    KvStoreError,
    KvStoreUnsupported,
    PartialSyncError,
    RecordWriteError,
    UncommittedWriteError,
)
from kvtsjl.keymap import Change, ConditionalWrite, KeyMap, Put, Remove, Versioned
from kvtsjl.store.schema.policy import SyncPolicy
from kvtsjl.store.schema.stat import RecordStat
from kvtsjl.store.schema.ttl import TtlPolicy
from kvtsjl.store.streaming import StreamSource, close_stream

logger = logging.getLogger(__name__)


class Role(Enum):
    """What a target is to the record; the role implies every ordering."""

    COMPONENT = "component"
    AUTHORITY = "authority"
    DEPENDENT = "dependent"
    FALLBACK = "fallback"


class Lookup(Enum):
    """When a read consults a target that is neither authority nor component."""

    BEFORE_AUTHORITY = "before_authority"
    AFTER_AUTHORITY = "after_authority"
    NEVER = "never"


_DEFAULT_LOOKUP = {Role.DEPENDENT: Lookup.NEVER, Role.FALLBACK: Lookup.AFTER_AUTHORITY}


@dataclass(frozen=True, slots=True)
class WriteOptions:
    """The per-write options every target receives."""

    ttl: TtlPolicy | None = None
    created_at: datetime | None = None


class Target[K, V, P](ABC):
    """One place a record lives: what it is to the record, and how to reach it.

    ``V`` is the record a write hands it; ``P`` is what it holds and reads
    back — the record itself for most targets, one part of it for a
    component, and ``Never`` for a target that cannot be read, such as an
    index. ``P`` appears only in what a target returns, so a target holding a
    ``str`` part is a ``Target[K, V, object]`` too, and parts of different
    types sit in one tuple without erasing anything to ``Any``.

    Every operation has a neutral default, so a target implements only what
    its role needs. :class:`KeyMapTarget` covers any keymap; subclass for a
    place that is not one.
    """

    def __init__(
        self,
        name: str,
        role: Role,
        *,
        lookup: Lookup | None = None,
        fill: bool = False,
        policy: SyncPolicy = SyncPolicy.REQUIRED,
    ) -> None:
        if lookup is not None and role in (Role.AUTHORITY, Role.COMPONENT):
            raise ValueError(
                f"{name}: the authority and components are always read; "
                "lookup places the other targets around them"
            )
        if fill and role in (Role.COMPONENT, Role.FALLBACK):
            raise ValueError(f"{name}: only a target that is written can be filled")
        self.name = name
        self.role = role
        self.lookup = lookup if lookup is not None else _DEFAULT_LOOKUP.get(role)
        self.fill = fill
        self.policy = policy

    # --- writes --------------------------------------------------------------

    def write(self, key: K, value: V, options: WriteOptions) -> None:
        raise KvStoreError(f"{self.name} cannot be written")

    def batch_read_versioned(self, keys: Sequence[K]) -> Mapping[K, Versioned[P]]:
        raise KvStoreUnsupported(f"{self.name} has no version to write against")

    def batch_write_if_unchanged(
        self, writes: Mapping[K, ConditionalWrite[V]], options: WriteOptions
    ) -> set[K]:
        raise KvStoreUnsupported(f"{self.name} cannot write conditionally")

    def batch_write(self, items: Mapping[K, V], options: WriteOptions) -> None:
        for key, value in items.items():
            self.write(key, value, options)

    def write_stream[C: (bytes, str)](
        self: Target[K, C, C],
        key: K,
        source: StreamSource,
        options: WriteOptions,
        size: int | None,
    ) -> None:
        raise NotImplementedError(f"{self.name} cannot take a stream")

    def delete(self, key: K) -> bool:
        raise KvStoreError(f"{self.name} cannot be deleted from")

    def batch_delete(self, keys: Sequence[K]) -> int:
        return sum(1 for key in keys if self.delete(key))

    def unset(self, key: K) -> bool:
        """Remove only this target's part of the record; for most, ``delete``."""
        return self.delete(key)

    def batch_unset(self, keys: Sequence[K]) -> int:
        return sum(1 for key in keys if self.unset(key))

    # --- reads ---------------------------------------------------------------

    def read(self, key: K) -> P | None:
        raise KvStoreError(f"{self.name} cannot be read")

    def batch_read(self, keys: Sequence[K]) -> Mapping[K, P]:
        found = {key: self.read(key) for key in keys}
        return {key: value for key, value in found.items() if value is not None}

    def exists(self, key: K) -> bool:
        return self.read(key) is not None

    def batch_exists(self, keys: Sequence[K]) -> set[K]:
        return set(self.batch_read(keys))

    def stat(self, key: K) -> RecordStat | None:
        return RecordStat() if self.exists(key) else None

    def batch_stat(self, keys: Sequence[K]) -> dict[K, RecordStat]:
        return {key: RecordStat() for key in self.batch_exists(keys)}

    def read_stream[C: (bytes, str)](
        self: Target[K, V, C], key: K
    ) -> Iterator[C] | None:
        raise NotImplementedError(f"{self.name} cannot be streamed")

    def keys(self, page_size: int) -> Iterator[K]:
        """Every key held here, listed ``page_size`` at a time."""
        raise KvStoreUnsupported(f"{self.name} cannot list its keys")

    def missing_pages(self, page_size: int) -> Iterator[MissingPage[K, P]]:
        """What this target's own fallbacks hold and it lacks; none by default."""
        return iter(())

    # --- expiry and durability -----------------------------------------------

    @property
    def supports_write_clock(self) -> bool:
        """Whether ``created_at`` means something here; it is dropped if not."""
        return False

    def receives(self, options: WriteOptions) -> WriteOptions:
        """The options as this target takes them: without a clock it cannot keep."""
        if options.created_at is None or self.supports_write_clock:
            return options
        return replace(options, created_at=None)

    @property
    def expires(self) -> bool:
        """Whether entries expire here on their own, leaving garbage to collect."""
        return False

    def gc(self, max_entries: int) -> list[K]:
        return []

    @property
    def writes_are_deferred(self) -> bool:
        return False

    @property
    def creates_entries(self) -> bool:
        """Whether a write here can create the entry, not only update it."""
        return True

    def children(self) -> tuple[object, ...]:
        """What lies beneath this target, for operations over the whole tree."""
        return ()


def whole[T](record: T) -> T | None:
    """The slot of a target that holds the whole record."""
    return record


class KeyMapTarget[K, V, P](Target[K, V, P]):
    """A target that is a keymap, holding ``slot(record)``.

    ``slot`` is :func:`whole` for a target holding the record itself. A
    ``None`` slot unsets the entry — a record whose optional part is absent
    has no entry there.
    """

    def __init__(
        self,
        name: str,
        role: Role,
        keymap: KeyMap[K, P],
        slot: Callable[[V], P | None],
        *,
        lookup: Lookup | None = None,
        fill: bool = False,
        policy: SyncPolicy = SyncPolicy.REQUIRED,
    ) -> None:
        super().__init__(name, role, lookup=lookup, fill=fill, policy=policy)
        self.keymap = keymap
        self._slot = slot

    def write(self, key: K, value: V, options: WriteOptions) -> None:
        item = self._slot(value)
        if item is None:
            self.keymap.unset(key)
            return
        self.keymap.set(key, item, ttl=options.ttl, created_at=options.created_at)

    def batch_read_versioned(self, keys: Sequence[K]) -> Mapping[K, Versioned[P]]:
        return self.keymap.batch_read_versioned(keys)

    def batch_write_if_unchanged(
        self, writes: Mapping[K, ConditionalWrite[V]], options: WriteOptions
    ) -> set[K]:
        slotted: dict[K, ConditionalWrite[P]] = {}
        for key, w in writes.items():
            change: Change[P] = w.change if isinstance(w.change, Remove) else self._put(w.change)
            slotted[key] = ConditionalWrite(Versioned(None, w.seen.version), change, w.created_at)
        return self.keymap.batch_write_if_unchanged(slotted, ttl=options.ttl)

    def _put(self, put: Put[V]) -> Put[P]:
        item = self._slot(put.value)
        if item is None:
            raise KvStoreError(f"{self.name} decides existence and cannot be None")
        return Put(item)

    def batch_write(self, items: Mapping[K, V], options: WriteOptions) -> None:
        to_set: dict[K, P] = {}
        to_clear: list[K] = []
        for key, value in items.items():
            item = self._slot(value)
            if item is None:
                to_clear.append(key)
            else:
                to_set[key] = item
        if to_set:
            self.keymap.batch_set(to_set, ttl=options.ttl, created_at=options.created_at)
        if to_clear:
            self.keymap.batch_unset(to_clear)

    def delete(self, key: K) -> bool:
        return self.keymap.delete(key)

    def batch_delete(self, keys: Sequence[K]) -> int:
        return self.keymap.batch_delete(keys)

    def unset(self, key: K) -> bool:
        return self.keymap.unset(key)

    def batch_unset(self, keys: Sequence[K]) -> int:
        return self.keymap.batch_unset(keys)

    def read(self, key: K) -> P | None:
        return self.keymap.get(key)

    def batch_read(self, keys: Sequence[K]) -> Mapping[K, P]:
        return self.keymap.batch_get(keys)

    def exists(self, key: K) -> bool:
        return self.keymap.exists(key)

    def batch_exists(self, keys: Sequence[K]) -> set[K]:
        return self.keymap.batch_exists(keys)

    def stat(self, key: K) -> RecordStat | None:
        return self.keymap.stat(key)

    def batch_stat(self, keys: Sequence[K]) -> dict[K, RecordStat]:
        return self.keymap.batch_stat(keys)

    @property
    def creates_entries(self) -> bool:
        return self.keymap.creates_entries

    def children(self) -> tuple[object, ...]:
        return (self.keymap,)


@dataclass(frozen=True, slots=True)
class MissingPage[K, V]:
    """One page of keys a fallback holds, and which of them the record lacks.

    ``listed`` is the page in the fallback's order, ``missing`` the keys in it
    the record does not hold. ``read`` and ``stat`` fetch those from where
    they are. A store answers with these at every level, and a view in front
    of it translates them, so a fill can be asked for at any level.
    """

    listed: Sequence[K]
    missing: Sequence[K]
    read: Callable[[Sequence[K]], Mapping[K, V]]
    stat: Callable[[Sequence[K]], Mapping[K, RecordStat]]

    def map_values[U](self, forward: Callable[[V], U | None]) -> MissingPage[K, U]:
        """This page seen through a value view; ``None`` from ``forward`` is absent."""
        read = self.read

        def mapped(keys: Sequence[K]) -> dict[K, U]:
            return {k: u for k, v in read(keys).items() if (u := forward(v)) is not None}

        return MissingPage(self.listed, self.missing, mapped, self.stat)

    def map_keys[J](self, outward: Callable[[K], J]) -> MissingPage[J, V]:
        """This page under the keys a view shows; reads go back to the stored ones."""
        shown = {k: outward(k) for k in self.listed}
        stored = {j: k for k, j in shown.items()}
        read, stat = self.read, self.stat

        def mapped_read(keys: Sequence[J]) -> dict[J, V]:
            return {shown[k]: v for k, v in read([stored[j] for j in keys]).items()}

        def mapped_stat(keys: Sequence[J]) -> dict[J, RecordStat]:
            return {shown[k]: m for k, m in stat([stored[j] for j in keys]).items()}

        return MissingPage(
            [shown[k] for k in self.listed],
            [shown[k] for k in self.missing],
            mapped_read,
            mapped_stat,
        )


class Parts[K, V]:
    """What a record's parts hold for one key, looked up by the part itself.

    ``parts[row]`` has the type ``row`` holds, so an ``assemble`` written
    against its own parts needs no casts. A part that holds nothing for the
    key gives ``None``.
    """

    def __init__(self, held: Mapping[int, object]) -> None:
        self._held = held

    def __getitem__[P](self, part: Target[K, V, P]) -> P | None:
        # Each entry is filed under the part it was read from, so it has the
        # type that part's ``read`` returns; Python cannot say so of a mapping
        # whose value type depends on its key, so the one cast lives here.
        return cast(P | None, self._held.get(id(part)))


@dataclass(frozen=True, slots=True)
class _Queued:
    name: str
    what: str
    policy: SyncPolicy
    run: Callable[[], object]


class DependentSyncs:
    """Runs dependents' operations after the commit point, or holds them for it.

    One per composed node, so a deferred operation is flushed by the node
    that queued it, and the tree walk reaches every node.
    """

    def __init__(self) -> None:
        self._queue: list[_Queued] = []

    def run(
        self,
        name: str,
        what: str,
        policy: SyncPolicy,
        op: Callable[[], object],
        *,
        defer: bool,
        before_commit: bool = False,
    ) -> None:
        """Queue ``op`` for the commit, or run it now.

        Run now inside a commit the caller has not made yet — it joins that
        commit — it is before the commit point, and a failure says so.
        """
        if defer:
            self._queue.append(_Queued(name, what, policy, op))
            return
        _settle(name, what, policy, op, before_commit=before_commit)

    @property
    def pending(self) -> int:
        return len(self._queue)

    def flush(self) -> None:
        """Run every queued operation, even past failures; report them together.

        The queue clears either way, so a failed flush is never retried behind
        the caller's back.
        """
        queued, self._queue = self._queue, []
        failures: dict[str, Exception] = {}
        for position, item in enumerate(queued):
            try:
                _settle(item.name, item.what, item.policy, item.run)
            except PartialSyncError as exc:
                failures.update(
                    {f"{item.what}#{position}:{label}": e for label, e in exc.failures.items()}
                )
        if failures:
            raise PartialSyncError(
                "deferred syncs failed after commit", failures=failures
            )

    def discard(self) -> None:
        self._queue = []


@dataclass(frozen=True, slots=True)
class Topology[K, V]:
    """A record's targets, and the order of every operation their roles imply.

    ``targets`` hold the whole record — the authority when it does, and the
    dependents and fallbacks. ``parts`` hold a piece of it — the components,
    and the authority when it is one of the pieces — and ``assemble`` builds
    the record from what they hold, returning ``None`` when the pieces present
    do not make one. The split is what keeps reads typed: a whole target reads
    back a ``V``, a part whatever it holds.
    """

    targets: tuple[Target[K, V, V], ...] = ()
    parts: tuple[Target[K, V, object], ...] = ()
    assemble: Callable[[Parts[K, V]], V | None] | None = None

    def __post_init__(self) -> None:
        authorities = [t.name for t in self._all if t.role is Role.AUTHORITY]
        if len(authorities) > 1:
            raise ValueError(f"a record has one authority, got {authorities}")
        misplaced = [t.name for t in self.targets if t.role is Role.COMPONENT]
        if misplaced:
            raise ValueError(f"a component holds a part of the record: {misplaced}")
        fallback_parts = [t.name for t in self.parts if t.role is Role.FALLBACK]
        if fallback_parts:
            raise ValueError(f"a fallback is read whole, not as a part: {fallback_parts}")
        if not authorities and any(t.role is Role.DEPENDENT for t in self.parts):
            raise ValueError("a dependent part comes after a commit point; there is none")
        if not authorities and not self.parts:
            raise ValueError("a record needs an authority or components")
        if not authorities and any(t.role is Role.FALLBACK for t in self.targets):
            raise ValueError("a fallback is consulted when the authority misses")
        committing = self._by_role(Role.COMPONENT, Role.AUTHORITY)
        uncreating = [t.name for t in committing if not t.creates_entries]
        if uncreating and len(uncreating) < len(committing):
            raise ValueError(
                f"{uncreating} only write into entries another part creates, so "
                "a new record would be written without them; put them after the "
                "commit point (a zip's after_commit)"
            )
        if bool(self.parts) != (self.assemble is not None):
            raise ValueError("parts and assemble go together")
        looked_up = [t.name for t in self.targets if t.lookup not in (None, Lookup.NEVER)]
        if self.parts and looked_up:
            raise ValueError(f"an assembled record is read from its parts, not {looked_up}")

    # --- shape ---------------------------------------------------------------

    @property
    def _all(self) -> tuple[Target[K, V, object], ...]:
        return (*self.targets, *self.parts)

    @property
    def authority(self) -> Target[K, V, object] | None:
        return next((t for t in self._all if t.role is Role.AUTHORITY), None)

    @property
    def whole_authority(self) -> Target[K, V, V] | None:
        """The authority, when it holds the whole record."""
        return next((t for t in self.targets if t.role is Role.AUTHORITY), None)

    @property
    def components(self) -> list[Target[K, V, object]]:
        return [t for t in self.parts if t.role is Role.COMPONENT]

    @property
    def _unanchored_parts(self) -> list[Target[K, V, object]]:
        """The parts other than the authority, which is read first."""
        return [t for t in self.parts if t.role is not Role.AUTHORITY]

    @property
    def assembled(self) -> bool:
        """Whether a read builds the record from parts rather than finding it whole."""
        return bool(self.parts)

    @property
    def committing(self) -> list[Target[K, V, object]]:
        """The targets the record is committed to: components, then authority."""
        return self._by_role(Role.COMPONENT, Role.AUTHORITY)

    @property
    def anchor(self) -> Target[K, V, object]:
        """The authority, or the first component; it speaks for the record's medium."""
        return self.authority or self.components[0]

    @property
    def creates_entries(self) -> bool:
        """Whether a record can be created here; the committing targets agree."""
        return all(t.creates_entries for t in self.committing)

    @property
    def supports_write_clock(self) -> bool:
        """Every target the record is committed to must honour ``created_at``."""
        return all(t.supports_write_clock for t in self.committing)

    @property
    def deferred(self) -> bool:
        """Whether the commit is the caller's to make, so dependents must wait."""
        return any(t.writes_are_deferred for t in self.committing)

    def lookups(self) -> list[Target[K, V, V]]:
        """The order a read consults whole targets: before, the authority, after."""
        before = [t for t in self.targets if t.lookup is Lookup.BEFORE_AUTHORITY]
        after = [t for t in self.targets if t.lookup is Lookup.AFTER_AUTHORITY]
        authority = self.whole_authority
        return [*before, *([authority] if authority is not None else []), *after]

    def _by_role(self, *roles: Role) -> list[Target[K, V, object]]:
        """Targets with these roles, phase by phase, declared order within."""
        return [t for role in roles for t in self._all if t.role is role]

    def _require_authority(self, op: str) -> Target[K, V, object]:
        authority = self.authority
        if authority is None:
            raise KvStoreUnsupported(
                f"{op} needs a target that decides whether the record exists"
            )
        return authority

    def _require_whole(self, op: str) -> Target[K, V, V]:
        authority = self.whole_authority
        if authority is None:
            raise NotImplementedError(
                f"{op}: a record assembled from parts is not one value"
            )
        return authority

    @property
    def dependents(self) -> list[Target[K, V, object]]:
        """Targets written after the commit point: copies, and parts that refer to it."""
        return self._by_role(Role.DEPENDENT)

    def _to_dependents(
        self,
        what: str,
        syncs: DependentSyncs,
        op: Callable[[Target[K, V, object]], object],
        *,
        policy: SyncPolicy | None = None,
    ) -> None:
        for target in self.dependents:
            self._sync_one(target, what, syncs, lambda t=target: op(t), policy)

    def _sync_one(
        self,
        target: Target[K, V, object],
        what: str,
        syncs: DependentSyncs,
        run: Callable[[], object],
        policy: SyncPolicy | None = None,
    ) -> None:
        syncs.run(
            target.name,
            what,
            policy if policy is not None else target.policy,
            run,
            # One whose own writes are deferred joins the caller's commit,
            # so it runs now, before that commit; any other waits for it.
            defer=self.deferred and not target.writes_are_deferred,
            before_commit=self.deferred,
        )

    # --- writes --------------------------------------------------------------

    def write(
        self, key: K, value: V, options: WriteOptions, syncs: DependentSyncs
    ) -> None:
        """Components, then the authority, then dependents."""
        for target in self.committing:
            _committing(
                target.name,
                target.role,
                lambda t=target: t.write(key, value, t.receives(options)),
            )
        self._to_dependents("set", syncs, lambda t: t.write(key, value, t.receives(options)))

    def batch_read_versioned(self, keys: Sequence[K]) -> dict[K, Versioned[V]]:
        """The records as a read sees them, versioned by their authority.

        The authority decides whether a record exists, so its version is the
        record's: a conditional write is decided there. Where the authority
        misses, a fallback's value is the record's value, versioned as
        nothing stored — so writing it promotes it, once. A cache is not
        consulted: it may be stale, and an update must see the truth. One
        batch per target.
        """
        unique = list(dict.fromkeys(keys))
        if self.assembled:
            seen = self._require_authority("read_versioned").batch_read_versioned(unique)
            anchors = {k: v.value for k, v in seen.items() if v.value is not None}
            records = self._assemble_many(list(anchors), anchors=anchors)
            return {k: Versioned(records.get(k), seen[k].version) for k in unique}
        whole = self._require_whole("read_versioned").batch_read_versioned(unique)
        values = {k: v.value for k, v in whole.items() if v.value is not None}
        for fallback in (t for t in self.lookups() if t.role is Role.FALLBACK):
            missing = [k for k in unique if k not in values]
            if not missing:
                break
            values.update(fallback.batch_read(missing))
        return {k: Versioned(values.get(k), whole[k].version) for k in unique}

    def batch_write_if_unchanged(
        self,
        writes: Mapping[K, ConditionalWrite[V]],
        options: WriteOptions,
        syncs: DependentSyncs,
    ) -> set[K]:
        """Make each change — ``Put`` or ``Remove`` — only if the record's authority is still ``seen``.

        Against nothing stored this inserts the record: components first,
        then the authority's conditional insert, then dependents for those it
        wrote. Components written for an insert that loses are orphans until
        GC. Against a version, components cannot be written first — they
        would replace the live record's parts before knowing it had not
        changed — so a record with components refuses; update its parts after
        the commit point instead. Removing goes authority first, the rest
        after the commit point, as ``delete`` does. One batch per target.
        """
        authority = self._require_authority("write_if_unchanged")
        puts = {k: w.change.value for k, w in writes.items() if isinstance(w.change, Put)}
        if self.components and any(writes[k].seen.version is not None for k in puts):
            raise KvStoreUnsupported(
                f"{[t.name for t in self.components]} are written before the commit "
                "point and could not be taken back if the record changed; update "
                "the parts that come after it instead"
            )
        for target in self.components:
            for clock, items in _by_clock(puts, writes):
                o = WriteOptions(ttl=options.ttl, created_at=clock)
                _committing(
                    target.name,
                    target.role,
                    lambda t=target, i=items, o=o: t.batch_write(i, t.receives(o)),
                )
        written = _committing(
            authority.name,
            authority.role,
            lambda: authority.batch_write_if_unchanged(_received(authority, writes), options),
        )
        removed = [k for k in writes if k in written and k not in puts]
        if removed:
            for target in self._removed_after_commit():
                self._sync_one(
                    target, "batch delete", syncs, lambda t=target: t.batch_delete(removed)
                )
        stored = {k: v for k, v in puts.items() if k in written}
        for clock, items in _by_clock(stored, writes):
            o = WriteOptions(ttl=options.ttl, created_at=clock)
            self._to_dependents(
                "batch set", syncs, lambda t, i=items, o=o: t.batch_write(i, t.receives(o))
            )
        return written

    def batch_write(
        self, items: Mapping[K, V], options: WriteOptions, syncs: DependentSyncs
    ) -> None:
        """As ``write``, one batch per target."""
        if not items:
            return
        for target in self.committing:
            _committing(
                target.name,
                target.role,
                lambda t=target: t.batch_write(items, t.receives(options)),
            )
        self._to_dependents(
            "batch set", syncs, lambda t: t.batch_write(items, t.receives(options))
        )

    def write_stream[C: (bytes, str)](
        self: Topology[K, C],
        key: K,
        source: StreamSource,
        options: WriteOptions,
        size: int | None,
        syncs: DependentSyncs,
    ) -> None:
        """Stream into the authority, then feed each dependent by re-reading it.

        The source is consumed once, so the dependents are fed from what the
        authority stored — no buffer and no temporary file.

        **Race:** the re-read is a second operation. If another writer replaces
        the key in between, a dependent receives *that* writer's bytes under
        this call's options. The targets still converge on the same bytes; only
        the expiry attached to them can come from the losing call. Concurrent
        writers to one key need their own ordering — content-addressed keys,
        where the bytes cannot differ, make it moot.
        """
        authority = self._require_whole("set_stream")
        _committing(
            authority.name,
            authority.role,
            lambda: authority.write_stream(key, source, authority.receives(options), size),
        )

        def feed(target: Target[K, C, C]) -> None:
            chunks = authority.read_stream(key)
            if chunks is not None:
                target.write_stream(key, chunks, target.receives(options), None)

        # A whole record has no parts, so every dependent is a whole target.
        for target in self.targets:
            if target.role is Role.DEPENDENT:
                self._sync_one(target, "set_stream", syncs, lambda t=target: feed(t))

    # --- deletes -------------------------------------------------------------

    def delete(self, key: K, syncs: DependentSyncs) -> bool:
        """The authority first, then everything else; never stops early.

        Returns the authority's answer, or — with no authority — whether any
        target held the key. Every target is visited either way: stopping at
        the first one that held the key leaves the rest behind.
        """
        return self._remove(key, syncs, "delete", lambda t, k: t.delete(k))

    def unset(self, key: K, syncs: DependentSyncs) -> bool:
        """As ``delete``, but each target removes only its own part.

        What a part of a record written as absent means when that part is a
        composition: its targets unset — a view of a shared row nulls its own
        columns — and its dependents forget the key.
        """
        return self._remove(key, syncs, "unset", lambda t, k: t.unset(k))

    def batch_delete(
        self,
        keys: Sequence[K],
        syncs: DependentSyncs,
        *,
        policy: SyncPolicy | None = None,
    ) -> int:
        """As ``delete``, one batch per target.

        Counts what the authority removed; with no authority, the distinct
        keys any target held, probed before deleting.
        """
        return self._batch_remove(
            keys, syncs, "batch delete", lambda t, ks: t.batch_delete(ks), policy
        )

    def batch_unset(self, keys: Sequence[K], syncs: DependentSyncs) -> int:
        """As ``unset``, one batch per target."""
        return self._batch_remove(
            keys, syncs, "batch unset", lambda t, ks: t.batch_unset(ks), None
        )

    def _remove(
        self,
        key: K,
        syncs: DependentSyncs,
        what: str,
        op: Callable[[Target[K, V, object], K], bool],
    ) -> bool:
        removed = [op(t, key) for t in self._removed_now()]
        for target in self._removed_after_commit():
            self._sync_one(target, what, syncs, lambda t=target: op(t, key))
        return removed[0] if self.authority is not None else any(removed)

    def _batch_remove(
        self,
        keys: Sequence[K],
        syncs: DependentSyncs,
        what: str,
        op: Callable[[Target[K, V, object], Sequence[K]], int],
        policy: SyncPolicy | None,
    ) -> int:
        if not keys:
            return 0
        unique = list(dict.fromkeys(keys))
        authority = self.authority
        held: set[K] = set()
        if authority is None:
            for target in self.components:
                held |= target.batch_exists(unique)
        counts = [op(t, unique) for t in self._removed_now()]
        for target in self._removed_after_commit():
            self._sync_one(target, what, syncs, lambda t=target: op(t, unique), policy)
        return counts[0] if authority is not None else len(held)

    def _removed_now(self) -> list[Target[K, V, object]]:
        """The authority alone — or, with no commit point, every component."""
        authority = self.authority
        return [authority] if authority is not None else self.components

    def _removed_after_commit(self) -> list[Target[K, V, object]]:
        """Everything removed after the commit point, in removal order.

        Once the authority is removed the record is gone, so the rest is
        after-commit work, as a dependent's write is: it joins a deferred
        commit if its own writes are deferred, waits for the commit if it is
        durable at once — a component deleted before a rollback would leave
        the record pointing at nothing — and a failure leaves the record
        removed and the part stale. With no authority there is no commit
        point, and every part is removed at once.
        """
        if self.authority is None:
            return self.dependents
        return [*self.components, *self.dependents]

    def gc(
        self,
        max_entries: int,
        syncs: DependentSyncs,
        *,
        policy: SyncPolicy = SyncPolicy.BEST_EFFORT,
    ) -> list[K]:
        """Collect expired entries; return the distinct keys collected.

        The authority and each component collect up to ``max_entries`` of
        their own: they expire independently, and sharing one budget starved
        the later ones — the first consumed it and the rest were never
        visited while the call reported success. So the union may exceed
        ``max_entries``; the alternative is silently skipping targets.

        Dependents forget what the authority collected — the record is gone,
        so its copies are stale — and collect their own expiries. Both are
        best-effort unless ``policy`` says otherwise: the authority's cleanup
        already succeeded. Fallbacks are never written, so never collected.
        """
        if max_entries < 1:
            raise ValueError(f"max_entries must be >= 1, got {max_entries}")
        authority = self.authority
        collected: dict[K, None] = {}
        expired: list[K] = []
        for target in self._by_role(Role.AUTHORITY, Role.COMPONENT):
            keys = target.gc(max_entries)
            if target is authority:
                expired = keys
            collected.update(dict.fromkeys(keys))
        if expired:
            self._to_dependents(
                "gc", syncs, lambda t: t.batch_delete(expired), policy=policy
            )
        for target in self.dependents:
            if target.expires:
                own: list[K] = []

                def collect(t: Target[K, V, object] = target, out: list[K] = own) -> None:
                    out.extend(t.gc(max_entries))

                _settle(target.name, "gc", SyncPolicy.BEST_EFFORT, collect)
                collected.update(dict.fromkeys(own))
        return list(collected)

    # --- what the fallbacks hold -------------------------------------------

    def missing_pages(self, page_size: int) -> Iterator[MissingPage[K, V]]:
        """What a read finds through a fallback that the record does not hold.

        Down the chain of authorities first — what the authority's own
        fallbacks hold and it lacks — then each fallback of this record in
        lookup order, a page at a time. A fallback's key is missing where
        neither the authority nor an earlier fallback holds it, so it belongs
        to the first fallback a read would reach, and memory stays one page.

        Only fallbacks are listed: what a cache or a replica holds beyond the
        authority is stale, not missing. A record assembled from parts lists
        nothing, since no fallback holds a whole one.
        """
        authority = self.whole_authority
        if authority is None:
            return
        yield from authority.missing_pages(page_size)
        fallbacks = [t for t in self.lookups() if t.role is Role.FALLBACK]
        for index, fallback in enumerate(fallbacks):
            ahead = [authority, *fallbacks[:index]]
            for page in iter_chunks(fallback.keys(page_size), page_size):
                absent: list[K] = page
                for target in ahead:
                    if not absent:
                        break
                    held = target.batch_exists(absent)
                    absent = [k for k in absent if k not in held]
                yield MissingPage(page, absent, fallback.batch_read, fallback.batch_stat)

    # --- reads ---------------------------------------------------------------

    def read(self, key: K) -> V | None:
        """The first target in lookup order that holds the key, filling those before it."""
        if self.assembled:
            return self._assemble_one(key)
        consulted: list[Target[K, V, V]] = []
        for target in self.lookups():
            value = target.read(key)
            if value is not None:
                self._fill(key, value, target, consulted)
                return value
            consulted.append(target)
        return None

    def batch_read(self, keys: Sequence[K]) -> dict[K, V]:
        """As ``read``, asking each target only about keys still missing."""
        if self.assembled:
            return self._assemble_many(keys)
        found: dict[K, V] = {}
        consulted: list[Target[K, V, V]] = []
        for target in self.lookups():
            missing = [k for k in keys if k not in found]
            if not missing:
                break
            hits = target.batch_read(missing)
            if hits:
                self._batch_fill(hits, target, consulted)
                found.update(hits)
            consulted.append(target)
        return found

    def exists(self, key: K) -> bool:
        """Whether ``read`` would return a record, without filling anything.

        A whole record exists where a target in lookup order holds it. An
        assembled one exists where it assembles: the authority is necessary
        but not sufficient, since ``assemble`` may still refuse the parts —
        a mandatory part expired, say. The authority is read first, so the
        other parts are read only for keys it holds.
        """
        if self.assembled:
            return self.read(key) is not None
        return any(target.exists(key) for target in self.lookups())

    def batch_exists(self, keys: Sequence[K]) -> set[K]:
        """As ``exists``, asking each target only about keys still missing."""
        if self.assembled:
            return set(self.batch_read(keys))
        found: set[K] = set()
        for target in self.lookups():
            missing = [k for k in keys if k not in found]
            if not missing:
                break
            found |= target.batch_exists(missing)
        return found

    def stat(self, key: K) -> RecordStat | None:
        """The first target in lookup order with metadata for the key."""
        for target in self.lookups():
            meta = target.stat(key)
            if meta is not None:
                return meta
        return None

    def batch_stat(self, keys: Sequence[K]) -> dict[K, RecordStat]:
        found: dict[K, RecordStat] = {}
        for target in self.lookups():
            missing = [k for k in keys if k not in found]
            if not missing:
                break
            found.update(target.batch_stat(missing))
        return found

    def read_stream[C: (bytes, str)](
        self: Topology[K, C], key: K
    ) -> Iterator[C] | None:
        """As ``read``, streaming; a fill consumes the stream and is re-read."""
        self._require_whole("read_stream")
        consulted: list[Target[K, C, C]] = []
        for target in self.lookups():
            chunks = target.read_stream(key)
            if chunks is not None:
                return _fill_stream(key, chunks, target, consulted)
            consulted.append(target)
        return None

    def _assemble_one(self, key: K) -> V | None:
        assemble = self.assemble
        assert assemble is not None
        held: dict[int, object] = {}
        authority = self.authority
        if authority is not None:
            value = authority.read(key)
            if value is None:
                return None
            held[id(authority)] = value
        for part in self._unanchored_parts:
            held[id(part)] = part.read(key)
        return assemble(Parts(held))

    def _assemble_many(
        self, keys: Sequence[K], *, anchors: Mapping[K, object] | None = None
    ) -> dict[K, V]:
        """Each record from its parts; ``anchors`` are the authority's values, if read."""
        assemble = self.assemble
        assert assemble is not None
        if not keys:
            return {}
        wanted = list(dict.fromkeys(keys))
        found: dict[int, Mapping[K, object]] = {}
        authority = self.authority
        if authority is not None:
            hits = anchors if anchors is not None else authority.batch_read(wanted)
            found[id(authority)] = hits
            wanted = [k for k in wanted if k in hits]
        for part in self._unanchored_parts:
            found[id(part)] = part.batch_read(wanted) if wanted else {}
        out: dict[K, V] = {}
        for key in wanted:
            record = assemble(Parts({ident: hits.get(key) for ident, hits in found.items()}))
            if record is not None:
                out[key] = record
        return out

    # --- fills ---------------------------------------------------------------

    def _fill(
        self, key: K, value: V, source: Target[K, V, V], consulted: list[Target[K, V, V]]
    ) -> None:
        self._batch_fill({key: value}, source, consulted)

    def _batch_fill(
        self,
        hits: Mapping[K, V],
        source: Target[K, V, V],
        consulted: list[Target[K, V, V]],
    ) -> None:
        """Give each value to every target that asked for it, unless it has one now.

        A conditional write, so a value written between the miss and the fill
        is kept rather than replaced by the copy; each carries its record's
        write clock. One batch per target.
        """
        into = _fillable(source, consulted)
        if not into:
            return
        stats = source.batch_stat(list(hits)) if any(t.supports_write_clock for t in into) else {}
        writes = {
            k: ConditionalWrite(Versioned.absent(), Put(v), _clock(stats.get(k)))
            for k, v in hits.items()
        }
        for target in into:
            _try_fill(
                target.name,
                lambda t=target: t.batch_write_if_unchanged(_received(t, writes), WriteOptions()),
            )

    # --- the whole tree ------------------------------------------------------

    def children(self) -> tuple[object, ...]:
        """Everything beneath this record, for tree-wide operations."""
        return tuple(child for t in self._all for child in t.children())


def _fill_stream[K, C: (bytes, str)](
    key: K,
    chunks: Iterator[C],
    source: Target[K, C, C],
    consulted: list[Target[K, C, C]],
) -> Iterator[C] | None:
    """Fill each target from the stream, then serve the last one filled.

    Each fill consumes the stream it is given, so the next is fed by
    re-reading the target just written — one read of each, no buffer. A
    failed fill is best-effort: the spent stream is closed so the medium's
    response is released, and the source is read again.

    Unlike a point fill this one replaces: media have no conditional
    streaming write in common (S3's managed upload takes no precondition), so
    a value written between the miss and the fill can be overwritten.
    """
    into = _fillable(source, consulted)
    if not into:
        return chunks
    clock = _clock(source.stat(key)) if any(t.supports_write_clock for t in into) else None
    current: Iterator[C] | None = chunks
    for target in into:
        if current is None:
            return None
        options = target.receives(WriteOptions(created_at=clock))
        try:
            target.write_stream(key, current, options, None)
        except KvStoreUnsupported:
            raise
        except Exception as exc:
            _log("fill", target.name, exc)
            close_stream(current)
            current = source.read_stream(key)
            continue
        current = target.read_stream(key) or source.read_stream(key)
    return current


# Values flow downstream only: a fallback feeds the authority, the authority
# feeds its dependents. A copy never feeds what it was copied from, so a stale
# cache or a lagging replica cannot write a record back into the authority.
_STREAM_ORDER = {Role.FALLBACK: 0, Role.AUTHORITY: 1, Role.COMPONENT: 1, Role.DEPENDENT: 2}


def _fillable[K, V](
    source: Target[K, V, V], consulted: Sequence[Target[K, V, V]]
) -> list[Target[K, V, V]]:
    """The consulted targets that asked to be filled and lie downstream of ``source``."""
    rank = _STREAM_ORDER[source.role]
    return [t for t in consulted if t.fill and _STREAM_ORDER[t.role] > rank]


def _by_clock[K, V](
    values: Mapping[K, V], writes: Mapping[K, ConditionalWrite[V]]
) -> list[tuple[datetime | None, dict[K, V]]]:
    """``values`` grouped by the write clock each carries, so each group is one batch."""
    groups: dict[datetime | None, dict[K, V]] = {}
    for k, v in values.items():
        groups.setdefault(writes[k].created_at, {})[k] = v
    return list(groups.items())


def _received[K, V](
    target: Target[K, V, object], writes: Mapping[K, ConditionalWrite[V]]
) -> Mapping[K, ConditionalWrite[V]]:
    """As ``Target.receives``, per write: without the clocks it cannot keep."""
    if target.supports_write_clock:
        return writes
    return {k: replace(w, created_at=None) for k, w in writes.items()}


def _clock(meta: RecordStat | None) -> datetime | None:
    return None if meta is None else meta.write_clock


def _committing[T](name: str, role: Role, run: Callable[[], T]) -> T:
    """Run a write on or before the commit point, reclassifying a failure.

    - a ``COMPONENT`` failure is before the commit point, whatever the child
      says about its own record;
    - an ``AUTHORITY`` failure keeps a child composition's answer — the
      child's commit point *is* this record's.
    """
    try:
        return run()
    except Exception as exc:
        if role is Role.AUTHORITY and isinstance(exc, RecordWriteError):
            raise
        raise UncommittedWriteError(
            f"{name} failed before the record was committed",
            failures={name: exc},
        ) from exc


def _settle(
    name: str,
    what: str,
    policy: SyncPolicy,
    run: Callable[[], object],
    *,
    before_commit: bool = False,
) -> None:
    """Run an operation that follows the authority's write, under ``policy``.

    Usually that is after the commit point, whatever a child composition says
    about its own record: ``REQUIRED`` raises :class:`PartialSyncError`,
    ``BEST_EFFORT`` logs and carries on. But one that joins a commit the
    caller has not made yet runs before it: its failure raises
    :class:`UncommittedWriteError`, and rolling back leaves the record as it
    was. Best-effort tolerates a medium failing, not one asked for something
    it cannot do: a :class:`KvStoreUnsupported` is raised under either policy.
    """
    try:
        run()
    except Exception as exc:
        if policy is SyncPolicy.BEST_EFFORT and not isinstance(exc, KvStoreUnsupported):
            _log(what, name, exc)
            return
        if before_commit:
            if isinstance(exc, UncommittedWriteError):
                raise
            raise UncommittedWriteError(
                f"{name} {what} failed inside a commit the caller has not made; "
                "roll it back",
                failures={name: exc},
            ) from exc
        if isinstance(exc, PartialSyncError):
            raise
        raise PartialSyncError(
            f"{name} {what} failed after the record was committed",
            failures={name: exc},
        ) from exc


def _try_fill(name: str, run: Callable[[], object]) -> None:
    """A fill is best-effort — the read already succeeded — but loud when unsupported."""
    try:
        run()
    except KvStoreUnsupported:
        raise
    except Exception as exc:
        _log("fill", name, exc)


def _log(what: str, name: str, exc: Exception) -> None:
    try:
        logger.exception("%s on %s failed", what, name, exc_info=exc)
    except Exception:
        # Logging must not replace the failure it reports.
        pass


class RecordTopologyMixin[K, V](KeyMap[K, V], ABC):
    """A keymap whose operations over several targets derive from its topology.

    Mix in ahead of the keymap base and implement ``topology()``. Build
    targets with ``_target`` (holding the record) and ``_part`` (holding a
    piece of it) so a store-level subclass can give them the store's own
    operations.
    """

    @abstractmethod
    def topology(self) -> Topology[K, V]:
        """This record's targets and the role each plays."""

    def _target(
        self,
        name: str,
        role: Role,
        keymap: KeyMap[K, V],
        *,
        lookup: Lookup | None = None,
        fill: bool = False,
        policy: SyncPolicy = SyncPolicy.REQUIRED,
    ) -> Target[K, V, V]:
        return KeyMapTarget(
            name, role, keymap, whole, lookup=lookup, fill=fill, policy=policy
        )

    def _part[P](
        self,
        name: str,
        role: Role,
        keymap: KeyMap[K, P],
        slot: Callable[[V], P | None],
    ) -> Target[K, V, P]:
        return KeyMapTarget(name, role, keymap, slot)

    def _dependent_syncs(self) -> DependentSyncs:
        """This node's own queue; built on first use, so no ``__init__`` is needed."""
        syncs = getattr(self, "_record_syncs", None)
        if not isinstance(syncs, DependentSyncs):
            syncs = DependentSyncs()
            self._record_syncs = syncs
        return syncs

    def _admit(self, options: WriteOptions) -> None:
        """Refuse a write before any target sees it; a keymap ignores the clock."""

    def set(
        self,
        key: K,
        value: V,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        options = WriteOptions(ttl=ttl, created_at=created_at)
        self._admit(options)
        self.topology().write(key, value, options, self._dependent_syncs())

    def batch_read_versioned(self, keys: Sequence[K]) -> dict[K, Versioned[V]]:
        return self.topology().batch_read_versioned(keys)

    def batch_write_if_unchanged(
        self, writes: Mapping[K, ConditionalWrite[V]], *, ttl: TtlPolicy | None = None
    ) -> set[K]:
        for w in writes.values():
            self._admit(WriteOptions(ttl=ttl, created_at=w.created_at))
        if not writes:
            return set()
        return self.topology().batch_write_if_unchanged(
            writes, WriteOptions(ttl=ttl), self._dependent_syncs()
        )

    def batch_set(
        self,
        items: Mapping[K, V],
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        options = WriteOptions(ttl=ttl, created_at=created_at)
        self._admit(options)
        self.topology().batch_write(items, options, self._dependent_syncs())

    def delete(self, key: K) -> bool:
        return self.topology().delete(key, self._dependent_syncs())

    def batch_delete(self, keys: Sequence[K]) -> int:
        return self.topology().batch_delete(keys, self._dependent_syncs())

    def unset(self, key: K) -> bool:
        return self.topology().unset(key, self._dependent_syncs())

    def batch_unset(self, keys: Sequence[K]) -> int:
        return self.topology().batch_unset(keys, self._dependent_syncs())

    def get(self, key: K) -> V | None:
        return self.topology().read(key)

    def batch_get(self, keys: Sequence[K]) -> dict[K, V]:
        return self.topology().batch_read(keys)

    def exists(self, key: K) -> bool:
        return self.topology().exists(key)

    def batch_exists(self, keys: Sequence[K]) -> set[K]:
        return self.topology().batch_exists(keys)

    @property
    def creates_entries(self) -> bool:
        """A composition of views that only update shared entries is such a view too."""
        return self.topology().creates_entries

    def child_stores(self) -> tuple[object, ...]:
        """Everything directly beneath this composition."""
        return self.topology().children()
