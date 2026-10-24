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
- *Derived from* it — a copy, an index, a cache? ``DEPENDENT``. Written after
  the commit point, so a failure there leaves the record present and the
  dependent stale.
- Consulted only when the authority does not have the key? ``FALLBACK``.

**Lookup** — when a read consults a target that is neither the authority nor
a component, independently of when it is written: before the authority (a
cache), after it (a fallback, a failover replica) or never (an index).

**Fill** — whether a target consulted and missed by a read is given the value
found later. A fallback hit promoted into the authority and a cache filled on
a miss are the same rule.

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
Does not           ``DEPENDENT`` or          ``COMPONENT`` — a zip's other
                   ``FALLBACK``              fields
=================  ========================  ================================

So "a piece that decides existence" is the authority declared in ``parts``:
its read feeds ``assemble`` like a component's, but it is written last,
removed first and consulted first. When several pieces must *all* be present
for the record to exist, there is no authority: every piece is a component
and ``assemble`` decides — ``zip_with(..., mandatory=[True, True])``. No
single piece can then refuse an insert atomically, so ``set_if_absent`` is
not available.


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
``delete``        removal     never            the authority's answer  —
``gc``            removal     never            union of keys           dependents
                                                                       forget the
                                                                       authority's
``get``           lookup /    at the first     the hit / ``assemble``  fill
                  assembly    hit
``exists``        lookup /    at the first     whether ``get`` would   —
                  assembly    hit              return a record
``stat``          lookup      at the first     the hit's metadata      —
                              hit
``scan``          —           —                the authority's keys,   —
                                               or the parts' keys
                                               confirmed by ``get``
================  ==========  ===============  ======================  ==========

Batch forms take the same traversal, one call per target, and a batch read
asks each target only for the keys still missing. ``delete_where`` removes
the records a scan selects, along the removal traversal. ``delete`` without
an authority answers whether any target held the key. Streams take the same
traversals as their point forms: the authority takes the stream and each
dependent is fed by re-reading it, since a stream is consumed once.

A new operation — ``missing_keys``, say — is defined the same way: pick a
traversal, then the stop, combine, failure and side-effect rules. It never
introduces an order.


4. The laws
===========

These hold for every composition built from a declaration, at every level of
nesting. ``tests/test_record_topology.py`` checks each one, against the
compositions kvtsjl ships and one it does not.

1. **Commit before derive.** No dependent is written before the commit point
   succeeds; a refused conditional write writes no dependent.
2. **Referent before reference.** Components are written before the authority
   that refers to them and removed after it, so a reader following the
   authority never finds a part missing.
3. **Existence is a read.** ``exists(k)`` holds exactly when ``get(k)``
   returns a record. For an assembled record the authority is necessary, not
   sufficient: ``assemble`` has the last word.
4. **Scans list only records.** Every key a scan yields is one ``get``
   returns. A scan enumerates what decides existence — the authority — so a
   key held only by a fallback or a cache is readable but not listed.
5. **Reads write only fills.** A read never changes a target except to fill
   one marked ``fill``; a fill is best-effort and carries the write clock, so
   a filled value keeps its remaining life.
6. **Failures carry their side of the commit point.**
   :class:`UncommittedWriteError` (``committed=False``) before it,
   :class:`PartialSyncError` (``committed=True``) after it. A target that is
   itself a composition reports its own outcome, and the parent maps it
   through the role that child plays — so the answer is right at every level.
7. **The clock is all or nothing where it counts.** A write clock the
   authority or a component cannot keep is refused before any target is
   written; a dependent that cannot keep one receives the write without it.
8. **Budgets do not couple targets.** GC gives every target it collects the
   full budget; targets expire independently, and a shared budget starved the
   later ones. The union may therefore exceed the budget.
9. **Dependents wait for a deferred commit.** When the caller owns the commit,
   a dependent durable at once waits for ``flush_index_syncs``, and
   ``discard_index_syncs`` drops it; one whose writes are deferred too joins
   the caller's commit and runs at once.
10. **Composition is closed.** A composition is a target to its parent. The
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
- *A fill writes one target.* Promoting a value into the authority does not
  re-run the authority's dependents.
- *Conditional writes ask the medium.* ``set_if_absent`` asks the authority
  whether the key is absent to the medium, which can differ from law 3 — an
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

from kvtsjl.exceptions import (
    KvStoreError,
    KvStoreUnsupported,
    PartialSyncError,
    RecordWriteError,
    UncommittedWriteError,
)
from kvtsjl.keymap import KeyMap
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

    def write_if_absent(self, key: K, value: V, options: WriteOptions) -> bool:
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

    def children(self) -> tuple[object, ...]:
        """What lies beneath this target, for operations over the whole tree."""
        return ()


def whole[T](record: T) -> T | None:
    """The slot of a target that holds the whole record."""
    return record


class KeyMapTarget[K, V, P](Target[K, V, P]):
    """A target that is a keymap, holding ``slot(record)``.

    ``slot`` is :func:`whole` for a target holding the record itself. A
    ``None`` slot clears the entry — a record whose optional part is absent
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
            self.keymap.delete(key)
            return
        self.keymap.set(key, item, ttl=options.ttl, created_at=options.created_at)

    def write_if_absent(self, key: K, value: V, options: WriteOptions) -> bool:
        item = self._slot(value)
        if item is None:
            raise KvStoreError(f"{self.name} decides existence and cannot be None")
        return self.keymap.set_if_absent(
            key, item, ttl=options.ttl, created_at=options.created_at
        )

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
            self.keymap.batch_delete(to_clear)

    def delete(self, key: K) -> bool:
        return self.keymap.delete(key)

    def batch_delete(self, keys: Sequence[K]) -> int:
        return self.keymap.batch_delete(keys)

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

    def children(self) -> tuple[object, ...]:
        return (self.keymap,)


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
    ) -> None:
        if defer:
            self._queue.append(_Queued(name, what, policy, op))
            return
        _settle(name, what, policy, op)

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
        not_parts = [
            t.name for t in self.parts if t.role not in (Role.COMPONENT, Role.AUTHORITY)
        ]
        if not_parts:
            raise ValueError(f"only components and the authority are parts: {not_parts}")
        if not authorities and not self.parts:
            raise ValueError("a record needs an authority or components")
        if not authorities and any(t.role is Role.FALLBACK for t in self.targets):
            raise ValueError("a fallback is consulted when the authority misses")
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
    def dependents(self) -> list[Target[K, V, V]]:
        """Dependents hold the whole record: they are derived from all of it."""
        return [t for t in self.targets if t.role is Role.DEPENDENT]

    def _to_dependents(
        self,
        what: str,
        syncs: DependentSyncs,
        op: Callable[[Target[K, V, V]], object],
        *,
        policy: SyncPolicy | None = None,
    ) -> None:
        for target in self.dependents:
            syncs.run(
                target.name,
                what,
                policy if policy is not None else target.policy,
                lambda t=target: op(t),
                # One whose own writes are deferred joins the caller's commit.
                defer=self.deferred and not target.writes_are_deferred,
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

    def write_if_absent(
        self, key: K, value: V, options: WriteOptions, syncs: DependentSyncs
    ) -> bool:
        """As ``write``, but the authority writes only if the record is absent.

        Components are already written when the authority refuses — they are
        orphans until GC. Dependents are not written: the record they would
        copy is not ours. Returns whether the record was written.
        """
        authority = self._require_authority("write_if_absent")
        for target in self.components:
            _committing(
                target.name,
                target.role,
                lambda t=target: t.write(key, value, t.receives(options)),
            )
        wrote = _committing(
            authority.name,
            authority.role,
            lambda: authority.write_if_absent(key, value, authority.receives(options)),
        )
        if wrote:
            self._to_dependents(
                "set", syncs, lambda t: t.write(key, value, t.receives(options))
            )
        return wrote

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

        self._to_dependents("set_stream", syncs, feed)

    # --- deletes -------------------------------------------------------------

    def delete(self, key: K, syncs: DependentSyncs) -> bool:
        """The authority first, then everything else; never stops early.

        Returns the authority's answer, or — with no authority — whether any
        target held the key. Every target is visited either way: stopping at
        the first one that held the key leaves the rest behind.
        """
        removed = [t.delete(key) for t in self._by_role(Role.AUTHORITY, Role.COMPONENT)]
        self._to_dependents("delete", syncs, lambda t: t.delete(key))
        return removed[0] if self.authority is not None else any(removed)

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
        if not keys:
            return 0
        unique = list(dict.fromkeys(keys))
        authority = self.authority
        held: set[K] = set()
        if authority is None:
            for target in self.components:
                held |= target.batch_exists(unique)
        counts = [
            t.batch_delete(unique) for t in self._by_role(Role.AUTHORITY, Role.COMPONENT)
        ]
        self._to_dependents(
            "batch delete", syncs, lambda t: t.batch_delete(unique), policy=policy
        )
        return counts[0] if authority is not None else len(held)

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

                def collect(t: Target[K, V, V] = target, out: list[K] = own) -> None:
                    out.extend(t.gc(max_entries))

                _settle(target.name, "gc", SyncPolicy.BEST_EFFORT, collect)
                collected.update(dict.fromkeys(own))
        return list(collected)

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
        for part in self.components:
            held[id(part)] = part.read(key)
        return assemble(Parts(held))

    def _assemble_many(self, keys: Sequence[K]) -> dict[K, V]:
        assemble = self.assemble
        assert assemble is not None
        if not keys:
            return {}
        wanted = list(dict.fromkeys(keys))
        found: dict[int, Mapping[K, object]] = {}
        authority = self.authority
        if authority is not None:
            hits = authority.batch_read(wanted)
            found[id(authority)] = hits
            wanted = [k for k in wanted if k in hits]
        for part in self.components:
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
        into = [t for t in consulted if t.fill]
        if not into:
            return
        clock = _clock(source.stat(key)) if any(t.supports_write_clock for t in into) else None
        for target in into:
            options = target.receives(WriteOptions(created_at=clock))
            _settle(
                target.name,
                "fill",
                SyncPolicy.BEST_EFFORT,
                lambda t=target, o=options: t.write(key, value, o),
            )

    def _batch_fill(
        self,
        hits: Mapping[K, V],
        source: Target[K, V, V],
        consulted: list[Target[K, V, V]],
    ) -> None:
        into = [t for t in consulted if t.fill]
        if not into:
            return
        stats = source.batch_stat(list(hits)) if any(t.supports_write_clock for t in into) else {}
        for target in into:

            def fill(t: Target[K, V, V] = target) -> None:
                if not t.supports_write_clock:
                    t.batch_write(hits, WriteOptions())
                    return
                # One clock per key, so one write per key: a batch takes one.
                for k, v in hits.items():
                    t.write(k, v, WriteOptions(created_at=_clock(stats.get(k))))

            _settle(target.name, "fill", SyncPolicy.BEST_EFFORT, fill)

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
    """
    into = [t for t in consulted if t.fill]
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
        except Exception as exc:
            _log("fill", target.name, exc)
            close_stream(current)
            current = source.read_stream(key)
            continue
        current = target.read_stream(key) or source.read_stream(key)
    return current


def _clock(meta: RecordStat | None) -> datetime | None:
    """The clock a filled value carries, so it keeps its remaining life."""
    return None if meta is None else meta.created_at or meta.modified_at


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


def _settle(name: str, what: str, policy: SyncPolicy, run: Callable[[], object]) -> None:
    """Run an operation after the commit point under ``policy``.

    A failure here is after the commit point, whatever a child composition
    says about its own record: ``REQUIRED`` raises :class:`PartialSyncError`,
    ``BEST_EFFORT`` logs and carries on.
    """
    try:
        run()
    except Exception as exc:
        if policy is SyncPolicy.BEST_EFFORT:
            _log(what, name, exc)
            return
        if isinstance(exc, PartialSyncError):
            raise
        raise PartialSyncError(
            f"{name} {what} failed after the record was committed",
            failures={name: exc},
        ) from exc


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

    def set_if_absent(
        self,
        key: K,
        value: V,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> bool:
        options = WriteOptions(ttl=ttl, created_at=created_at)
        self._admit(options)
        return self.topology().write_if_absent(
            key, value, options, self._dependent_syncs()
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

    def get(self, key: K) -> V | None:
        return self.topology().read(key)

    def batch_get(self, keys: Sequence[K]) -> dict[K, V]:
        return self.topology().batch_read(keys)

    def exists(self, key: K) -> bool:
        return self.topology().exists(key)

    def batch_exists(self, keys: Sequence[K]) -> set[K]:
        return self.topology().batch_exists(keys)

    def child_stores(self) -> tuple[object, ...]:
        """Everything directly beneath this composition."""
        return self.topology().children()
