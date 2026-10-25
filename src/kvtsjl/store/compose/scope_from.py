"""``scope_from`` — a store that takes only some kinds of the scope it is given."""

from __future__ import annotations

from kvtsjl.keymap_algebra.topology import Role, Topology
from kvtsjl.scope import Scope
from kvtsjl.store.compose.delegating import DelegatingKvStore
from kvtsjl.store.compose.topology import StoreTopologyMixin
from kvtsjl.store.logical import KvStore


class ScopeFromKvStore[K, V](StoreTopologyMixin[K, V], DelegatingKvStore[K, V]):
    """``store``, scoped by only the ``kinds`` of whatever scope it is given.

    A part of a record may be keyed more coarsely than the record: a turn is
    scoped ``(user, session)``, its receipts only ``(user, consumer_turn_id)``.
    Scoping the record hands each part the record's scope; this part takes
    the ``user`` segment of it and drops the rest. Every operation is the
    store's own — a composition of one target — and only scoping differs.

    **The client owns the keys' uniqueness.** A part scoped more coarsely than
    its record sees the keys of every record that shares its coarser scope,
    so two records must not share a key there: a turn's receipts are keyed
    by its id, which is unique per user.
    """

    def __init__(
        self, store: KvStore[K, V], kinds: tuple[str, ...], given: Scope | None = None
    ) -> None:
        super().__init__(store)
        self._kinds = kinds
        if given is not None:
            self.scope = given

    def topology(self) -> Topology[K, V]:
        return Topology((self._target("store", Role.AUTHORITY, self._underlying),))

    def clone_with_scope(self, scope: Scope) -> ScopeFromKvStore[K, V]:
        """Remember the scope given; hand the store only the kinds it takes."""
        narrowed = self._underlying.clone_with_scope(scope.only(*self._kinds))
        return ScopeFromKvStore(narrowed, self._kinds, scope)

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import compose_repr

        return compose_repr("ScopeFromKvStore", store=self._underlying, kinds=self._kinds)
