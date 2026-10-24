"""Write-through mirror logical store."""

from __future__ import annotations

from kvtsjl.keymap_algebra.topology import Role, Topology
from kvtsjl.scope import Scope
from kvtsjl.store.compose.delegating import DelegatingKvStore
from kvtsjl.store.compose.topology import StoreTopologyMixin
from kvtsjl.store.logical import KvStore
from kvtsjl.store.schema.policy import SyncPolicy


class MirrorKvStore[K, V](StoreTopologyMixin[K, V], DelegatingKvStore[K, V]):
    """Reads/scans from primary; writes/deletes go to both.

    The primary is the authority and the secondary a dependent copy, so every
    operation's order follows from those roles — see
    :mod:`kvtsjl.keymap_algebra.topology`. Secondary failures follow
    ``sync_policy`` (default ``REQUIRED``); GC on the secondary is always
    best-effort.
    """

    def __init__(
        self,
        primary: KvStore[K, V],
        secondary: KvStore[K, V],
        *,
        sync_policy: SyncPolicy = SyncPolicy.REQUIRED,
    ) -> None:
        super().__init__(primary)
        self._primary = primary
        self._secondary = secondary
        self._sync_policy = sync_policy

    @property
    def sync_policy(self) -> SyncPolicy:
        return self._sync_policy

    def topology(self) -> Topology[K, V]:
        """The primary is the authority; the secondary a dependent copy."""
        return Topology(
            (
                self._target("primary", Role.AUTHORITY, self._primary),
                self._target(
                    "secondary",
                    Role.DEPENDENT,
                    self._secondary,
                    policy=self._sync_policy,
                ),
            )
        )

    def clone_with_scope(self, scope: Scope) -> KvStore[K, V]:
        return MirrorKvStore(
            self._primary.clone_with_scope(scope),
            self._secondary.clone_with_scope(scope),
            sync_policy=self._sync_policy,
        )

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import compose_repr

        return compose_repr(
            "MirrorKvStore",
            primary=self._primary,
            secondary=self._secondary,
            sync_policy=self._sync_policy,
        )
