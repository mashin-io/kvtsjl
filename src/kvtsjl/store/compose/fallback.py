"""Coalesce / fallback-read — left-biased merge with optional promote."""

from __future__ import annotations

from kvtsjl.keymap_algebra.coalesce import CoalescedKeyMap
from kvtsjl.scope import Scope, Scoped
from kvtsjl.store.compose.topology import StoreTopologyMixin
from kvtsjl.store.fanout import ScopeFanoutMixin
from kvtsjl.store.logical import KvStore
from kvtsjl.store.schema.ttl import CopyClock


class CoalescedKvStore[K, V](
    StoreTopologyMixin[K, V],
    CoalescedKeyMap[K, V],
    ScopeFanoutMixin,
    KvStore[K, V],
):
    """Read primary then secondary; writes go to primary only. Scan: primary only.

    The primary is the authority and the secondary a fallback — see
    :mod:`kvtsjl.keymap_algebra.topology` for what that implies.
    ``exists`` / ``stat`` probe the secondary on a primary miss but **do not
    promote** — only ``get`` / ``batch_get`` / ``read_stream`` write a
    secondary hit into the primary. By default it carries the record's write
    clock, so a promoted value keeps its remaining life; with
    ``copy_clock=CopyClock.COPY`` — a cache in front of a durable store — it
    stays for the primary's own TTL from when it is promoted, and the
    secondary is not asked for a clock. Promotion is **best-effort**: the read
    still returns the secondary value. Scan never promotes: it is the
    primary's own scan.
    """

    def __init__(
        self,
        primary: KvStore[K, V],
        secondary: KvStore[K, V],
        *,
        promote: bool = True,
        copy_clock: CopyClock = CopyClock.RECORD,
    ) -> None:
        super().__init__(primary, secondary, promote=promote, copy_clock=copy_clock)
        self.scope = primary.scope
        self.batch_size = primary.batch_size
        self._primary = primary
        self._secondary = secondary

    def _clone_fanout(self, scope: Scope) -> CoalescedKvStore[K, V]:
        return CoalescedKvStore(
            self._primary.clone_with_scope(scope),
            self._secondary.clone_with_scope(scope),
            promote=self._promote,
            copy_clock=self._copy_clock,
        )

    def beneath(self) -> KvStore[Scoped[K], V]:
        """Rebuilt over each side's ``beneath()``: one batch per side, across scopes."""
        return CoalescedKvStore(
            self._primary.beneath(),
            self._secondary.beneath(),
            promote=self._promote,
            copy_clock=self._copy_clock,
        )

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import compose_repr

        return compose_repr(
            type(self).__name__,
            primary=self._primary,
            secondary=self._secondary,
            promote=self._promote,
            copy_clock=self._copy_clock.value,
        )


# Public alias — same class, preserve import path and historical name.
FallbackReadKvStore = CoalescedKvStore
