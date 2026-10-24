"""``coalesce`` — left-biased merge (same as store ``fallback_read``)."""

from __future__ import annotations

from kvtsjl.keymap import KeyMap
from kvtsjl.keymap_algebra.topology import RecordTopologyMixin, Role, Topology


class CoalescedKeyMap[K, T](RecordTopologyMixin[K, T], KeyMap[K, T]):
    """``get`` prefers left; writes go to left only.

    The left side is the authority and the right a fallback, consulted when
    the left misses. With ``promote`` a fallback hit is written into the left,
    best-effort — the read already succeeded.
    """

    def __init__(
        self,
        left: KeyMap[K, T],
        right: KeyMap[K, T],
        *,
        promote: bool = True,
    ) -> None:
        self._left = left
        self._right = right
        self._promote = promote

    def topology(self) -> Topology[K, T]:
        return Topology(
            (
                self._target("primary", Role.AUTHORITY, self._left, fill=self._promote),
                self._target("secondary", Role.FALLBACK, self._right),
            )
        )
