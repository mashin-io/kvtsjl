"""Fan-out and related store policies."""

from __future__ import annotations

from enum import Enum


class SyncPolicy(str, Enum):
    """How compose operators treat fan-out (index / mirror secondary) failures.

    Does not roll back the primary write. ``REQUIRED`` only means the sync must
    succeed for the call to succeed — see ``PartialSyncError``.
    """

    REQUIRED = "required"
    """Fan-out failure fails the call; primary write stays."""

    BEST_EFFORT = "best_effort"
    """Keep the primary write, log the fan-out failure, continue."""
