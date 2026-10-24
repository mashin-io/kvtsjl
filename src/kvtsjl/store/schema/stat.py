"""Per-key record statistics (presence metadata)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class RecordStat:
    """Medium metadata for a present key (not a presence flag by itself).

    ``modified_at`` is the medium write time when the leaf tracks one separately
    from the expiry clock. On Filesystem ``MTIME`` mode, mtime *is* the expiry
    clock (``created_at=`` rewrites it via ``os.utime``), so ``modified_at``
    equals that clock — there is no second timestamp.
    """

    created_at: datetime | None = None
    modified_at: datetime | None = None
    expires_at: datetime | None = None
    size: int | None = None
    etag: str | None = None
    persistent: bool = False

    @property
    def write_clock(self) -> datetime | None:
        """The clock a copy of this value carries, so it keeps its remaining life.

        ``created_at`` where the medium keeps one, else the last write time.
        """
        return self.created_at or self.modified_at
