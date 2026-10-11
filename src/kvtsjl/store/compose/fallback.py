"""Coalesce / fallback-read — left-biased merge with optional promote."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from datetime import datetime
import logging
from typing import Any, cast

from kvtsjl.keymap_algebra.coalesce import CoalescedKeyMap
from kvtsjl.scope import Scope
from kvtsjl.store.fanout import GcFanoutMixin, ScanPrimaryMixin, ScopeFanoutMixin
from kvtsjl.store.logical import KvStore
from kvtsjl.store.schema.layout import KeyLayout
from kvtsjl.store.schema.stat import RecordStat
from kvtsjl.store.schema.ttl import TtlPolicy
from kvtsjl.store.streaming import StreamSource, close_stream

logger = logging.getLogger(__name__)


class CoalescedKvStore[K, V](
    CoalescedKeyMap[K, V],
    ScanPrimaryMixin[K, V],
    GcFanoutMixin[K],
    ScopeFanoutMixin,
    KvStore[K, V],
):
    """Read primary then secondary; writes go to primary only. Scan: primary only.

    ``exists`` / ``stat`` probe secondary on primary miss but **do not promote**
    — only ``get`` / ``batch_get`` may write a secondary hit into the primary.

    Promote failures are **best-effort**: the read still returns the secondary
    value; a failed promote is logged (correct behaviour after a successful
    read, not a sync bug).

    Scan never promotes: values come from ``primary.batch_get``, not ``self.batch_get``.
    """

    def __init__(
        self,
        primary: KvStore[K, V],
        secondary: KvStore[K, V],
        *,
        promote: bool = True,
    ) -> None:
        super().__init__(primary, secondary, promote=promote)
        self.scope = primary.scope
        self.batch_size = primary.batch_size
        self._primary = primary
        self._secondary = secondary
        self._scan_primary = primary
        self._gc_parts = (primary, secondary)

    def _left_store(self) -> KvStore[K, V]:
        return cast(KvStore[K, V], self._left)

    def _right_store(self) -> KvStore[K, V]:
        return cast(KvStore[K, V], self._right)

    @property
    def supports_write_clock(self) -> bool:
        return self._primary.supports_write_clock

    def key_layout(self) -> KeyLayout:
        return self._primary.key_layout()

    def get(self, key: K) -> V | None:
        value = self._primary.get(key)
        if value is not None:
            return value
        value = self._secondary.get(key)
        if value is not None and self._promote:
            try:
                self._promote_one(key, value)
            except Exception:
                # BEST_EFFORT: keep the read result; promote is non-fatal.
                try:
                    logger.exception("fallback promote failed for key")
                except Exception:
                    # Logging must not replace the successful read result.
                    pass
        return value

    def exists(self, key: K) -> bool:
        if self._primary.exists(key):
            return True
        return self._secondary.exists(key)

    def batch_exists(self, keys: Sequence[K]) -> set[K]:
        found = self._primary.batch_exists(keys)
        missing = [k for k in keys if k not in found]
        if missing:
            found |= self._secondary.batch_exists(missing)
        return found

    def stat(self, key: K) -> RecordStat | None:
        meta = self._primary.stat(key)
        if meta is not None:
            return meta
        return self._secondary.stat(key)

    def batch_stat(self, keys: Sequence[K]) -> dict[K, RecordStat]:
        out = self._primary.batch_stat(keys)
        missing = [k for k in keys if k not in out]
        if missing:
            out.update(self._secondary.batch_stat(missing))
        return out

    def set(
        self,
        key: K,
        value: V,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        self._primary.set(key, value, ttl=ttl, created_at=created_at)

    def delete(self, key: K) -> bool:
        return self._primary.delete(key)

    def batch_get(self, keys: Sequence[K]) -> dict[K, V]:
        found = self._primary.batch_get(keys)
        missing = [k for k in keys if k not in found]
        if not missing:
            return found
        secondary_found = self._secondary.batch_get(missing)
        if secondary_found and self._promote:
            try:
                self._promote_many(secondary_found)
            except Exception:
                # BEST_EFFORT: keep the read result; promote is non-fatal.
                try:
                    logger.exception("fallback batch promote failed")
                except Exception:
                    # Logging must not replace the successful read result.
                    pass
        found.update(secondary_found)
        return found

    def batch_set(
        self,
        items: Mapping[K, V],
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        self._primary.batch_set(items, ttl=ttl, created_at=created_at)

    def batch_delete(self, keys: Sequence[K]) -> int:
        return self._primary.batch_delete(keys)

    def set_stream[Chunkable: (bytes, str)](
        self: CoalescedKvStore[K, Chunkable],
        key: K,
        source: StreamSource,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
        size: int | None = None,
    ) -> None:
        """Writes go to the primary only, exactly as ``set`` does."""
        self._primary.set_stream(key, source, ttl=ttl, created_at=created_at, size=size)

    def stream_chunk_type(self) -> type[Any]:
        return self._primary.stream_chunk_type()

    def read_stream[Chunkable: (bytes, str)](
        self: CoalescedKvStore[K, Chunkable], key: K
    ) -> Iterator[Chunkable] | None:
        """Serve from the primary, else stream the secondary through.

        When promoting, the secondary's chunks are consumed by the write into
        the primary and the caller is then served from the primary — one read
        of each side, no temporary file and no buffer. The write clock is
        carried across so a promoted value keeps its remaining life rather than
        restarting it.

        A failed promote is BEST_EFFORT, as the non-streaming promote is: the
        secondary is re-read so the caller still gets the value.
        """
        chunks = self._primary.read_stream(key)
        if chunks is not None:
            return chunks
        if not self._promote:
            return self._secondary.read_stream(key)

        clock = None
        if self._primary.supports_write_clock:
            meta = self._secondary.stat(key)
            if meta is not None:
                clock = meta.created_at or meta.modified_at
        source = self._secondary.read_stream(key)
        if source is None:
            return None
        if not self._promote_stream(key, source, clock):
            # ``source`` is spent part-way through; close it so the medium's
            # response is released before the secondary is read again.
            close_stream(source)
            return self._secondary.read_stream(key)
        return self._primary.read_stream(key) or self._secondary.read_stream(key)

    def _promote_stream[Chunkable: (bytes, str)](
        self: CoalescedKvStore[K, Chunkable],
        key: K,
        chunks: Iterator[Chunkable],
        created_at: datetime | None,
    ) -> bool:
        try:
            self._primary.set_stream(key, chunks, created_at=created_at)
        except Exception:
            # BEST_EFFORT, same as the non-streaming promote: the read already
            # succeeded, so a failed promote must not fail the call.
            try:
                logger.exception("fallback promote set_stream failed for key")
            except Exception:
                # Logging must not replace the successful read result.
                pass
            return False
        return True

    def _promote_one(self, key: K, value: V) -> None:
        if self._primary.supports_write_clock:
            meta = self._secondary.stat(key)
            clock = None
            if meta is not None:
                clock = meta.created_at or meta.modified_at
            self._primary.set(key, value, created_at=clock)
        else:
            self._primary.set(key, value)

    def _promote_many(self, items: Mapping[K, V]) -> None:
        if not self._primary.supports_write_clock:
            self._primary.batch_set(items)
            return
        stats = self._secondary.batch_stat(list(items.keys()))
        for key, value in items.items():
            meta = stats.get(key)
            clock = None
            if meta is not None:
                clock = meta.created_at or meta.modified_at
            self._primary.set(key, value, created_at=clock)

    def _clone_fanout(self, scope: Scope) -> CoalescedKvStore[K, V]:
        return CoalescedKvStore(
            self._primary.clone_with_scope(scope),
            self._secondary.clone_with_scope(scope),
            promote=self._promote,
        )

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import compose_repr

        return compose_repr(
            type(self).__name__,
            primary=self._primary,
            secondary=self._secondary,
            promote=self._promote,
        )


# Public alias — same class, preserve import path and historical name.
FallbackReadKvStore = CoalescedKvStore
