"""Write-through mirror logical store."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from datetime import datetime
import logging

from kvtsjl.exceptions import PartialSyncError
from kvtsjl.scope import Scope
from kvtsjl.store.compose.delegating import DelegatingKvStore
from kvtsjl.store.logical import KvStore
from kvtsjl.store.schema.layout import ScanQuery
from kvtsjl.store.schema.policy import SyncPolicy
from kvtsjl.store.schema.scan import Predicate
from kvtsjl.store.schema.ttl import TtlPolicy, require_write_clock_supported

logger = logging.getLogger(__name__)

_SECONDARY = "secondary"


class MirrorKvStore[K, V](DelegatingKvStore[K, V]):
    """Reads/scans from primary; writes/deletes go to both.

    Secondary fan-out failures follow ``sync_policy`` (default ``REQUIRED``).
    GC on the secondary is always best-effort (log and continue).
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

    @property
    def supports_write_clock(self) -> bool:
        return self._primary.supports_write_clock

    def get(self, key: K) -> V | None:
        return self._primary.get(key)

    def set(
        self,
        key: K,
        value: V,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        require_write_clock_supported(
            created_at,
            allowed=self._primary.supports_write_clock,
            backend=type(self).__name__,
        )
        self._primary.set(key, value, ttl=ttl, created_at=created_at)
        sec_created = created_at if self._secondary.supports_write_clock else None
        self._fanout_secondary(
            lambda: self._secondary.set(key, value, ttl=ttl, created_at=sec_created),
            what="set",
        )

    def delete(self, key: K) -> bool:
        deleted = self._primary.delete(key)
        self._fanout_secondary(
            lambda: self._secondary.delete(key),
            what="delete",
        )
        return deleted

    def batch_get(self, keys: Sequence[K]) -> dict[K, V]:
        return self._primary.batch_get(keys)

    def batch_set(
        self,
        items: Mapping[K, V],
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        require_write_clock_supported(
            created_at,
            allowed=self._primary.supports_write_clock,
            backend=type(self).__name__,
        )
        self._primary.batch_set(items, ttl=ttl, created_at=created_at)
        sec_created = created_at if self._secondary.supports_write_clock else None
        self._fanout_secondary(
            lambda: self._secondary.batch_set(items, ttl=ttl, created_at=sec_created),
            what="batch_set",
        )

    def batch_delete(self, keys: Sequence[K]) -> int:
        n = self._primary.batch_delete(keys)
        self._fanout_secondary(
            lambda: self._secondary.batch_delete(keys),
            what="batch_delete",
        )
        return n

    def scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        yield from self._primary.scan_entries(query)

    def delete_where(
        self,
        where: Predicate[K],
        *,
        limit: int | None = None,
        sync_policy: SyncPolicy | None = None,
    ) -> int:
        from kvtsjl.store.schema.scan import is_gc_predicate

        if is_gc_predicate(where):
            max_entries = limit if limit is not None else 2_147_483_647
            return len(self.gc_expired_keys(max_entries=max_entries))
        n = self._primary.delete_where(where, limit=limit, sync_policy=sync_policy)
        policy = sync_policy if sync_policy is not None else self._sync_policy
        self._fanout_secondary(
            lambda: self._secondary.delete_where(
                where, limit=limit, sync_policy=sync_policy
            ),
            what="delete_where",
            policy=policy,
        )
        return n

    def gc_expired_keys(self, *, max_entries: int) -> list[K]:
        if max_entries < 1:
            raise ValueError(f"max_entries must be >= 1, got {max_entries}")
        deleted = self._primary.gc_expired_keys(max_entries=max_entries)
        remaining = max_entries - len(deleted)
        if remaining < 1:
            return deleted
        # Always BEST_EFFORT — never fail a successful primary GC.
        try:
            deleted.extend(self._secondary.gc_expired_keys(max_entries=remaining))
        except Exception:
            try:
                logger.exception("mirror gc_expired on secondary failed")
            except Exception:
                # Logging must not replace / mask the GC path (already BEST_EFFORT).
                pass
        return deleted

    def _fanout_secondary(
        self,
        op: Callable[[], object],
        *,
        what: str,
        policy: SyncPolicy | None = None,
    ) -> None:
        effective = policy if policy is not None else self._sync_policy
        try:
            op()
        except Exception as exc:
            if effective is SyncPolicy.BEST_EFFORT:
                try:
                    logger.exception(
                        "mirror %s to secondary failed",
                        what,
                        exc_info=exc,
                    )
                except Exception:
                    # Logging must not replace the sync failure already in `failures`.
                    pass
                return
            raise PartialSyncError(
                f"mirror {what} to secondary failed",
                failures={_SECONDARY: exc},
            ) from exc

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
