"""Logical ``KvStore`` ABC: domain K/V, scope, scan, composition."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any, Self

from kvtsjl.batching import chunk_sequence
from kvtsjl.keymap import KeyMap
from kvtsjl.scope import Scope
from kvtsjl.store.capabilities import ExpiringMixin, TtlWritesMixin
from kvtsjl.store.readable import ReadableKvStore
from kvtsjl.store.schema.policy import SyncPolicy
from kvtsjl.store.schema.ttl import TtlPolicy

if TYPE_CHECKING:
    from kvtsjl.index.logical.abc import Index
    from kvtsjl.store.compose.indexed import IndexedKvStore
    from kvtsjl.store.readable import ReadableKvStore as ReadableKvStoreType


class KvStore[K, V](
    ReadableKvStore[K, V],
    KeyMap[K, V],
    TtlWritesMixin[K, V],
    ExpiringMixin[K],
    ABC,
):
    """Logical document store: read + write + scope + scan + TTL + GC.

    Physical schema (``KvSet``, blob serdes, collection binding) lives on
    ``KvBackend`` / ``PhysicalBackend`` only. Logical wrappers delegate I/O to a leaf.

    Batch contract: default ``batch_get`` may loop; leaf overrides should use
    native bulk. Composed stores (``zip``, ``fallback``, ``mirror``, …) must
    fan out to part ``batch_get``, not ``N×get``.
    """

    @abstractmethod
    def set(
        self,
        key: K,
        value: V,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        """Insert or replace ``key``. ``ttl=None`` uses the ``KvSet`` policy.

        ``created_at`` is only accepted when ``supports_write_clock`` is True;
        otherwise raise (same family as unsupported per-write ``ttl=``).
        """

    def clone_with_scope(self, scope: Scope) -> Self:
        raise NotImplementedError(
            f"{type(self).__name__} must implement clone_with_scope"
        )

    def get_or_set(self, key: K, compute_value_fn: Callable[[], V]) -> V:
        existing = self.get(key)
        if existing is not None:
            return existing
        value = compute_value_fn()
        self.set(key, value)
        return value

    def batch_get_or_set(
        self,
        keys: Sequence[K],
        compute_missing_values_fn: Callable[[Sequence[K]], Mapping[K, V]],
    ) -> dict[K, V]:
        result: dict[K, V] = {}
        for chunk in chunk_sequence(keys, self.batch_size):
            found = self.batch_get(chunk)
            result.update(found)
            missing = [k for k in chunk if k not in found]
            if not missing:
                continue
            computed = compute_missing_values_fn(missing)
            to_set = {k: computed[k] for k in missing if k in computed}
            if to_set:
                self.batch_set(to_set)
                result.update(to_set)
        return result

    def readonly(self) -> ReadableKvStoreType[K, V]:
        from kvtsjl.store.compose.readonly import ReadonlyKvStore

        return ReadonlyKvStore(self)

    @classmethod
    def readonly_of(cls, store: KvStore[K, V]) -> ReadableKvStoreType[K, V]:
        from kvtsjl.store.compose.readonly import ReadonlyKvStore

        return ReadonlyKvStore(store)

    def fallback_read(
        self,
        secondary: KvStore[K, V],
        *,
        promote: bool = True,
    ) -> KvStore[K, V]:
        """Read-through secondary on miss (e.g. new version → old version)."""
        from kvtsjl.store.compose.fallback import FallbackReadKvStore

        return FallbackReadKvStore(self, secondary, promote=promote)

    def mirror(
        self,
        secondary: KvStore[K, V],
        *,
        sync_policy: SyncPolicy = SyncPolicy.REQUIRED,
    ) -> KvStore[K, V]:
        """Write-through to secondary; reads stay on primary."""
        from kvtsjl.store.compose.mirror import MirrorKvStore

        return MirrorKvStore(self, secondary, sync_policy=sync_policy)

    def indexed(
        self,
        index: Index[Any, K, V, Any],
        /,
        *more: Index[Any, K, V, Any],
        sync_policy: SyncPolicy = SyncPolicy.REQUIRED,
    ) -> IndexedKvStore[K, V, None]:
        from kvtsjl.store.compose.indexed import IndexedKvStore

        indexes = (index, *more)
        default = index if not more else None
        return IndexedKvStore(
            self, indexes, default_index=default, sync_policy=sync_policy
        )

    def indexed_as[ViaT](
        self,
        bundle: ViaT,
        *,
        sync_policy: SyncPolicy = SyncPolicy.REQUIRED,
    ) -> IndexedKvStore[K, V, ViaT]:
        from kvtsjl.index.logical.bundle import indexes_from_bundle
        from kvtsjl.store.compose.indexed import IndexedKvStore

        indexes = indexes_from_bundle(bundle)
        default = indexes[0] if len(indexes) == 1 else None
        return IndexedKvStore(
            self,
            indexes,  # type: ignore[arg-type]
            default_index=default,  # type: ignore[arg-type]
            bundle=bundle,
            sync_policy=sync_policy,
        )
