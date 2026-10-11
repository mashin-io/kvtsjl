"""Logical ``KvStore`` ABC: domain K/V, scope, scan, composition."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any, Self, cast

from kvtsjl.batching import chunk_sequence
from kvtsjl.keymap import KeyMap
from kvtsjl.scope import Scope
from kvtsjl.store.capabilities import ExpiringMixin, TtlWritesMixin
from kvtsjl.store.readable import ReadableKvStore
from kvtsjl.store.schema.policy import SyncPolicy
from kvtsjl.store.schema.ttl import TtlPolicy
from kvtsjl.store.streaming import StreamingView, StreamSource

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

    # --- streaming (byte-valued stores only) ---------------------------------
    #
    # ``self: KvStore[K, bytes]`` restricts these to stores whose value *is* the
    # a chunkable surface type. Calling them on any other value type is a type
    # error, not a method that exists and refuses — the distinction this
    # codebase keeps coming back to. NotImplementedError is left for a real
    # backend limit.
    #
    # ``Chunkable`` is bytes-or-str because those are the value types that can
    # be split and rejoined at all; whether the *serde* can also be applied
    # chunk by chunk is a separate question, answered at runtime by
    # ``require_streaming_serde`` (a str store with a UTF-8 serde streams; one
    # with a non-incremental codec does not). The type rules out the cases that
    # can never work; the guard rules out the rest.

    def streaming[C](self, chunk_type: type[C]) -> StreamingView[K, C]:
        """A writable streaming view typed in ``chunk_type``.

        Narrows ``ReadableKvStore.streaming`` — a store that can be written to
        yields a view that can write.
        """
        self._check_chunk_type(chunk_type)
        return StreamingView[K, C](self, chunk_type)

    def set_stream[Chunkable: (bytes, str)](
        self: KvStore[K, Chunkable],
        key: K,
        source: StreamSource,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
        size: int | None = None,
    ) -> None:
        """Write ``source`` to ``key`` without holding the whole value.

        ``source`` is a path, an open stream whose ``read(n)`` returns chunks
        of the value type, or an iterable of such chunks. ``ttl`` and ``created_at``
        behave exactly as on ``set``, so a stream-written value expires, scans
        and garbage-collects identically to a ``set`` one.

        ``size`` is the **exact** encoded byte count, or ``None`` when it is
        not known. It is not a hint: GCS receives it as ``size=`` and Azure as
        ``length=``, and both upload exactly that many bytes — too small
        truncates the object, too large fails the upload. ``None`` is always
        safe, and a serde that changes the length reports ``None`` from
        ``encoded_size`` for exactly this reason.

        Stores that cannot stream raise rather than buffering: buffering is
        precisely what callers use this for. Small-value leaves (memory, Redis)
        may buffer, but they override and say so.
        """
        raise NotImplementedError(f"{type(self).__name__} does not support set_stream")

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
        from kvtsjl.keymap_algebra.build import build_coalesce

        return cast(KvStore[K, V], build_coalesce(self, secondary, promote=promote))

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
            cast(Any, indexes),
            default_index=cast(Any, default),
            bundle=bundle,
            sync_policy=sync_policy,
        )
