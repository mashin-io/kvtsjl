"""Logical ``KvStore`` ABC: domain K/V, scope, scan, composition."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Literal, Self, overload

from kvtsjl.batching import chunk_sequence
from kvtsjl.exceptions import KvStoreScanUnsupported, KvStoreScopeError
from kvtsjl.keymap import KeyMap
from kvtsjl.scope import Scope
from kvtsjl.store.schema.layout import KeyLayout, ScanQuery, supports_prefix_scan

if TYPE_CHECKING:
    from kvtsjl.index.logical.abc import Index
    from kvtsjl.store.compose.indexed import IndexedKvStore


class KvStore[K, V](KeyMap[K, V], ABC):
    """Logical document store: domain ``K`` / ``V``, scope views, and composition.

    Physical schema (``KvSet``, blob serdes, collection binding) lives on
    ``KvBackend`` / ``PhysicalBackend`` only. Logical wrappers delegate I/O to a leaf.
    """

    scope: Scope
    batch_size: int

    @abstractmethod
    def key_layout(self) -> KeyLayout:
        """Key physicalization policy (from the leaf ``KvSet``)."""

    @abstractmethod
    def _scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        """Yield decoded keys (and values if requested) under scope + prefix."""

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

    @overload
    def scan(
        self,
        prefix: K | None = None,
        *,
        include_values: Literal[False] = False,
        page_size: int = 100,
    ) -> Iterator[K]: ...

    @overload
    def scan(
        self,
        prefix: K | None = None,
        *,
        include_values: Literal[True],
        page_size: int = 100,
    ) -> Iterator[tuple[K, V]]: ...

    def scan(
        self,
        prefix: K | None = None,
        *,
        include_values: bool = False,
        page_size: int = 100,
    ) -> Iterator[K] | Iterator[tuple[K, V]]:
        if prefix is not None and not supports_prefix_scan(self.key_layout()):
            raise KvStoreScanUnsupported(
                f"prefix scan unsupported for layout {self.key_layout()!r}"
            )
        query = ScanQuery(
            prefix=prefix, include_values=include_values, page_size=page_size
        )
        if include_values:
            for key, value in self._scan_entries(query):
                if value is None:
                    continue
                yield key, value
        else:
            for key, _value in self._scan_entries(query):
                yield key

    def list(self, prefix: K | None = None) -> list[K]:
        return list(self.scan(prefix=prefix, include_values=False))

    @overload
    def scan_batches(
        self,
        prefix: K | None = None,
        *,
        batch_size: int | None = None,
        include_values: Literal[False] = False,
        page_size: int = 100,
    ) -> Iterator[list[K]]: ...

    @overload
    def scan_batches(
        self,
        prefix: K | None = None,
        *,
        batch_size: int | None = None,
        include_values: Literal[True],
        page_size: int = 100,
    ) -> Iterator[list[tuple[K, V]]]: ...

    def scan_batches(
        self,
        prefix: K | None = None,
        *,
        batch_size: int | None = None,
        include_values: bool = False,
        page_size: int = 100,
    ) -> Iterator[list[K]] | Iterator[list[tuple[K, V]]]:
        size = batch_size or self.batch_size
        if include_values:
            batch_v: list[tuple[K, V]] = []
            for item in self.scan(
                prefix=prefix, include_values=True, page_size=page_size
            ):
                batch_v.append(item)
                if len(batch_v) >= size:
                    yield batch_v
                    batch_v = []
            if batch_v:
                yield batch_v
        else:
            batch_k: list[K] = []
            for item in self.scan(
                prefix=prefix, include_values=False, page_size=page_size
            ):
                batch_k.append(item)
                if len(batch_k) >= size:
                    yield batch_k
                    batch_k = []
            if batch_k:
                yield batch_k

    def scoped(
        self, scope: Scope | None = None, **kinds_to_ids: str
    ) -> Self:
        """Narrow to a longer logical key prefix (appends scope segments)."""
        if scope is not None and kinds_to_ids:
            raise KvStoreScopeError("pass scope= or kwargs, not both")
        if scope is None:
            scope = self.scope.child(**kinds_to_ids) if kinds_to_ids else self.scope
        else:
            scope = Scope(segments=self.scope.segments + scope.segments)
        return self._clone_with_scope(scope)

    def prefixed(
        self, scope: Scope | None = None, **kinds_to_ids: str
    ) -> Self:
        """Alias of ``scoped`` — scope is a logical key prefix."""
        return self.scoped(scope, **kinds_to_ids)

    def _clone_with_scope(self, scope: Scope) -> Self:
        raise NotImplementedError(
            f"{type(self).__name__} must implement _clone_with_scope"
        )

    def readonly(self) -> KvStore[K, V]:
        from kvtsjl.store.compose.readonly import ReadonlyKvStore

        return ReadonlyKvStore(self)

    @classmethod
    def readonly_of(cls, store: KvStore[K, V]) -> KvStore[K, V]:
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

    def mirror(self, secondary: KvStore[K, V]) -> KvStore[K, V]:
        """Write-through to secondary; reads stay on primary."""
        from kvtsjl.store.compose.mirror import MirrorKvStore

        return MirrorKvStore(self, secondary)

    def indexed(
        self,
        index: Index[Any, K, V, Any],
        /,
        *more: Index[Any, K, V, Any],
    ) -> IndexedKvStore[K, V, None]:
        from kvtsjl.store.compose.indexed import IndexedKvStore

        indexes = (index, *more)
        default = index if not more else None
        return IndexedKvStore(self, indexes, default_index=default)

    def indexed_as[ViaT](self, bundle: ViaT) -> IndexedKvStore[K, V, ViaT]:
        from kvtsjl.index.logical.bundle import indexes_from_bundle
        from kvtsjl.store.compose.indexed import IndexedKvStore

        indexes = indexes_from_bundle(bundle)
        default = indexes[0] if len(indexes) == 1 else None
        return IndexedKvStore(
            self,
            indexes,  # type: ignore[arg-type]
            default_index=default,  # type: ignore[arg-type]
            bundle=bundle,
        )
