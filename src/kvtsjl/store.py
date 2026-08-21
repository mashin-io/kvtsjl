"""KvStore ABC, mixins, and shared high-level ops."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Literal, overload

from kvtsjl.batching import DEFAULT_BATCH_SIZE, chunk_sequence
from kvtsjl.exceptions import KvStoreScanUnsupported, KvStoreScopeError
from kvtsjl.key_layout import ScanQuery, supports_prefix_scan
from kvtsjl.kvset import KvSet
from kvtsjl.namespace import CollectionBinding, NamespaceBinder
from kvtsjl.scope import Scope


class KvStore[K, V, KBLOB, VBLOB, COLL](ABC):
    """Typed key-value store bound to a KvSet, Scope, and namespace binding.

    ``COLL`` is the backend collection-handle type (e.g. ``str`` for named
    collections, ``None`` for flat key-prefix binders).
    """

    def __init__(
        self,
        kvset: KvSet[K, V, KBLOB, VBLOB],
        *,
        scope: Scope | None = None,
        binder: NamespaceBinder[KBLOB, COLL] | None = None,
        binding: CollectionBinding[KBLOB, COLL] | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        self.kvset = kvset
        self.scope = scope or Scope.empty()
        kvset.validate_scope(self.scope)
        self.batch_size = batch_size
        if binding is not None:
            self._binding = binding
        elif binder is not None:
            self._binding = binder.bind(kvset)
        else:
            raise TypeError("KvStore requires binder= or binding=")

    @property
    def binding(self) -> CollectionBinding[KBLOB, COLL]:
        return self._binding

    # --- abstract core ---

    @abstractmethod
    def get(self, key: K) -> V | None: ...

    @abstractmethod
    def set(self, key: K, value: V) -> None: ...

    @abstractmethod
    def delete(self, key: K) -> bool: ...

    @abstractmethod
    def batch_get(self, keys: Sequence[K]) -> dict[K, V]: ...

    @abstractmethod
    def batch_set(self, items: Mapping[K, V]) -> None: ...

    @abstractmethod
    def batch_delete(self, keys: Sequence[K]) -> int: ...

    @abstractmethod
    def _scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        """Yield decoded keys (and values if requested) under scope + prefix."""

    # --- high-level ---

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
        if prefix is not None and not supports_prefix_scan(self.kvset.key_layout):
            raise KvStoreScanUnsupported(
                f"prefix scan unsupported for layout {self.kvset.key_layout!r}"
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

    # --- views / composition ---

    def scoped(
        self, scope: Scope | None = None, **kinds_to_ids: str
    ) -> KvStore[K, V, KBLOB, VBLOB, COLL]:
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
    ) -> KvStore[K, V, KBLOB, VBLOB, COLL]:
        """Alias of ``scoped`` — scope is a logical key prefix."""
        return self.scoped(scope, **kinds_to_ids)

    def _clone_with_scope(self, scope: Scope) -> KvStore[K, V, KBLOB, VBLOB, COLL]:
        raise NotImplementedError(
            f"{type(self).__name__} must implement _clone_with_scope"
        )

    def readonly(self) -> KvStore[K, V, KBLOB, VBLOB, COLL]:
        from kvtsjl.compose import ReadonlyKvStore

        return ReadonlyKvStore(self)

    @classmethod
    def readonly_of(
        cls, store: KvStore[K, V, KBLOB, VBLOB, COLL]
    ) -> KvStore[K, V, KBLOB, VBLOB, COLL]:
        from kvtsjl.compose import ReadonlyKvStore

        return ReadonlyKvStore(store)

    def fallback_read[COLL2](
        self,
        secondary: KvStore[K, V, KBLOB, VBLOB, COLL2],
        *,
        promote: bool = True,
    ) -> KvStore[K, V, KBLOB, VBLOB, COLL]:
        """Read-through secondary on miss (e.g. new version → old version).

        Name/version/scope/COLL need not match — callers compose deliberately
        (``v2.fallback_read(v1)``) for migrations and layered caches.
        """
        from kvtsjl.compose import FallbackReadKvStore

        return FallbackReadKvStore(self, secondary, promote=promote)

    def mirror[COLL2](
        self, secondary: KvStore[K, V, KBLOB, VBLOB, COLL2]
    ) -> KvStore[K, V, KBLOB, VBLOB, COLL]:
        """Write-through to secondary; reads stay on primary."""
        from kvtsjl.compose import MirrorKvStore

        return MirrorKvStore(self, secondary)

    # --- helpers for backends ---

    def _physical_key_blob(self, key: K) -> KBLOB:
        leaf = self.kvset.key_serde.serialize(key)
        return self._binding.item_key(
            self.scope,
            leaf,
            str_serde=self.kvset.str_serde,
            blob_ops=self.kvset.blob_ops,
        )

    def _scan_prefix_blob(self, key_prefix: K | None) -> KBLOB:
        leaf: KBLOB | None = None
        if key_prefix is not None:
            leaf = self.kvset.key_serde.serialize(key_prefix)
        return self._binding.scope_prefix(
            self.scope,
            str_serde=self.kvset.str_serde,
            blob_ops=self.kvset.blob_ops,
            key_prefix_blob=leaf,
        )

    def _decode_key_from_physical(self, physical_key: KBLOB) -> K | None:
        """Decode leaf K from a full in-key physical key under this scope."""
        ops = self.kvset.blob_ops
        binding = self._binding
        name_version = list(binding._name_version_prefix)
        rest = physical_key
        if name_version:
            nv = ops.join(name_version)
            if not ops.startswith(rest, nv):
                return None
            stripped = ops.strip_prefix(rest, nv)
            if stripped is None:
                return None
            if ops.len(stripped) >= ops.len(ops.separator) and ops.startswith(
                stripped, ops.separator
            ):
                stripped = ops.strip_prefix(stripped, ops.separator)
            if stripped is None:
                return None
            rest = stripped
        return self.kvset.decode_leaf_from_in_key(self.scope, rest)

    def ttl_seconds(self) -> int | None:
        return self.kvset.ttl_policy.ttl_seconds()
