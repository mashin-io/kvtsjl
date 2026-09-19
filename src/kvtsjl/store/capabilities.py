"""Capability mixins for store composition (not part of the public ``__all__``)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator, Mapping
from datetime import datetime
from typing import Literal, Self, overload

from kvtsjl.exceptions import KvStoreScanUnsupported, KvStoreScopeError
from kvtsjl.scope import Scope
from kvtsjl.store.schema.layout import KeyLayout, ScanQuery, supports_prefix_scan
from kvtsjl.store.schema.ttl import TtlPolicy, require_write_clock_supported


class ScopedMixin(ABC):
    """Logical key-prefix scoping.

    ``clone_with_scope`` has a concrete raising default (P1 fail-closed). Leaves
    and scopable indexes override; vector indexes may inherit the default.
    """

    scope: Scope

    def scoped(self, scope: Scope | None = None, **kinds_to_ids: str) -> Self:
        """Narrow to a longer logical key prefix (appends scope segments)."""
        if scope is not None and kinds_to_ids:
            raise KvStoreScopeError("pass scope= or kwargs, not both")
        if scope is None:
            scope = self.scope.child(**kinds_to_ids) if kinds_to_ids else self.scope
        else:
            scope = Scope(segments=self.scope.segments + scope.segments)
        return self.clone_with_scope(scope)

    def prefixed(self, scope: Scope | None = None, **kinds_to_ids: str) -> Self:
        """Alias of ``scoped`` — scope is a logical key prefix."""
        return self.scoped(scope, **kinds_to_ids)

    def clone_with_scope(self, scope: Scope) -> Self:
        raise KvStoreScopeError(
            f"{type(self).__name__} cannot be scoped with the store"
        )


class ScannableMixin[K, V](ABC):
    """Prefix scan / list over a keyed document space."""

    batch_size: int

    @abstractmethod
    def key_layout(self) -> KeyLayout:
        """Key physicalization policy (from the leaf ``KvSet``)."""

    @abstractmethod
    def scan_entries(self, query: ScanQuery[K]) -> Iterator[tuple[K, V | None]]:
        """Yield decoded keys (and values if requested) under scope + prefix."""

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
            for key, value in self.scan_entries(query):
                if value is None:
                    continue
                yield key, value
        else:
            for key, _value in self.scan_entries(query):
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


class ExpiringMixin[K](ABC):
    """Explicit GC of expired entries."""

    def gc_expired_keys(self, *, max_entries: int) -> list[K]:
        """Delete up to ``max_entries`` expired entries; return deleted keys."""
        if max_entries < 1:
            raise ValueError(f"max_entries must be >= 1, got {max_entries}")
        return []

    def gc_expired(self, *, max_entries: int) -> int:
        """Delete expired entries under this scope, up to ``max_entries``."""
        return len(self.gc_expired_keys(max_entries=max_entries))


class TtlWritesMixin[K, V](ABC):
    """Writes that may carry per-write ``ttl=`` / ``created_at=``."""

    @property
    def supports_write_clock(self) -> bool:
        return False

    @abstractmethod
    def set(
        self,
        key: K,
        value: V,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        """Insert or replace ``key``. ``ttl=None`` uses the collection policy."""

    def batch_set(
        self,
        items: Mapping[K, V],
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        require_write_clock_supported(
            created_at,
            allowed=self.supports_write_clock,
            backend=type(self).__name__,
        )
        for key, value in items.items():
            self.set(key, value, ttl=ttl, created_at=created_at)
