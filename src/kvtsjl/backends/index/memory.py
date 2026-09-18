"""In-memory index store leaf backends."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import cast

from kvtsjl.backends.index._null_meta import NullMetaIndexBackend, null_meta_index_set
from kvtsjl.bind import NativeStrCollectionBinder
from kvtsjl.exceptions import KvStoreIndexError
from kvtsjl.index.logical.abc import IndexHit
from kvtsjl.index.schema.index_set import IndexSet
from kvtsjl.scope import Scope, ScopeSegment
from kvtsjl.serde import SerDe


@dataclass
class _KeyIndexRoot[K]:
    """Shared key-membership postings, bucketed by ``Scope.segments``.

    Must not key on ``path_display()``: ``/`` in a segment id would collide with
    multi-segment scopes (e.g. ``tenant=a`` + ``x=b`` vs ``tenant=a/x/b``).
    """

    by_scope: dict[tuple[ScopeSegment, ...], set[K]] = field(default_factory=dict)

    def for_scope(self, scope: Scope) -> set[K]:
        key = scope.segments
        bucket = self.by_scope.get(key)
        if bucket is None:
            bucket = set()
            self.by_scope[key] = bucket
        return bucket


@dataclass
class _TermPostings[K]:
    term_to_keys: dict[str, set[K]] = field(
        default_factory=lambda: defaultdict(set)
    )
    key_to_terms: dict[K, set[str]] = field(default_factory=dict)


@dataclass
class _TermIndexRoot[K]:
    """Shared term postings, bucketed by ``Scope.segments`` (not ``path_display()``)."""

    by_scope: dict[tuple[ScopeSegment, ...], _TermPostings[K]] = field(
        default_factory=dict
    )

    def for_scope(self, scope: Scope) -> _TermPostings[K]:
        key = scope.segments
        bucket = self.by_scope.get(key)
        if bucket is None:
            bucket = _TermPostings()
            self.by_scope[key] = bucket
        return bucket


class MemoryKeyIndex[K, V](NullMetaIndexBackend[K, K, V]):
    """Exact membership index: query type is the document key ``K``."""

    def __init__(
        self,
        *,
        index_set: IndexSet[K, None, str, str] | None = None,
        sync_on_write: bool = True,
        scope: Scope | None = None,
        root: _KeyIndexRoot[K] | None = None,
    ) -> None:
        wire = index_set or cast(
            IndexSet[K, None, str, str],
            null_meta_index_set("mem-keys", id_serde=SerDe.identity(str)),
        )
        super().__init__(wire, scope=scope, binder=NativeStrCollectionBinder())
        self.sync_on_write = sync_on_write
        self._root: _KeyIndexRoot[K] = root if root is not None else _KeyIndexRoot()

    def _keys(self) -> set[K]:
        return self._root.for_scope(self.scope)

    def clone_with_scope(self, scope: Scope) -> MemoryKeyIndex[K, V]:
        return MemoryKeyIndex(
            index_set=self.index_set,
            sync_on_write=self.sync_on_write,
            scope=scope,
            root=self._root,
        )

    def search(self, query: K, *, limit: int = 100) -> Sequence[IndexHit[K, None]]:
        if limit <= 0:
            return []
        if query in self._keys():
            return [IndexHit(key=query, meta=None)]
        return []

    def get(self, key: K) -> None:
        return None

    def meta_of(self, key: K, value: V, *, previous: None) -> None:
        return None

    def upsert(self, key: K, value: V, meta: None) -> None:
        self._keys().add(key)

    def set(self, key: K, value: None) -> None:
        if key not in self._keys():
            raise KvStoreIndexError("key is not in the index")

    def delete(self, key: K) -> bool:
        keys = self._keys()
        if key not in keys:
            return False
        keys.discard(key)
        return True

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import index_backend_repr

        return index_backend_repr(self)


class MemoryTermIndex[K, V](NullMetaIndexBackend[str, K, V]):
    """Exact term → keys multimaps (in-memory)."""

    def __init__(
        self,
        terms_of: Callable[[K, V], Sequence[str]],
        *,
        index_set: IndexSet[K, None, str, str] | None = None,
        sync_on_write: bool = True,
        scope: Scope | None = None,
        root: _TermIndexRoot[K] | None = None,
    ) -> None:
        wire = index_set or cast(
            IndexSet[K, None, str, str],
            null_meta_index_set("mem-terms", id_serde=SerDe.identity(str)),
        )
        super().__init__(wire, scope=scope, binder=NativeStrCollectionBinder())
        self.terms_of = terms_of
        self.sync_on_write = sync_on_write
        self._root: _TermIndexRoot[K] = root if root is not None else _TermIndexRoot()

    def _postings(self) -> _TermPostings[K]:
        return self._root.for_scope(self.scope)

    def clone_with_scope(self, scope: Scope) -> MemoryTermIndex[K, V]:
        return MemoryTermIndex(
            self.terms_of,
            index_set=self.index_set,
            sync_on_write=self.sync_on_write,
            scope=scope,
            root=self._root,
        )

    def search(self, query: str, *, limit: int = 100) -> Sequence[IndexHit[K, None]]:
        if limit <= 0:
            return []
        keys = self._postings().term_to_keys.get(query, set())
        out: list[IndexHit[K, None]] = []
        for key in keys:
            out.append(IndexHit(key=key, meta=None))
            if len(out) >= limit:
                break
        return out

    def get(self, key: K) -> None:
        return None

    def meta_of(self, key: K, value: V, *, previous: None) -> None:
        return None

    def upsert(self, key: K, value: V, meta: None) -> None:
        postings = self._postings()
        new_terms = set(self.terms_of(key, value))
        old_terms = postings.key_to_terms.get(key, set())
        for term in old_terms - new_terms:
            bucket = postings.term_to_keys.get(term)
            if bucket is not None:
                bucket.discard(key)
                if not bucket:
                    del postings.term_to_keys[term]
        for term in new_terms - old_terms:
            postings.term_to_keys[term].add(key)
        if new_terms:
            postings.key_to_terms[key] = new_terms
        else:
            postings.key_to_terms.pop(key, None)

    def set(self, key: K, value: None) -> None:
        if key not in self._postings().key_to_terms:
            raise KvStoreIndexError("key is not in the index")

    def delete(self, key: K) -> bool:
        postings = self._postings()
        terms = postings.key_to_terms.pop(key, None)
        if not terms:
            return False
        for term in terms:
            bucket = postings.term_to_keys.get(term)
            if bucket is not None:
                bucket.discard(key)
                if not bucket:
                    del postings.term_to_keys[term]
        return True

    def __repr__(self) -> str:
        from kvtsjl.store.repr_util import callable_label, index_backend_repr

        return index_backend_repr(self, terms_of=callable_label(self.terms_of))
