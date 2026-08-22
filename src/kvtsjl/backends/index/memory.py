"""In-memory index store leaf backends."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Sequence
from typing import cast

from kvtsjl.backends.index._null_meta import NullMetaIndexBackend, null_meta_index_set
from kvtsjl.bind import NativeStrCollectionBinder
from kvtsjl.exceptions import KvStoreIndexError
from kvtsjl.index.logical.abc import IndexHit
from kvtsjl.serde import SerDe
from kvtsjl.index.schema.index_set import IndexSet


class MemoryKeyIndex[K, V](NullMetaIndexBackend[K, K, V]):
    """Exact membership index: query type is the document key ``K``."""

    def __init__(
        self,
        *,
        index_set: IndexSet[K, None, str, str] | None = None,
        sync_on_write: bool = True,
    ) -> None:
        wire = index_set or cast(
            IndexSet[K, None, str, str],
            null_meta_index_set("mem-keys", id_serde=SerDe.identity(str)),
        )
        super().__init__(wire, binder=NativeStrCollectionBinder())
        self.sync_on_write = sync_on_write
        self._keys: set[K] = set()

    def search(self, query: K, *, limit: int = 100) -> Sequence[IndexHit[K, None]]:
        if limit <= 0:
            return []
        if query in self._keys:
            return [IndexHit(key=query, meta=None)]
        return []

    def get(self, key: K) -> None:
        return None

    def meta_of(self, key: K, value: V, *, previous: None) -> None:
        return None

    def upsert(self, key: K, value: V, meta: None) -> None:
        self._keys.add(key)

    def set(self, key: K, value: None) -> None:
        if key not in self._keys:
            raise KvStoreIndexError("key is not in the index")

    def delete(self, key: K) -> bool:
        if key not in self._keys:
            return False
        self._keys.discard(key)
        return True


class MemoryTermIndex[K, V](NullMetaIndexBackend[str, K, V]):
    """Exact term → keys multimaps (in-memory)."""

    def __init__(
        self,
        terms_of: Callable[[K, V], Sequence[str]],
        *,
        index_set: IndexSet[K, None, str, str] | None = None,
        sync_on_write: bool = True,
    ) -> None:
        wire = index_set or cast(
            IndexSet[K, None, str, str],
            null_meta_index_set("mem-terms", id_serde=SerDe.identity(str)),
        )
        super().__init__(wire, binder=NativeStrCollectionBinder())
        self.terms_of = terms_of
        self.sync_on_write = sync_on_write
        self._term_to_keys: dict[str, set[K]] = defaultdict(set)
        self._key_to_terms: dict[K, set[str]] = {}

    def search(self, query: str, *, limit: int = 100) -> Sequence[IndexHit[K, None]]:
        if limit <= 0:
            return []
        keys = self._term_to_keys.get(query, set())
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
        new_terms = set(self.terms_of(key, value))
        old_terms = self._key_to_terms.get(key, set())
        for term in old_terms - new_terms:
            bucket = self._term_to_keys.get(term)
            if bucket is not None:
                bucket.discard(key)
                if not bucket:
                    del self._term_to_keys[term]
        for term in new_terms - old_terms:
            self._term_to_keys[term].add(key)
        if new_terms:
            self._key_to_terms[key] = new_terms
        else:
            self._key_to_terms.pop(key, None)

    def set(self, key: K, value: None) -> None:
        if key not in self._key_to_terms:
            raise KvStoreIndexError("key is not in the index")

    def delete(self, key: K) -> bool:
        terms = self._key_to_terms.pop(key, None)
        if not terms:
            return False
        for term in terms:
            bucket = self._term_to_keys.get(term)
            if bucket is not None:
                bucket.discard(key)
                if not bucket:
                    del self._term_to_keys[term]
        return True
