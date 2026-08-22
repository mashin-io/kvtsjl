"""In-memory index store leaf backends."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from kvtsjl.exceptions import KvStoreIndexError
from kvtsjl.index.abc import Index, IndexHit


@dataclass
class MemoryKeyIndex[K, V](Index[K, K, V, None]):
    """Exact membership index: query type is the document key ``K``."""

    sync_on_write: bool = True
    _keys: set[K] = field(default_factory=set)

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


@dataclass
class MemoryTermIndex[K, V](Index[str, K, V, None]):
    """Exact term → keys multimaps (in-memory)."""

    terms_of: Callable[[K, V], Sequence[str]]
    sync_on_write: bool = True
    _term_to_keys: dict[str, set[K]] = field(default_factory=lambda: defaultdict(set))
    _key_to_terms: dict[K, set[str]] = field(default_factory=dict)

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
