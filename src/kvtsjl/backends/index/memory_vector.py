"""In-memory brute-force vector index leaf backend."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import datetime
import math
from typing import Any, cast, override

from kvtsjl.bind import NativeStrCollectionBinder
from kvtsjl.exceptions import KvStoreIndexError
from kvtsjl.index.logical.abc import IndexHit
from kvtsjl.index.logical.vector import VectorQuery, VectorRecord, query_has_content
from kvtsjl.index.schema.index_set import IndexSet
from kvtsjl.index.vector_backend import VectorIndexBackend
from kvtsjl.scope import Scope
from kvtsjl.store.schema.ttl import TtlPolicy


def _l2_distance(a: Sequence[float], b: Sequence[float]) -> float:
    return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b, strict=True)))


_COLLECTIONS: dict[tuple[str, object], Any] = {}
"""Storage keyed by collection name, as a real vector service is.

A real backend resolves ``IndexSet.name`` to one collection, so two index
objects over that name share entries — which is how a column-group view
updates a field on an entry another view wrote. The reference backend keeps
its records the same way, or tests against it would pass while the same code
corrupted a shared collection.
"""


def reset_memory_vector_collections() -> None:
    """Drop every shared collection (tests that reuse a name)."""
    _COLLECTIONS.clear()


class _VectorRoot[K, D]:
    """Storage shared by every scoped view of one index.

    Keyed by the *scoped* physical id, so two scopes writing the same leaf key
    occupy two entries. Filtering metadata after a keyed lookup cannot do that:
    the second write would already have replaced the first.
    """

    def __init__(self) -> None:
        self.vectors: dict[Any, tuple[float, ...]] = {}
        self.records: dict[Any, tuple[K, VectorRecord[D]]] = {}


class MemoryVectorIndex[K, V, D, ID, META, Q](
    VectorIndexBackend[VectorQuery[Q], K, V, D, ID, META, str]
):
    """Exact nearest-neighbor scan in memory (L2 distance as ``score``)."""

    def __init__(
        self,
        index_set: IndexSet[K, D, ID, META],
        merge_data_fn: Callable[[K, V, D | None], D],
        *,
        embed_content: Callable[[Q], Sequence[float]] | None = None,
        sync_on_write: bool = True,
        scope: Scope | None = None,
        root: _VectorRoot[K, D] | None = None,
    ) -> None:
        super().__init__(index_set, scope=scope, binder=NativeStrCollectionBinder())
        self.merge_data_fn = merge_data_fn
        self.embed_content = embed_content
        self.sync_on_write = sync_on_write
        if root is None:
            collection = (index_set.name, index_set.version)
            root = cast(
                "_VectorRoot[K, D]",
                _COLLECTIONS.setdefault(collection, _VectorRoot()),
            )
        self._root: _VectorRoot[K, D] = root

    def supports_id_prefix_filter(self) -> bool:
        # Brute force: the scope test runs per candidate, before anything is
        # ranked, so the top-k is drawn from in-scope entries only.
        return True

    def clone_with_scope(self, scope: Scope) -> MemoryVectorIndex[K, V, D, ID, META, Q]:
        """A view of the same storage, bound to ``scope``."""
        return type(self)(
            self.index_set,
            self.merge_data_fn,
            embed_content=self.embed_content,
            sync_on_write=self.sync_on_write,
            scope=scope,
            root=self._root,
        )

    @property
    def _vectors(self) -> dict[Any, tuple[float, ...]]:
        return self._root.vectors

    @property
    def _records(self) -> dict[Any, tuple[K, VectorRecord[D]]]:
        return self._root.records

    @override
    def merge_data(self, key: K, value: V, *, previous: D | None) -> D:
        return self.merge_data_fn(key, value, previous)

    def _resolve_query_embedding(self, query: VectorQuery[Q]) -> tuple[float, ...] | None:
        if query.embedding is not None:
            return tuple(query.embedding)
        content = query.content
        if query_has_content(content) and self.embed_content is not None:
            return tuple(self.embed_content(cast(Q, content)))
        return None

    def _embedding_for(self, key: K, value: V, meta: VectorRecord[D]) -> tuple[float, ...]:
        if meta.embedding is not None:
            return meta.embedding
        if self.index_set.embedding_of is not None:
            raw = self.index_set.embedding_of(key, value)
            if raw is not None:
                return tuple(raw)
        raise KvStoreIndexError(
            "no embedding for key; set IndexSet.embedding_of or pass embedding on meta"
        )

    @override
    def search(
        self, query: VectorQuery[Q], *, limit: int = 100
    ) -> Sequence[IndexHit[K, VectorRecord[D]]]:
        self.assert_scoped_search_supported()
        if limit <= 0:
            return []
        q_emb = self._resolve_query_embedding(query)
        if q_emb is None:
            return []
        scored: list[tuple[float, IndexHit[K, VectorRecord[D]]]] = []
        for pid, vec in self._vectors.items():
            key, stored = self._records[pid]
            if not self.in_bound_scope_id(pid):
                continue
            rank = _l2_distance(q_emb, vec)
            hit_meta = VectorRecord(
                data=stored.data,
                document=stored.document,
                embedding=stored.embedding,
                score=rank,
            )
            scored.append((rank, IndexHit(key=key, meta=hit_meta)))
        scored.sort(key=lambda row: row[0])
        return [hit for _, hit in scored[:limit]]

    @override
    def get(self, key: K) -> VectorRecord[D] | None:
        found = self._records.get(self._physical_id_blob(key))
        if found is None:
            return None
        _leaf, rec = found
        return VectorRecord(
            data=rec.data,
            document=rec.document,
            embedding=rec.embedding,
            score=None,
        )

    @override
    def upsert(self, key: K, value: V, meta: VectorRecord[D]) -> None:
        pid_existing = self._physical_id_blob(key)
        if self.patch_write_is_skippable(
            self._records.get(pid_existing, (None, None))[1]
        ):
            return
        emb = self._embedding_for(key, value, meta)
        stored = VectorRecord(
            data=meta.data,
            document=meta.document,
            embedding=emb,
            score=None,
        )
        pid = self._physical_id_blob(key)
        self._vectors[pid] = emb
        self._records[pid] = (key, stored)

    @override
    def set(self, key: K, value: VectorRecord[D], *, ttl: TtlPolicy | None = None, created_at: datetime | None = None) -> None:
        pid = self._physical_id_blob(key)
        if pid not in self._records:
            raise KvStoreIndexError("key is not in the index")
        _leaf, old = self._records[pid]
        emb = value.embedding if value.embedding is not None else old.embedding
        if emb is None:
            raise KvStoreIndexError("set requires embedding on record or existing index entry")
        self._vectors[pid] = emb
        self._records[pid] = (
            key,
            VectorRecord(
                data=value.data,
                document=value.document,
                embedding=emb,
                score=None,
            ),
        )

    @override
    def delete(self, key: K) -> bool:
        pid = self._physical_id_blob(key)
        if pid not in self._records:
            return False
        if self.is_patch_view():
            leaf, stored = self._records[pid]
            self._records[pid] = (
                leaf,
                VectorRecord(
                    data=self.clear_owned_fields(stored.data),
                    document=stored.document,
                    embedding=stored.embedding,
                    score=None,
                ),
            )
            return True
        self._records.pop(pid, None)
        self._vectors.pop(pid, None)
        return True
