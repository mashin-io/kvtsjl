"""In-memory brute-force vector index leaf backend."""

from __future__ import annotations

from collections.abc import Callable, Sequence
import math
from typing import cast, override

from kvtsjl.bind import NativeStrCollectionBinder
from kvtsjl.exceptions import KvStoreIndexError
from kvtsjl.index.logical.abc import IndexHit
from kvtsjl.index.logical.vector import VectorQuery, VectorRecord, query_has_content
from kvtsjl.index.schema.index_set import IndexSet
from kvtsjl.index.vector_backend import VectorIndexBackend


def _l2_distance(a: Sequence[float], b: Sequence[float]) -> float:
    return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b, strict=True)))


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
    ) -> None:
        super().__init__(index_set, binder=NativeStrCollectionBinder())
        self.merge_data_fn = merge_data_fn
        self.embed_content = embed_content
        self.sync_on_write = sync_on_write
        self._vectors: dict[K, tuple[float, ...]] = {}
        self._records: dict[K, VectorRecord[D]] = {}

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
        if limit <= 0:
            return []
        q_emb = self._resolve_query_embedding(query)
        if q_emb is None:
            return []
        scored: list[tuple[float, IndexHit[K, VectorRecord[D]]]] = []
        for key, vec in self._vectors.items():
            rank = _l2_distance(q_emb, vec)
            stored = self._records[key]
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
        rec = self._records.get(key)
        if rec is None:
            return None
        return VectorRecord(
            data=rec.data,
            document=rec.document,
            embedding=rec.embedding,
            score=None,
        )

    @override
    def upsert(self, key: K, value: V, meta: VectorRecord[D]) -> None:
        emb = self._embedding_for(key, value, meta)
        stored = VectorRecord(
            data=meta.data,
            document=meta.document,
            embedding=emb,
            score=None,
        )
        self._vectors[key] = emb
        self._records[key] = stored

    @override
    def set(self, key: K, value: VectorRecord[D]) -> None:
        if key not in self._records:
            raise KvStoreIndexError("key is not in the index")
        old = self._records[key]
        emb = value.embedding if value.embedding is not None else old.embedding
        if emb is None:
            raise KvStoreIndexError("set requires embedding on record or existing index entry")
        self._vectors[key] = emb
        self._records[key] = VectorRecord(
            data=value.data,
            document=value.document,
            embedding=emb,
            score=None,
        )

    @override
    def delete(self, key: K) -> bool:
        if key not in self._records:
            return False
        self._records.pop(key, None)
        self._vectors.pop(key, None)
        return True
