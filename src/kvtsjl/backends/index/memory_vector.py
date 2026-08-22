"""In-memory brute-force vector index leaf backend."""

from __future__ import annotations

import math
from collections.abc import Callable, Sequence

from kvtsjl.bind import NativeStrCollectionBinder
from kvtsjl.exceptions import KvStoreIndexError
from kvtsjl.index.abc import IndexHit
from kvtsjl.index.vector import VectorQuery, VectorRecord
from kvtsjl.physical.vector import VectorIndexBackend
from kvtsjl.wire.index_set import IndexSet


def _l2_distance(a: Sequence[float], b: Sequence[float]) -> float:
    return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b, strict=True)))


class MemoryVectorIndex[K, V, D, ID, META](
    VectorIndexBackend[VectorQuery, K, V, D, ID, META, str]
):
    """Exact nearest-neighbor scan in memory (L2 distance)."""

    def __init__(
        self,
        index_set: IndexSet[K, D, ID, META],
        merge_data_fn: Callable[[K, V, D | None], D],
        *,
        embed_query: Callable[[str], Sequence[float]] | None = None,
        sync_on_write: bool = True,
    ) -> None:
        super().__init__(index_set, binder=NativeStrCollectionBinder())
        self.merge_data_fn = merge_data_fn
        self.embed_query = embed_query
        self.sync_on_write = sync_on_write
        self._vectors: dict[K, tuple[float, ...]] = {}
        self._records: dict[K, VectorRecord[D]] = {}

    def merge_data(self, key: K, value: V, *, previous: D | None) -> D:
        return self.merge_data_fn(key, value, previous)

    def _resolve_query_embedding(self, query: VectorQuery) -> tuple[float, ...] | None:
        if query.embedding is not None:
            return tuple(query.embedding)
        if query.text is not None and self.embed_query is not None:
            return tuple(self.embed_query(query.text))
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

    def search(
        self, query: VectorQuery, *, limit: int = 100
    ) -> Sequence[IndexHit[K, VectorRecord[D]]]:
        if limit <= 0:
            return []
        q_emb = self._resolve_query_embedding(query)
        if q_emb is None:
            return []
        scored: list[tuple[float, IndexHit[K, VectorRecord[D]]]] = []
        for key, vec in self._vectors.items():
            dist = _l2_distance(q_emb, vec)
            stored = self._records[key]
            hit_meta = VectorRecord(
                data=stored.data,
                document=stored.document,
                embedding=stored.embedding,
                distance=dist,
            )
            scored.append((dist, IndexHit(key=key, meta=hit_meta)))
        scored.sort(key=lambda row: row[0])
        return [hit for _, hit in scored[:limit]]

    def get(self, key: K) -> VectorRecord[D] | None:
        rec = self._records.get(key)
        if rec is None:
            return None
        return VectorRecord(
            data=rec.data,
            document=rec.document,
            embedding=rec.embedding,
            distance=None,
        )

    def upsert(self, key: K, value: V, meta: VectorRecord[D]) -> None:
        emb = self._embedding_for(key, value, meta)
        stored = VectorRecord(
            data=meta.data,
            document=meta.document,
            embedding=emb,
            distance=None,
        )
        self._vectors[key] = emb
        self._records[key] = stored

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
            distance=None,
        )

    def delete(self, key: K) -> bool:
        if key not in self._records:
            return False
        self._records.pop(key, None)
        self._vectors.pop(key, None)
        return True
