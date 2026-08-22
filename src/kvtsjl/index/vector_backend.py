"""Physical ``VectorIndexBackend``: ``IndexSet``, binding, and envelope wrap."""

from __future__ import annotations

from abc import ABC
from typing import override

from kvtsjl.index.backend import IndexBackend
from kvtsjl.index.logical.envelope import VectorEnvelope
from kvtsjl.index.logical.vector import VectorIndex, VectorRecord


class VectorIndexBackend[Q, K, V, D, ID, META, COLL](
    VectorIndex[Q, K, V, D],
    IndexBackend[Q, K, V, D, VectorRecord[D], ID, META, COLL, VectorEnvelope],
    ABC,
):
    """Leaf vector index: ``VectorIndex`` + ``IndexSet`` + medium I/O.

    ``IndexSet`` projection hooks (``embedding_of``, ``document_of``) apply here on
    ``meta_of``; logical ``VectorIndex`` only builds ``D`` via ``merge_data``.
    """

    @override
    def meta_of(
        self,
        key: K,
        value: V,
        *,
        previous: VectorRecord[D] | None,
    ) -> VectorRecord[D]:
        prev_d = previous.data if previous is not None else None
        data = self.merge_data(key, value, previous=prev_d)
        document: str | None = None
        embedding: tuple[float, ...] | None = None
        wire = self.index_set
        if wire.document_of is not None:
            document = wire.document_of(key, value)
        if wire.embedding_of is not None:
            raw = wire.embedding_of(key, value)
            if raw is not None:
                embedding = tuple(raw)
        return VectorRecord(data=data, document=document, embedding=embedding)

    @override
    def wrap_data(self, data: D, extras: VectorEnvelope) -> VectorRecord[D]:
        return VectorRecord(
            data=data,
            document=extras.document,
            embedding=extras.embedding,
            score=extras.score,
        )

    @override
    def unwrap_data(self, record: VectorRecord[D]) -> D:
        return record.data

    @override
    def unwrap_envelope(self, record: VectorRecord[D]) -> VectorEnvelope:
        return VectorEnvelope(
            document=record.document,
            embedding=record.embedding,
            score=record.score,
        )
