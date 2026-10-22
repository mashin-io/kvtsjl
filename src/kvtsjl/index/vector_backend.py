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
        # Decide reuse *before* calling embedding_of: calling it is the cost
        # being avoided, so an embedder that runs and is then discarded saves
        # nothing.
        if self._can_reuse_embedding(previous, document):
            embedding = previous.embedding if previous is not None else None
        elif wire.embedding_of is not None:
            raw = wire.embedding_of(key, value)
            if raw is not None:
                embedding = tuple(raw)
        if wire.document_of is None and previous is not None and wire.reuse_embedding:
            # This view owns no document — a metadata-only view over an entry
            # another view wrote. Carry both forward, or writing `feedback`
            # would erase the document and its vector.
            document = previous.document
            if embedding is None:
                embedding = previous.embedding
        return VectorRecord(data=data, document=document, embedding=embedding)

    def _can_reuse_embedding(
        self, previous: VectorRecord[D] | None, document: str | None
    ) -> bool:
        """Whether the previous vector still stands for this document.

        Only when the schema allows it, a previous embedding exists, and the
        document is byte-for-byte what it was. It is checked before
        ``embedding_of`` runs, because running it is the cost being saved — so
        a reused vector wins over one the schema would have computed. Turn
        ``reuse_embedding`` off where every write must re-embed.

        ``document_of`` returning ``None`` is not the same as a view having no
        ``document_of``: the first says this value has no document any more, so
        the old vector is stale and must not be kept. That case is handled
        above.
        """
        if not self.index_set.reuse_embedding:
            return False
        if previous is None or previous.embedding is None:
            return False
        return document == previous.document

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
