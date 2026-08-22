"""Chroma-backed vector index leaf backend.

Install with::

    pip install 'kvtsjl[chroma]'
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast, override

from kvtsjl.bind import NativeStrCollectionBinder
from kvtsjl.exceptions import KvStoreIndexError
from kvtsjl.index.logical.abc import IndexHit
from kvtsjl.index.logical.vector import VectorRecord, query_has_content
from kvtsjl.index.schema.index_set import IndexSet
from kvtsjl.index.vector_backend import VectorIndexBackend

if TYPE_CHECKING:
    from chromadb import Collection

_DEFAULT_QUERY_INCLUDE = ("metadatas", "documents", "distances")
_DEFAULT_GET_INCLUDE = ("metadatas", "documents", "embeddings")


@dataclass(frozen=True, slots=True)
class ChromaQuery:
    """Chroma ``collection.query`` surface: ANN plus optional metadata filters.

    Distance metric (``l2``, ``cosine``, ``ip``) and index algorithm (HNSW vs
    SPANN) are **collection** configuration, not query fields — configure the
    ``Collection`` you pass to ``ChromaVectorIndex``. Query-time vector probes:
    exactly one of ``content`` (text), ``image``, ``uri``, or ``embedding``.
    """

    content: str | None = None
    image: object | None = None
    uri: str | None = None
    embedding: Sequence[float] | None = None
    where: Mapping[str, Any] | None = None
    where_document: Mapping[str, Any] | None = None
    include: Sequence[str] | None = None

    def __post_init__(self) -> None:
        probes = (
            query_has_content(self.content),
            self.image is not None,
            self.uri is not None and self.uri != "",
            self.embedding is not None and len(self.embedding) > 0,
        )
        n_probes = sum(probes)
        if n_probes > 1:
            raise ValueError(
                "ChromaQuery: set only one of content, image, uri, or embedding"
            )
        if n_probes == 0:
            raise ValueError(
                "ChromaQuery: content, image, uri, or embedding is required"
            )


class ChromaVectorIndex[K, V, D, META](
    VectorIndexBackend[ChromaQuery, K, V, D, str, META, str]
):
    """``VectorIndexBackend`` over a Chroma ``Collection``.

    ``IndexSet.meta_serde`` round-trips domain ``D`` to Chroma flat metadatas.
    ``embedding_of`` / ``document_of`` on ``IndexSet`` supply upsert payloads;
    text queries require the collection's embedding function (or pass
    ``ChromaQuery(embedding=...)``).
    """

    def __init__(
        self,
        index_set: IndexSet[K, D, str, META],
        collection: Collection,
        merge_data_fn: Callable[[K, V, D | None], D],
        *,
        sync_on_write: bool = True,
    ) -> None:
        super().__init__(index_set, binder=NativeStrCollectionBinder())
        self._collection = collection
        self.merge_data_fn = merge_data_fn
        self.sync_on_write = sync_on_write

    @override
    def merge_data(self, key: K, value: V, *, previous: D | None) -> D:
        return self.merge_data_fn(key, value, previous)

    def _id_str(self, key: K) -> str:
        return self.index_set.id_serde.serialize(key)

    def _key_from_id(self, id_str: str) -> K:
        return self.index_set.id_serde.deserialize(id_str)

    def _wire_meta(self, data: D) -> META:
        return self.index_set.meta_serde.serialize(data)

    def _domain_from_wire(self, wire: META | Mapping[str, Any] | None) -> D:
        if wire is None:
            raise KvStoreIndexError("missing Chroma metadata for indexed id")
        return self.index_set.meta_serde.deserialize(wire)  # type: ignore[arg-type]

    @staticmethod
    def _embedding_tuple(raw: object | None) -> tuple[float, ...] | None:
        if raw is None:
            return None
        if hasattr(raw, "tolist"):
            raw = raw.tolist()  # type: ignore[union-attr]  # numpy from chromadb
        if not isinstance(raw, Sequence):
            return None
        return tuple(float(x) for x in raw)

    def _record_from_parts(
        self,
        *,
        data: D,
        document: str | None,
        embedding: object | None,
        score: float | None,
    ) -> VectorRecord[D]:
        return VectorRecord(
            data=data,
            document=document,
            embedding=self._embedding_tuple(embedding),
            score=score,
        )

    def _upsert_row(self, key: K, value: V, meta: VectorRecord[D]) -> None:
        id_str = self._id_str(key)
        wire_meta = self._wire_meta(meta.data)
        document = meta.document
        if document is None and self.index_set.document_of is not None:
            document = self.index_set.document_of(key, value)
        embedding = meta.embedding
        if embedding is None and self.index_set.embedding_of is not None:
            raw = self.index_set.embedding_of(key, value)
            if raw is not None:
                embedding = tuple(raw)
        if embedding is None and document is None:
            raise KvStoreIndexError(
                "Chroma upsert requires embedding and/or document; "
                "set IndexSet.embedding_of / document_of or pass on meta"
            )
        kwargs: dict[str, object] = {
            "ids": [id_str],
            "metadatas": [wire_meta],
        }
        if document is not None:
            kwargs["documents"] = [document]
        if embedding is not None:
            kwargs["embeddings"] = [list(embedding)]
        self._collection.upsert(**kwargs)  # type: ignore[arg-type]

    @override
    def search(
        self, query: ChromaQuery, *, limit: int = 100
    ) -> Sequence[IndexHit[K, VectorRecord[D]]]:
        if limit <= 0:
            return []
        kwargs: dict[str, object] = {
            "n_results": limit,
            "include": list(query.include or _DEFAULT_QUERY_INCLUDE),
        }
        if query.where is not None:
            kwargs["where"] = dict(query.where)
        if query.where_document is not None:
            kwargs["where_document"] = dict(query.where_document)
        if query.content is not None:
            kwargs["query_texts"] = [query.content]
        elif query.image is not None:
            kwargs["query_images"] = [query.image]
        elif query.uri is not None:
            kwargs["query_uris"] = [query.uri]
        else:
            kwargs["query_embeddings"] = [list(query.embedding or [])]
        result = self._collection.query(**kwargs)  # type: ignore[arg-type]
        ids_batch = result.get("ids") or [[]]
        metas_batch = result.get("metadatas") or [[]]
        docs_batch = result.get("documents") or [[]]
        dist_batch = result.get("distances") or [[]]
        hits: list[IndexHit[K, VectorRecord[D]]] = []
        for id_str, wire, doc, dist in zip(
            ids_batch[0],
            metas_batch[0],
            docs_batch[0],
            dist_batch[0],
            strict=True,
        ):
            key = self._key_from_id(id_str)
            data = self._domain_from_wire(wire)
            record = self._record_from_parts(
                data=data,
                document=doc,
                embedding=None,
                score=float(dist) if dist is not None else None,
            )
            hits.append(IndexHit(key=key, meta=record))
        return hits

    @staticmethod
    def _first_field(result: Mapping[str, object], field: str) -> object | None:
        batch = result.get(field)
        if batch is None:
            return None
        if not isinstance(batch, list) or len(batch) == 0:
            return None
        return batch[0]

    @override
    def get(self, key: K) -> VectorRecord[D] | None:
        id_str = self._id_str(key)
        result = self._collection.get(
            ids=[id_str],
            include=list(_DEFAULT_GET_INCLUDE),
        )
        ids = result.get("ids") or []
        if not ids:
            return None
        wire = self._first_field(result, "metadatas")
        doc = self._first_field(result, "documents")
        if isinstance(doc, str):
            document: str | None = doc
        else:
            document = None
        emb = self._first_field(result, "embeddings")
        data = self._domain_from_wire(wire)  # type: ignore[arg-type]
        return self._record_from_parts(
            data=data,
            document=document,
            embedding=emb,
            score=None,
        )

    @override
    def upsert(self, key: K, value: V, meta: VectorRecord[D]) -> None:
        self._upsert_row(key, value, meta)

    @override
    def batch_upsert(self, items: Mapping[K, tuple[V, VectorRecord[D]]]) -> None:
        if not items:
            return
        ids: list[str] = []
        metadatas: list[META] = []
        documents: list[str | None] = []
        embeddings: list[list[float] | None] = []
        for key, (value, meta) in items.items():
            ids.append(self._id_str(key))
            metadatas.append(self._wire_meta(meta.data))
            document = meta.document
            if document is None and self.index_set.document_of is not None:
                document = self.index_set.document_of(key, value)
            embedding = meta.embedding
            if embedding is None and self.index_set.embedding_of is not None:
                raw = self.index_set.embedding_of(key, value)
                if raw is not None:
                    embedding = tuple(raw)
            documents.append(document)
            embeddings.append(list(embedding) if embedding is not None else None)
        if not any(documents) and not any(embeddings):
            raise KvStoreIndexError(
                "Chroma batch upsert requires embeddings and/or documents"
            )
        kwargs: dict[str, object] = {"ids": ids, "metadatas": metadatas}
        if all(document is not None for document in documents):
            kwargs["documents"] = documents
        if all(embedding is not None for embedding in embeddings):
            kwargs["embeddings"] = embeddings
        elif any(embedding is not None for embedding in embeddings):
            for key, (value, meta) in items.items():
                self._upsert_row(key, value, meta)
            return
        self._collection.upsert(**kwargs)  # type: ignore[arg-type]

    @override
    def set(self, key: K, value: VectorRecord[D]) -> None:
        """Update wired metadata only (no re-embed)."""
        id_str = self._id_str(key)
        existing = self._collection.get(ids=[id_str], include=[])
        if not existing.get("ids"):
            raise KvStoreIndexError("key is not in the index")
        wire_meta = self._wire_meta(value.data)
        self._collection.update(
            ids=[id_str],
            metadatas=[cast(Any, wire_meta)],
        )

    @override
    def delete(self, key: K) -> bool:
        id_str = self._id_str(key)
        existing = self._collection.get(ids=[id_str], include=[])
        if not existing.get("ids"):
            return False
        self._collection.delete(ids=[id_str])
        return True

    @override
    def batch_delete(self, keys: Sequence[K]) -> int:
        if not keys:
            return 0
        id_strs = [self._id_str(key) for key in keys]
        self._collection.delete(ids=id_strs)
        return len(id_strs)
