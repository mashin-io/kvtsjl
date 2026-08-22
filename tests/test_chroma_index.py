"""Chroma vector index backend tests."""

from __future__ import annotations

import pytest

chromadb = pytest.importorskip("chromadb")

from kvtsjl import IndexSet, KvSet, MemoryKvStore, SerDe, VectorRecord
from kvtsjl.backends.index.chroma import ChromaQuery, ChromaVectorIndex

pytestmark = pytest.mark.chroma


def _embedding_of(_key: str, value: str) -> list[float]:
    return [float(len(value)), float(value.count(" "))]


def _merge_data(_key: str, _value: str, previous: dict[str, float] | None) -> dict[str, float]:
    if previous is None:
        return {"score": 0.0}
    return {"score": previous["score"]}


def _memory_store() -> MemoryKvStore[str, str, str, bytes]:
    kvset = KvSet.with_str_keys(
        "docs",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
    )
    return MemoryKvStore(kvset)


def _chroma_index(collection_name: str = "kvtsjl-chroma-test") -> ChromaVectorIndex[
    str, str, dict[str, float], dict[str, float]
]:
    client = chromadb.EphemeralClient()
    collection = client.get_or_create_collection(collection_name)
    index_set = IndexSet.with_str_ids(
        "chroma",
        id_serde=SerDe.identity(str),
        meta_serde=SerDe.identity(dict),
        embedding_of=_embedding_of,
        document_of=lambda _k, v: v,
    )
    return ChromaVectorIndex(
        index_set=index_set,
        collection=collection,
        merge_data_fn=_merge_data,
    )


def test_chroma_sync_and_search_by_embedding() -> None:
    vec = _chroma_index("sync-search")
    store = _memory_store().indexed(vec)
    store.set("a", "short")
    store.set("b", "much longer text")
    hits = store.search_hits(vec, ChromaQuery(embedding=[16.0, 1.0]))
    assert len(hits) == 2
    assert hits[0].key == "b"
    assert hits[0].meta.score is not None
    assert hits[0].meta.data["score"] == 0.0


def test_chroma_set_updates_metadata_only() -> None:
    vec = _chroma_index("meta-set")
    store = _memory_store().indexed(vec)
    store.set("a", "one")
    hit = store.search_hits(vec, ChromaQuery(embedding=[3.0, 0.0]))[0]
    vec.set(hit.key, VectorRecord(data={"score": 0.9}))
    got = vec.get("a")
    assert got is not None
    assert got.data["score"] == 0.9


def test_chroma_meta_persists_on_value_update() -> None:
    vec = _chroma_index("meta-sync")
    store = _memory_store().indexed(vec)
    store.set("a", "one")
    hit = store.search_hits(vec, ChromaQuery(embedding=[3.0, 0.0]))[0]
    vec.set(hit.key, VectorRecord(data={"score": 0.9}))
    store.set("a", "one updated")
    m = next(h.meta for h in store.search_hits(vec, ChromaQuery(embedding=[12.0, 1.0])))
    assert m.data["score"] == 0.9
    assert m.document == "one updated"


def test_chroma_delete() -> None:
    vec = _chroma_index("delete")
    store = _memory_store().indexed(vec)
    store.set("a", "alpha")
    assert store.search(vec, ChromaQuery(embedding=[5.0, 0.0]))
    store.delete("a")
    assert store.search(vec, ChromaQuery(embedding=[5.0, 0.0])) == []
