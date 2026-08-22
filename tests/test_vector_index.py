"""Vector index: VectorRecord, VectorIndex, MemoryVectorIndex."""

from __future__ import annotations

from kvtsjl import (
    IndexSet,
    KvSet,
    MemoryKvStore,
    MemoryVectorIndex,
    SerDe,
    VectorQuery,
    VectorRecord,
)


def _embedding_of(_key: str, value: str) -> list[float]:
    return [float(len(value)), float(value.count(" "))]


def _merge_data(_key: str, _value: str, previous: dict[str, float] | None) -> dict[str, float]:
    if previous is None:
        return {"score": 0.0}
    return {"score": previous["score"]}


def _memory_store() -> MemoryKvStore[str, str, str, bytes]:
    kvset = KvSet.with_str_keys(
        "vec",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
    )
    return MemoryKvStore(kvset)


def _vector_index() -> MemoryVectorIndex[str, str, dict[str, float], str, dict[str, float]]:
    index_set = IndexSet.with_str_ids(
        "vec",
        id_serde=SerDe.identity(str),
        meta_serde=SerDe.identity(dict),
        embedding_of=_embedding_of,
        document_of=lambda _k, v: v,
    )
    return MemoryVectorIndex(
        index_set=index_set,
        merge_data_fn=_merge_data,
        embed_content=lambda text: _embedding_of("", text),
    )


def test_vector_search_by_embedding() -> None:
    vec = _vector_index()
    store = _memory_store().indexed(vec)
    store.set("a", "short")
    store.set("b", "much longer text")
    hits = store.search_hits(vec, VectorQuery(embedding=[16.0, 1.0]))
    assert len(hits) == 2
    assert hits[0].key == "b"
    assert hits[0].meta.score is not None
    assert hits[0].meta.data["score"] == 0.0
    assert hits[0].meta.document == "much longer text"


def test_vector_search_by_text_query() -> None:
    vec = _vector_index()
    store = _memory_store().indexed(vec)
    store.set("a", "hello")
    store.set("b", "hello world")
    results = store.search(vec, VectorQuery(content="hello world"))
    assert results[0] == "hello world"
    assert len(results) == 2


def test_vector_meta_persists_on_value_update() -> None:
    vec = _vector_index()
    store = _memory_store().indexed(vec)
    store.set("a", "one")
    hit = store.search_hits(vec, VectorQuery(content="one"))[0]
    vec.set(
        hit.key,
        VectorRecord(data={"score": 0.9}, document=hit.meta.document),
    )
    store.set("a", "one updated")
    m = next(h.meta for h in store.search_hits(vec, VectorQuery(content="one updated")))
    assert m.data["score"] == 0.9
    assert m.document == "one updated"


def test_vector_delete_removes_from_search() -> None:
    vec = _vector_index()
    store = _memory_store().indexed(vec)
    store.set("a", "alpha")
    assert store.search(vec, VectorQuery(content="alpha"))
    store.delete("a")
    assert store.search(vec, VectorQuery(content="alpha")) == []
