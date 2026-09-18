"""Indexed retrieval: A (default search) + B (typed via) + explicit search."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta

from freezegun import freeze_time
import pytest

from kvtsjl import (
    Index,
    IndexHit,
    KvSet,
    KvStoreIndexError,
    MemoryKeyIndex,
    MemoryKvStore,
    MemoryTermIndex,
    SerDe,
    TtlPolicy,
)
from tests.conformance import assert_basic_crud


def _memory_store() -> MemoryKvStore[str, str, str, bytes]:
    kvset = KvSet.with_str_keys(
        "idx",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
    )
    return MemoryKvStore(kvset)


def test_indexed_crud_delegates() -> None:
    store = _memory_store().indexed(MemoryKeyIndex[str, str]())
    assert_basic_crud(store)  # type: ignore[arg-type]


def test_a_default_search_query_only() -> None:
    keys = MemoryKeyIndex[str, str]()
    store = _memory_store().indexed(keys)
    store.set("a", "one")
    assert store.search("a") == ["one"]
    assert store.search("missing") == []
    assert store.search("a", include_keys=True) == [("a", "one")]
    assert store.search(keys, "a") == ["one"]
    assert keys("a") == ["one"]
    assert store.via is None


def test_explicit_search_required_when_multiple() -> None:
    keys = MemoryKeyIndex[str, str]()
    tags = MemoryTermIndex[str, str](terms_of=lambda _k, v: v.split())
    store = _memory_store().indexed(keys, tags)
    store.set("a", "red blue")
    with pytest.raises(KvStoreIndexError, match="no default index"):
        store.search("a")
    assert store.search(keys, "a") == ["red blue"]
    assert store.search(tags, "red") == ["red blue"]


def test_memory_term_index_resync() -> None:
    def tag_terms(_key: str, value: str) -> list[str]:
        return [t for t in value.split() if t.startswith("#")]

    by_tag = MemoryTermIndex[str, str](terms_of=tag_terms)
    store = _memory_store().indexed(by_tag)
    store.set("1", "hello #x")
    store.set("2", "world #y #x")
    assert set(store.search("#x")) == {"hello #x", "world #y #x"}
    store.set("2", "world #z")
    assert store.search("#y") == []
    assert store.delete("1") is True
    assert store.search("#x") == []


def test_unattached_index_raises() -> None:
    keys = MemoryKeyIndex[str, str]()
    other = MemoryKeyIndex[str, str]()
    store = _memory_store().indexed(keys)
    with pytest.raises(KvStoreIndexError, match="not attached"):
        store.search(other, "a")


def test_unbound_index_call_raises() -> None:
    keys = MemoryKeyIndex[str, str]()
    with pytest.raises(KvStoreIndexError, match="not bound"):
        keys("a")


@dataclass
class DocIndexes:
    tags: MemoryTermIndex[str, str]
    keys: MemoryKeyIndex[str, str]
    label: str = "docs"


def test_b_typed_via_same_dataclass() -> None:
    tags = MemoryTermIndex[str, str](
        terms_of=lambda _k, v: [t for t in v.split() if t.startswith("#")]
    )
    keys = MemoryKeyIndex[str, str]()
    store = _memory_store().indexed_as(DocIndexes(tags=tags, keys=keys))
    via = store.via
    assert via is not None
    assert via.tags is tags
    assert via.keys is keys
    assert via.label == "docs"

    store.set("1", "hello #x")
    store.set("2", "world #y")
    assert set(via.tags("#x")) == {"hello #x"}
    assert via.keys("2") == ["world #y"]
    assert store.search(tags, "#y") == ["world #y"]

    store.set("1", "hello #z")
    assert via.tags("#x") == []
    assert via.tags("#z") == ["hello #z"]


def test_scoped_default_search() -> None:
    by_tag = MemoryTermIndex[str, str](terms_of=lambda _k, v: v.split())
    store = _memory_store().indexed(by_tag)
    store.set("a", "red")
    assert store.search("red") == ["red"]
    scoped = store.scoped(tenant="t")
    # Remapped default_index must be one of the scoped indexes.
    assert scoped.default_index is not None
    assert scoped.default_index in scoped.indexes
    assert id(scoped.default_index) != id(by_tag)
    scoped.set("b", "blue")
    assert scoped.search("blue") == ["blue"]
    assert scoped.search("red") == []
    assert store.search("red") == ["red"]
    assert scoped.via is None


def test_scoped_via_remaps_bundle() -> None:
    tags = MemoryTermIndex[str, str](terms_of=lambda _k, v: v.split())
    keys = MemoryKeyIndex[str, str]()
    store = _memory_store().indexed_as(DocIndexes(tags=tags, keys=keys))
    store.set("a", "red")
    scoped = store.scoped(tenant="t")
    assert scoped.via is not None
    assert scoped.via.tags in scoped.indexes
    assert scoped.via.keys in scoped.indexes
    assert id(scoped.via.tags) != id(tags)
    scoped.set("b", "blue")
    assert scoped.via.tags("blue") == ["blue"]
    assert scoped.via.tags("red") == []
    assert store.via is not None
    assert store.via.tags("red") == ["red"]
    assert store.via.tags("blue") == []


def test_cross_scope_term_search_isolated() -> None:
    idx = MemoryTermIndex[str, str](terms_of=lambda _k, v: [v])
    root = _memory_store().indexed(idx)
    acme, zeta = root.scoped(tenant="acme"), root.scoped(tenant="zeta")
    acme.set("doc1", "acme-secret")
    zeta.set("doc1", "zeta-secret")
    assert acme.get("doc1") == "acme-secret"
    assert acme.search("zeta-secret") == []
    assert acme.search("acme-secret") == ["acme-secret"]
    assert zeta.search("zeta-secret") == ["zeta-secret"]
    assert zeta.search("acme-secret") == []
    with pytest.raises(KvStoreIndexError, match="not attached"):
        acme.search(idx, "acme-secret")


def test_scope_slash_in_id_does_not_collide_index_buckets() -> None:
    """``path_display()`` joins with ``/``; segment tuples must stay distinct."""
    idx = MemoryTermIndex[str, str](terms_of=lambda _k, v: [v])
    root = _memory_store().indexed(idx)
    nested = root.scoped(tenant="a").scoped(x="b")
    slash_id = root.scoped(**{"tenant": "a/x/b"})
    nested.set("doc1", "A-secret")
    slash_id.set("doc1", "B-secret")
    assert nested.get("doc1") == "A-secret"
    assert slash_id.get("doc1") == "B-secret"
    assert nested.search("A-secret") == ["A-secret"]
    assert nested.search("B-secret") == []
    assert slash_id.search("B-secret") == ["B-secret"]
    assert slash_id.search("A-secret") == []


def test_scoped_via_resolves_against_scoped_store() -> None:
    tags = MemoryTermIndex[str, str](terms_of=lambda _k, v: [v])
    root = _memory_store().indexed_as(DocIndexes(tags=tags, keys=MemoryKeyIndex()))
    acme = root.scoped(tenant="acme")
    zeta = root.scoped(tenant="zeta")
    assert acme.via is not None and zeta.via is not None
    acme.set("doc1", "acme-secret")
    zeta.set("doc1", "zeta-secret")
    assert acme.via.tags("acme-secret") == ["acme-secret"]
    assert acme.via.tags("zeta-secret") == []
    assert zeta.via.tags("zeta-secret") == ["zeta-secret"]
    assert zeta.via.tags("acme-secret") == []
    assert acme.via.tags("acme-secret") == acme.search(acme.via.tags, "acme-secret")


def test_rescope_idempotent_search() -> None:
    idx = MemoryTermIndex[str, str](terms_of=lambda _k, v: [v])
    root = _memory_store().indexed(idx)
    a1 = root.scoped(tenant="acme")
    a1.set("doc1", "hello")
    a2 = root.scoped(tenant="acme")
    assert a1.get("doc1") == "hello"
    assert a1.search("hello") == ["hello"]
    assert a2.search("hello") == ["hello"]
    a2.set("doc2", "world")
    assert a1.search("world") == ["world"]
    assert a2.search("world") == ["world"]


def test_index_bind_once_rejects_rebind() -> None:
    tags = MemoryTermIndex[str, str](terms_of=lambda _k, v: v.split())
    store = _memory_store().indexed(tags)
    other = _memory_store().indexed(MemoryKeyIndex())
    with pytest.raises(KvStoreIndexError, match="already bound"):
        tags._bind(other)


@dataclass(frozen=True, slots=True)
class RankMeta:
    score: float
    title: str


class RankedTitleIndex(Index[str, str, str, RankMeta]):
    """Toy index: title follows ``value``; other meta fields follow ``previous``."""

    def __init__(self) -> None:
        self._by_term: dict[str, list[tuple[str, RankMeta]]] = {}
        self._meta: dict[str, RankMeta] = {}

    def search(
        self, query: str, *, limit: int = 100
    ) -> Sequence[IndexHit[str, RankMeta]]:
        rows = self._by_term.get(query, [])
        rows = sorted(rows, key=lambda r: r[1].score, reverse=True)
        return [IndexHit(key=k, meta=m) for k, m in rows[:limit]]

    def get(self, key: str) -> RankMeta | None:
        return self._meta.get(key)

    def meta_of(
        self, key: str, value: str, *, previous: RankMeta | None
    ) -> RankMeta:
        # value format: "title|term|initial_score"
        title, _term, initial_s = value.split("|", 2)
        if previous is None:
            return RankMeta(score=float(initial_s), title=title)
        return RankMeta(score=previous.score, title=title)

    def upsert(self, key: str, value: str, meta: RankMeta) -> None:
        _title, term, _initial_s = value.split("|", 2)
        self._meta[key] = meta
        bucket = self._by_term.setdefault(term, [])
        bucket[:] = [(k, m) for k, m in bucket if k != key]
        bucket.append((key, meta))

    def set(self, key: str, value: RankMeta) -> None:
        if key not in self._meta:
            raise KvStoreIndexError("key is not in the index")
        self._meta[key] = value
        for bucket in self._by_term.values():
            for i, (k, _old) in enumerate(bucket):
                if k == key:
                    bucket[i] = (key, value)
                    return

    def delete(self, key: str) -> bool:
        if key not in self._meta:
            return False
        self._meta.pop(key, None)
        for term, bucket in list(self._by_term.items()):
            bucket[:] = [(k, m) for k, m in bucket if k != key]
            if not bucket:
                del self._by_term[term]
        return True


def test_index_hit_metadata() -> None:
    ranked = RankedTitleIndex()
    store = _memory_store().indexed(ranked)
    store.set("1", "Alpha|#x|0.9")
    store.set("2", "Beta|#x|0.5")
    hits = store.search_hits(ranked, "#x")
    assert [h.meta for h in hits] == [
        RankMeta(score=0.9, title="Alpha"),
        RankMeta(score=0.5, title="Beta"),
    ]
    assert store.search(ranked, "#x") == ["Alpha|#x|0.9", "Beta|#x|0.5"]

    # KeyMap.set from hit.meta — no second meta fetch.
    hit = next(h for h in hits if h.key == "2")
    ranked.set(hit.key, RankMeta(score=0.95, title=hit.meta.title))
    assert store.search_hits(ranked, "#x")[0].meta.score == 0.95
    assert store.get("2") == "Beta|#x|0.5"

    # Mutate value: title updates; other meta fields kept via previous=.
    store.set("2", "BetaPrime|#x|0.0")
    m2 = next(h.meta for h in store.search_hits(ranked, "#x") if h.key == "2")
    assert m2 == RankMeta(score=0.95, title="BetaPrime")
    assert store.get("2") == "BetaPrime|#x|0.0"


def test_indexed_gc_expired_syncs_indexes() -> None:
    kvset = KvSet.with_str_keys(
        "ttl-idx",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=30)),
    )
    keys = MemoryKeyIndex[str, str]()
    store = MemoryKvStore(kvset).indexed(keys)
    with freeze_time("2024-01-01 12:00:00") as frozen:
        store.set("gone", "x")
        store.set("stay", "y", ttl=TtlPolicy.none())
        assert store.search("gone") == ["x"]
        frozen.move_to("2024-01-01 12:01:00")
        assert store.gc_expired(max_entries=10) == 1
        assert store.search("gone") == []
        assert store.search("stay") == ["y"]
        assert store.get("stay") == "y"
