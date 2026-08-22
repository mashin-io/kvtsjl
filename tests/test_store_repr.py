"""Store repr smoke tests for debugging composed stores."""

from __future__ import annotations

from kvtsjl import KvSet, MemoryKvStore, MemoryTermIndex, SerDe


def _memory_store(name: str = "docs") -> MemoryKvStore[str, str, str, bytes]:
    kvset = KvSet.with_str_keys(
        name,
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
    )
    return MemoryKvStore(kvset)


def test_memory_kvstore_repr() -> None:
    store = _memory_store()
    text = repr(store)
    assert text.startswith("MemoryKvStore(")
    assert "docs@v1" in text


def test_mirror_repr_shows_tree() -> None:
    primary = _memory_store("primary")
    secondary = _memory_store("secondary")
    store = primary.mirror(secondary)
    text = repr(store)
    assert "MirrorKvStore(" in text
    assert "primary=MemoryKvStore" in text
    assert "secondary=MemoryKvStore" in text
    assert "primary@v1" in text
    assert "secondary@v1" in text


def test_indexed_repr_shows_indexes() -> None:
    store = _memory_store().indexed(
        MemoryTermIndex(lambda _k, v: v.split())
    )
    text = repr(store)
    assert "IndexedKvStore(" in text
    assert "MemoryTermIndex(" in text
    assert "terms_of=" in text


def test_scoped_repr() -> None:
    store = _memory_store().scoped(tenant="1")
    text = repr(store)
    assert "tenant/1" in text
