"""R10: a vector index isolates entries by the scope it is bound to.

Scope metadata alone could not do this. ``get`` and ``delete`` are exact-key
lookups, so two scopes writing leaf "k" collided in storage *before* any
filter ran — the second write replaced the first, and there was nothing left
to filter. Isolation comes from the scoped physical id; the metadata fields
are what ``search`` filters on.
"""

from __future__ import annotations

from typing import Any

import pytest

from kvtsjl.backends.index.memory_vector import MemoryVectorIndex
from kvtsjl.index.logical.vector import VectorQuery
from kvtsjl.index.schema.index_set import IndexSet
from kvtsjl.scope import Scope
from kvtsjl.serde import SerDe


def _index() -> Any:
    return MemoryVectorIndex(
        IndexSet.with_str_ids(
            "vec",
            id_serde=SerDe.identity(str),
            meta_serde=SerDe.identity(dict),
            document_of=lambda k, v: v.get("text"),
            embedding_of=lambda k, v: v.get("vec"),
        ),
        lambda k, v, previous: dict(previous or {}) | dict(v),
    )


def _write(index: Any, key: str, text: str, vec: list[float]) -> None:
    value = {"text": text, "vec": vec}
    index.upsert(key, value, index.meta_of(key, value, previous=index.get(key)))


class _NoPrefixFilter(MemoryVectorIndex[Any, Any, Any, Any, Any, Any]):
    """Stands in for an engine that filters only on payload (Chroma, Qdrant)."""

    def supports_id_prefix_filter(self) -> bool:
        return False


def _no_prefix_filter_index(**extra: Any) -> Any:
    return _NoPrefixFilter(
        IndexSet.with_str_ids(
            "vec",
            id_serde=SerDe.identity(str),
            meta_serde=SerDe.identity(dict),
            document_of=lambda k, v: v.get("text"),
            embedding_of=lambda k, v: v.get("vec"),
            **extra,
        ),
        lambda k, v, previous: dict(previous or {}) | dict(v),
    )


def _two_scopes() -> tuple[Any, Any]:
    root = _index()
    return (
        root.clone_with_scope(Scope.of(user="alice")),
        root.clone_with_scope(Scope.of(user="bob")),
    )


def test_upsert_and_get_do_not_collide_on_the_same_leaf_key() -> None:
    alice, bob = _two_scopes()
    _write(alice, "k", "alice doc", [1.0])
    _write(bob, "k", "bob doc", [2.0])

    assert alice.get("k").document == "alice doc", "another scope overwrote this entry"
    assert bob.get("k").document == "bob doc"


def test_delete_removes_only_the_bound_scopes_entry() -> None:
    alice, bob = _two_scopes()
    _write(alice, "k", "alice doc", [1.0])
    _write(bob, "k", "bob doc", [2.0])

    assert bob.delete("k") is True
    assert bob.get("k") is None
    assert alice.get("k") is not None, "delete crossed into another scope"


def test_search_returns_only_the_bound_scopes_hits() -> None:
    alice, bob = _two_scopes()
    _write(alice, "k", "alice doc", [1.0])
    _write(bob, "k", "bob doc", [1.0])   # identical vector: only scope separates them

    hits = alice.search(VectorQuery(embedding=[1.0]), limit=10)
    assert [h.meta.document for h in hits] == ["alice doc"]
    assert all(h.key == "k" for h in hits), "search returned a scoped id, not the leaf"


def test_a_prefix_scope_searches_across_its_children() -> None:
    root = _index()
    root.clone_with_scope(Scope.of(user="alice", kind="a"))
    deep_a = root.clone_with_scope(Scope.of(user="alice", kind="a"))
    deep_b = root.clone_with_scope(Scope.of(user="alice", kind="b"))
    other = root.clone_with_scope(Scope.of(user="bob", kind="a"))
    _write(deep_a, "k1", "a doc", [1.0])
    _write(deep_b, "k2", "b doc", [1.0])
    _write(other, "k3", "other doc", [1.0])

    prefix = root.clone_with_scope(Scope.of(user="alice"))
    found = {h.meta.document for h in prefix.search(VectorQuery(embedding=[1.0]), limit=10)}
    assert found == {"a doc", "b doc"}, "a prefix scope must span its children only"


def test_no_scope_metadata_is_written_into_the_record() -> None:
    """kvtsjl is footprint-free: the scope lives in the id, not in their data.

    Isolation must not cost the caller a field they never declared. Swapping
    kvtsjl out should leave records with nothing to explain.
    """
    alice, _bob = _two_scopes()
    value = {"text": "doc", "vec": [1.0]}
    record = alice.meta_of("k", value, previous=None)

    assert record.data == value, f"kvtsjl added fields of its own: {record.data}"

    _write(alice, "k", "doc", [1.0])
    stored = alice.get("k")
    assert set(stored.data) == set(value), f"stored extra fields: {stored.data}"

    # and a client field named like a scope is left exactly as given
    forged = {"text": "d", "vec": [1.0], "scope_user": "bob"}
    assert alice.meta_of("j", forged, previous=None).data["scope_user"] == "bob"


def test_an_unscoped_view_still_works() -> None:
    index = _index()
    _write(index, "k", "doc", [1.0])
    assert index.get("k").document == "doc"
    assert index.supports_scoped_index() is True


@pytest.mark.parametrize("op", ["get", "delete", "search"])
def test_every_read_path_is_scoped(op: str) -> None:
    """R9's lesson: one fixed path and one forgotten path is how isolation leaks."""
    alice, bob = _two_scopes()
    _write(alice, "k", "alice doc", [1.0])

    if op == "get":
        assert bob.get("k") is None
    elif op == "delete":
        assert bob.delete("k") is False
        assert alice.get("k") is not None
    else:
        assert bob.search(VectorQuery(embedding=[1.0]), limit=10) == []


def test_a_scoped_search_is_not_starved_by_nearer_entries_elsewhere() -> None:
    """The filter must run inside the search, not trim a global top-k.

    Twenty of another scope's entries sit nearer the query than any of the
    bound scope's. Trimming afterwards would return nothing; filtering inside
    returns the bound scope's k best.
    """
    root = _index()
    alice = root.clone_with_scope(Scope.of(user="alice"))
    bob = root.clone_with_scope(Scope.of(user="bob"))

    for i in range(20):
        _write(bob, f"b{i}", f"bob {i}", [0.01 * i])      # all within 0.2 of the query
    for i in range(5):
        _write(alice, f"a{i}", f"alice {i}", [10.0 + i])  # all far away

    hits = alice.search(VectorQuery(embedding=[0.0]), limit=5)
    assert len(hits) == 5, f"starved: got {len(hits)} of 5"
    assert all(h.meta.document.startswith("alice") for h in hits)
    assert [h.key for h in hits] == ["a0", "a1", "a2", "a3", "a4"], "wrong ranking"


def test_a_backend_that_cannot_filter_inside_the_search_refuses() -> None:
    """Better to raise than to return a trimmed top-k that looks like a result."""
    from kvtsjl.exceptions import KvStoreIndexError, KvStoreUnsupported

    index = _no_prefix_filter_index()
    # unscoped is fine: there is nothing to filter
    assert index.search(VectorQuery(embedding=[0.0]), limit=5) == []

    scoped = index.clone_with_scope(Scope.of(user="alice"))
    with pytest.raises(KvStoreIndexError, match="inside the search"):
        scoped.search(VectorQuery(embedding=[0.0]), limit=5)
    with pytest.raises(KvStoreUnsupported, match="inside the search"):
        scoped.search(VectorQuery(embedding=[0.0]), limit=5)


def test_a_declared_scope_field_satisfies_the_gate() -> None:
    """The fallback for payload-only engines: a field the caller owns and names."""

    index = _no_prefix_filter_index(scope_fields={"user": "owner_id"})
    scoped = index.clone_with_scope(Scope.of(user="alice"))
    assert scoped.search(VectorQuery(embedding=[0.0]), limit=5) == []


def test_a_backend_that_declares_nothing_is_assumed_unable() -> None:
    """The default must be fail-closed.

    A new backend that never overrides ``supports_id_prefix_filter`` must be
    treated as unable to filter inside the search. If the default were True,
    forgetting to implement it would silently produce starved results rather
    than an error.
    """
    from kvtsjl.index.vector_backend import VectorIndexBackend

    assert VectorIndexBackend.supports_id_prefix_filter(object()) is False  # type: ignore[arg-type]
