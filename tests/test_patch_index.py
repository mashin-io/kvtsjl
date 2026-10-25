"""R12: a view that owns part of an index entry.

A column-group view — ``feedback``, ``cluster_id`` — writes its own fields on
an entry another view created. It has no document, must not create the entry,
and must not remove it.
"""

from __future__ import annotations

from typing import Any

import pytest

from kvtsjl.backends.index.memory_vector import MemoryVectorIndex
from kvtsjl.exceptions import KvStoreIndexError, KvStoreUnsupported
from kvtsjl.index.logical.vector import VectorQuery
from kvtsjl.index.schema.index_set import IndexSet
from kvtsjl.serde import SerDe

MERGE = lambda k, v, previous: dict(previous or {}) | dict(v)  # noqa: E731


def _owner() -> Any:
    """The view that creates entries: it has the document."""
    return MemoryVectorIndex(
        IndexSet.with_str_ids(
            "shared",
            id_serde=SerDe.identity(str),
            meta_serde=SerDe.identity(dict),
            document_of=lambda k, v: v.get("text"),
            embedding_of=lambda k, v: v.get("vec"),
        ),
        MERGE,
    )


def _patch(**extra: Any) -> Any:
    """A view owning only its own fields, over the same collection name."""
    return MemoryVectorIndex(
        IndexSet.with_str_ids(
            "shared",
            id_serde=SerDe.identity(str),
            meta_serde=SerDe.identity(dict),
            embedding_of=lambda k, v: v.get("vec"),
            **extra,
        ),
        MERGE,
    )


def _write(index: Any, key: str, value: dict[str, Any]) -> None:
    index.upsert(key, value, index.meta_of(key, value, previous=index.get(key)))


def test_two_views_of_one_name_share_the_entry() -> None:
    """(a) Identity is (IndexSet.name, scoped id), as a real collection is."""
    owner, patch = _owner(), _patch(owned_fields=["feedback"])
    _write(owner, "k", {"text": "hello", "vec": [1.0]})
    _write(patch, "k", {"feedback": "good"})

    entry = owner.get("k")
    assert entry.document == "hello", "the patch view replaced the entry"
    assert entry.data["feedback"] == "good", "views did not share the entry"
    assert entry.embedding == (1.0,)


def test_a_patch_write_to_a_missing_entry_is_skipped() -> None:
    """(b) Not an error, and no half-made entry either."""
    patch = _patch(owned_fields=["feedback"])
    _write(patch, "ghost", {"feedback": "good"})

    assert patch.get("ghost") is None, "created an entry with no document"


def test_a_skipped_patch_write_does_not_block_the_real_one() -> None:
    owner, patch = _owner(), _patch(owned_fields=["feedback"])
    _write(patch, "k", {"feedback": "early"})          # skipped
    _write(owner, "k", {"text": "hello", "vec": [1.0]})
    _write(patch, "k", {"feedback": "late"})           # now lands

    assert owner.get("k").data["feedback"] == "late"


def test_deleting_from_a_patch_view_clears_only_its_fields() -> None:
    """(c) The entry survives; the owning view's data is untouched."""
    owner, patch = _owner(), _patch(owned_fields=["feedback"])
    _write(owner, "k", {"text": "hello", "vec": [1.0]})
    _write(patch, "k", {"feedback": "good"})

    assert patch.delete("k") is True
    entry = owner.get("k")
    assert entry is not None, "a patch-view delete removed the whole entry"
    assert entry.document == "hello"
    assert entry.embedding == (1.0,)
    assert "feedback" not in entry.data
    assert entry.data["text"] == "hello"
    assert owner.search(VectorQuery(embedding=[1.0]), limit=5)[0].key == "k"


def test_the_owning_view_still_deletes_the_whole_entry() -> None:
    owner, patch = _owner(), _patch(owned_fields=["feedback"])
    _write(owner, "k", {"text": "hello", "vec": [1.0]})
    _write(patch, "k", {"feedback": "good"})

    assert owner.delete("k") is True
    assert owner.get("k") is None
    assert owner.search(VectorQuery(embedding=[1.0]), limit=5) == []


def test_a_patch_view_that_declares_nothing_refuses_to_delete() -> None:
    """kvtsjl will not guess which fields are someone else's."""
    owner, patch = _owner(), _patch()
    _write(owner, "k", {"text": "hello", "vec": [1.0]})

    with pytest.raises(KvStoreIndexError, match="owned_fields"):
        patch.delete("k")
    with pytest.raises(KvStoreUnsupported, match="owned_fields"):
        patch.unset("k")
    assert owner.get("k") is not None, "the entry was damaged before refusing"
