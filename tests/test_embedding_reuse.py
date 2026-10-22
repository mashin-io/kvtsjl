"""R11: don't re-embed a document that has not changed.

``meta_of`` built a fresh ``VectorRecord`` each sync and only ever read
``previous.data``, so the embedding — the expensive half, usually a network
call — was recomputed on every write, including writes that never touched the
document.
"""

from __future__ import annotations

from typing import Any

from kvtsjl.index.logical.vector import VectorRecord
from kvtsjl.index.schema.index_set import IndexSet
from kvtsjl.serde import SerDe


class Embedder:
    """Counts how often the expensive call actually happens."""

    def __init__(self) -> None:
        self.calls = 0

    def __call__(self, key: str, value: dict[str, Any]) -> list[float] | None:
        if "text" not in value:
            return None
        self.calls += 1
        return [float(len(value["text"]))]


def _backend(*, reuse: bool = True, with_document: bool = True) -> Any:
    from kvtsjl.index.vector_backend import VectorIndexBackend

    embedder = Embedder()
    index_set = IndexSet.with_str_ids(
        "idx",
        id_serde=SerDe.identity(str),
        meta_serde=SerDe.identity(dict),
        document_of=((lambda k, v: v.get("text")) if with_document else None),
        embedding_of=embedder,
        reuse_embedding=reuse,
    )

    class Backend(VectorIndexBackend[Any, str, dict[str, Any], dict[str, Any], str, Any, Any]):
        def __init__(self) -> None:
            self.index_set = index_set

        def merge_data(self, key: str, value: Any, *, previous: Any) -> Any:
            return dict(previous or {}) | dict(value)

        # Unused here: meta_of is pure, so the medium never gets touched.
        def upsert(self, *a: Any, **kw: Any) -> None: ...
        def get(self, *a: Any, **kw: Any) -> None: ...
        def set(self, *a: Any, **kw: Any) -> None: ...
        def delete(self, *a: Any, **kw: Any) -> None: ...
        def search(self, *a: Any, **kw: Any) -> Any:
            return iter(())

    return Backend(), embedder


def test_an_unchanged_document_keeps_its_embedding() -> None:
    backend, embedder = _backend()
    first = backend.meta_of("k", {"text": "hello"}, previous=None)
    assert embedder.calls == 1
    assert first.embedding == (5.0,)

    again = backend.meta_of("k", {"text": "hello", "tag": "x"}, previous=first)
    assert again.embedding == (5.0,)
    assert embedder.calls == 1, "re-embedded a document that did not change"


def test_a_changed_document_is_re_embedded() -> None:
    backend, embedder = _backend()
    first = backend.meta_of("k", {"text": "hello"}, previous=None)
    second = backend.meta_of("k", {"text": "goodbye!"}, previous=first)
    assert second.embedding == (8.0,)
    assert embedder.calls == 2


def test_reuse_can_be_turned_off() -> None:
    """A vector that depends on more than the document must re-embed."""
    backend, embedder = _backend(reuse=False)
    first = backend.meta_of("k", {"text": "hello"}, previous=None)
    backend.meta_of("k", {"text": "hello"}, previous=first)
    assert embedder.calls == 2


def test_a_document_removed_is_not_papered_over_with_the_old_vector() -> None:
    """``document_of`` returning None means the document is gone, not absent."""
    backend, _ = _backend()
    first = backend.meta_of("k", {"text": "hello"}, previous=None)
    after = backend.meta_of("k", {"tag": "only"}, previous=first)
    assert after.document is None
    assert after.embedding is None, "kept a vector for a document that was removed"


def test_a_metadata_only_view_keeps_the_entry_it_does_not_own() -> None:
    """A view with no ``document_of`` writes its fields and nothing else.

    This is what a column-group index — ``feedback``, ``cluster_id`` — needs:
    update its own metadata on a shared entry without having the document and
    without embedding anything.
    """
    writer, writer_embedder = _backend(with_document=True)
    patcher, patch_embedder = _backend(with_document=False)

    entry = writer.meta_of("k", {"text": "hello"}, previous=None)
    assert writer_embedder.calls == 1

    patched = patcher.meta_of("k", {"feedback": "good"}, previous=entry)
    assert patched.document == "hello", "patching erased the document"
    assert patched.embedding == (5.0,), "patching erased the vector"
    assert patched.data["feedback"] == "good"
    assert patch_embedder.calls == 0, "a metadata-only write embedded"
