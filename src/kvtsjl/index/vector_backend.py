"""Physical ``VectorIndexBackend``: ``IndexSet``, binding, and envelope wrap."""

from __future__ import annotations

from abc import ABC
from typing import cast, override

from kvtsjl.exceptions import KvStoreIndexError
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

    def supports_scoped_index(self) -> bool:
        """Whether this index isolates entries by the scope it is bound to."""
        return True

    def is_patch_view(self) -> bool:
        """Whether this view owns part of an entry rather than the whole.

        A view with no ``document_of`` supplies no document, so it cannot have
        created the entry and must not behave as though it owns it.
        """
        return self.index_set.document_of is None

    def patch_write_is_skippable(self, previous: VectorRecord[D] | None) -> bool:
        """Whether a patch write should be passed over rather than performed.

        A patch view writing before the entry exists has nothing to attach to.
        Creating a record with no document would put an entry in the index
        that no search can rank and no reader can use, so the write is skipped.
        It is not an error: the view wrote what it owns, and the entry it
        belongs to simply is not there yet.
        """
        return self.is_patch_view() and previous is None

    def clear_owned_fields(self, data: D) -> D:
        """Drop this view's own fields, keeping the rest of the entry."""
        owned = self.index_set.owned_fields
        if owned is None:
            raise KvStoreIndexError(
                f"{type(self).__name__} is a patch view with no declared "
                "owned_fields, so deleting from it cannot know which fields "
                "are its own; declare them or delete through the owning view"
            )
        if not isinstance(data, dict):
            return data
        remaining = dict(cast("dict[str, object]", data))
        for field in owned:
            remaining.pop(field, None)
        return cast("D", remaining)

    def supports_id_prefix_filter(self) -> bool:
        """Whether the scope filter can run *inside* the nearest-neighbour search.

        Default ``False``: a new backend has to opt in. Filtering after the
        search is not an alternative. Dropping out-of-scope hits from a global
        top-k starves the result — a search for one user's best 5 returns
        whatever few of the global 5 happened to be theirs, so nothing leaks
        but recall collapses, worst exactly where a collection holds many
        scopes.

        Engines whose filter reaches the id (``WHERE id LIKE 'prefix%'`` on
        pgvector, a SQL-style id filter on LanceDB) return ``True``. Engines
        that filter only on payload must instead be given a scope field the
        caller declares, as ``SqlDbKvSet.scope_fields`` declares scope columns.
        """
        return False

    def assert_scoped_search_supported(self) -> None:
        """Refuse a scoped search the backend cannot do inside the search.

        Backends call this at the top of ``search``. Returning a trimmed
        global top-k instead would look like a working scoped search while
        quietly losing most of the results.
        """
        if not self.scope.segments:
            return
        if self.supports_id_prefix_filter():
            return
        if self.index_set.scope_fields:
            return
        raise KvStoreIndexError(
            f"{type(self).__name__} cannot filter by scope inside the search: "
            "it reports no id-prefix filter and the index set declares no "
            "scope_fields. Filtering after the search would starve the top-k"
        )

    def scope_id_prefix(self) -> ID:
        """The bound scope as an id prefix — where the scope actually lives.

        kvtsjl is footprint-free: a write stores the caller's payload and the
        medium's own metadata, nothing else. So the scope goes in the id, which
        kvtsjl already composes, and never into an invented metadata field. The
        *same* ``Scope`` produces both this prefix and every physical id through
        ``_physical_id_blob``, so a stored entry and the filter cannot disagree.

        A backend that can only filter on metadata (most ANN engines) must have
        the caller declare the field it may write, the way ``SqlDbKvSet``
        declares its scope columns. It must not invent one.
        """
        return self._scope_prefix_blob()

    def in_bound_scope_id(self, physical_id: ID) -> bool:
        """Whether a stored id belongs to the scope this index is bound to.

        A prefix scope matches its children, because its id prefix is a prefix
        of theirs.
        """
        prefix = self.scope_id_prefix()
        if not self.scope.segments:
            return True
        return self._blob_ops().startswith(physical_id, prefix)

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
