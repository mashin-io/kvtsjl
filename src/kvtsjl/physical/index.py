"""``IndexBackend`` ABC for leaf index backends."""

from __future__ import annotations

from abc import ABC, abstractmethod

from kvtsjl.bind.namespace import CollectionBinding, NamespaceBinder, resolve_collection_binding
from kvtsjl.physical.base import PhysicalBackend
from kvtsjl.scope import Scope
from kvtsjl.serde import SerDe
from kvtsjl.wire.blob_ops import BlobOps
from kvtsjl.wire.index_set import IndexSet
from kvtsjl.wire.ref import WireRef


class IndexBackend[Q, K, V, D, M, ID, META, COLL, E](
    PhysicalBackend[K, M, ID, META, COLL],
    ABC,
):
    """Leaf index backend: ``IndexSet`` physicalizes ``D``; ``KeyMap`` surface is ``M``.

    ``E`` types envelope fields on ``M`` beyond wired ``D`` (e.g. search-only
    ``distance``, denormalized ``document``). Use ``EmptyEnvelope`` when ``M`` is
    ``D`` alone or extras are not modeled separately.
    """

    index_set: IndexSet[K, D, ID, META]

    def __init__(
        self,
        index_set: IndexSet[K, D, ID, META],
        *,
        scope: Scope | None = None,
        binder: NamespaceBinder[ID, COLL] | None = None,
        binding: CollectionBinding[ID, COLL] | None = None,
    ) -> None:
        self.index_set = index_set
        self.scope = scope or Scope.empty()
        self._binding = resolve_collection_binding(
            WireRef.from_index_set(index_set),
            binder=binder,
            binding=binding,
        )

    @property
    def physical(self) -> IndexSet[K, D, ID, META]:
        return self.index_set

    def _str_serde(self) -> SerDe[str, ID]:
        return self.index_set.str_serde

    def _blob_ops(self) -> BlobOps[ID]:
        return self.index_set.blob_ops

    @abstractmethod
    def wrap_data(self, data: D, extras: E) -> M:
        """Build KeyMap ``M`` from wired ``D`` plus envelope ``E``."""

    @abstractmethod
    def unwrap_data(self, record: M) -> D:
        """Extract wired ``D`` from KeyMap ``M`` (drops envelope fields)."""

    @abstractmethod
    def unwrap_envelope(self, record: M) -> E:
        """Extract envelope ``E`` from KeyMap ``M``."""

    def _physical_id_blob(self, key: K) -> ID:
        leaf = self.index_set.id_serde.serialize(key)
        return self._item_key_blob(leaf)
