"""Common base for leaf physical backends (document and index)."""

from __future__ import annotations

from abc import ABC, abstractmethod

from kvtsjl.bind.namespace import CollectionBinding
from kvtsjl.keymap import KeyMap
from kvtsjl.scope import Scope
from kvtsjl.serde import SerDe
from kvtsjl.wire.blob_ops import BlobOps
from kvtsjl.wire.schema import PhysicalSchema


class PhysicalBackend[K, T, KBLOB, TBLOB, COLL](KeyMap[K, T], ABC):
    """Leaf backend: physical schema, collection binding, and medium I/O.

    Shared between document ``KvBackend`` and ``IndexBackend``. Logical wrappers
    (readonly, indexed, mirror) are not physical backends.
    """

    scope: Scope
    _binding: CollectionBinding[KBLOB, COLL]

    @property
    @abstractmethod
    def physical(self) -> PhysicalSchema[K, T, KBLOB, TBLOB]:
        """Sole wire schema for this backend."""

    @property
    def binding(self) -> CollectionBinding[KBLOB, COLL]:
        return self._binding

    @abstractmethod
    def _str_serde(self) -> SerDe[str, KBLOB]:
        """Serialize scope segment labels into in-key material."""

    @abstractmethod
    def _blob_ops(self) -> BlobOps[KBLOB]:
        """Join / split in-key blobs for this medium."""

    def _item_key_blob(self, leaf_blob: KBLOB) -> KBLOB:
        return self._binding.item_key(
            self.scope,
            leaf_blob,
            str_serde=self._str_serde(),
            blob_ops=self._blob_ops(),
        )

    def _scope_prefix_blob(self, key_prefix_blob: KBLOB | None = None) -> KBLOB:
        return self._binding.scope_prefix(
            self.scope,
            str_serde=self._str_serde(),
            blob_ops=self._blob_ops(),
            key_prefix_blob=key_prefix_blob,
        )
