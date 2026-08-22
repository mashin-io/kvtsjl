"""Index wire identity: id + domain metadata SerDe and optional projections."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass

from kvtsjl.wire.blob_ops import BlobOps, StrBlobOps
from kvtsjl.wire.schema import PhysicalSchema
from kvtsjl.serde import SerDe


@dataclass(frozen=True, slots=True)
class IndexSet[K, D, ID, META](PhysicalSchema[K, D, ID, META]):
    """Wire descriptor for a leaf index backend.

    Serdes round-trip domain meta ``D`` only. KeyMap ``M`` on ``IndexBackend`` is
    composed over ``D`` at the physical boundary (search-only fields stay off-wire).
    """

    str_serde: SerDe[str, ID]
    blob_ops: BlobOps[ID]
    text_of: Callable[..., str | None] | None = None
    embedding_of: Callable[..., Sequence[float] | None] | None = None
    document_of: Callable[..., str | None] | None = None

    @property
    def id_serde(self) -> SerDe[K, ID]:
        return self.key_serde

    @property
    def meta_serde(self) -> SerDe[D, META]:
        return self.data_serde

    @classmethod
    def create(
        cls,
        name: str,
        *,
        version: int | str = 1,
        id_serde: SerDe[K, ID],
        meta_serde: SerDe[D, META],
        str_serde: SerDe[str, ID],
        blob_ops: BlobOps[ID],
        text_of: Callable[..., str | None] | None = None,
        embedding_of: Callable[..., Sequence[float] | None] | None = None,
        document_of: Callable[..., str | None] | None = None,
    ) -> IndexSet[K, D, ID, META]:
        return cls(
            name=name,
            version=version,
            key_serde=id_serde,
            data_serde=meta_serde,
            str_serde=str_serde,
            blob_ops=blob_ops,
            text_of=text_of,
            embedding_of=embedding_of,
            document_of=document_of,
        )

    @staticmethod
    def with_str_ids[KK, DD, META_T](
        name: str,
        *,
        version: int | str = 1,
        id_serde: SerDe[KK, str],
        meta_serde: SerDe[DD, META_T],
        text_of: Callable[..., str | None] | None = None,
        embedding_of: Callable[..., Sequence[float] | None] | None = None,
        document_of: Callable[..., str | None] | None = None,
    ) -> IndexSet[KK, DD, str, META_T]:
        return IndexSet(
            name=name,
            version=version,
            key_serde=id_serde,
            data_serde=meta_serde,
            str_serde=SerDe.safe_str(),
            blob_ops=StrBlobOps(),
            text_of=text_of,
            embedding_of=embedding_of,
            document_of=document_of,
        )

    def same_schema_as(self, other: IndexSet[K, D, ID, META]) -> bool:
        return (
            self.same_physical_as(other)
            and self.str_serde is other.str_serde
            and self.blob_ops is other.blob_ops
            and self.text_of is other.text_of
            and self.embedding_of is other.embedding_of
            and self.document_of is other.document_of
        )
