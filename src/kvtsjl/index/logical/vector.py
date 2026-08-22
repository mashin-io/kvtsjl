"""Vector search types and logical ``VectorIndex`` ABC."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass

from kvtsjl.index.logical.abc import Index

type FlatMeta = dict[str, str | int | float | bool]


@dataclass(frozen=True, slots=True)
class VectorRecord[D]:
    """Index metadata ``M`` for vector indexes: wired ``D`` plus optional search extras."""

    data: D
    document: str | None = None
    embedding: tuple[float, ...] | None = None
    distance: float | None = None  # search-only; ignored on set/upsert


@dataclass(frozen=True, slots=True)
class VectorQuery:
    """Query for vector indexes: exactly one of ``text`` or ``embedding``."""

    text: str | None = None
    embedding: Sequence[float] | None = None

    def __post_init__(self) -> None:
        has_text = self.text is not None and self.text != ""
        has_emb = self.embedding is not None and len(self.embedding) > 0
        if has_text and has_emb:
            raise ValueError("VectorQuery: set only one of text or embedding")
        if not has_text and not has_emb:
            raise ValueError("VectorQuery: text or embedding is required")


class VectorIndex[Q, K, V, D](Index[Q, K, V, VectorRecord[D]], ABC):
    """Logical vector index: ``search`` + ``sync`` from document ``V``.

    No ``IndexSet`` here — wire identity and projection hooks live on
    ``VectorIndexBackend`` (``Index`` + ``IndexSet`` + binding), same split as
    ``KvStore`` vs ``KvBackend``.
    """

    @abstractmethod
    def merge_data(self, key: K, value: V, *, previous: D | None) -> D:
        """Build wired domain meta ``D`` from the document ``value`` and stored data."""

    def meta_of(
        self,
        key: K,
        value: V,
        *,
        previous: VectorRecord[D] | None,
    ) -> VectorRecord[D]:
        prev_d = previous.data if previous is not None else None
        return VectorRecord(data=self.merge_data(key, value, previous=prev_d))
