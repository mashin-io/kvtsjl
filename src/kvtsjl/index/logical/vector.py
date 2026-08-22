"""Vector search types and logical ``VectorIndex`` ABC."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass

from kvtsjl.index.logical.abc import Index

type FlatMeta = dict[str, str | int | float | bool]


def query_has_content(content: object | None) -> bool:
    """Whether ``content`` is a non-empty embeddable query payload."""
    if content is None:
        return False
    if isinstance(content, str):
        return content != ""
    if isinstance(content, (bytes, bytearray, memoryview)):
        return len(content) > 0
    return True


@dataclass(frozen=True, slots=True)
class VectorRecord[D]:
    """Index metadata ``M`` for vector indexes: wired ``D`` plus optional search extras."""

    data: D
    document: str | None = None
    embedding: tuple[float, ...] | None = None
    score: float | None = None  # search-only rank measure; semantics are backend-defined


@dataclass(frozen=True, slots=True)
class VectorQuery[T]:
    """Vector search query: exactly one of embeddable ``content`` or raw ``embedding``.

    ``T`` is the query modality (text, image bytes, URI string, etc.) understood by
    the index backend's embedder.
    """

    content: T | None = None
    embedding: Sequence[float] | None = None

    def __post_init__(self) -> None:
        has_content = query_has_content(self.content)
        has_emb = self.embedding is not None and len(self.embedding) > 0
        if has_content and has_emb:
            raise ValueError("VectorQuery: set only one of content or embedding")
        if not has_content and not has_emb:
            raise ValueError("VectorQuery: content or embedding is required")


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
