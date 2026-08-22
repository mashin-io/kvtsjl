"""Wire identity for namespace binders (name / version / str materialization)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from kvtsjl.serde import SerDe

if TYPE_CHECKING:
    from kvtsjl.index.schema.index_set import IndexSet
    from kvtsjl.store.schema.kvset import KvSet


@dataclass(frozen=True, slots=True)
class WireRef[KBLOB]:
    """Name / version / str_serde — what binders need to form a collection id."""

    name: str
    version: int | str
    str_serde: SerDe[str, KBLOB]

    def version_label(self) -> str:
        return f"v{self.version}"

    @classmethod
    def from_kvset[K, V, VBLOB](
        cls, kvset: KvSet[K, V, KBLOB, VBLOB]
    ) -> WireRef[KBLOB]:
        return cls(name=kvset.name, version=kvset.version, str_serde=kvset.str_serde)

    @classmethod
    def from_index_set[K, D, META](
        cls, index_set: IndexSet[K, D, KBLOB, META]
    ) -> WireRef[KBLOB]:
        return cls(
            name=index_set.name,
            version=index_set.version,
            str_serde=index_set.str_serde,
        )
