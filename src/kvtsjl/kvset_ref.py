"""Minimal KvSet identity for namespace binders (no K/V erasure)."""

from __future__ import annotations

from dataclasses import dataclass

from kvtsjl.kvset import KvSet
from kvtsjl.serde import SerDe


@dataclass(frozen=True, slots=True)
class KvSetRef[KBLOB]:
    """Name / version / str_serde — what binders need to form a collection id."""

    name: str
    version: int | str
    str_serde: SerDe[str, KBLOB]

    @classmethod
    def from_kvset[K, V, VBLOB](
        cls, kvset: KvSet[K, V, KBLOB, VBLOB]
    ) -> KvSetRef[KBLOB]:
        return cls(name=kvset.name, version=kvset.version, str_serde=kvset.str_serde)

    def version_label(self) -> str:
        return f"v{self.version}"
