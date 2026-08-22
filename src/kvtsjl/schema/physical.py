"""Physical schema: keyed SerDe pair shared by document and index side-stores."""

from __future__ import annotations

from dataclasses import dataclass

from kvtsjl.serde import SerDe


@dataclass(frozen=True, slots=True)
class PhysicalSchema[K, T, KBLOB, TBLOB]:
    """Logical keyed physical layout — descriptor, not a runtime store.

    ``K`` / ``KBLOB``: leaf key or id on the medium.
    ``T`` / ``TBLOB``: payload type on the medium (document ``V`` or domain meta ``D``).
    """

    name: str
    version: int | str
    key_serde: SerDe[K, KBLOB]
    data_serde: SerDe[T, TBLOB]

    def version_label(self) -> str:
        return f"v{self.version}"

    def identity_tuple(self) -> tuple[str, str]:
        return (self.name, self.version_label())

    def same_physical_as(self, other: PhysicalSchema[K, T, KBLOB, TBLOB]) -> bool:
        return (
            self.name == other.name
            and self.version == other.version
            and self.key_serde is other.key_serde
            and self.data_serde is other.data_serde
        )
