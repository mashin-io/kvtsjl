"""Shared wire helpers for in-memory index backends with no domain metadata."""

from __future__ import annotations

from kvtsjl.index.logical.envelope import EmptyEnvelope
from kvtsjl.index.backend import IndexBackend
from kvtsjl.serde import SerDe
from kvtsjl.index.schema.index_set import IndexSet

_NONE_META_SERDE = SerDe[None, str](
    serializer=lambda _: "",
    deserializer=lambda _: None,
    blob_type=str,
)


def null_meta_index_set[K](
    name: str,
    *,
    id_serde: SerDe[K, str],
) -> IndexSet[K, None, str, str]:
    return IndexSet.with_str_ids(
        name,
        id_serde=id_serde,
        meta_serde=_NONE_META_SERDE,
    )


class NullMetaIndexBackend[Q, K, V](
    IndexBackend[Q, K, V, None, None, str, str, str, EmptyEnvelope],
):
    """In-memory ``IndexBackend`` with no per-key domain metadata (`M` is always ``None``)."""

    def wrap_data(self, data: None, extras: EmptyEnvelope) -> None:
        return None

    def unwrap_data(self, record: None) -> None:
        return None

    def unwrap_envelope(self, record: None) -> EmptyEnvelope:
        return EmptyEnvelope()
