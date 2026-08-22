"""PhysicalSchema, PhysicalBackend, KvBackend, IndexSet, and envelope typing."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TypedDict

from kvtsjl import (
    IndexBackend,
    IndexHit,
    IndexSet,
    KvBackend,
    KvSet,
    MemoryKeyIndex,
    MemoryKvStore,
    NativeStrCollectionBinder,
    PhysicalBackend,
    PhysicalSchema,
    SerDe,
)
from kvtsjl import IndexedKvStore, ReadonlyKvStore

type _Meta = dict[str, str]

_STUB_INDEX_SET = IndexSet.with_str_ids(
    "stub",
    id_serde=SerDe.identity(str),
    meta_serde=SerDe.identity(dict),
)


def test_kvset_extends_physical_schema() -> None:
    kvset = KvSet.with_str_keys(
        "docs",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
    )
    assert isinstance(kvset, PhysicalSchema)
    assert kvset.value_serde is kvset.data_serde
    assert kvset.identity_tuple() == ("docs", "v1")


def test_index_set_aliases_serde() -> None:
    idx = IndexSet.with_str_ids(
        "vec",
        id_serde=SerDe.identity(str),
        meta_serde=SerDe.identity(dict),
    )
    assert idx.id_serde is idx.key_serde
    assert idx.meta_serde is idx.data_serde
    assert idx.same_schema_as(idx)


def test_memory_key_index_is_index_backend() -> None:
    keys = MemoryKeyIndex[str, str]()
    assert isinstance(keys, IndexBackend)
    assert isinstance(keys, PhysicalBackend)
    assert keys.physical is keys.index_set


def test_memory_kvstore_is_kv_backend() -> None:
    kvset = KvSet.with_str_keys(
        "mem",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.identity(str),
    )
    store = MemoryKvStore(kvset)
    assert isinstance(store, KvBackend)
    assert isinstance(store, PhysicalBackend)
    assert store.physical is store.kvset
    assert store.physical.same_physical_as(kvset)
    assert store.binding is store._binding


def test_logical_wrapper_is_not_physical_backend() -> None:
    kvset = KvSet.with_str_keys(
        "mem",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.identity(str),
    )
    leaf = MemoryKvStore(kvset)
    readonly = leaf.readonly()
    indexed = leaf.indexed(MemoryKeyIndex[str, str]())

    assert not isinstance(readonly, PhysicalBackend)
    assert not isinstance(indexed, PhysicalBackend)
    assert isinstance(readonly, ReadonlyKvStore)
    assert isinstance(indexed, IndexedKvStore)
    assert readonly.key_layout() == kvset.key_layout
    assert not hasattr(readonly, "binding")


def test_index_set_identity_physical() -> None:
    idx = IndexSet.with_str_ids(
        "meta",
        id_serde=SerDe.identity(str),
        meta_serde=SerDe.identity(dict),
    )
    blob = idx.meta_serde.serialize({"tag": "x"})
    assert idx.meta_serde.deserialize(blob)["tag"] == "x"


class _VectorEnvelope(TypedDict, total=False):
    document: str | None
    score: float | None


@dataclass(frozen=True, slots=True)
class _VectorRecord[D]:
    data: D
    document: str | None = None
    score: float | None = None


class _StubIndexBackend(
    IndexBackend[str, str, str, _Meta, _VectorRecord[_Meta], str, _Meta, str, _VectorEnvelope]
):
    def __init__(self) -> None:
        super().__init__(
            _STUB_INDEX_SET,
            binder=NativeStrCollectionBinder(),
        )

    def search(self, query: str, *, limit: int = 100) -> list[IndexHit[str, _VectorRecord[_Meta]]]:
        return []

    def meta_of(
        self, key: str, value: str, *, previous: _VectorRecord[_Meta] | None
    ) -> _VectorRecord[_Meta]:
        return _VectorRecord(data={"tag": value})

    def upsert(self, key: str, value: str, meta: _VectorRecord[_Meta]) -> None:
        return None

    def wrap_data(self, data: _Meta, extras: _VectorEnvelope) -> _VectorRecord[_Meta]:
        return _VectorRecord(
            data=data,
            document=extras.get("document"),
            score=extras.get("score"),
        )

    def unwrap_data(self, record: _VectorRecord[_Meta]) -> _Meta:
        return record.data

    def unwrap_envelope(self, record: _VectorRecord[_Meta]) -> _VectorEnvelope:
        return {"document": record.document, "score": record.score}

    def get(self, key: str) -> _VectorRecord[_Meta] | None:
        return None

    def set(self, key: str, value: _VectorRecord[_Meta]) -> None:
        return None

    def delete(self, key: str) -> bool:
        return False


def test_index_backend_envelope_type_param() -> None:
    backend = _StubIndexBackend()
    assert backend.binding.collection == "stub:v1"
    record = backend.wrap_data({"tag": "x"}, {"document": "doc", "score": 0.1})
    assert record.data == {"tag": "x"}
    assert record.document == "doc"
    assert record.score == 0.1
    envelope = backend.unwrap_envelope(record)
    assert envelope.get("score") == 0.1
