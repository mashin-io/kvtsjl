"""Compression SerDe and KvSet integration."""

from __future__ import annotations

import pytest

from kvtsjl import KvSet, MemoryKvStore, SerDe
from kvtsjl.exceptions import KvStoreSerDeError
from kvtsjl.store.compose.mirror import MirrorKvStore


def _memory_store(kvset: KvSet[str, str, str, bytes]) -> MemoryKvStore[str, str, str, bytes]:
    return MemoryKvStore(kvset)


@pytest.mark.parametrize("codec", ["gzip", "zlib"])
def test_compressed_roundtrip_stdlib(codec: str) -> None:
    inner = SerDe.utf8_bytes()
    serde = SerDe.compressed(codec, inner)
    blob = serde.serialize("hello world")
    assert blob != b"hello world"
    assert serde.deserialize(blob) == "hello world"


def test_compressed_json_bytes_roundtrip() -> None:
    serde = SerDe.compressed("gzip", SerDe.json_bytes())
    blob = serde.serialize({"a": 1, "b": "x"})
    assert serde.deserialize(blob) == {"a": 1, "b": "x"}


def test_compressed_via_pipeline_operator() -> None:
    serde = SerDe.json_bytes() >> SerDe.wire_compressed("gzip")
    blob = serde.serialize({"a": 1})
    assert serde.deserialize(blob) == {"a": 1}


def test_compressed_corrupt_blob() -> None:
    serde = SerDe.compressed("gzip", SerDe.utf8_bytes())
    with pytest.raises(KvStoreSerDeError, match="decompress"):
        serde.deserialize(b"not-gzip-data")


def test_compressed_requires_bytes_inner() -> None:
    with pytest.raises(ValueError, match="bytes blobs"):
        SerDe.compressed("gzip", SerDe.identity(str))


def test_kvset_with_compressed_value() -> None:
    kvset = KvSet.with_str_keys(
        "cmp",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
    ).with_compressed_value("gzip")
    store = _memory_store(kvset)
    store.set("k", "payload")
    assert store.get("k") == "payload"
    raw = store._bucket()[store._physical_key_blob("k")].value_blob
    assert raw != b"payload"


def test_mirror_compressed_stores() -> None:
    kvset = KvSet.with_str_keys(
        "cmp",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
    ).with_compressed_value("gzip")
    primary = _memory_store(kvset)
    secondary = _memory_store(kvset)
    store = MirrorKvStore(primary, secondary)
    store.set("k", "synced")
    assert primary.get("k") == "synced"
    assert secondary.get("k") == "synced"


def test_zstd_roundtrip() -> None:
    zstandard = pytest.importorskip("zstandard")
    _ = zstandard
    serde = SerDe.compressed("zstd", SerDe.utf8_bytes())
    assert serde.deserialize(serde.serialize("zstd")) == "zstd"


def test_lz4_roundtrip() -> None:
    lz4 = pytest.importorskip("lz4.frame")
    _ = lz4
    serde = SerDe.compressed("lz4", SerDe.utf8_bytes())
    assert serde.deserialize(serde.serialize("lz4")) == "lz4"
