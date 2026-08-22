"""SerDe unit tests."""

from __future__ import annotations

import pytest

from kvtsjl import SerDe
from kvtsjl.exceptions import KvStoreSerDeError


def test_identity_roundtrip() -> None:
    serde = SerDe.identity(str)
    assert serde.serialize("x") == "x"
    assert serde.deserialize("x") == "x"
    assert serde.blob_type is str


def test_utf8_bytes_roundtrip() -> None:
    serde = SerDe.utf8_bytes()
    assert serde.serialize("hi") == b"hi"
    assert serde.deserialize(b"hi") == "hi"
    assert serde.blob_type is bytes


def test_json_bytes_roundtrip() -> None:
    serde = SerDe.json_bytes()
    blob = serde.serialize({"a": 1})
    assert isinstance(blob, bytes)
    assert serde.deserialize(blob) == {"a": 1}


def test_serialize_wraps_errors() -> None:
    def bad(_value: str) -> bytes:
        raise RuntimeError("boom")

    serde = SerDe(serializer=bad, deserializer=lambda b: b.decode(), blob_type=bytes)
    with pytest.raises(KvStoreSerDeError, match="serialize failed"):
        serde.serialize("x")


def test_then_chains_serdes() -> None:
    serde = SerDe.identity(str).then(SerDe.utf8_bytes())
    assert serde.serialize("hi") == b"hi"
    assert serde.deserialize(b"hi") == "hi"


def test_rshift_chains_serdes() -> None:
    serde = SerDe.identity(str) >> SerDe.utf8_bytes()
    assert serde.serialize("pipe") == b"pipe"
    assert serde.deserialize(b"pipe") == "pipe"


def test_then_matches_compressed_helper() -> None:
    inner = SerDe.json_bytes()
    via_then = inner.then(SerDe.wire_compressed("gzip"))
    via_helper = SerDe.compressed("gzip", inner)
    value = {"a": 1}
    blob = via_then.serialize(value)
    assert via_helper.deserialize(blob) == value
    assert via_then.deserialize(via_helper.serialize(value)) == value
