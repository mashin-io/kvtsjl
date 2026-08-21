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
