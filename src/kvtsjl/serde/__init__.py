"""Typed SerDe codecs for KV keys and values."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import json
import pickle

from kvtsjl.exceptions import KvStoreSerDeError
from kvtsjl.serde.compress import CompressionCodec, compressed as _compressed, wire_compressed

# JSON-encodable surface for json_* factories (deliberately broad, not object/Any).
type JsonScalar = None | bool | int | float | str
type JsonValue = JsonScalar | list[JsonValue] | dict[str, JsonValue]

__all__ = [
    "CompressionCodec",
    "JsonScalar",
    "JsonValue",
    "SerDe",
]


@dataclass(frozen=True, slots=True)
class SerDe[T, BLOB]:
    """Codec between surface type ``T`` and storage blob ``BLOB``.

    ``blob_type`` is the runtime class of ``BLOB`` (e.g. ``str`` or ``bytes``).
    Callers and backends trust this instead of probing encode/decode.
    """

    serializer: Callable[[T], BLOB]
    deserializer: Callable[[BLOB], T]
    blob_type: type[BLOB]

    def serialize(self, value: T) -> BLOB:
        try:
            return self.serializer(value)
        except Exception as exc:
            raise KvStoreSerDeError(f"serialize failed: {exc}") from exc

    def deserialize(self, blob: BLOB) -> T:
        try:
            return self.deserializer(blob)
        except Exception as exc:
            raise KvStoreSerDeError(f"deserialize failed: {exc}") from exc

    def then[B2](self, other: SerDe[BLOB, B2]) -> SerDe[T, B2]:
        """Pipeline: ``other(self.serialize(v))`` on write; reverse on read."""
        return SerDe[T, B2](
            serializer=lambda value: other.serialize(self.serialize(value)),
            deserializer=lambda blob: self.deserialize(other.deserialize(blob)),
            blob_type=other.blob_type,
        )

    def __rshift__[B2](self, other: SerDe[BLOB, B2]) -> SerDe[T, B2]:
        """Pipeline sugar: ``serde1 >> serde2`` equals ``serde1.then(serde2)``."""
        return self.then(other)

    @staticmethod
    def identity[U](blob_type: type[U]) -> SerDe[U, U]:
        return SerDe[U, U](
            serializer=lambda x: x,
            deserializer=lambda x: x,
            blob_type=blob_type,
        )

    @staticmethod
    def utf8_bytes() -> SerDe[str, bytes]:
        return SerDe[str, bytes](
            serializer=lambda s: s.encode("utf-8"),
            deserializer=lambda b: b.decode("utf-8"),
            blob_type=bytes,
        )

    @staticmethod
    def safe_str() -> SerDe[str, str]:
        """Identity for string KBLOB (path/redis-safe encoding is layout's job)."""
        return SerDe[str, str](
            serializer=lambda s: s,
            deserializer=lambda s: s,
            blob_type=str,
        )

    @staticmethod
    def json_str() -> SerDe[JsonValue, str]:
        return SerDe[JsonValue, str](
            serializer=lambda v: json.dumps(v, separators=(",", ":"), default=str),
            deserializer=json.loads,
            blob_type=str,
        )

    @staticmethod
    def json_bytes() -> SerDe[JsonValue, bytes]:
        return SerDe[JsonValue, bytes](
            serializer=lambda v: json.dumps(
                v, separators=(",", ":"), default=str
            ).encode("utf-8"),
            deserializer=lambda b: json.loads(b.decode("utf-8")),
            blob_type=bytes,
        )

    @staticmethod
    def pickle_bytes[U](_value_type: type[U]) -> SerDe[U, bytes]:
        return SerDe[U, bytes](
            serializer=pickle.dumps,
            deserializer=pickle.loads,
            blob_type=bytes,
        )

    @staticmethod
    def wire_compressed(codec: CompressionCodec) -> SerDe[bytes, bytes]:
        """Wire-stage compression on ``bytes`` (compose with ``.then`` or ``>>``)."""
        return wire_compressed(codec)

    @staticmethod
    def compressed[UV](codec: CompressionCodec, inner: SerDe[UV, bytes]) -> SerDe[UV, bytes]:
        """Compress wire bytes from ``inner`` using a fixed ``codec``."""
        return _compressed(codec, inner)
