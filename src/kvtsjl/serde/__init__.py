"""Typed SerDe codecs for KV keys and values."""

from __future__ import annotations

import json
import pickle
from collections.abc import Callable
from dataclasses import dataclass

from kvtsjl.exceptions import KvStoreSerDeError

# JSON-encodable surface for json_* factories (deliberately broad, not object/Any).
type JsonScalar = None | bool | int | float | str
type JsonValue = JsonScalar | list[JsonValue] | dict[str, JsonValue]

__all__ = [
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

    @classmethod
    def identity[U](cls, blob_type: type[U]) -> SerDe[U, U]:
        return cls(
            serializer=lambda x: x,
            deserializer=lambda x: x,
            blob_type=blob_type,
        )

    @classmethod
    def utf8_bytes(cls) -> SerDe[str, bytes]:
        return cls(
            serializer=lambda s: s.encode("utf-8"),
            deserializer=lambda b: b.decode("utf-8"),
            blob_type=bytes,
        )

    @classmethod
    def safe_str(cls) -> SerDe[str, str]:
        """Identity for string KBLOB (path/redis-safe encoding is layout's job)."""
        return cls(
            serializer=lambda s: s,
            deserializer=lambda s: s,
            blob_type=str,
        )

    @classmethod
    def json_str(cls) -> SerDe[JsonValue, str]:
        return cls(
            serializer=lambda v: json.dumps(v, separators=(",", ":"), default=str),
            deserializer=json.loads,
            blob_type=str,
        )

    @classmethod
    def json_bytes(cls) -> SerDe[JsonValue, bytes]:
        return cls(
            serializer=lambda v: json.dumps(
                v, separators=(",", ":"), default=str
            ).encode("utf-8"),
            deserializer=lambda b: json.loads(b.decode("utf-8")),
            blob_type=bytes,
        )

    @classmethod
    def pickle_bytes[U](cls) -> SerDe[U, bytes]:
        return cls(
            serializer=pickle.dumps,
            deserializer=pickle.loads,
            blob_type=bytes,
        )
