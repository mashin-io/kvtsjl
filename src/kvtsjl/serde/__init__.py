"""Typed SerDe codecs for KV keys and values."""

from __future__ import annotations

from abc import ABC, abstractmethod
import codecs
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
import json
import pickle
from typing import cast

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
    def identity[U](blob_type: type[U]) -> IdentitySerDe[U]:
        return IdentitySerDe[U](
            serializer=lambda x: x,
            deserializer=lambda x: x,
            blob_type=blob_type,
        )

    @staticmethod
    def utf8_bytes() -> Utf8SerDe:
        return Utf8SerDe(
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


@dataclass(frozen=True, slots=True)
class StreamingSerDe[T, CHUNK, BLOB](SerDe[T, BLOB], ABC):
    """A serde whose transform can be applied incrementally.

    Streaming is not about bytes — it is about whether the transform can be
    computed chunk by chunk. ``identity`` trivially can; compression can, with
    a stateful compressor; UTF-8 can, with an incremental codec. JSON cannot:
    splitting a dict and encoding the pieces gives ``{"a": 1}{"b": 2}``, not
    ``{"a": 1, "b": 2}`` — the surface type is not chunkable at all.

    The store pipes chunks through these methods exactly as it calls
    ``serialize`` for a whole value. It never inspects the transform itself.
    """

    @property
    @abstractmethod
    def chunk_type(self) -> type[CHUNK]:
        """The type of one chunk.

        Usually ``T`` itself, because a chunk of ``bytes`` is ``bytes``. It is
        separate because it need not be: a serde for ``list[Row]`` may chunk
        into ``Row``. Nothing in the store depends on what this is — it exists
        so ``KvStore.streaming()`` can verify a caller's declaration instead of
        handing back a view whose type is a guess.
        """

    @abstractmethod
    def encode_stream(self, chunks: Iterable[CHUNK]) -> Iterator[BLOB]:
        """Encode value chunks into wire chunks."""

    @abstractmethod
    def decode_stream(self, chunks: Iterable[BLOB]) -> Iterator[CHUNK]:
        """Decode wire chunks back into value chunks."""

    def encoded_size(self, size: int | None) -> int | None:
        """Wire length for a value of ``size``, if it is known up front.

        Transports pass this to the medium as an exact length, not a hint, so
        only a serde that does not change the length can answer. The default is
        ``None`` ("unknown"), which is always safe. Guessing would truncate a
        real upload, so a transform that cannot know says so.
        """
        return None

    def then[B2](self, other: SerDe[BLOB, B2]) -> SerDe[T, B2]:
        """Streaming composes: a chain of incremental steps is incremental."""
        if isinstance(other, StreamingSerDe):
            return ChainedStreamingSerDe[T, CHUNK, BLOB, B2](
                serializer=lambda v: other.serialize(self.serialize(v)),
                deserializer=lambda b: self.deserialize(other.deserialize(b)),
                blob_type=other.blob_type,
                first=self,
                # ``isinstance`` cannot state it, but a serde reached through a
                # pipe chunks in its own input type: it is handed this serde's
                # wire chunks, so those are its chunks.
                second=cast("StreamingSerDe[BLOB, BLOB, B2]", other),
            )
        return super().then(other)


@dataclass(frozen=True, slots=True)
class ChainedStreamingSerDe[T, CHUNK, MID, BLOB](StreamingSerDe[T, CHUNK, BLOB]):
    """Two streaming serdes piped together.

    ``second`` must chunk in its own input type (``StreamingSerDe[MID, MID, …]``)
    — the pipe hands it ``first``'s wire chunks, so that is what its chunks are.
    A record serde can therefore start a chain but not continue one, which is
    exactly right: once records have become bytes, they are bytes.
    """

    first: StreamingSerDe[T, CHUNK, MID]
    second: StreamingSerDe[MID, MID, BLOB]

    @property
    def chunk_type(self) -> type[CHUNK]:
        return self.first.chunk_type  # the chain's input end

    def encode_stream(self, chunks: Iterable[CHUNK]) -> Iterator[BLOB]:
        return self.second.encode_stream(self.first.encode_stream(chunks))

    def decode_stream(self, chunks: Iterable[BLOB]) -> Iterator[CHUNK]:
        return self.first.decode_stream(self.second.decode_stream(chunks))

    def encoded_size(self, size: int | None) -> int | None:
        return self.second.encoded_size(self.first.encoded_size(size))


@dataclass(frozen=True, slots=True)
class IdentitySerDe[B](StreamingSerDe[B, B, B]):
    """The no-op codec: the value *is* the wire representation."""

    @property
    def chunk_type(self) -> type[B]:
        return self.blob_type  # the value is the wire form

    def encode_stream(self, chunks: Iterable[B]) -> Iterator[B]:
        return iter(chunks)

    def decode_stream(self, chunks: Iterable[B]) -> Iterator[B]:
        return iter(chunks)

    def encoded_size(self, size: int | None) -> int | None:
        return size  # the value is the wire form, byte for byte

    def then[B2](self, other: SerDe[B, B2]) -> SerDe[B, B2]:
        """``identity.then(x) is x`` — identity is the left unit of composition."""
        return other


@dataclass(frozen=True, slots=True)
class Utf8SerDe(StreamingSerDe[str, str, bytes]):
    """``str`` <-> UTF-8 ``bytes``, incrementally — chunks may split characters."""

    @property
    def chunk_type(self) -> type[str]:
        return str

    def encode_stream(self, chunks: Iterable[str]) -> Iterator[bytes]:
        encoder = codecs.getincrementalencoder("utf-8")()
        for chunk in chunks:
            if out := encoder.encode(chunk):
                yield out
        if tail := encoder.encode("", final=True):
            yield tail

    def decode_stream(self, chunks: Iterable[bytes]) -> Iterator[str]:
        decoder = codecs.getincrementaldecoder("utf-8")()
        for chunk in chunks:
            if out := decoder.decode(chunk):
                yield out
        if tail := decoder.decode(b"", final=True):
            yield tail


@dataclass(frozen=True, slots=True)
class CompressedSerDe[T, CHUNK](StreamingSerDe[T, CHUNK, bytes]):
    """A compression codec over an inner streaming serde."""

    codec: CompressionCodec
    inner: StreamingSerDe[T, CHUNK, bytes]

    @property
    def chunk_type(self) -> type[CHUNK]:
        return self.inner.chunk_type  # compression does not change the chunk

    def encode_stream(self, chunks: Iterable[CHUNK]) -> Iterator[bytes]:
        from kvtsjl.serde.compress import compress_chunks

        return compress_chunks(self.codec, self.inner.encode_stream(chunks))

    def decode_stream(self, chunks: Iterable[bytes]) -> Iterator[CHUNK]:
        from kvtsjl.serde.compress import decompress_chunks

        return self.inner.decode_stream(decompress_chunks(self.codec, chunks))
