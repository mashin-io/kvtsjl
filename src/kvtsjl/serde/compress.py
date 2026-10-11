"""Wire-level compression wrappers for byte ``SerDe`` codecs."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
import gzip
import importlib
import io
from typing import TYPE_CHECKING, Any, BinaryIO, Literal, TypeVar, cast
import zlib

from kvtsjl.exceptions import KvStoreSerDeError

if TYPE_CHECKING:
    from kvtsjl.serde import CompressedSerDe, SerDe

CompressionCodec = Literal["gzip", "zlib", "zstd", "lz4"]

T = TypeVar("T")

__all__ = [
    "ChunkReader",
    "CompressionCodec",
    "compress_chunks",
    "compressed",
    "decompress_chunks",
    "read_chunks",
    "wire_compressed",
]


def _missing_extra(codec: CompressionCodec, extra: str) -> KvStoreSerDeError:
    return KvStoreSerDeError(
        f"compression codec {codec!r} requires optional dependency; "
        f"install with: pip install 'kvtsjl[{extra}]'"
    )


def _compress_gzip(data: bytes) -> bytes:
    return gzip.compress(data)


def _decompress_gzip(data: bytes) -> bytes:
    return gzip.decompress(data)


def _compress_zlib(data: bytes) -> bytes:
    return zlib.compress(data)


def _decompress_zlib(data: bytes) -> bytes:
    return zlib.decompress(data)


def _compress_zstd(data: bytes) -> bytes:
    try:
        zstandard = importlib.import_module("zstandard")
    except ImportError as exc:
        raise _missing_extra("zstd", "zstd") from exc
    return zstandard.ZstdCompressor().compress(data)


def _decompress_zstd(data: bytes) -> bytes:
    """Decompress any valid zstd input, not only one-shot single frames.

    ``ZstdDecompressor().decompress(data)`` is wrong on two kinds of input that
    other writers produce:

    - a frame without a content size in its header (what every *streaming*
      compressor emits unless told the size up front) raises
      ``ZstdError: could not determine content size in frame header``;
    - concatenated frames are **silently truncated** to the first frame, with
      no error at all — the worse of the two, since the caller gets short data
      that looks valid.

    ``stream_reader`` handles both. ``read_across_frames`` is passed explicitly:
    it already behaves this way in zstandard 0.25, but the library documents the
    default as ``False``, so relying on it would tie correctness to the
    installed version.
    """
    try:
        zstandard = importlib.import_module("zstandard")
    except ImportError as exc:
        raise _missing_extra("zstd", "zstd") from exc
    reader = zstandard.ZstdDecompressor().stream_reader(
        io.BytesIO(data), read_across_frames=True
    )
    with reader:
        return reader.read()


def _compress_lz4(data: bytes) -> bytes:
    try:
        lz4_frame = importlib.import_module("lz4.frame")
    except ImportError as exc:
        raise _missing_extra("lz4", "lz4") from exc
    return lz4_frame.compress(data)


def _decompress_lz4(data: bytes) -> bytes:
    try:
        lz4_frame = importlib.import_module("lz4.frame")
    except ImportError as exc:
        raise _missing_extra("lz4", "lz4") from exc
    return lz4_frame.decompress(data)


_COMPRESSORS: dict[CompressionCodec, Callable[[bytes], bytes]] = {
    "gzip": _compress_gzip,
    "zlib": _compress_zlib,
    "zstd": _compress_zstd,
    "lz4": _compress_lz4,
}

_DECOMPRESSORS: dict[CompressionCodec, Callable[[bytes], bytes]] = {
    "gzip": _decompress_gzip,
    "zlib": _decompress_zlib,
    "zstd": _decompress_zstd,
    "lz4": _decompress_lz4,
}


def wire_compressed(codec: CompressionCodec) -> CompressedSerDe[bytes, bytes]:
    """Compress/decompress wire ``bytes`` with a fixed ``codec``."""
    compress_fn = _COMPRESSORS[codec]
    decompress_fn = _DECOMPRESSORS[codec]

    def serialize(blob: bytes) -> bytes:
        try:
            return compress_fn(blob)
        except KvStoreSerDeError:
            raise
        except Exception as exc:
            raise KvStoreSerDeError(f"compress ({codec}) failed: {exc}") from exc

    def deserialize(blob: bytes) -> bytes:
        try:
            return decompress_fn(blob)
        except KvStoreSerDeError:
            raise
        except Exception as exc:
            raise KvStoreSerDeError(f"decompress ({codec}) failed: {exc}") from exc

    from kvtsjl.serde import CompressedSerDe, SerDe as _SerDe

    return CompressedSerDe[bytes, bytes](
        serializer=serialize,
        deserializer=deserialize,
        blob_type=bytes,
        codec=codec,
        inner=_SerDe.identity(bytes),
    )


def compressed(codec: CompressionCodec, inner: SerDe[T, bytes]) -> SerDe[T, bytes]:
    """Wrap a byte ``SerDe`` with fixed ``codec`` compress/decompress on the wire."""
    if inner.blob_type is not bytes:
        raise ValueError(f"compressed inner SerDe must use bytes blobs, got {inner.blob_type!r}")
    return inner.then(wire_compressed(codec))


# --- streaming codecs ---------------------------------------------------------

STREAM_CHUNK = 1 << 20
"""Chunk size for streaming compress / decompress (1 MiB)."""


def _incremental_compressor(codec: CompressionCodec) -> Any:
    """An object with ``compress(bytes) -> bytes`` and ``flush() -> bytes``.

    Compression is chunk-homomorphic only because the compressor carries state
    across chunks: the concatenated output is one valid frame, which is not the
    same as compressing each chunk alone.
    """
    if codec == "gzip":
        # wbits=31 writes a gzip wrapper, so this needs no GzipFile buffer.
        return _ZlibCompressor(zlib.compressobj(wbits=31))
    if codec == "zlib":
        return _ZlibCompressor(zlib.compressobj())
    if codec == "zstd":
        zstandard = importlib.import_module("zstandard")
        return _ZlibCompressor(zstandard.ZstdCompressor().compressobj())
    if codec == "lz4":
        lz4_frame = importlib.import_module("lz4.frame")
        return _Lz4Compressor(lz4_frame.LZ4FrameCompressor())
    raise ValueError(f"unknown codec {codec!r}")


class _ZlibCompressor:
    """``compressobj`` shape, shared by zlib, gzip and zstd."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def compress(self, data: bytes) -> bytes:
        return cast(bytes, self._inner.compress(data))

    def flush(self) -> bytes:
        return cast(bytes, self._inner.flush())


class _Lz4Compressor:
    """LZ4 frames need an explicit header before the first block."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self._begun = False

    def compress(self, data: bytes) -> bytes:
        head = b""
        if not self._begun:
            head = cast(bytes, self._inner.begin())
            self._begun = True
        return head + cast(bytes, self._inner.compress(data))

    def flush(self) -> bytes:
        head = b"" if self._begun else cast(bytes, self._inner.begin())
        self._begun = True
        return head + cast(bytes, self._inner.flush())


def compress_chunks(codec: CompressionCodec | None, chunks: Iterable[bytes]) -> Iterator[bytes]:
    """Compress a chunk stream; the output is one frame, not one per chunk."""
    if codec is None:
        yield from chunks
        return
    compressor = _incremental_compressor(codec)
    for chunk in chunks:
        if out := compressor.compress(chunk):
            yield out
    if tail := compressor.flush():
        yield tail


def decompress_chunks(codec: CompressionCodec | None, chunks: Iterable[bytes]) -> Iterator[bytes]:
    """Decompress a chunk stream. Chunk boundaries need not match the writer's."""
    if codec is None:
        yield from chunks
        return
    decompressor = _incremental_decompressor(codec)
    for chunk in chunks:
        if out := decompressor.decompress(chunk):
            yield out
    if tail := decompressor.flush():
        yield tail


def read_chunks(src: BinaryIO, size: int = STREAM_CHUNK) -> Iterator[bytes]:
    """Chunk a binary source. ``read(n)`` is the chunking primitive."""
    while chunk := src.read(size):
        yield chunk


class ChunkReader(io.RawIOBase):
    """Readable stream over an iterator of chunks (pull side of a pipeline).

    Lets a driver's ``write_stream`` pull compressed bytes without the whole
    encoded body existing at once.
    """

    def __init__(self, chunks: Iterator[bytes]) -> None:
        self._chunks = chunks
        self._buf = b""
        self._done = False

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Any) -> int:  # pragma: no cover - via read()
        data = self.read(len(buffer))
        if not data:
            return 0
        buffer[: len(data)] = data
        return len(data)

    def read(self, size: int | None = -1) -> bytes:
        if size is None or size < 0:
            parts = [self._buf, *self._chunks]
            self._buf = b""
            self._done = True
            return b"".join(parts)
        while len(self._buf) < size and not self._done:
            try:
                self._buf += next(self._chunks)
            except StopIteration:
                self._done = True
        out, self._buf = self._buf[:size], self._buf[size:]
        return out


def _incremental_decompressor(codec: CompressionCodec) -> Any:
    if codec == "gzip":
        return _ZlibIncremental(zlib.decompressobj(wbits=31))
    if codec == "zlib":
        return _ZlibIncremental(zlib.decompressobj())
    if codec == "zstd":
        zstandard = importlib.import_module("zstandard")
        return _ZstdIncremental(zstandard.ZstdDecompressor())
    if codec == "lz4":
        lz4_frame = importlib.import_module("lz4.frame")
        return _Lz4Incremental(lz4_frame.LZ4FrameDecompressor())
    raise ValueError(f"unknown codec {codec!r}")


class _ZlibIncremental:
    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def decompress(self, data: bytes) -> bytes:
        return self._inner.decompress(data)

    def flush(self) -> bytes:
        return self._inner.flush()


class _ZstdIncremental:
    """Feeds frames one at a time so concatenated frames do not truncate."""

    def __init__(self, decompressor: Any) -> None:
        self._decompressor = decompressor
        self._obj = decompressor.decompressobj()

    def decompress(self, data: bytes) -> bytes:
        out = b""
        while data:
            out += self._obj.decompress(data)
            if not self._obj.eof:
                break
            # A frame ended mid-chunk: continue with a fresh frame decoder.
            data = self._obj.unused_data
            self._obj = self._decompressor.decompressobj()
        return out

    def flush(self) -> bytes:
        return b""


class _Lz4Incremental:
    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def decompress(self, data: bytes) -> bytes:
        return self._inner.decompress(data)

    def flush(self) -> bytes:
        return b""
