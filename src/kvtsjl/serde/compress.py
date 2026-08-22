"""Wire-level compression wrappers for byte ``SerDe`` codecs."""

from __future__ import annotations

from collections.abc import Callable
import gzip
from typing import TYPE_CHECKING, Literal, TypeVar
import zlib

from kvtsjl.exceptions import KvStoreSerDeError

if TYPE_CHECKING:
    from kvtsjl.serde import SerDe

CompressionCodec = Literal["gzip", "zlib", "zstd", "lz4"]

T = TypeVar("T")

__all__ = ["CompressionCodec", "compressed", "wire_compressed"]


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
        import zstandard  # type: ignore[import-not-found]
    except ImportError as exc:
        raise _missing_extra("zstd", "zstd") from exc
    return zstandard.ZstdCompressor().compress(data)


def _decompress_zstd(data: bytes) -> bytes:
    try:
        import zstandard  # type: ignore[import-not-found]
    except ImportError as exc:
        raise _missing_extra("zstd", "zstd") from exc
    return zstandard.ZstdDecompressor().decompress(data)


def _compress_lz4(data: bytes) -> bytes:
    try:
        import lz4.frame  # type: ignore[import-not-found]
    except ImportError as exc:
        raise _missing_extra("lz4", "lz4") from exc
    return lz4.frame.compress(data)


def _decompress_lz4(data: bytes) -> bytes:
    try:
        import lz4.frame  # type: ignore[import-not-found]
    except ImportError as exc:
        raise _missing_extra("lz4", "lz4") from exc
    return lz4.frame.decompress(data)


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


def wire_compressed(codec: CompressionCodec) -> SerDe[bytes, bytes]:
    """Compress/decompress wire ``bytes`` with a fixed ``codec``."""
    from kvtsjl.serde import SerDe

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

    return SerDe[bytes, bytes](serializer=serialize, deserializer=deserialize, blob_type=bytes)


def compressed(codec: CompressionCodec, inner: SerDe[T, bytes]) -> SerDe[T, bytes]:
    """Wrap a byte ``SerDe`` with fixed ``codec`` compress/decompress on the wire."""
    if inner.blob_type is not bytes:
        raise ValueError(f"compressed inner SerDe must use bytes blobs, got {inner.blob_type!r}")
    return inner.then(wire_compressed(codec))
