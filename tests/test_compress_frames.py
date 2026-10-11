"""Codec readers must accept any valid frame shape, not only their own output.

A writer other than ``_compress_*`` — a streaming compressor, another library,
another tool — produces frames that are valid but shaped differently. The
readers have to cope, otherwise values written elsewhere are unreadable (or,
worse, silently short).
"""

from __future__ import annotations

import gzip
import io
import zlib

import pytest

from kvtsjl.serde.compress import _COMPRESSORS, _DECOMPRESSORS

zstandard = pytest.importorskip("zstandard")

PAYLOAD = b"kvtsjl" * 2000


def _streamed_zstd(data: bytes) -> bytes:
    """What a streaming compressor emits: no content size in the header."""
    buf = io.BytesIO()
    with zstandard.ZstdCompressor().stream_writer(buf, closefd=False) as writer:
        writer.write(data)
    return buf.getvalue()


def test_zstd_reads_its_own_one_shot_frame() -> None:
    assert _DECOMPRESSORS["zstd"](_COMPRESSORS["zstd"](PAYLOAD)) == PAYLOAD


def test_zstd_reads_a_streamed_frame_without_a_content_size() -> None:
    """Fails on the old reader: ZstdError, could not determine content size."""
    streamed = _streamed_zstd(PAYLOAD)
    assert zstandard.frame_content_size(streamed) == -1  # genuinely unsized
    assert _DECOMPRESSORS["zstd"](streamed) == PAYLOAD


def test_zstd_reads_concatenated_frames_without_truncating() -> None:
    """Fails on the old reader by returning only the first frame — silently."""
    first, second = b"A" * 1000, b"B" * 1000
    two_frames = _COMPRESSORS["zstd"](first) + _COMPRESSORS["zstd"](second)
    assert _DECOMPRESSORS["zstd"](two_frames) == first + second


def test_zstd_reads_an_empty_value() -> None:
    assert _DECOMPRESSORS["zstd"](_COMPRESSORS["zstd"](b"")) == b""


def test_gzip_reads_a_streamed_frame() -> None:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as handle:
        handle.write(PAYLOAD)
    assert _DECOMPRESSORS["gzip"](buf.getvalue()) == PAYLOAD


def test_zlib_reads_a_streamed_frame() -> None:
    compressor = zlib.compressobj()
    streamed = compressor.compress(PAYLOAD) + compressor.flush()
    assert _DECOMPRESSORS["zlib"](streamed) == PAYLOAD


def test_lz4_reads_a_streamed_frame() -> None:
    lz4_frame = pytest.importorskip("lz4.frame")
    compressor = lz4_frame.LZ4FrameCompressor()
    streamed = (
        compressor.begin() + compressor.compress(PAYLOAD) + compressor.flush()
    )
    assert _DECOMPRESSORS["lz4"](streamed) == PAYLOAD
