"""Chunking helpers for efficient batch ops."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence

DEFAULT_BATCH_SIZE = 500


def iter_chunks[T](iterable: Iterable[T], size: int) -> Iterator[list[T]]:
    """Yield lists of at most ``size`` items from ``iterable``."""
    if size < 1:
        raise ValueError("chunk size must be >= 1")
    chunk: list[T] = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def chunk_sequence[T](items: Sequence[T], size: int) -> Iterator[Sequence[T]]:
    if size < 1:
        raise ValueError("chunk size must be >= 1")
    for i in range(0, len(items), size):
        yield items[i : i + size]
