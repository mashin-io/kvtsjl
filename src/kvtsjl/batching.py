"""Chunking helpers for efficient batch ops."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor

DEFAULT_BATCH_SIZE = 500
DEFAULT_IO_CONCURRENCY = 32
"""Default concurrent GETs for cloud object stores (S3 / GCS / Azure)."""

DEFAULT_FS_IO_CONCURRENCY = 8
"""Default concurrent GETs for ``FilesystemKvStore`` (local/dev-friendlier)."""

S3_DELETE_OBJECTS_MAX = 1000
"""S3 ``delete_objects`` allows at most 1000 keys per request."""

AZURE_DELETE_BLOBS_MAX = 256
"""Azure Blob batch delete allows at most 256 blobs per request."""

GCS_BATCH_OPS_MAX = 1000
"""GCS JSON API batch requests allow at most 1000 ops."""


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


def map_concurrent[T, R](
    items: Sequence[T],
    fn: Callable[[T], R],
    *,
    max_workers: int,
) -> list[R]:
    """Apply ``fn`` to each item; return results in input order.

    ``max_workers <= 1`` or a single item runs inline (no thread pool).
    Worker exceptions propagate like a serial mid-batch failure.
    """
    if max_workers < 1:
        raise ValueError(f"max_workers must be >= 1, got {max_workers}")
    if not items:
        return []
    if max_workers <= 1 or len(items) <= 1:
        return [fn(item) for item in items]
    workers = min(max_workers, len(items))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(fn, items))
