"""Streaming reads and writes for stores whose serde can work incrementally.

Streaming is not a property of the value type — it is a property of the
*transform*. A store can stream when its value serde can encode and decode
chunk by chunk, which is what ``StreamingSerDe`` declares. Identity can
(trivially), compression can (with a stateful compressor), UTF-8 can (with an
incremental codec). JSON cannot: splitting a dict and encoding the pieces
yields ``{"a": 1}{"b": 2}``, not ``{"a": 1, "b": 2}``.

The store never inspects the transform. It reads chunks of ``V`` from the
source, hands them to the serde, and gives the resulting wire chunks to the
driver — the same division of labour as ``set``, one chunk at a time.

``KvStore`` annotates ``self: KvStore[K, bytes]`` on these methods, so the
common bytes case is caught at type-check time. That annotation is narrower
than the real rule (a ``str`` store with a UTF-8 serde streams fine), so
``require_streaming_serde`` is the authority at runtime.
"""

from __future__ import annotations

from collections.abc import Generator, Iterable, Iterator
from contextlib import contextmanager
from datetime import datetime
import os
from typing import IO, TYPE_CHECKING, Any, cast

from kvtsjl.serde import SerDe, StreamingSerDe
from kvtsjl.store.schema.ttl import TtlPolicy

if TYPE_CHECKING:
    from kvtsjl.store.logical import KvStore
    from kvtsjl.store.readable import ReadableKvStore
from kvtsjl.serde.compress import STREAM_CHUNK

StreamSource = IO[Any] | Iterable[Any] | str | os.PathLike[str]
StreamDest = IO[Any] | str | os.PathLike[str]


@contextmanager
def iter_source(source: StreamSource) -> Generator[Iterator[Any]]:
    """Yield chunks of ``V`` from a path, an open file, or an iterable.

    A path opens in binary mode. For a non-bytes value type, pass an already
    open handle (whose ``read(n)`` returns that type) or an iterable of chunks
    — the file mode is the caller's choice, not something to infer from here.
    """
    if isinstance(source, (str, os.PathLike)):
        with open(source, "rb") as handle:
            yield _read_chunks(handle)
    elif hasattr(source, "read"):
        yield _read_chunks(source)
    else:
        yield iter(source)


def _read_chunks(handle: Any) -> Iterator[Any]:
    while chunk := handle.read(STREAM_CHUNK):
        yield chunk


@contextmanager
def open_dest(dest: StreamDest) -> Generator[IO[Any]]:
    """Yield a writable stream, opening a path in binary mode if one was given."""
    if isinstance(dest, (str, os.PathLike)):
        with open(dest, "wb") as handle:
            yield handle
    else:
        yield dest


def close_stream(chunks: Iterator[Any]) -> None:
    """Release a chunk stream that will not be drained.

    ``read_stream`` returns an ``Iterator``, not a ``Generator``: a driver may
    hand back a plain iterator with nothing to release. Generators — which is
    what the streaming drivers return — close their underlying response here
    instead of at the next collection.
    """
    closer = getattr(chunks, "close", None)
    if callable(closer):
        closer()


def require_streaming_serde[T, BLOB](
    value_serde: SerDe[T, BLOB], *, op: str, backend: str
) -> StreamingSerDe[T, Any, BLOB]:
    """Return the serde as a streaming one, or explain why it is not.

    The named serde subtypes are what make this answerable: an anonymous pair
    of closures cannot be asked whether it can be applied incrementally.
    """
    if isinstance(value_serde, StreamingSerDe):
        # The chunk type is deliberately opaque here: the store moves chunks
        # without inspecting them. ``KvStore.streaming()`` is where a caller
        # names it and it is checked.
        return cast("StreamingSerDe[T, Any, BLOB]", value_serde)
    raise NotImplementedError(
        f"{backend}.{op} needs a value serde that can encode incrementally "
        f"(a StreamingSerDe); {type(value_serde).__name__} has to see the "
        "whole value, so there is nothing to stream"
    )


class StreamingReadView[K, C]:
    """Typed streaming reads from a store whose chunks are ``C``.

    ``KvStore.set_stream`` / ``read_stream`` are restricted to ``bytes`` and
    ``str`` because ``KvStore[K, V]`` has nowhere to put a chunk type, and a
    chunk is not always a value of ``V`` — a ``list[Row]`` store may stream
    ``Row``. The implementation never cared: chunks pass through the store, the
    codecs, the drivers and the compositions untouched. Only the annotation
    could not say so.

    This view says it. The caller declares the chunk type once, it is checked
    against the serde there and then, and everything downstream is typed::

        rows = catalog.streaming(Row)       # StreamingView[str, Row]
        for row in rows.read_stream(key) or ():
            ...                             # row: Row

    The declaration is verified, not inferred — no type checker can recover
    ``C`` from ``KvStore[K, V]``. That is the whole cost of the generality, and
    it is one line. Stores whose chunks really are ``bytes`` or ``str`` need no
    view at all and keep calling ``read_stream`` directly.

    Read-only on purpose: this is what ``ReadableKvStore`` hands out, so a
    ``readonly()`` store cannot produce a view that advertises writes.
    """

    __slots__ = ("_store", "_chunk_type")

    # ``Any`` is the store's value type, which this view genuinely does not
    # care about: it is parameterized by the *chunk* type, and a chunk need not
    # be a value. Naming it would mean ``StreamingView[K, V, C]`` at every call
    # site to carry something no method here touches.
    def __init__(self, store: ReadableKvStore[K, Any], chunk_type: type[C]) -> None:
        self._store = store
        self._chunk_type = chunk_type

    @property
    def chunk_type(self) -> type[C]:
        return self._chunk_type

    def read_stream(self, key: K) -> Iterator[C] | None:
        """As ``KvStore.read_stream``, with the chunks typed in ``C``.

        The cast is what the constructor's check bought: the store's own
        signature is restricted to ``bytes``/``str`` and cannot describe ``C``.
        """
        return cast("Iterator[C] | None", self._store.read_stream(key))

    def __repr__(self) -> str:
        name = type(self).__name__
        return f"{name}({self._store!r}, chunk_type={self._chunk_type.__name__})"


class StreamingView[K, C](StreamingReadView[K, C]):
    """A ``StreamingReadView`` that can also write — handed out by ``KvStore``."""

    __slots__ = ("_writable",)

    def __init__(self, store: KvStore[K, Any], chunk_type: type[C]) -> None:
        super().__init__(store, chunk_type)
        self._writable = store

    def set_stream(
        self,
        key: K,
        # ``IO[Any]`` rather than ``IO[C]``: ``typing.IO`` is generic in
        # ``AnyStr``, so it cannot be parameterized by an unconstrained ``C``.
        # A file handle is only meaningful when chunks are ``bytes`` or ``str``
        # anyway — for anything else, pass an iterable of chunks.
        source: Iterable[C] | IO[Any] | str | os.PathLike[str],
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
        size: int | None = None,
    ) -> None:
        """As ``KvStore.set_stream``, with the source typed in ``C``.

        ``size`` carries the same contract: the exact encoded length or ``None``.
        """
        self._writable.set_stream(
            key, source, ttl=ttl, created_at=created_at, size=size
        )
