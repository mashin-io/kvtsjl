"""Read-only document store ABC (scope + scan, no writes)."""

from __future__ import annotations

from abc import ABC
from collections.abc import Iterator
from typing import Any

from kvtsjl.keymap import ReadableKeyMap
from kvtsjl.store.capabilities import ScannableMixin, ScopedMixin
from kvtsjl.store.streaming import StreamDest, StreamingReadView, open_dest


class ReadableKvStore[K, V](ReadableKeyMap[K, V], ScopedMixin, ScannableMixin[K, V], ABC):
    """Logical read-only store: ``ReadableKeyMap`` + scope + scan.

    Returned by ``readonly()``, ``map``, ``then``, and ``expand``. Mutations are
    absent from the type; ``ReadonlyKvStore`` still raises ``KvStoreReadOnlyError``
    at runtime for accidental untyped writes.
    """

    def stream_chunk_type(self) -> type[Any]:
        """The type of one streamed chunk, or raise if this store cannot stream.

        Forwarded through compositions exactly as ``supports_write_clock`` is;
        leaves answer from their value serde.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not support streaming"
        )

    def streaming[C](self, chunk_type: type[C]) -> StreamingReadView[K, C]:
        """A streaming view typed in ``chunk_type``, checked against the serde.

        Use this when chunks are not ``bytes`` or ``str`` — ``set_stream`` and
        ``read_stream`` cover those directly. Raises ``TypeError`` on a
        mismatch rather than returning a view whose type is a lie.

        Read-only here; ``KvStore`` overrides it to return a writable view, so
        a ``readonly()`` store cannot produce one that advertises writes.
        """
        self._check_chunk_type(chunk_type)
        return StreamingReadView[K, C](self, chunk_type)

    def _check_chunk_type(self, chunk_type: type[Any]) -> None:
        """Verify a caller's declaration against the serde.

        One implementation, used by both ``streaming`` overloads — the view's
        type is only honest because this ran.
        """
        actual = self.stream_chunk_type()
        if actual is not chunk_type:
            raise TypeError(
                f"{type(self).__name__} streams {actual.__name__} chunks, "
                f"not {chunk_type.__name__}"
            )

    def read_stream[Chunkable: (bytes, str)](
        self: ReadableKvStore[K, Chunkable], key: K
    ) -> Iterator[Chunkable] | None:
        """Chunks of ``key``'s value, or ``None`` if it is missing or expired.

        This is the streaming *read* primitive, not ``get_to``. Pull-shaped so
        that one store's read can be handed straight to another store's
        ``set_stream`` — which is what ``mirror`` and ``fallback_read`` do, with
        no temporary file and no assumption that a chunk is ``bytes``.

        The iterator may hold a file handle or HTTP response open; callers
        drain it or close it.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not support read_stream"
        )

    def get_to[Chunkable: (bytes, str)](
        self: ReadableKvStore[K, Chunkable], key: K, dest: StreamDest
    ) -> bool:
        """Write ``key``'s value into ``dest`` without holding it whole.

        Sugar over ``read_stream``: stores implement that one and inherit this.

        Returns ``True`` when the key was found and written — **including a
        zero-byte value** — and ``False`` when missing or expired. "Wrote
        nothing" and "found nothing" look alike at the call site, so test the
        return value, not the byte count.
        """
        chunks = self.read_stream(key)
        if chunks is None:
            return False
        with open_dest(dest) as out:
            for chunk in chunks:
                out.write(chunk)
        return True
