"""Streaming reads and writes for byte-valued stores.

The memory tests use a source that **raises if read in full** rather than
measuring with ``tracemalloc``. Buffering then cannot pass, instead of being
something a reviewer has to notice afterwards.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
import io
import json
import os
from pathlib import Path
import random
import tempfile
from typing import Any, cast

import pytest

from kvtsjl import KvSet, MemoryKvStore, SerDe
from kvtsjl.backends.filesystem import FilesystemKvStore
from kvtsjl.exceptions import UncommittedWriteError
from kvtsjl.serde import ChainedStreamingSerDe, CompressedSerDe, StreamingSerDe
from kvtsjl.serde.compress import STREAM_CHUNK, CompressionCodec
from kvtsjl.store.streaming import StreamingReadView, StreamingView, close_stream

CODECS: list[CompressionCodec | None] = [None, "gzip", "zlib", "zstd", "lz4"]
CHUNK = 1 << 16


class RefusesFullRead(io.RawIOBase):
    """A source that fails if anyone tries to read it whole.

    ``read(-1)`` / ``readall()`` is exactly what buffering does, so a store that
    buffers cannot get past this — no measurement required.
    """

    def __init__(self, total: int, chunk: int = CHUNK) -> None:
        self._remaining = total
        self._chunk = chunk
        self.max_single_read = 0

    def readable(self) -> bool:
        return True

    def read(self, size: int | None = -1) -> bytes:
        if size is None or size < 0:
            raise AssertionError("source was read in full — the write path is buffering")
        self.max_single_read = max(self.max_single_read, size)
        n = min(size, self._remaining, self._chunk)
        self._remaining -= n
        return b"x" * n

    def readinto(self, buffer: object) -> int:  # pragma: no cover - via read
        raise AssertionError("readinto bypasses the chunk guard")


def byte_kvset(name: str, codec: CompressionCodec | None) -> KvSet[str, bytes, str, bytes]:
    kvset = KvSet.with_str_keys(
        name, key_serde=SerDe.identity(str), value_serde=SerDe.identity(bytes)
    )
    return kvset.with_compressed_value(codec) if codec else kvset


def fs_store(codec: CompressionCodec | None) -> FilesystemKvStore[str, bytes, str]:
    return FilesystemKvStore(byte_kvset("blobs", codec), root=tempfile.mkdtemp())


@pytest.mark.parametrize("codec", CODECS)
def test_round_trip_all_four_combinations(codec: CompressionCodec | None) -> None:
    """{set, set_stream} x {get, get_to} must all agree."""
    payload = b"kvtsjl-" * 20_000
    store = fs_store(codec)

    store.set("a", payload)
    store.set_stream("b", io.BytesIO(payload))

    assert store.get("a") == payload
    assert store.get("b") == payload, "set_stream wrote bytes get cannot read"

    for key in ("a", "b"):
        sink = io.BytesIO()
        assert store.get_to(key, sink) is True
        assert sink.getvalue() == payload


@pytest.mark.parametrize("codec", CODECS)
def test_set_stream_never_reads_the_source_whole(
    codec: CompressionCodec | None,
) -> None:
    total = 8 << 20  # 8 MiB, far above the chunk size
    source = RefusesFullRead(total)
    store = fs_store(codec)

    store.set_stream("big", source)  # raises if the source is read in full

    # Bounded by the designed chunk size, not by the value size.
    assert source.max_single_read <= STREAM_CHUNK
    sink = io.BytesIO()
    assert store.get_to("big", sink) is True
    assert sink.getvalue() == b"x" * total


@pytest.mark.parametrize("codec", CODECS)
def test_get_to_writes_incrementally(codec: CompressionCodec | None) -> None:
    """The destination must receive several writes, not one big one.

    Uses incompressible data on purpose: a highly compressible payload fits in
    one compressed chunk, which legitimately decompresses to the whole value in
    a single write, so chunking would be unobservable.
    """
    total = 4 << 20
    payload = os.urandom(total)
    store = fs_store(codec)
    store.set_stream("big", io.BytesIO(payload))

    writes: list[int] = []

    class CountingSink(io.RawIOBase):
        def writable(self) -> bool:
            return True

        def write(self, data: object) -> int:
            chunk = bytes(data)  # type: ignore[arg-type]
            writes.append(len(chunk))
            return len(chunk)

    assert store.get_to("big", CountingSink()) is True
    assert sum(writes) == total
    assert len(writes) > 1, "value arrived in a single write — read is buffering"


def test_get_to_reports_missing_and_empty_distinctly() -> None:
    store = fs_store(None)
    store.set("empty", b"")

    sink = io.BytesIO()
    assert store.get_to("empty", sink) is True, "empty value must report found"
    assert sink.getvalue() == b""

    assert store.get_to("absent", io.BytesIO()) is False


def test_stream_accepts_paths_on_both_sides() -> None:
    store = fs_store("zstd")
    payload = b"path-based-" * 5000
    with tempfile.TemporaryDirectory() as tmp:
        src = Path(tmp) / "in.bin"
        dst = Path(tmp) / "out.bin"
        src.write_bytes(payload)

        store.set_stream("k", src)
        assert store.get_to("k", dst) is True
        assert dst.read_bytes() == payload


def test_non_byte_store_raises_rather_than_buffering() -> None:
    """The type rules this out; the runtime guard covers unchecked callers."""
    store = MemoryKvStore(
        KvSet.with_str_keys("docs", key_serde=SerDe.identity(str), value_serde=SerDe.json_bytes())
    )
    with pytest.raises(NotImplementedError, match="encode incrementally"):
        store.set_stream("k", io.BytesIO(b"{}"))  # type: ignore[arg-type]


# --- object backends ---------------------------------------------------------


def _byte_stores(codec: CompressionCodec | None) -> dict[str, object]:
    """One byte-valued store per object backend, sharing a kvset shape."""
    import boto3
    from moto import mock_aws

    from kvtsjl.backends.azure import AzureBlobKvStore
    from kvtsjl.backends.gcs import GcsKvStore
    from kvtsjl.backends.s3 import S3KvStore
    from tests.fake_azure import FakeAzureContainer
    from tests.fake_gcs import FakeGcsBucket

    ctx = mock_aws()
    ctx.start()
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="stream-bucket")
    return {
        "_moto": ctx,
        "s3": S3KvStore(byte_kvset("blobs", codec), client=client, bucket="stream-bucket"),
        "gcs": GcsKvStore(byte_kvset("blobs", codec), bucket=FakeGcsBucket()),
        "azure": AzureBlobKvStore(byte_kvset("blobs", codec), container=FakeAzureContainer()),
        "fs": fs_store(codec),
    }


@pytest.mark.parametrize("codec", CODECS)
@pytest.mark.parametrize("backend", ["s3", "gcs", "azure", "fs"])
def test_object_backends_round_trip_all_combinations(
    backend: str, codec: CompressionCodec | None
) -> None:
    stores = _byte_stores(codec)
    try:
        store = stores[backend]
        payload = b"object-backend-" * 8000

        store.set("a", payload)  # type: ignore[attr-defined]
        store.set_stream("b", io.BytesIO(payload))  # type: ignore[attr-defined]

        assert store.get("a") == payload  # type: ignore[attr-defined]
        assert store.get("b") == payload  # type: ignore[attr-defined]
        for key in ("a", "b"):
            sink = io.BytesIO()
            assert store.get_to(key, sink) is True  # type: ignore[attr-defined]
            assert sink.getvalue() == payload
    finally:
        stores["_moto"].stop()  # type: ignore[attr-defined]


@pytest.mark.parametrize("backend", ["s3", "gcs", "azure", "fs"])
def test_object_backends_never_read_the_source_whole(backend: str) -> None:
    stores = _byte_stores("zstd")
    try:
        store = stores[backend]
        total = 8 << 20
        source = RefusesFullRead(total)

        store.set_stream("big", source)  # type: ignore[attr-defined]

        assert source.max_single_read <= STREAM_CHUNK
        sink = io.BytesIO()
        assert store.get_to("big", sink) is True  # type: ignore[attr-defined]
        assert sink.getvalue() == b"x" * total
    finally:
        stores["_moto"].stop()  # type: ignore[attr-defined]


@pytest.mark.parametrize("backend", ["s3", "gcs", "azure", "fs"])
def test_stream_written_values_match_set_written_ones_for_stat_and_scan(
    backend: str,
) -> None:
    """A stream-written value must be indistinguishable to stat/exists/scan."""
    stores = _byte_stores(None)
    try:
        store = stores[backend]
        store.set("by_set", b"payload")  # type: ignore[attr-defined]
        store.set_stream("by_stream", io.BytesIO(b"payload"))  # type: ignore[attr-defined]

        assert store.exists("by_stream") is True  # type: ignore[attr-defined]
        assert store.stat("by_stream") is not None  # type: ignore[attr-defined]
        assert sorted(store.scan()) == ["by_set", "by_stream"]  # type: ignore[attr-defined]
        assert store.batch_exists(["by_set", "by_stream"]) == {  # type: ignore[attr-defined]
            "by_set",
            "by_stream",
        }
    finally:
        stores["_moto"].stop()  # type: ignore[attr-defined]


# --- compositions ------------------------------------------------------------


def test_mirror_streams_to_both_without_reading_the_source_whole() -> None:
    primary, secondary = fs_store("zstd"), fs_store("zstd")
    mirrored = primary.mirror(secondary)
    total = 8 << 20
    source = RefusesFullRead(total)

    mirrored.set_stream("big", source)

    assert source.max_single_read <= STREAM_CHUNK
    for store in (primary, secondary):
        sink = io.BytesIO()
        assert store.get_to("big", sink) is True
        assert sink.getvalue() == b"x" * total


def test_mirror_get_to_reads_the_primary_only() -> None:
    primary, secondary = fs_store(None), fs_store(None)
    mirrored = primary.mirror(secondary)
    secondary.set("only_secondary", b"should not be served")

    assert mirrored.get_to("only_secondary", io.BytesIO()) is False


def test_fallback_get_to_promotes_and_keeps_the_write_clock() -> None:
    """A promoted blob must keep its remaining life, not restart it (F21)."""
    from datetime import UTC, datetime, timedelta

    from freezegun import freeze_time

    from kvtsjl import TtlPolicy

    def ttl_store() -> FilesystemKvStore[str, bytes, str]:
        kvset = KvSet.with_str_keys(
            "blobs",
            key_serde=SerDe.identity(str),
            value_serde=SerDe.identity(bytes),
            ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=600)),
        )
        return FilesystemKvStore(kvset, root=tempfile.mkdtemp(), ttl_mode="sidecar")

    created = datetime(2024, 1, 1, 11, 55, 0, tzinfo=UTC)
    with freeze_time("2024-01-01 12:00:00"):
        primary, secondary = ttl_store(), ttl_store()
        secondary.set_stream("k", io.BytesIO(b"payload"), created_at=created)

        coalesced = primary.coalesce(secondary, promote=True)
        sink = io.BytesIO()
        assert coalesced.get_to("k", sink) is True
        assert sink.getvalue() == b"payload"

        promoted = primary.stat("k")
        original = secondary.stat("k")
        assert promoted is not None and original is not None
        assert promoted.expires_at is not None
        assert original.expires_at is not None
        drift = abs((promoted.expires_at - original.expires_at).total_seconds())
        assert drift <= 1.0, "promote restarted the TTL instead of carrying it"


def test_fallback_get_to_serves_without_promoting_when_disabled() -> None:
    primary, secondary = fs_store(None), fs_store(None)
    secondary.set("k", b"from-secondary")
    coalesced = primary.coalesce(secondary, promote=False)

    sink = io.BytesIO()
    assert coalesced.get_to("k", sink) is True
    assert sink.getvalue() == b"from-secondary"
    assert primary.exists("k") is False


# --- streaming is a serde capability, not a value type -----------------------
#
# These are the tests the StreamingSerDe generalization exists for: the value
# type is ``str``, so nothing here could work while streaming meant "bytes".


def text_kvset(name: str, codec: CompressionCodec | None) -> KvSet[str, str, str, bytes]:
    kvset = KvSet.with_str_keys(name, key_serde=SerDe.identity(str), value_serde=SerDe.utf8_bytes())
    return kvset.with_compressed_value(codec) if codec else kvset


def text_store(codec: CompressionCodec | None) -> FilesystemKvStore[str, str, str]:
    return FilesystemKvStore(text_kvset("docs", codec), root=tempfile.mkdtemp())


@pytest.mark.parametrize("codec", CODECS)
def test_text_store_streams_round_trip(codec: CompressionCodec | None) -> None:
    """A ``str`` store streams because UTF-8 encodes incrementally."""
    store = text_store(codec)
    # Multi-byte characters, so chunk boundaries can land mid-character.
    text = "héllo wörld — ünicode ✓\n" * 5000
    store.set_stream("doc", io.StringIO(text))

    assert store.get("doc") == text
    out = io.StringIO()
    assert store.get_to("doc", out) is True
    assert out.getvalue() == text


@pytest.mark.parametrize("codec", CODECS)
def test_text_stream_interoperates_with_set_and_get(
    codec: CompressionCodec | None,
) -> None:
    """``set``-written text reads back through ``get_to`` and vice versa."""
    store = text_store(codec)
    text = "ünicode ✓ " * 3000
    store.set("whole", text)

    out = io.StringIO()
    assert store.get_to("whole", out) is True
    assert out.getvalue() == text


def test_text_stream_splits_multibyte_characters_across_chunks() -> None:
    """The incremental codec must hold a partial character across a boundary.

    A non-incremental ``encode``/``decode`` per chunk raises
    ``UnicodeDecodeError`` here, so this fails loudly if the serde ever stops
    carrying state between chunks.
    """
    store = text_store(None)
    # A 3-byte character straddling the chunk boundary on the decode side.
    text = "✓" * ((STREAM_CHUNK // 2) + 1)
    store.set_stream("doc", iter([text[:7], text[7:]]))

    out = io.StringIO()
    assert store.get_to("doc", out) is True
    assert out.getvalue() == text


def test_text_store_refuses_when_the_serde_cannot_chunk() -> None:
    """``str`` alone does not make a store streamable — the serde decides."""
    store = MemoryKvStore(
        KvSet.with_str_keys(
            "weird",
            key_serde=SerDe.identity(str),
            # A str value whose serde is not incremental.
            value_serde=SerDe[str, bytes](
                serializer=lambda s: s[::-1].encode("utf-8"),
                deserializer=lambda b: b.decode("utf-8")[::-1],
                blob_type=bytes,
            ),
        )
    )
    with pytest.raises(NotImplementedError, match="encode incrementally"):
        store.set_stream("k", io.StringIO("abc"))


def test_streaming_composes_through_the_serde_chain() -> None:
    """utf8 >> compress streams: both halves are incremental, so the chain is."""
    from kvtsjl.serde import ChainedStreamingSerDe, StreamingSerDe

    chained = text_kvset("docs", "zstd").value_serde
    assert isinstance(chained, StreamingSerDe)
    assert isinstance(chained, ChainedStreamingSerDe)

    text = "ünicode ✓ " * 1000
    wire = b"".join(chained.encode_stream(iter([text[:50], text[50:]])))
    assert wire != text.encode("utf-8")  # it really compressed
    assert "".join(chained.decode_stream(iter([wire]))) == text


def test_streaming_does_not_compose_through_a_whole_value_serde() -> None:
    """json >> compress cannot stream: json has to see the whole value."""
    from kvtsjl.serde import StreamingSerDe

    kvset = KvSet.with_str_keys(
        "rows", key_serde=SerDe.identity(str), value_serde=SerDe.json_bytes()
    ).with_compressed_value("zstd")
    assert not isinstance(kvset.value_serde, StreamingSerDe)


@pytest.mark.parametrize(
    ("codec", "expected"),
    [(None, 4096), ("zstd", None), ("gzip", None)],
)
def test_size_hint_is_withheld_when_the_serde_changes_the_length(
    codec: CompressionCodec | None, expected: int | None
) -> None:
    """The driver sizes multipart uploads from this; a compressed body is shorter.

    Forwarding the caller's ``size`` through a codec would hand the transport a
    length the body does not have. Only a serde that preserves length answers.
    """
    store = fs_store(codec)
    seen: list[int | None] = []
    driver = store._driver  # pyright: ignore[reportPrivateUsage]
    original = driver.write_stream

    def spy(key: str, src: object, *, size: int | None = None, **kw: object) -> None:
        seen.append(size)
        return original(key, src, size=size, **kw)  # pyright: ignore

    driver.write_stream = spy  # pyright: ignore[reportAttributeAccessIssue]
    store.set_stream("k", io.BytesIO(os.urandom(4096)), size=4096)

    assert seen == [expected]


# --- streaming across store composition --------------------------------------
#
# A composition can stream exactly when it does not need to see the whole
# value — the same rule as the serde. Pass-throughs (scoped, readonly reads,
# mirror, fallback) stream; anything that transforms or assembles a value
# (map/imap, zip) must refuse rather than buffer behind the caller's back.


def test_readonly_keeps_streaming_reads_and_refuses_streaming_writes() -> None:
    """``readonly()`` must not remove a *read* capability."""
    from kvtsjl.exceptions import KvStoreReadOnlyError

    store = fs_store(None)
    payload = os.urandom(CHUNK * 3)
    store.set_stream("k", io.BytesIO(payload))

    view = store.readonly()
    out = io.BytesIO()
    assert view.get_to("k", out) is True
    assert out.getvalue() == payload

    with pytest.raises(KvStoreReadOnlyError):
        view.set_stream("k", io.BytesIO(b"nope"))


def test_scoped_streams_through_to_the_underlying_store() -> None:
    """A scope is a key prefix; it does not touch the value, so it streams."""
    store = fs_store("zstd")
    scoped = store.scoped(store.scope)
    payload = os.urandom(CHUNK * 3)
    scoped.set_stream("k", RefusesFullRead(len(payload)))

    out = io.BytesIO()
    assert scoped.get_to("k", out) is True
    assert len(out.getvalue()) == len(payload)


def test_value_transforming_compositions_refuse_rather_than_buffer() -> None:
    """``imap`` and ``zip`` must see the whole value, so they must not pretend.

    Buffering here is exactly what a caller reaches for ``set_stream`` to
    avoid, so a silent buffer would defeat the call rather than serve it.
    """
    from kvtsjl.store.logical import KvStore

    a, b = fs_store(None), fs_store(None)
    for composed in (a.imap(lambda v: v, lambda v: v), KvStore.zip(a, b)):
        with pytest.raises(NotImplementedError):
            composed.set_stream("k", io.BytesIO(b"x"))
        with pytest.raises(NotImplementedError):
            composed.get_to("k", io.BytesIO())


def test_text_streams_through_mirror_and_fallback() -> None:
    """The pull-side primitive is what makes non-bytes chunks compose.

    These two bridged a push-side ``get_to`` into a pull-side ``set_stream``
    through a binary ``SpooledTemporaryFile``, which silently restricted them
    to ``bytes`` — a text mirror lost its secondary under BEST_EFFORT. Handing
    ``read_stream`` straight to ``set_stream`` removes the bridge entirely.
    """
    a, b = text_store(None), text_store(None)
    text = "héllo wörld ✓ " * 2000

    a.mirror(b).set_stream("m", io.StringIO(text))
    assert a.get("m") == text
    assert b.get("m") == text, "secondary silently lost the value"

    c, d = text_store(None), text_store(None)
    d.set("f", text)
    out = io.StringIO()
    assert c.fallback_read(d, promote=True).get_to("f", out) is True
    assert out.getvalue() == text
    assert c.get("f") == text, "promote did not reach the primary"


def test_fallback_promote_keeps_serving_when_the_primary_write_fails() -> None:
    """A failed promote is BEST_EFFORT: the caller still gets the value.

    The promote consumes the secondary's chunk iterator, so recovering means
    re-reading the secondary rather than replaying a spent stream.
    """
    primary, secondary = fs_store(None), fs_store(None)
    payload = os.urandom(CHUNK * 3)
    secondary.set("k", payload)

    def boom(*_args: object, **_kw: object) -> None:
        raise OSError("primary write refused")

    primary.set_stream = boom  # pyright: ignore[reportAttributeAccessIssue]
    out = io.BytesIO()
    assert primary.fallback_read(secondary, promote=True).get_to("k", out) is True
    assert out.getvalue() == payload


# --- chunk type: streaming a value whose chunks are not values ---------------
#
# The serde carries the chunk type, so a store can stream records without the
# store, the codecs, the drivers or the compositions knowing what a record is.


@dataclass(frozen=True)
class Row:
    id: int
    name: str


class JsonLinesSerDe(StreamingSerDe[list[Row], Row, bytes]):
    """whole = ``list[Row]``, chunk = ``Row``, wire = ``bytes``."""

    @property
    def chunk_type(self) -> type[Row]:
        return Row

    def encode_stream(self, chunks: Iterable[Row]) -> Iterator[bytes]:
        for row in chunks:
            yield (json.dumps({"id": row.id, "name": row.name}) + "\n").encode()

    def decode_stream(self, chunks: Iterable[bytes]) -> Iterator[Row]:
        tail = b""
        for blob in chunks:
            tail += blob
            *lines, tail = tail.split(b"\n")
            for line in lines:
                yield Row(**json.loads(line))
        if tail.strip():
            yield Row(**json.loads(tail))


def _jsonl() -> JsonLinesSerDe:
    return JsonLinesSerDe(
        serializer=lambda rows: b"".join(
            (json.dumps({"id": r.id, "name": r.name}) + "\n").encode() for r in rows
        ),
        deserializer=lambda b: [
            Row(**json.loads(x)) for x in b.splitlines() if x.strip()
        ],
        blob_type=bytes,
    )


def row_store(codec: CompressionCodec | None) -> FilesystemKvStore[str, list[Row], str]:
    kvset = KvSet.with_str_keys(
        "log", key_serde=SerDe.identity(str), value_serde=_jsonl()
    )
    if codec:
        kvset = kvset.with_compressed_value(codec)
    return FilesystemKvStore(kvset, root=tempfile.mkdtemp())


ROWS = [Row(i, f"row-{i}") for i in range(5000)]


@pytest.mark.parametrize("codec", CODECS)
def test_record_chunks_stream_end_to_end(codec: CompressionCodec | None) -> None:
    """Chunks are ``Row``; nothing between the serde and the disk knows that."""
    store = row_store(codec)
    view = store.streaming(Row)
    view.set_stream("today", iter(ROWS))

    assert list(view.read_stream("today") or ()) == ROWS
    # the whole-value path must agree with the streamed one
    assert store.get("today") == ROWS


def test_record_chunks_stream_through_compositions() -> None:
    """``mirror`` and ``fallback_read`` move chunks without inspecting them."""
    a, b = row_store("zstd"), row_store("zstd")
    a.mirror(b).streaming(Row).set_stream("m", iter(ROWS[:50]))
    assert b.get("m") == ROWS[:50], "secondary did not receive the records"

    c, d = row_store("zstd"), row_store("zstd")
    d.set("f", ROWS[:20])
    promoted = c.fallback_read(d, promote=True).streaming(Row)
    assert list(promoted.read_stream("f") or ()) == ROWS[:20]
    assert c.get("f") == ROWS[:20], "promote did not reach the primary"


def test_declaring_the_wrong_chunk_type_raises_rather_than_lying() -> None:
    """The view's type is only honest because the declaration is checked."""
    store = row_store(None)
    with pytest.raises(TypeError, match="streams Row chunks, not str"):
        store.streaming(str)

    blobs = fs_store(None)
    with pytest.raises(TypeError, match="streams bytes chunks, not str"):
        blobs.streaming(str)


def test_readonly_hands_out_a_view_that_cannot_write() -> None:
    """``readonly()`` must not produce a view advertising ``set_stream``.

    Checked at runtime here; pyright reports it as an attribute error on
    ``StreamingReadView``, which is the point of the split.
    """
    store = row_store(None)
    store.streaming(Row).set_stream("k", iter(ROWS[:3]))

    view = store.readonly().streaming(Row)
    assert isinstance(view, StreamingReadView)
    assert not isinstance(view, StreamingView)
    assert not hasattr(view, "set_stream")
    assert list(view.read_stream("k") or ()) == ROWS[:3]


def test_streaming_view_refused_when_the_serde_cannot_chunk() -> None:
    """No view at all for a serde that must see the whole value."""
    store = MemoryKvStore(
        KvSet.with_str_keys(
            "docs", key_serde=SerDe.identity(str), value_serde=SerDe.json_bytes()
        )
    )
    with pytest.raises(NotImplementedError, match="encode incrementally"):
        store.streaming(dict)


# --- chunk_type propagation --------------------------------------------------


@pytest.mark.parametrize("codec", ["gzip", "zlib", "zstd", "lz4"])
def test_chunk_type_survives_wrapping_and_chaining(codec: CompressionCodec) -> None:
    """Compression and chaining must not change what a chunk is.

    ``CompressedSerDe`` reports its inner's chunk type and a chain reports its
    input end's. Getting this wrong would make ``streaming()`` reject a correct
    declaration, or accept a wrong one.
    """
    assert SerDe.identity(bytes).chunk_type is bytes
    assert SerDe.utf8_bytes().chunk_type is str
    assert _jsonl().chunk_type is Row

    # compression wraps without changing the chunk
    assert byte_kvset("b", codec).value_serde.chunk_type is bytes
    assert text_kvset("t", codec).value_serde.chunk_type is str
    assert row_store(codec).kvset.value_serde.chunk_type is Row

    # a compression wrapper over a non-bytes inner: the shipped factories only
    # ever wrap identity(bytes), so without this the propagation is invisible
    wrapped = CompressedSerDe[str, str](
        serializer=lambda v: v.encode(),
        deserializer=lambda b: b.decode(),
        blob_type=bytes,
        codec=codec,
        inner=SerDe.utf8_bytes(),
    )
    assert wrapped.chunk_type is str

    # a chain reports the type it is fed, not the type it emits
    chained = text_kvset("t", codec).value_serde
    assert isinstance(chained, ChainedStreamingSerDe)
    assert chained.chunk_type is str


def test_a_record_serde_can_start_a_chain_but_not_continue_one() -> None:
    """Once records have become bytes they are bytes — enforced, not documented."""
    chained = row_store("zstd").kvset.value_serde
    assert isinstance(chained, StreamingSerDe)
    assert chained.chunk_type is Row


# --- the round-trip law ------------------------------------------------------
#
# The streaming path and the whole-value path must agree, in both directions.
# This is the property the earlier tests only sampled: a UTF-8 chunk boundary
# landing mid-character was one instance of it, found by hand.


def _split_bytes(value: Any, rng: random.Random) -> list[Any]:
    """Random slices — a chunk of bytes/str is a shorter bytes/str."""
    out, i = [], 0
    while i < len(value):
        step = rng.randint(1, max(1, len(value) // 7))
        out.append(value[i : i + step])
        i += step
    return out


def _law_cases() -> list[tuple[str, Any, Any, Any, Any]]:
    """(name, serde, whole value, split, combine) for every streaming serde.

    ``split``/``combine`` differ per case because the chunk type does: slicing
    a ``list[Row]`` gives sublists, but its chunks are single ``Row``s. That is
    the whole point of ``chunk_type`` being separate from the value type.
    """
    blob = os.urandom(200_000)
    text = "héllo wörld — ünicode ✓\n" * 4000
    join_bytes = lambda ps: b"".join(ps)  # noqa: E731
    join_str = lambda ps: "".join(ps)  # noqa: E731
    as_list = lambda ps: list(ps)  # noqa: E731
    each_row = lambda v, _rng: list(v)  # noqa: E731

    cases: list[tuple[str, Any, Any, Any, Any]] = []
    for codec in CODECS:
        cases.append(
            (f"bytes/{codec}", byte_kvset("b", codec).value_serde, blob,
             _split_bytes, join_bytes)
        )
        cases.append(
            (f"str/{codec}", text_kvset("t", codec).value_serde, text,
             _split_bytes, join_str)
        )
        cases.append(
            (f"rows/{codec}", row_store(codec).kvset.value_serde, ROWS,
             each_row, as_list)
        )
    return cases


@pytest.mark.parametrize(
    "name,serde,value,split,combine", _law_cases(), ids=lambda v: v if isinstance(v, str) else ""
)
def test_streaming_and_whole_value_paths_agree(
    name: str, serde: Any, value: Any, split: Any, combine: Any
) -> None:
    """``combine(decode(encode(split(v)))) == v``, at randomized boundaries.

    Plus the two cross-path laws the store depends on: a stream-written value
    must be readable by ``get``, and a ``set``-written one by ``read_stream``.
    """
    rng = random.Random(name)
    chunks = split(value, rng)
    assert combine(chunks) == value, "the splitter itself is wrong"

    # streaming round-trip
    wire = b"".join(serde.encode_stream(iter(chunks)))
    assert combine(serde.decode_stream(iter([wire]))) == value

    # stream-written, whole-read  (set_stream -> get)
    assert serde.deserialize(wire) == value

    # whole-written, stream-read  (set -> read_stream)
    assert combine(serde.decode_stream(iter([serde.serialize(value)]))) == value

    # the decoder must not depend on the writer's chunk boundaries
    rechunked = _split_bytes(wire, random.Random(name + "re"))
    assert combine(serde.decode_stream(iter(rechunked))) == value


def test_truncated_text_raises_rather_than_silently_dropping_bytes() -> None:
    """A stream cut mid-character is corruption, not a short value.

    The incremental decoder holds a partial sequence back until it completes;
    without the final ``decode(b"", final=True)`` those bytes are dropped and
    the caller gets a silently shorter value. Truncation must surface.
    """
    serde = SerDe.utf8_bytes()
    truncated = "héllo ✓".encode()[:-1]  # last character cut mid-sequence

    with pytest.raises(UnicodeDecodeError):
        list(serde.decode_stream(iter([truncated])))

    # and the same across a chunk boundary, where the carry-over lives
    with pytest.raises(UnicodeDecodeError):
        list(serde.decode_stream(iter([truncated[:3], truncated[3:]])))


# --- a source that fails halfway --------------------------------------------


class FailsMidStream(io.RawIOBase):
    """A source that raises after a few chunks, mid-value."""

    def __init__(self, total: int, fail_after: int = 2, chunk: int = CHUNK) -> None:
        self._remaining = total
        self._chunk = chunk
        self._reads = 0
        self._fail_after = fail_after

    def readable(self) -> bool:
        return True

    def read(self, size: int | None = -1) -> bytes:
        self._reads += 1
        if self._reads > self._fail_after:
            raise OSError("source failed midway")
        want = self._chunk if size is None or size < 0 else min(size, self._chunk)
        take = min(want, self._remaining)
        self._remaining -= take
        return b"x" * take


@pytest.mark.parametrize("backend", ["s3", "gcs", "azure", "fs"])
def test_a_failed_stream_leaves_no_partial_value(backend: str) -> None:
    """``set_stream`` is all-or-nothing, like ``set``.

    Mirror, promote and the best-effort fan-out all assume a failed write
    leaves the previous value intact rather than a truncated one. The drivers
    are built for it — multipart abort, resumable session, uncommitted blocks,
    temp file plus ``os.replace`` — but nothing pinned the contract.
    """
    stores = _byte_stores(None)
    try:
        store = cast(Any, stores[backend])
        store.set("existing", b"previous value")

        with pytest.raises(OSError, match="source failed midway"):
            store.set_stream("existing", FailsMidStream(CHUNK * 20))
        assert store.get("existing") == b"previous value", "left a partial value"

        with pytest.raises(OSError, match="source failed midway"):
            store.set_stream("fresh", FailsMidStream(CHUNK * 20))
        assert store.get("fresh") is None, "created a key from a failed write"
    finally:
        cast(Any, stores["_moto"]).stop()


@pytest.mark.parametrize("backend", ["s3", "gcs", "azure", "fs"])
def test_a_failed_mirror_write_does_not_touch_the_secondary(backend: str) -> None:
    """A primary that fails must not leave the secondary holding a value."""
    stores = _byte_stores(None)
    try:
        primary = cast(Any, stores[backend])
        secondary = fs_store(None)
        with pytest.raises(UncommittedWriteError) as caught:
            primary.mirror(secondary).set_stream("k", FailsMidStream(CHUNK * 20))
        assert caught.value.committed is False
        assert "source failed midway" in str(caught.value.failures["primary"])
        assert primary.get("k") is None
        assert secondary.get("k") is None, "secondary got a value the primary rejected"
    finally:
        cast(Any, stores["_moto"]).stop()


# --- the exact-length contract ----------------------------------------------
#
# GCS takes ``size=`` and Azure ``length=`` as exact byte counts, not hints:
# too small truncates the object, too large fails the upload. The fakes
# enforce that, so these tests fail rather than silently losing data.


@pytest.mark.parametrize("backend", ["gcs", "azure"])
def test_an_exact_size_reaches_media_that_require_one(backend: str) -> None:
    stores = _byte_stores(None)
    try:
        store = cast(Any, stores[backend])
        payload = os.urandom(CHUNK * 3)
        store.set_stream("ok", io.BytesIO(payload), size=len(payload))
        assert store.get("ok") == payload

        with pytest.raises(ValueError, match="but stream had"):
            store.set_stream("bad", io.BytesIO(payload), size=len(payload) + 1)
    finally:
        cast(Any, stores["_moto"]).stop()


@pytest.mark.parametrize("backend", ["gcs", "azure"])
@pytest.mark.parametrize("codec", ["zstd", "gzip"])
def test_compression_withholds_the_size_instead_of_truncating(
    backend: str, codec: CompressionCodec
) -> None:
    """The caller's ``size`` describes the value, not the compressed wire.

    Forwarding it would hand the medium a length the body does not have. This
    is what ``encoded_size`` returning ``None`` buys, measured end to end.
    """
    stores = _byte_stores(codec)
    try:
        store = cast(Any, stores[backend])
        payload = os.urandom(CHUNK * 3)
        store.set_stream("k", io.BytesIO(payload), size=len(payload))
        assert store.get("k") == payload
    finally:
        cast(Any, stores["_moto"]).stop()


def test_abandoning_a_read_stream_releases_the_body() -> None:
    """A caller that stops early must not pin the medium's response open.

    ``read_stream`` returns an ``Iterator``, so the driver's generator is what
    carries the ``finally``; closing it is what releases the HTTP body rather
    than waiting for a collection that may never come.
    """
    closed: list[str] = []

    class TrackingBody:
        def read(self, *_a: object) -> bytes:
            return b""

        def iter_chunks(self, chunk_size: int = 0) -> Iterator[bytes]:
            yield b"first"
            yield b"second"

        def close(self) -> None:
            closed.append("body")

    from kvtsjl.backends.object.s3_driver import S3BlobDriver

    driver = object.__new__(S3BlobDriver)
    driver._client = cast(Any, _StubS3Client(TrackingBody()))  # pyright: ignore[reportPrivateUsage]
    driver._bucket = "b"  # pyright: ignore[reportPrivateUsage]

    found = driver.read_chunks("k")
    assert found is not None
    chunks, _ = found
    assert next(chunks) == b"first"      # stop early, mid-body
    assert closed == [], "closed before the caller let go"

    close_stream(chunks)
    assert closed == ["body"], "abandoned stream left the body open"


class _StubS3Client:
    def __init__(self, body: object) -> None:
        self._body = body

    def get_object(self, **_kw: object) -> dict[str, object]:
        return {"Body": self._body, "ContentLength": 11}
