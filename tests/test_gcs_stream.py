"""GCS ``set_stream`` with a source that cannot seek.

The store hands the driver a stream it encodes as it goes, which cannot seek.
google-cloud-storage seeks on its resumable upload — taken for an unknown size
(every compressed value) or one above 8 MiB — so those uploads failed before a
byte was sent. Each test runs on the in-memory fake, which enforces the
client's rule, and, with ``KVTSJL_FAKE_GCS`` set, on the real client against
fake-gcs-server.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
import os
from typing import Any

import pytest

from kvtsjl import KvSet, SerDe, TtlPolicy
from kvtsjl.backends.gcs import GcsKvStore, GcsTtlMode
from tests.fake_gcs import FakeGcsBucket

MIB = 1024 * 1024
TTL = TtlPolicy(ttl_duration=timedelta(hours=1))


@pytest.fixture(params=["fake", "emulator"])
def bucket(request: pytest.FixtureRequest) -> Any:
    if request.param == "fake":
        return FakeGcsBucket()
    if "KVTSJL_FAKE_GCS" not in os.environ:
        pytest.skip("set KVTSJL_FAKE_GCS to a fake-gcs-server endpoint")
    return request.getfixturevalue("gcs_emulator_bucket")


def _store(bucket: Any, *, compressed: bool, ttl: TtlPolicy | None = None) -> Any:
    kvset = KvSet.with_str_keys(
        "blobs", key_serde=SerDe.identity(str), value_serde=SerDe.identity(bytes), ttl_policy=ttl
    )
    if compressed:
        kvset = kvset.with_compressed_value("zstd")
    mode = GcsTtlMode.CUSTOM_TIME if ttl is not None else GcsTtlMode.OBJECT_TIME
    return GcsKvStore(kvset, bucket=bucket, key_prefix="p/", ttl_mode=mode)


def _chunks(payload: bytes, step: int = 64 * 1024) -> Iterator[bytes]:
    """A source that can only be read forward."""
    for start in range(0, len(payload), step):
        yield payload[start : start + step]


def _read(store: Any, key: str) -> bytes:
    chunks = store.read_stream(key)
    assert chunks is not None
    return b"".join(chunks)


@pytest.mark.parametrize("size", [8, 9 * MIB], ids=["small", "above-8MiB"])
def test_a_compressed_value_streams_in_whatever_its_size(bucket: Any, size: int) -> None:
    store = _store(bucket, compressed=True)
    payload = os.urandom(size)

    store.set_stream("k", _chunks(payload), size=size)

    assert store.get("k") == payload
    assert _read(store, "k") == payload


def test_an_uncompressed_value_above_8_mib_streams(bucket: Any) -> None:
    store = _store(bucket, compressed=False)
    payload = os.urandom(9 * MIB)
    store.set_stream("k", _chunks(payload), size=len(payload))
    assert store.get("k") == payload


def test_a_small_value_of_known_size_still_takes_one_request() -> None:
    """The one-request upload only reads, so it is kept where it applies."""
    bucket = FakeGcsBucket()
    opened: list[str] = []
    blob = bucket.blob

    def spying(name: str) -> Any:
        made = blob(name)
        open_ = made.open

        def watched(mode: str = "rb") -> Any:
            opened.append(mode)
            return open_(mode)

        made.open = watched  # type: ignore[method-assign]
        return made

    bucket.blob = spying  # type: ignore[method-assign]
    store = _store(bucket, compressed=False)
    store.set_stream("k", _chunks(b"a,b\n1,2\n"), size=8)
    assert opened == [], "a small value of known size went through a resumable session"
    assert store.get("k") == b"a,b\n1,2\n"


@pytest.mark.parametrize("compressed", [True, False], ids=["compressed", "above-8MiB"])
def test_a_streamed_value_keeps_its_custom_time(bucket: Any, compressed: bool) -> None:
    """The TTL clock rides on the writer's session, so expiry still works."""
    store = _store(bucket, compressed=compressed, ttl=TTL)
    payload = os.urandom(8 if compressed else 9 * MIB)
    clock = datetime.now(UTC) - timedelta(minutes=30)

    store.set_stream("k", _chunks(payload), size=len(payload), created_at=clock)

    stat = store.stat("k")
    assert stat is not None and stat.expires_at is not None
    assert abs((stat.expires_at - (clock + timedelta(hours=1))).total_seconds()) < 1


def test_a_size_the_stream_does_not_have_stores_nothing(bucket: Any) -> None:
    store = _store(bucket, compressed=False)
    store.set("k", b"previous value")
    payload = os.urandom(9 * MIB)

    with pytest.raises(ValueError, match="but stream had"):
        store.set_stream("k", _chunks(payload), size=len(payload) + 1)
    assert store.get("k") == b"previous value"
