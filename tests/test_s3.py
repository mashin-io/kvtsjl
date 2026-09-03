"""S3KvStore tests against moto (in-process S3)."""

from __future__ import annotations

from datetime import timedelta

from freezegun import freeze_time
import pytest

from kvtsjl import ExpiryGc, KeyLayout, KvSet, SerDe, TtlPolicy
from kvtsjl.backends.s3 import S3KvStore, S3TtlMode
from kvtsjl.exceptions import KvStoreScanUnsupported, KvStoreTtlUnsupported
from tests.conformance import assert_basic_crud, assert_batch_ops, assert_scan_and_scope


@pytest.mark.s3
def test_s3_crud(s3_store: S3KvStore[str, str, str]) -> None:
    assert_basic_crud(s3_store)


@pytest.mark.s3
def test_s3_batch(s3_store: S3KvStore[str, str, str]) -> None:
    assert_batch_ops(s3_store)


@pytest.mark.s3
def test_s3_scan_and_scope(s3_store: S3KvStore[str, str, str]) -> None:
    assert_scan_and_scope(s3_store)


@pytest.mark.s3
def test_s3_ttl_via_last_modified(s3_client: object, s3_bucket: str) -> None:
    kvset = KvSet.with_str_keys(
        "ttl",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=30)),
    )
    with freeze_time("2024-01-01 12:00:00") as frozen:
        store = S3KvStore(
            kvset,
            client=s3_client,  # type: ignore[arg-type]
            bucket=s3_bucket,
        )
        store.set("a", "live")
        assert store.get("a") == "live"
        frozen.move_to("2024-01-01 12:01:00")
        assert store.get("a") is None


@pytest.mark.s3
def test_s3_explicit_ttl_raises_by_default(s3_client: object, s3_bucket: str) -> None:
    kvset = KvSet.with_str_keys(
        "ttl",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        ttl_policy=TtlPolicy.hourly(),
    )
    store = S3KvStore(
        kvset,
        client=s3_client,  # type: ignore[arg-type]
        bucket=s3_bucket,
    )
    with pytest.raises(KvStoreTtlUnsupported):
        store.set("a", "v", ttl=TtlPolicy.hourly())


@pytest.mark.s3
def test_s3_expires_mode_ttl_and_none(s3_client: object, s3_bucket: str) -> None:
    kvset = KvSet.with_str_keys(
        "ttl",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        ttl_policy=TtlPolicy.hourly(),
    )
    with freeze_time("2024-01-01 12:00:00") as frozen:
        store = S3KvStore(
            kvset,
            client=s3_client,  # type: ignore[arg-type]
            bucket=s3_bucket,
            ttl_mode=S3TtlMode.EXPIRES,
        )
        store.set("short", "b", ttl=TtlPolicy(ttl_duration=timedelta(seconds=30)))
        store.set("pinned", "c", ttl=TtlPolicy.none())
        frozen.move_to("2024-01-01 12:01:00")
        assert store.get("short") is None
        assert store.get("pinned") == "c"


@pytest.mark.s3
def test_s3_expiry_gc_hide(s3_client: object, s3_bucket: str) -> None:
    kvset = KvSet.with_str_keys(
        "ttl",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=30)),
    )
    with freeze_time("2024-01-01 12:00:00") as frozen:
        store = S3KvStore(
            kvset,
            client=s3_client,  # type: ignore[arg-type]
            bucket=s3_bucket,
            expiry_gc=ExpiryGc.HIDE,
        )
        store.set("a", "live")
        object_key = store._object_key(store._physical_key_blob("a"))
        frozen.move_to("2024-01-01 12:01:00")
        assert store.get("a") is None
        s3_client.head_object(Bucket=s3_bucket, Key=object_key)  # type: ignore[attr-defined]


@pytest.mark.s3
def test_s3_hashed_scan_unsupported(s3_client: object, s3_bucket: str) -> None:
    kvset = KvSet.with_str_keys(
        "hashed",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        key_layout=KeyLayout.HASHED,
    )
    store = S3KvStore(
        kvset,
        client=s3_client,  # type: ignore[arg-type]
        bucket=s3_bucket,
    )
    store.set("k", "v")
    assert store.get("k") == "v"
    with pytest.raises(KvStoreScanUnsupported):
        list(store.scan())
