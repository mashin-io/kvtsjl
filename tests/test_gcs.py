"""GcsKvStore tests against an in-memory fake bucket."""

from __future__ import annotations

from datetime import timedelta

import pytest
from freezegun import freeze_time

from kvtsjl import KeyLayout, KvSet, SerDe, TtlPolicy
from kvtsjl.backends.gcs import GcsKvStore, GcsTtlMode
from kvtsjl.exceptions import KvStoreScanUnsupported
from tests.conformance import assert_basic_crud, assert_batch_ops, assert_scan_and_scope
from tests.fake_gcs import FakeGcsBucket


@pytest.mark.gcs
def test_gcs_crud(gcs_store: GcsKvStore[str, str, str]) -> None:
    assert_basic_crud(gcs_store)


@pytest.mark.gcs
def test_gcs_batch(gcs_store: GcsKvStore[str, str, str]) -> None:
    assert_batch_ops(gcs_store)


@pytest.mark.gcs
def test_gcs_scan_and_scope(gcs_store: GcsKvStore[str, str, str]) -> None:
    assert_scan_and_scope(gcs_store)


@pytest.mark.gcs
def test_gcs_ttl_object_time_default(fake_gcs_bucket: FakeGcsBucket) -> None:
    kvset = KvSet.with_str_keys(
        "ttl",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=30)),
    )
    with freeze_time("2024-01-01 12:00:00") as frozen:
        store = GcsKvStore(kvset, bucket=fake_gcs_bucket)
        store.set("a", "live")
        assert store.get("a") == "live"
        blob = next(fake_gcs_bucket.list_blobs())
        assert blob.custom_time is None
        assert blob.updated is not None
        frozen.move_to("2024-01-01 12:01:00")
        assert store.get("a") is None
        assert list(fake_gcs_bucket.list_blobs()) == []


@pytest.mark.gcs
def test_gcs_ttl_via_custom_time(fake_gcs_bucket: FakeGcsBucket) -> None:
    kvset = KvSet.with_str_keys(
        "ttl",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=30)),
    )
    with freeze_time("2024-01-01 12:00:00") as frozen:
        store = GcsKvStore(
            kvset,
            bucket=fake_gcs_bucket,
            ttl_mode=GcsTtlMode.CUSTOM_TIME,
        )
        store.set("a", "live")
        assert store.get("a") == "live"
        blob = next(fake_gcs_bucket.list_blobs())
        assert blob.custom_time is not None
        frozen.move_to("2024-01-01 12:01:00")
        assert store.get("a") is None
        assert list(fake_gcs_bucket.list_blobs()) == []


@pytest.mark.gcs
def test_gcs_hashed_scan_unsupported(fake_gcs_bucket: FakeGcsBucket) -> None:
    kvset = KvSet.with_str_keys(
        "hashed",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        key_layout=KeyLayout.HASHED,
    )
    store = GcsKvStore(kvset, bucket=fake_gcs_bucket)
    store.set("k", "v")
    assert store.get("k") == "v"
    with pytest.raises(KvStoreScanUnsupported):
        list(store.scan())
