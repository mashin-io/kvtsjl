"""AzureBlobKvStore tests against an in-memory fake container."""

from __future__ import annotations

from datetime import timedelta

from freezegun import freeze_time
import pytest

from kvtsjl import ExpiryGc, KeyLayout, KvSet, SerDe, TtlPolicy
from kvtsjl.backends.azure import AzureBlobKvStore, AzureTtlMode
from kvtsjl.exceptions import KvStoreScanUnsupported, KvStoreTtlUnsupported
from tests.conformance import assert_basic_crud, assert_batch_ops, assert_scan_and_scope
from tests.fake_azure import FakeAzureContainer


@pytest.mark.azure
def test_azure_crud(azure_store: AzureBlobKvStore[str, str, str]) -> None:
    assert_basic_crud(azure_store)


@pytest.mark.azure
def test_azure_batch(azure_store: AzureBlobKvStore[str, str, str]) -> None:
    assert_batch_ops(azure_store)


@pytest.mark.azure
def test_azure_scan_and_scope(azure_store: AzureBlobKvStore[str, str, str]) -> None:
    assert_scan_and_scope(azure_store)


@pytest.mark.azure
def test_azure_ttl_object_time_default(fake_azure_container: FakeAzureContainer) -> None:
    kvset = KvSet.with_str_keys(
        "ttl",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=30)),
    )
    with freeze_time("2024-01-01 12:00:00") as frozen:
        store = AzureBlobKvStore(kvset, container=fake_azure_container)
        store.set("a", "live")
        assert store.get("a") == "live"
        blob = next(fake_azure_container.list_blobs())
        assert blob.metadata == {}
        assert blob.last_modified is not None
        frozen.move_to("2024-01-01 12:01:00")
        assert store.get("a") is None
        assert list(fake_azure_container.list_blobs()) == []


@pytest.mark.azure
def test_azure_ttl_via_metadata(fake_azure_container: FakeAzureContainer) -> None:
    kvset = KvSet.with_str_keys(
        "ttl",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=30)),
    )
    with freeze_time("2024-01-01 12:00:00") as frozen:
        store = AzureBlobKvStore(
            kvset,
            container=fake_azure_container,
            ttl_mode=AzureTtlMode.METADATA,
        )
        store.set("a", "live", ttl=TtlPolicy(ttl_duration=timedelta(seconds=30)))
        assert store.get("a") == "live"
        blob = next(fake_azure_container.list_blobs())
        assert "expires" in blob.metadata
        frozen.move_to("2024-01-01 12:01:00")
        assert store.get("a") is None
        assert list(fake_azure_container.list_blobs()) == []


@pytest.mark.azure
def test_azure_explicit_ttl_raises_on_object_time(
    fake_azure_container: FakeAzureContainer,
) -> None:
    kvset = KvSet.with_str_keys(
        "ttl",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        ttl_policy=TtlPolicy.hourly(),
    )
    store = AzureBlobKvStore(kvset, container=fake_azure_container)
    with pytest.raises(KvStoreTtlUnsupported):
        store.set("a", "v", ttl=TtlPolicy.hourly())


@pytest.mark.azure
def test_azure_metadata_per_write_none(fake_azure_container: FakeAzureContainer) -> None:
    kvset = KvSet.with_str_keys(
        "ttl",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        ttl_policy=TtlPolicy.hourly(),
    )
    with freeze_time("2024-01-01 12:00:00") as frozen:
        store = AzureBlobKvStore(
            kvset,
            container=fake_azure_container,
            ttl_mode=AzureTtlMode.METADATA,
        )
        store.set("short", "b", ttl=TtlPolicy(ttl_duration=timedelta(seconds=30)))
        store.set("pinned", "c", ttl=TtlPolicy.none())
        frozen.move_to("2024-01-01 12:01:00")
        assert store.get("short") is None
        assert store.get("pinned") == "c"


@pytest.mark.azure
def test_azure_expiry_gc_hide(fake_azure_container: FakeAzureContainer) -> None:
    kvset = KvSet.with_str_keys(
        "ttl",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=30)),
    )
    with freeze_time("2024-01-01 12:00:00") as frozen:
        store = AzureBlobKvStore(
            kvset,
            container=fake_azure_container,
            expiry_gc=ExpiryGc.HIDE,
        )
        store.set("a", "live")
        frozen.move_to("2024-01-01 12:01:00")
        assert store.get("a") is None
        assert list(fake_azure_container.list_blobs())


@pytest.mark.azure
def test_azure_hashed_scan_unsupported(fake_azure_container: FakeAzureContainer) -> None:
    kvset = KvSet.with_str_keys(
        "hashed",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        key_layout=KeyLayout.HASHED,
    )
    store = AzureBlobKvStore(kvset, container=fake_azure_container)
    store.set("k", "v")
    assert store.get("k") == "v"
    with pytest.raises(KvStoreScanUnsupported):
        list(store.scan())
