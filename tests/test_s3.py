"""S3KvStore tests against moto (in-process S3)."""

from __future__ import annotations

from datetime import timedelta

import pytest
from freezegun import freeze_time

from kvtsjl import KeyLayout, KvSet, SerDe, TtlPolicy
from kvtsjl.backends.s3 import S3KvStore
from kvtsjl.exceptions import KvStoreScanUnsupported
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
