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
def test_s3_scope_isolation(s3_store: S3KvStore[str, str, str]) -> None:
    from tests.conformance import assert_scope_isolation

    assert_scope_isolation(s3_store)


@pytest.mark.s3
def test_s3_batch_return_counts(s3_store: S3KvStore[str, str, str]) -> None:
    from tests.conformance import assert_batch_return_counts

    assert_batch_return_counts(s3_store)


@pytest.mark.s3
def test_s3_key_only_scan_skips_body_reads(s3_store: S3KvStore[str, str, str]) -> None:
    from tests.conformance import assert_key_only_scan_skips_body_reads

    assert_key_only_scan_skips_body_reads(s3_store)


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
        object_key = store._key_codec.encode_physical("a")
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


@pytest.mark.s3
def test_object_time_mirror_secondary_resets_ttl_window(
    s3_client: object, s3_bucket: str, str_bytes_kvset_ttl: KvSet[str, str, str, bytes]
) -> None:
    """Characterization: default OBJECT_TIME secondary ignores created_at.

    If this starts failing because the secondary preserved the write clock, the
    residual hole closed — update or remove this test.
    """
    from datetime import UTC, datetime

    from kvtsjl import MemoryKvStore, MirrorKvStore

    primary = MemoryKvStore(str_bytes_kvset_ttl)
    secondary = S3KvStore(
        str_bytes_kvset_ttl,
        client=s3_client,  # type: ignore[arg-type]
        bucket=s3_bucket,
    )
    assert primary.supports_write_clock is True
    assert secondary.supports_write_clock is False
    mirrored = MirrorKvStore(primary, secondary)
    # At noon, stamp create 45s earlier → Memory expires in 15s; S3 LastModified=noon.
    created = datetime(2024, 1, 1, 11, 59, 15, tzinfo=UTC)
    with freeze_time("2024-01-01 12:00:00") as frozen:
        mirrored.set("k", "v", created_at=created)
        frozen.move_to("2024-01-01 12:00:20")
        assert primary.get("k") is None
        assert secondary.get("k") == "v"
