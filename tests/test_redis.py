"""RedisKvStore tests against fakeredis."""

from __future__ import annotations

from datetime import timedelta

import fakeredis
from freezegun import freeze_time
import pytest

from kvtsjl import KvSet, SerDe, TtlPolicy
from kvtsjl.backends.redis import RedisKvStore
from kvtsjl.exceptions import KvStoreTtlUnsupported
from tests.conformance import assert_basic_crud, assert_batch_ops, assert_scan_and_scope


@pytest.mark.redis
def test_redis_flat_crud(
    redis_flat_store: RedisKvStore[str, str, str, bytes, None],
) -> None:
    assert_basic_crud(redis_flat_store)


@pytest.mark.redis
def test_redis_flat_batch(
    redis_flat_store: RedisKvStore[str, str, str, bytes, None],
) -> None:
    assert_batch_ops(redis_flat_store)


@pytest.mark.redis
def test_redis_flat_scan_and_scope(
    redis_flat_store: RedisKvStore[str, str, str, bytes, None],
) -> None:
    assert_scan_and_scope(redis_flat_store)


@pytest.mark.redis
def test_redis_hash_crud(
    redis_hash_store: RedisKvStore[str, str, str, bytes, str],
) -> None:
    assert_basic_crud(redis_hash_store)


@pytest.mark.redis
def test_redis_hash_batch(
    redis_hash_store: RedisKvStore[str, str, str, bytes, str],
) -> None:
    assert_batch_ops(redis_hash_store)


@pytest.mark.redis
def test_redis_hash_scan_and_scope(
    redis_hash_store: RedisKvStore[str, str, str, bytes, str],
) -> None:
    assert_scan_and_scope(redis_hash_store)


@pytest.mark.redis
def test_redis_flat_scope_isolation(
    redis_flat_store: RedisKvStore[str, str, str, bytes, None],
) -> None:
    from tests.conformance import assert_scope_isolation

    assert_scope_isolation(redis_flat_store)


@pytest.mark.redis
def test_redis_flat_batch_return_counts(
    redis_flat_store: RedisKvStore[str, str, str, bytes, None],
) -> None:
    from tests.conformance import assert_batch_return_counts

    assert_batch_return_counts(redis_flat_store)


@pytest.mark.redis
def test_redis_hash_scope_isolation(
    redis_hash_store: RedisKvStore[str, str, str, bytes, str],
) -> None:
    from tests.conformance import assert_scope_isolation

    assert_scope_isolation(redis_hash_store)


@pytest.mark.redis
def test_redis_hash_batch_return_counts(
    redis_hash_store: RedisKvStore[str, str, str, bytes, str],
) -> None:
    from tests.conformance import assert_batch_return_counts

    assert_batch_return_counts(redis_hash_store)


@pytest.mark.redis
def test_redis_hash_rejects_collection_ttl(fake_redis: fakeredis.FakeRedis) -> None:
    kvset = KvSet.with_str_keys(
        "ttl",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        ttl_policy=TtlPolicy.hourly(),
    )
    with pytest.raises(KvStoreTtlUnsupported, match="hash"):
        RedisKvStore.hash_collection(kvset, fake_redis)


@pytest.mark.redis
def test_redis_flat_ttl(fake_redis: fakeredis.FakeRedis) -> None:
    kvset = KvSet.with_str_keys(
        "ttl",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=30)),
    )
    with freeze_time("2024-01-01 12:00:00") as frozen:
        store = RedisKvStore.flat(kvset, fake_redis)
        store.set("a", "live")
        assert store.get("a") == "live"
        frozen.move_to("2024-01-01 12:01:00")
        assert store.get("a") is None


@pytest.mark.redis
def test_redis_flat_per_write_ttl(fake_redis: fakeredis.FakeRedis) -> None:
    kvset = KvSet.with_str_keys(
        "ttl",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        ttl_policy=TtlPolicy.hourly(),
    )
    with freeze_time("2024-01-01 12:00:00") as frozen:
        store = RedisKvStore.flat(kvset, fake_redis)
        store.set("short", "b", ttl=TtlPolicy(ttl_duration=timedelta(seconds=30)))
        store.set("pinned", "c", ttl=TtlPolicy.none())
        frozen.move_to("2024-01-01 12:01:00")
        assert store.get("short") is None
        assert store.get("pinned") == "c"


@pytest.mark.redis
def test_redis_hash_explicit_ttl_raises(
    fake_redis: fakeredis.FakeRedis,
) -> None:
    kvset = KvSet.with_str_keys(
        "ttl",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
    )
    store = RedisKvStore.hash_collection(kvset, fake_redis)
    with pytest.raises(KvStoreTtlUnsupported):
        store.set("a", "v", ttl=TtlPolicy.hourly())


@pytest.mark.redis
def test_redis_gc_expired_noop(
    redis_flat_store: RedisKvStore[str, str, str, bytes, None],
) -> None:
    redis_flat_store.set("a", "v")
    assert redis_flat_store.gc_expired(max_entries=10) == 0
    assert redis_flat_store.get("a") == "v"


@pytest.mark.redis
def test_redis_hash_created_at_raises(fake_redis: fakeredis.FakeRedis) -> None:
    from datetime import UTC, datetime

    kvset = KvSet.with_str_keys(
        "hash",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
    )
    store = RedisKvStore.hash_collection(kvset, fake_redis)
    assert store.supports_write_clock is False
    with pytest.raises(KvStoreTtlUnsupported, match="created_at"):
        store.set("a", "v", created_at=datetime(2024, 1, 1, tzinfo=UTC))


@pytest.mark.redis
def test_redis_flat_batch_exists(
    redis_flat_store: RedisKvStore[str, str, str, bytes, None],
) -> None:
    from tests.conformance import assert_batch_exists_and_stat

    assert_batch_exists_and_stat(redis_flat_store)
