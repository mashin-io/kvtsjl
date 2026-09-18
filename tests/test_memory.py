"""MemoryKvStore tests."""

from __future__ import annotations

from datetime import timedelta

from freezegun import freeze_time
import pytest

from kvtsjl import ExpiryGc, KvSet, MemoryKvStore, SerDe, TtlPolicy
from tests.conformance import assert_basic_crud, assert_batch_ops, assert_scan_and_scope


@pytest.fixture
def store(str_bytes_kvset: KvSet[str, str, str, bytes]) -> MemoryKvStore[str, str, str, bytes]:
    return MemoryKvStore(str_bytes_kvset)


def test_memory_crud(store: MemoryKvStore[str, str, str, bytes]) -> None:
    assert_basic_crud(store)


def test_memory_batch(store: MemoryKvStore[str, str, str, bytes]) -> None:
    assert_batch_ops(store)


def test_memory_scan_and_scope(store: MemoryKvStore[str, str, str, bytes]) -> None:
    assert_scan_and_scope(store)


def test_memory_scope_isolation(store: MemoryKvStore[str, str, str, bytes]) -> None:
    from tests.conformance import assert_scope_isolation

    assert_scope_isolation(store)


def test_memory_batch_return_counts(store: MemoryKvStore[str, str, str, bytes]) -> None:
    from tests.conformance import assert_batch_return_counts

    assert_batch_return_counts(store)


def test_memory_get_or_set(store: MemoryKvStore[str, str, str, bytes]) -> None:
    calls = {"n": 0}

    def compute() -> str:
        calls["n"] += 1
        return "computed"

    assert store.get_or_set("x", compute) == "computed"
    assert store.get_or_set("x", compute) == "computed"
    assert calls["n"] == 1


def test_memory_per_write_ttl_and_none() -> None:
    kvset = KvSet.with_str_keys(
        "ttl",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        ttl_policy=TtlPolicy.hourly(),
    )
    store = MemoryKvStore(kvset)
    with freeze_time("2024-01-01 12:00:00") as frozen:
        store.set("default", "a")
        store.set("short", "b", ttl=TtlPolicy(ttl_duration=timedelta(seconds=30)))
        store.set("pinned", "c", ttl=TtlPolicy.none())
        store.batch_set({"batch": "d"}, ttl=TtlPolicy(ttl_duration=timedelta(seconds=30)))
        frozen.move_to("2024-01-01 12:01:00")
        assert store.get("short") is None
        assert store.get("batch") is None
        assert store.get("default") == "a"
        assert store.get("pinned") == "c"
        frozen.move_to("2024-01-01 13:01:00")
        assert store.get("default") is None
        assert store.get("pinned") == "c"


def test_memory_expiry_gc_hide() -> None:
    kvset = KvSet.with_str_keys(
        "ttl",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=30)),
    )
    store = MemoryKvStore(kvset, expiry_gc=ExpiryGc.HIDE)
    with freeze_time("2024-01-01 12:00:00") as frozen:
        store.set("a", "live")
        pk = store._physical_key_blob("a")
        frozen.move_to("2024-01-01 12:01:00")
        assert store.get("a") is None
        assert pk in store._bucket()


def test_memory_gc_expired_effort_and_hide() -> None:
    kvset = KvSet.with_str_keys(
        "ttl",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=30)),
    )
    store = MemoryKvStore(kvset, expiry_gc=ExpiryGc.HIDE)
    with freeze_time("2024-01-01 12:00:00") as frozen:
        store.set("a", "1")
        store.set("b", "2")
        store.set("live", "3", ttl=TtlPolicy.none())
        frozen.move_to("2024-01-01 12:01:00")
        assert store.gc_expired(max_entries=1) == 1
        assert store.get("live") == "3"
        remaining_expired = sum(
            1 for k in ("a", "b") if store._physical_key_blob(k) in store._bucket()
        )
        assert remaining_expired == 1
        assert store.gc_expired(max_entries=10) == 1
        assert store.get("a") is None
        assert store.get("b") is None
        assert store.get("live") == "3"
        assert store.gc_expired(max_entries=10) == 0


def test_memory_gc_expired_rejects_bad_effort(
    store: MemoryKvStore[str, str, str, bytes],
) -> None:
    with pytest.raises(ValueError, match="max_entries"):
        store.gc_expired(max_entries=0)


def test_readonly_gc_expired_raises(
    store: MemoryKvStore[str, str, str, bytes],
) -> None:
    from kvtsjl.exceptions import KvStoreReadOnlyError

    with pytest.raises(KvStoreReadOnlyError):
        store.readonly().gc_expired(max_entries=1)


def test_memory_ttl_expiry_gc_conformance() -> None:
    from tests.conformance import assert_ttl_expiry_gc

    kvset = KvSet.with_str_keys(
        "ttl",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=30)),
    )

    def make_store(gc: ExpiryGc) -> MemoryKvStore[str, str, str, bytes]:
        return MemoryKvStore(kvset, expiry_gc=gc)

    assert_ttl_expiry_gc(make_store)


def test_memory_exists_ttl_expiry_gc_conformance() -> None:
    from tests.conformance import assert_exists_ttl_expiry_gc

    kvset = KvSet.with_str_keys(
        "ttl-exists",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=30)),
    )

    def make_store(gc: ExpiryGc) -> MemoryKvStore[str, str, str, bytes]:
        return MemoryKvStore(kvset, expiry_gc=gc)

    assert_exists_ttl_expiry_gc(make_store)


def test_memory_batch_exists_and_stat(
    store: MemoryKvStore[str, str, str, bytes],
) -> None:
    from tests.conformance import assert_batch_exists_and_stat

    assert_batch_exists_and_stat(store)


def test_memory_mirror_created_at_preserves_expiry(
    str_bytes_kvset_ttl: KvSet[str, str, str, bytes],
) -> None:
    from tests.conformance import assert_mirror_created_at_preserves_expiry

    assert_mirror_created_at_preserves_expiry(
        MemoryKvStore(str_bytes_kvset_ttl),
        MemoryKvStore(str_bytes_kvset_ttl),
    )


def test_memory_promote_preserves_expiry(
    str_bytes_kvset_ttl: KvSet[str, str, str, bytes],
) -> None:
    from tests.conformance import assert_promote_preserves_expiry

    assert_promote_preserves_expiry(
        MemoryKvStore(str_bytes_kvset_ttl),
        MemoryKvStore(str_bytes_kvset_ttl),
    )


def test_memory_created_at_rejected_when_unsupported() -> None:
    """Direct leaf always supports write clock; hash redis is the raise case."""
    from datetime import UTC, datetime

    store = MemoryKvStore(
        KvSet.with_str_keys(
            "clk",
            key_serde=SerDe.identity(str),
            value_serde=SerDe.utf8_bytes(),
        )
    )
    assert store.supports_write_clock is True
    store.set("a", "v", created_at=datetime(2024, 1, 1, tzinfo=UTC))
    assert store.get("a") == "v"
