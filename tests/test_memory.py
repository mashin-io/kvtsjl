"""MemoryKvStore tests."""

from __future__ import annotations

from datetime import timedelta

from freezegun import freeze_time
import pytest

from kvtsjl import KvSet, MemoryKvStore, SerDe, TtlPolicy
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
