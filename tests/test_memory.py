"""MemoryKvStore tests."""

from __future__ import annotations

import pytest

from kvtsjl import KvSet, MemoryKvStore, SerDe
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
