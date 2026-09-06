"""Tests for kvtsjl.batching helpers."""

from __future__ import annotations

import threading
import time

import pytest

from kvtsjl.batching import map_concurrent


def test_map_concurrent_empty() -> None:
    assert map_concurrent([], lambda x: x, max_workers=4) == []


def test_map_concurrent_order_preserving() -> None:
    assert map_concurrent([3, 1, 2], lambda x: x * 10, max_workers=4) == [30, 10, 20]


def test_map_concurrent_inline_when_workers_one() -> None:
    thread_ids: list[int] = []

    def record(x: int) -> int:
        thread_ids.append(threading.get_ident())
        return x

    main = threading.get_ident()
    assert map_concurrent([1, 2, 3], record, max_workers=1) == [1, 2, 3]
    assert thread_ids == [main, main, main]


def test_map_concurrent_inline_single_item() -> None:
    thread_ids: list[int] = []

    def record(x: int) -> int:
        thread_ids.append(threading.get_ident())
        return x

    main = threading.get_ident()
    assert map_concurrent([7], record, max_workers=8) == [7]
    assert thread_ids == [main]


def test_map_concurrent_propagates_exception() -> None:
    def boom(x: int) -> int:
        if x == 2:
            raise RuntimeError("fail")
        return x

    with pytest.raises(RuntimeError, match="fail"):
        map_concurrent([1, 2, 3], boom, max_workers=4)


def test_map_concurrent_rejects_bad_workers() -> None:
    with pytest.raises(ValueError, match="max_workers"):
        map_concurrent([1], lambda x: x, max_workers=0)


def test_map_concurrent_overlaps() -> None:
    started = threading.Barrier(4)
    active = 0
    peak = 0
    lock = threading.Lock()

    def work(x: int) -> int:
        nonlocal active, peak
        started.wait(timeout=5)
        with lock:
            active += 1
            peak = max(peak, active)
        time.sleep(0.05)
        with lock:
            active -= 1
        return x

    assert map_concurrent([1, 2, 3, 4], work, max_workers=4) == [1, 2, 3, 4]
    assert peak >= 2
