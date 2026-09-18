"""Fan-out visibility: SyncPolicy + PartialSyncError on indexed / mirror."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import timedelta

from freezegun import freeze_time
import pytest

from kvtsjl import (
    KvSet,
    MemoryKeyIndex,
    MemoryKvStore,
    MemoryTermIndex,
    PartialSyncError,
    Scope,
    SerDe,
    SyncPolicy,
    TtlPolicy,
)
from kvtsjl.scope import ScopeSegment
from kvtsjl.store.compose.indexed import IndexedKvStore, _index_fanout_label
from kvtsjl.store.compose.mirror import MirrorKvStore


def _memory_store(name: str = "fanout") -> MemoryKvStore[str, str, str, bytes]:
    kvset = KvSet.with_str_keys(
        name,
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
    )
    return MemoryKvStore(kvset)


def _ttl_store(name: str) -> MemoryKvStore[str, str, str, bytes]:
    kvset = KvSet.with_str_keys(
        name,
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=30)),
    )
    return MemoryKvStore(kvset)


class _BoomKeyIndex(MemoryKeyIndex[str, str]):
    def __init__(self, *, label: str = "boom") -> None:
        super().__init__()
        self._label = label

    def upsert(self, key: str, value: str, meta: None) -> None:
        raise RuntimeError(f"upsert-{self._label}")

    def batch_upsert(self, items: Mapping[str, tuple[str, None]]) -> None:
        raise RuntimeError(f"batch-upsert-{self._label}")

    def delete(self, key: str) -> bool:
        raise RuntimeError(f"delete-{self._label}")

    def batch_delete(self, keys: Sequence[str]) -> int:
        raise RuntimeError(f"batch-delete-{self._label}")


class _BoomTermIndex(MemoryTermIndex[str, str]):
    def __init__(self, tag: str) -> None:
        super().__init__(terms_of=lambda _k, v: [v])
        self._tag = tag
        self.calls = 0

    def sync(self, key: str, value: str) -> None:
        self.calls += 1
        raise RuntimeError(f"term-{self._tag}")

    def batch_sync(self, items: Mapping[str, str]) -> None:
        self.calls += 1
        raise RuntimeError(f"term-batch-{self._tag}")


class _BoomSecondary(MemoryKvStore[str, str, str, bytes]):
    def set(self, key, value, *, ttl=None, created_at=None):  # type: ignore[no-untyped-def]
        raise RuntimeError("secondary-set")

    def batch_set(self, items, *, ttl=None, created_at=None):  # type: ignore[no-untyped-def]
        raise RuntimeError("secondary-batch-set")

    def delete(self, key):  # type: ignore[no-untyped-def]
        raise RuntimeError("secondary-delete")

    def batch_delete(self, keys):  # type: ignore[no-untyped-def]
        raise RuntimeError("secondary-batch-delete")

    def _gc_expired_keys(self, *, max_entries: int) -> list[str]:
        raise RuntimeError("secondary-gc")


def test_index_label_safe_without_index_set() -> None:
    class NoSet:
        pass

    assert _index_fanout_label(0, NoSet()) == "0:NoSet"  # type: ignore[arg-type]


def test_indexed_required_raises_primary_kept() -> None:
    boom = _BoomKeyIndex()
    store = _memory_store().indexed(boom)
    with pytest.raises(PartialSyncError) as caught:
        store.set("k", "v")
    err = caught.value
    assert store.get("k") == "v"
    assert len(err.failures) == 1
    assert "0:mem-keys" in err.failures
    assert isinstance(err.__cause__, RuntimeError)


def test_indexed_required_two_same_type_both_fail() -> None:
    """Collision case: same class + default wire name — keys must stay distinct."""
    left = _BoomTermIndex("left")
    right = _BoomTermIndex("right")
    assert left.index_set.name == right.index_set.name == "mem-terms"

    store = _memory_store().indexed(left, right)
    with pytest.raises(PartialSyncError) as caught:
        store.set("k", "hello")
    err = caught.value
    assert store.get("k") == "hello"
    assert len(err.failures) == 2
    assert set(err.failures) == {"0:mem-terms", "1:mem-terms"}
    assert "term-left" in str(err.failures["0:mem-terms"])
    assert "term-right" in str(err.failures["1:mem-terms"])
    assert err.__cause__ is err.failures["0:mem-terms"]


def test_indexed_best_effort_keeps_write_no_raise() -> None:
    boom = _BoomKeyIndex()
    store = _memory_store().indexed(boom, sync_policy=SyncPolicy.BEST_EFFORT)
    store.set("k", "v")
    assert store.get("k") == "v"


def test_indexed_collects_all_before_raise() -> None:
    first = _BoomTermIndex("a")
    second = _BoomTermIndex("b")
    third = _BoomTermIndex("c")
    store = _memory_store().indexed(first, second, third)
    with pytest.raises(PartialSyncError) as caught:
        store.set("k", "v")
    assert first.calls == second.calls == third.calls == 1
    assert len(caught.value.failures) == 3


def test_indexed_gc_best_effort_even_under_required() -> None:
    boom = _BoomKeyIndex()
    store = IndexedKvStore(
        _ttl_store("gc-idx"),
        [boom],
        sync_policy=SyncPolicy.REQUIRED,
    )
    with freeze_time("2024-01-01 12:00:00") as frozen:
        # Bypass index sync for the initial write.
        store._underlying.set("k", "v")
        frozen.move_to("2024-01-01 12:01:00")
        deleted = store.gc_expired(max_entries=10)
        assert deleted == 1
        assert store.get("k") is None


def test_indexed_scoped_preserves_sync_policy() -> None:
    store = _memory_store().indexed(
        MemoryKeyIndex[str, str](),
        sync_policy=SyncPolicy.BEST_EFFORT,
    )
    scoped = store.scoped(Scope((ScopeSegment("t", "1"),)))
    assert isinstance(scoped, IndexedKvStore)
    assert scoped.sync_policy is SyncPolicy.BEST_EFFORT


def test_mirror_required_raises_primary_kept() -> None:
    primary = _memory_store("m-pri")
    secondary = _BoomSecondary(_memory_store("m-sec").kvset)
    store = MirrorKvStore(primary, secondary)
    with pytest.raises(PartialSyncError) as caught:
        store.set("k", "v")
    assert primary.get("k") == "v"
    assert set(caught.value.failures) == {"secondary"}
    assert caught.value.__cause__ is caught.value.failures["secondary"]


def test_mirror_best_effort_no_raise() -> None:
    primary = _memory_store("m2-pri")
    secondary = _BoomSecondary(_memory_store("m2-sec").kvset)
    store = MirrorKvStore(primary, secondary, sync_policy=SyncPolicy.BEST_EFFORT)
    store.set("k", "v")
    assert primary.get("k") == "v"


def test_mirror_gc_best_effort_even_under_required() -> None:
    primary = _ttl_store("mgc-pri")
    secondary = _BoomSecondary(_ttl_store("mgc-sec").kvset)
    store = MirrorKvStore(primary, secondary, sync_policy=SyncPolicy.REQUIRED)
    with freeze_time("2024-01-01 12:00:00") as frozen:
        primary.set("k", "v")
        frozen.move_to("2024-01-01 12:01:00")
        deleted = store.gc_expired(max_entries=10)
        assert deleted == 1
        assert primary.get("k") is None


def test_mirror_scoped_preserves_sync_policy() -> None:
    store = _memory_store("sc-pri").mirror(
        _memory_store("sc-sec"),
        sync_policy=SyncPolicy.BEST_EFFORT,
    )
    scoped = store.scoped(Scope((ScopeSegment("t", "1"),)))
    assert isinstance(scoped, MirrorKvStore)
    assert scoped.sync_policy is SyncPolicy.BEST_EFFORT
