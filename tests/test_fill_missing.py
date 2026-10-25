"""R17: copy into a store what its fallback holds and it lacks.

"What the primary lacks from a source" is what a fallback already is, so
filling is an operation of the composition — ``primary.fallback_read(source)``
— not a function beside it. It lists each fallback's keys, keeps those the
authority lacks, and writes each as the record with a conditional write, so
nothing is ever replaced.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from kvtsjl import FillStats, KvSet, MemoryKvStore, SerDe, TtlPolicy
from kvtsjl.exceptions import UncommittedWriteError
from kvtsjl.keymap_algebra.topology import Lookup, Role, Topology
from kvtsjl.store.compose.delegating import DelegatingKvStore
from kvtsjl.store.compose.topology import StoreTopologyMixin

CLOCK = datetime(2024, 1, 1, tzinfo=UTC)


def _leaf(name: str, ttl: TtlPolicy | None = None) -> Any:
    return MemoryKvStore(
        KvSet.with_str_keys(
            name,
            key_serde=SerDe.identity(str),
            value_serde=SerDe.identity(str),
            ttl_policy=ttl,
        )
    )


def _filled(**items: str) -> Any:
    store = _leaf("source")
    for key, value in items.items():
        store.set(key, value)
    return store


# --- what Nexus asked for ---------------------------------------------------


def test_only_missing_keys_are_copied_and_existing_ones_are_untouched() -> None:
    primary = _filled(a="mine")
    source = _filled(a="theirs", b="2", c="3")

    stats = primary.fallback_read(source).fill_missing()

    assert stats == FillStats(scanned=3, missing=2, written=2)
    assert primary.get("a") == "mine", "an existing record was replaced"
    assert (primary.get("b"), primary.get("c")) == ("2", "3")


def test_a_writer_that_wins_the_race_is_counted_not_overwritten() -> None:
    class Raced(MemoryKvStore[Any, Any, Any, Any]):
        """Another writer lands right after the check says the key is absent."""

        def batch_exists(self, keys: Sequence[Any]) -> set[Any]:
            held = super().batch_exists(keys)
            for key in keys:
                if key not in held:
                    self.set(key, "winner")
            return held

    primary = Raced(_leaf("primary").kvset)
    stats = primary.fallback_read(_filled(k="copy")).fill_missing()

    assert stats == FillStats(scanned=1, missing=1, lost_race=1)
    assert primary.get("k") == "winner"


@pytest.mark.parametrize("page_size", [1, 2, 3, 100])
def test_paging_gives_the_same_result_as_one_page(page_size: int) -> None:
    source = _filled(**{f"k{i}": str(i) for i in range(7)})
    primary = _filled(k1="mine", k4="mine")

    stats = primary.fallback_read(source).fill_missing(page_size=page_size)

    assert stats == FillStats(scanned=7, missing=5, written=5)
    assert sorted(primary.list()) == sorted(f"k{i}" for i in range(7))
    assert primary.get("k4") == "mine"


def test_a_limit_stops_early_and_a_later_run_continues() -> None:
    source = _filled(**{f"k{i}": str(i) for i in range(5)})
    primary = _leaf("primary")
    filling = primary.fallback_read(source)

    first = filling.fill_missing(limit=2, page_size=10)
    assert first.written == 2
    assert len(list(primary.list())) == 2

    second = filling.fill_missing(limit=2, page_size=10)
    assert second.written == 2, "the second run did not continue"
    assert second.missing == 2, "it re-copied what the first run wrote"

    assert filling.fill_missing().written == 1
    assert sorted(primary.list()) == [f"k{i}" for i in range(5)]
    with pytest.raises(ValueError, match="limit"):
        filling.fill_missing(limit=0)


def test_the_source_s_clock_is_carried_so_a_copy_keeps_its_remaining_life() -> None:
    ttl = TtlPolicy(ttl_duration=timedelta(days=3650))
    source, primary = _leaf("source", ttl), _leaf("primary", ttl)
    source.set("k", "v", created_at=CLOCK)

    primary.fallback_read(source).fill_missing()

    meta = primary.stat("k")
    assert meta is not None and meta.created_at == CLOCK, "the copy restarted its life"


# --- what the meta-algebra implies ------------------------------------------


def test_missing_keys_lists_what_a_fill_would_copy() -> None:
    primary = _filled(a="1")
    filling = primary.fallback_read(_filled(a="1", b="2", c="3"))
    assert sorted(filling.missing_keys(page_size=1)) == ["b", "c"]
    assert primary.get("b") is None, "listing wrote something"


def test_a_key_listed_but_gone_before_it_is_read_is_counted() -> None:
    class Vanishing(MemoryKvStore[Any, Any, Any, Any]):
        def batch_get(self, keys: Sequence[Any]) -> dict[Any, Any]:
            return {k: v for k, v in super().batch_get(keys).items() if k != "gone"}

    source = Vanishing(_leaf("source").kvset)
    source.set("gone", "x")
    source.set("here", "y")

    stats = _leaf("primary").fallback_read(source).fill_missing()

    assert stats == FillStats(scanned=2, missing=2, written=1, source_miss=1)


class _SpyIndex:
    name = "spy"
    sync_on_write = True
    sync_policy = None

    def __init__(self, synced: list[str]) -> None:
        self._synced = synced

    def bind_store(self, store: Any) -> None: ...
    def sync(self, key: str, value: Any) -> None:
        self._synced.append(key)

    def batch_sync(self, items: Any) -> None:
        self._synced.extend(items)

    def delete(self, key: str) -> None: ...
    def batch_delete(self, keys: Any) -> None: ...
    def clone_with_scope(self, scope: Any) -> Any:
        return self


def test_the_copy_is_written_as_the_record_so_its_dependents_follow() -> None:
    """Filling an indexed store indexes what it copies."""
    synced: list[str] = []
    primary = _leaf("primary").indexed(_SpyIndex(synced))
    primary.fallback_read(_filled(k="v")).fill_missing()
    assert synced == ["k"]


def test_a_failed_copy_raises_rather_than_being_logged() -> None:
    """A read's fill is best-effort; an explicit fill is a write, and says so."""

    class Down(MemoryKvStore[Any, Any, Any, Any]):
        def batch_write_if_unchanged(self, *_a: Any, **_kw: Any) -> set[Any]:
            raise RuntimeError("primary down")

    primary = Down(_leaf("primary").kvset)
    with pytest.raises(UncommittedWriteError):
        primary.fallback_read(_filled(k="v")).fill_missing()


def test_a_store_without_a_fallback_lacks_nothing() -> None:
    primary, copy = _leaf("primary"), _filled(k="v")
    mirrored = primary.mirror(copy)
    assert list(mirrored.missing_keys()) == []
    assert mirrored.fill_missing() == FillStats()
    assert primary.get("k") is None


def test_a_chain_of_fallbacks_fills_the_innermost_store_from_the_nearest_tier() -> None:
    """Composition is closed: the outer authority is itself a fallback_read.

    ``b`` is missing from ``hot`` although ``warm`` holds it — the chain of
    authorities ends at ``hot`` — and it is filled from ``warm``, the tier a
    read reaches first, not from ``cold``.
    """
    hot, warm, cold = _filled(a="hot"), _filled(b="warm"), _filled(a="x", b="y", c="cold")
    chained = hot.fallback_read(warm).fallback_read(cold)

    assert sorted(chained.missing_keys()) == ["b", "c"]
    assert chained.fill_missing().written == 2
    assert (hot.get("b"), hot.get("c")) == ("warm", "cold"), "a farther tier won"
    assert (warm.get("c"), hot.get("a")) == (None, "hot")


def test_a_fallback_inside_an_index_is_filled_through_the_index() -> None:
    """Filling at the outer level indexes the copy; the inner level cannot."""
    synced: list[str] = []
    primary = _leaf("primary")
    indexed = primary.fallback_read(_filled(k="v")).indexed(_SpyIndex(synced))

    assert list(indexed.missing_keys()) == ["k"], "the nested fallback was not seen"
    assert indexed.fill_missing() == FillStats(scanned=1, missing=1, written=1)
    assert primary.get("k") == "v"
    assert synced == ["k"], "the copy skipped the index around it"


def test_a_fallback_inside_a_mirror_is_filled_through_the_mirror() -> None:
    primary, backup = _leaf("primary"), _leaf("backup")
    mirrored = primary.fallback_read(_filled(k="v")).mirror(backup)

    assert mirrored.fill_missing().written == 1
    assert (primary.get("k"), backup.get("k")) == ("v", "v"), "the backup missed the copy"


def test_a_value_view_shows_what_the_store_behind_it_lacks() -> None:
    primary = _leaf("primary")
    view = primary.fallback_read(_filled(k="v")).imap(lambda s: s + "!", lambda s: s[:-1])

    assert list(view.missing_keys()) == ["k"]
    assert view.fill_missing().written == 1
    assert primary.get("k") == "v", "the value went through the view twice or not at all"


def test_a_key_view_shows_it_under_its_own_keys() -> None:
    primary = _leaf("primary")
    stored = primary.fallback_read(_filled(**{"n:k": "v"}))
    view = stored.imap_keys(lambda k: f"n:{k}", lambda sk: sk[2:])

    assert list(view.missing_keys()) == ["k"]
    assert view.fill_missing().written == 1
    assert primary.get("n:k") == "v"


def test_a_key_view_without_an_inverse_refuses() -> None:
    """It cannot show a stored key, so it says so rather than claiming nothing is missing."""
    from kvtsjl.exceptions import KvStoreUnsupported

    view = _leaf("p").fallback_read(_filled(k="v")).imap_keys(lambda k: f"n:{k}")
    with pytest.raises(KvStoreUnsupported):
        list(view.missing_keys())
    with pytest.raises(KvStoreUnsupported):
        view.fill_missing()


def test_a_leaf_lacks_nothing() -> None:
    leaf = _filled(k="v")
    assert list(leaf.missing_keys()) == []
    assert leaf.fill_missing() == FillStats()


class TwoFallbacks[K, V](StoreTopologyMixin[K, V], DelegatingKvStore[K, V]):
    """Two fallbacks and a replica in one record. Not part of kvtsjl."""

    def __init__(self, store: Any, first: Any, second: Any, replica: Any) -> None:
        super().__init__(store)
        self._parts = (store, first, second, replica)

    def topology(self) -> Topology[K, V]:
        store, first, second, replica = self._parts
        return Topology(
            (
                self._target("store", Role.AUTHORITY, store),
                self._target("first", Role.FALLBACK, first),
                self._target("second", Role.FALLBACK, second),
                self._target(
                    "replica", Role.DEPENDENT, replica, lookup=Lookup.AFTER_AUTHORITY
                ),
            )
        )


def test_a_key_belongs_to_the_first_fallback_a_read_would_reach() -> None:
    store = _leaf("store")
    first, second = _filled(k="first", a="1"), _filled(k="second", b="2")
    replica = _filled(stale="deleted from the store, still in the copy")
    composed: Any = TwoFallbacks(store, first, second, replica)

    assert sorted(composed.missing_keys()) == ["a", "b", "k"], "listed a key twice"
    composed.fill_missing()
    assert store.get("k") == "first", "a later fallback won over an earlier one"
    assert store.get("stale") is None, "a copy was written back into the authority"
    assert replica.get("a") == "1", "the copy skipped the record's dependents"


def test_a_page_must_hold_at_least_one_key() -> None:
    filling = _leaf("p").fallback_read(_filled(k="v"))
    with pytest.raises(ValueError, match="page_size"):
        list(filling.missing_keys(page_size=0))
    with pytest.raises(ValueError, match="page_size"):
        filling.fill_missing(page_size=0)
