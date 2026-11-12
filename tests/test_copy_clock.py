"""When a copy kvtsjl makes into a store starts its life.

A fill copies a record into a target. Whether the target's TTL then counts
from the record's write clock or from the copy is what that TTL measures,
which only the composition knows: a new version of an old store keeps each
record's remaining life (``CopyClock.RECORD``, the default); a cache in front
of a durable store keeps a copy for its own TTL (``CopyClock.COPY``). Nexus
caches day-old memories for a day: under ``RECORD`` each arrived expired, so
the cache never served one.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from freezegun import freeze_time
import pytest

from kvtsjl import CopyClock, KvSet, MemoryKvStore, SerDe, TtlPolicy
from kvtsjl.keymap_algebra.topology import Lookup, Role, Topology
from kvtsjl.store.compose.delegating import DelegatingKvStore
from kvtsjl.store.compose.topology import StoreTopologyMixin

DAY = timedelta(days=1)
NOW = datetime(2024, 1, 10, 12, tzinfo=UTC)
WRITTEN = NOW - 3 * DAY


class _Counted(MemoryKvStore[Any, Any, Any, Any]):
    """Counts every read of values and of write clocks."""

    def __init__(self, *a: Any, **kw: Any) -> None:
        super().__init__(*a, **kw)
        self.calls: list[str] = []

    def _count(self, name: str, run: Callable[[], Any]) -> Any:
        self.calls.append(name)
        return run()

    def get(self, key: Any) -> Any:
        return self._count("get", lambda: super(_Counted, self).get(key))

    def batch_get(self, keys: Any) -> Any:
        return self._count("get", lambda: super(_Counted, self).batch_get(keys))

    def stat(self, key: Any) -> Any:
        return self._count("stat", lambda: super(_Counted, self).stat(key))

    def batch_stat(self, keys: Any) -> Any:
        return self._count("stat", lambda: super(_Counted, self).batch_stat(keys))


def _store(name: str, ttl: timedelta | None) -> _Counted:
    return _Counted(
        KvSet.with_str_keys(
            name,
            key_serde=SerDe.identity(str),
            value_serde=SerDe.identity(bytes),
            ttl_policy=TtlPolicy(ttl_duration=ttl),
        )
    )


def _tiers(clock: CopyClock) -> tuple[Any, _Counted, _Counted]:
    durable, cache = _store("durable", None), _store("cache", DAY)
    durable.set("k", b"body", created_at=WRITTEN)
    durable.calls.clear()
    return cache.fallback_read(durable, copy_clock=clock), cache, durable


READS: dict[str, Callable[[Any], object]] = {
    "get": lambda s: s.get("k"),
    "batch_get": lambda s: s.batch_get(["k"])["k"],
    "read_stream": lambda s: b"".join(s.read_stream("k")),
}


@pytest.mark.parametrize("read", list(READS))
def test_a_cached_copy_older_than_the_cache_ttl_is_served_from_the_cache(read: str) -> None:
    with freeze_time(NOW):
        tiered, cache, durable = _tiers(CopyClock.COPY)

        assert READS[read](tiered) == b"body"
        durable.calls.clear()
        assert READS[read](tiered) == b"body"

        assert durable.calls == [], "the second read went to the durable store"
        assert cache.get("k") == b"body"


@pytest.mark.parametrize("read", list(READS))
def test_a_cached_copy_expires_one_cache_ttl_after_it_was_copied(read: str) -> None:
    with freeze_time(NOW) as clock:
        tiered, cache, _ = _tiers(CopyClock.COPY)
        READS[read](tiered)

        clock.tick(DAY - timedelta(minutes=1))
        assert cache.get("k") == b"body", "the copy expired before its TTL"
        clock.tick(timedelta(minutes=2))
        assert cache.get("k") is None, "the copy outlived its TTL"


@pytest.mark.parametrize("read", list(READS))
def test_a_cached_copy_does_not_ask_the_source_for_a_clock(read: str) -> None:
    with freeze_time(NOW):
        tiered, _, durable = _tiers(CopyClock.COPY)
        READS[read](tiered)
        assert "stat" not in durable.calls


@pytest.mark.parametrize("read", list(READS))
def test_a_migrated_record_keeps_its_remaining_life(read: str) -> None:
    """The default: a new version of an old store expires each record when it would have."""
    with freeze_time(NOW):
        old, new = _store("old", None), _store("new", 4 * DAY)
        old.set("k", b"body", created_at=WRITTEN)
        READS[read](new.fallback_read(old))

        stat = new.stat("k")
        assert stat is not None and stat.expires_at == WRITTEN + 4 * DAY


@pytest.mark.parametrize(
    ("clock", "expires"),
    [(CopyClock.COPY, NOW + DAY), (CopyClock.RECORD, WRITTEN + 4 * DAY)],
)
def test_fill_missing_follows_the_same_clock(clock: CopyClock, expires: datetime) -> None:
    with freeze_time(NOW):
        durable, target = _store("durable", None), _store("target", 4 * DAY if clock is CopyClock.RECORD else DAY)
        durable.set("k", b"body", created_at=WRITTEN)
        durable.calls.clear()

        stats = target.fallback_read(durable, copy_clock=clock).fill_missing()

        assert stats.written == 1
        stat = target.stat("k")
        assert stat is not None and stat.expires_at == expires
        assert ("stat" in durable.calls) is (clock is CopyClock.RECORD)


# --- each copy follows the target it lands in -------------------------------


class _Tiered(StoreTopologyMixin[str, bytes], DelegatingKvStore[str, bytes]):
    """A cache read first, a primary, and a cold fallback: two targets filled."""

    def __init__(self, cache: Any, primary: Any, cold: Any) -> None:
        super().__init__(primary)
        self._cache, self._primary, self._cold = cache, primary, cold

    def topology(self) -> Topology[str, bytes]:
        return Topology((
            self._target(
                "cache", Role.DEPENDENT, self._cache,
                lookup=Lookup.BEFORE_AUTHORITY, fill=True, copy_clock=CopyClock.COPY,
            ),
            self._target("primary", Role.AUTHORITY, self._primary, fill=True),
            self._target("cold", Role.FALLBACK, self._cold),
        ))


@pytest.mark.parametrize("read", list(READS))
def test_one_fill_gives_each_target_the_clock_it_keeps(read: str) -> None:
    with freeze_time(NOW):
        cache, primary, cold = _store("cache", DAY), _store("primary", 4 * DAY), _store("cold", None)
        cold.set("k", b"body", created_at=WRITTEN)

        READS[read](_Tiered(cache, primary, cold))

        cached, kept = cache.stat("k"), primary.stat("k")
        assert cached is not None and cached.expires_at == NOW + DAY
        assert kept is not None and kept.expires_at == WRITTEN + 4 * DAY


def test_a_copy_through_nested_fallbacks_is_fresh_where_any_layer_says_so() -> None:
    """The copy lands in the inner store, which the outer composition fills as a cache."""
    with freeze_time(NOW):
        cache, durable = _store("cache", DAY), _store("durable", None)
        durable.set("k", b"body", created_at=WRITTEN)
        inner = cache.fallback_read(durable)  # the record's clock, by default
        outer = inner.fallback_read(_store("empty", None), copy_clock=CopyClock.COPY)

        assert outer.fill_missing().written == 1

        stat = cache.stat("k")
        assert stat is not None and stat.expires_at == NOW + DAY


def test_a_scoped_view_keeps_the_choice() -> None:
    def scoped(name: str, ttl: timedelta | None) -> Any:
        return MemoryKvStore(
            KvSet.with_str_keys(
                name, key_serde=SerDe.identity(str), value_serde=SerDe.identity(bytes),
                ttl_policy=TtlPolicy(ttl_duration=ttl), scope_schema=("user",),
            )
        )

    with freeze_time(NOW):
        cache, durable = scoped("cache", DAY), scoped("durable", None)
        durable.scoped(user="u").set("k", b"body", created_at=WRITTEN)

        cache.fallback_read(durable, copy_clock=CopyClock.COPY).scoped(user="u").get("k")

        stat = cache.scoped(user="u").stat("k")
        assert stat is not None and stat.expires_at == NOW + DAY
