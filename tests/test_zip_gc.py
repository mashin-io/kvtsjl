"""Zip GC budget and stat/presence agreement.

Expiry is leaf-only: each part owns its ``TtlPolicy`` and expires on its own
schedule. These tests pin the two places a zip must not invent its own rule.

All assertions inspect the medium (or use ``ExpiryGc.HIDE``). A ``scan``-based
check passes against the broken code, because ``LAZY_DELETE`` reaps expired
entries during the scan that is meant to detect them.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from freezegun import freeze_time

from kvtsjl import KvSet, KvStore, MemoryKvStore, SerDe, TtlPolicy
from kvtsjl.store.schema.ttl import ExpiryGc


@dataclass
class Article:
    meta: str | None = None
    body: str | None = None


def _ttl_kvset(name: str, seconds: int) -> KvSet[str, str, str, bytes]:
    return KvSet.with_str_keys(
        name,
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=seconds)),
    )


def _medium(store: MemoryKvStore[str, str, str, bytes]) -> list[str]:
    """Keys still on the medium, bypassing lazy expiry reaping."""
    return sorted(store.backend._bucket().keys())


def test_zip_gc_budget_does_not_starve_later_parts() -> None:
    """A shared budget let part 0 consume it all and part 1 never run."""
    with freeze_time("2024-01-01 12:00:00") as frozen:
        meta = MemoryKvStore(_ttl_kvset("meta", 60), expiry_gc=ExpiryGc.HIDE)
        body = MemoryKvStore(_ttl_kvset("body", 60), expiry_gc=ExpiryGc.HIDE)
        articles = KvStore.zip_with(Article, meta=meta, body=body)
        for index in range(3):
            articles.set(f"a{index}", Article(meta=f"m{index}", body=f"b{index}"))

        frozen.move_to("2024-01-01 12:01:00")
        # Budget equal to the number of expired rows — the natural call, and the
        # one that hid the bug: a larger budget happened to reach part 1.
        articles.gc_expired(max_entries=3)

        assert _medium(meta) == []
        assert _medium(body) == []


def test_zip_gc_visits_every_part() -> None:
    visited: list[str] = []

    with freeze_time("2024-01-01 12:00:00") as frozen:
        meta = MemoryKvStore(_ttl_kvset("meta", 60), expiry_gc=ExpiryGc.HIDE)
        body = MemoryKvStore(_ttl_kvset("body", 60), expiry_gc=ExpiryGc.HIDE)
        for name, store in (("meta", meta), ("body", body)):
            original = store.gc_expired_keys

            def spy(
                *, max_entries: int, _name: str = name, _original: object = original
            ) -> list[str]:
                visited.append(_name)
                return _original(max_entries=max_entries)  # type: ignore[operator]

            store.gc_expired_keys = spy  # type: ignore[method-assign]

        articles = KvStore.zip_with(Article, meta=meta, body=body)
        for index in range(3):
            articles.set(f"a{index}", Article(meta=f"m{index}", body=f"b{index}"))
        frozen.move_to("2024-01-01 12:01:00")
        articles.gc_expired(max_entries=3)

    assert visited == ["meta", "body"]


def test_zip_gc_cleans_parts_expiring_disjointly() -> None:
    """Parts expire independently; GC must not stop at the first part's keys."""
    with freeze_time("2024-01-01 12:00:00") as frozen:
        meta = MemoryKvStore(_ttl_kvset("meta", 60), expiry_gc=ExpiryGc.HIDE)
        body = MemoryKvStore(_ttl_kvset("body", 60), expiry_gc=ExpiryGc.HIDE)
        articles = KvStore.zip_with(Article, meta=meta, body=body)
        articles.set("only-meta", Article(meta="m"))
        articles.set("only-body", Article(body="b"))

        frozen.move_to("2024-01-01 12:01:00")
        cleaned = articles.gc_expired(max_entries=1)

        assert _medium(meta) == []
        assert _medium(body) == []
        assert cleaned == 2


def test_zip_stat_expiry_agrees_with_or_presence() -> None:
    """Default OR: the row lives until the last part dies, so stat reports that."""
    with freeze_time("2024-01-01 12:00:00") as frozen:
        meta = MemoryKvStore(_ttl_kvset("meta", 60))
        body = MemoryKvStore(_ttl_kvset("body", 3600))
        articles = KvStore.zip_with(Article, meta=meta, body=body)
        articles.set("a", Article(meta="m", body="b"))

        frozen.move_to("2024-01-01 12:00:30")
        stat = articles.stat("a")
        assert stat is not None and stat.expires_at is not None
        # Row is still readable, so stat must not claim it has expired.
        assert articles.get("a") is not None
        assert stat.expires_at.hour == 13  # body's expiry, not meta's

        assert articles.batch_stat(["a"])["a"].expires_at == stat.expires_at


def test_zip_stat_expiry_agrees_with_mandatory_presence() -> None:
    """A mandatory part ends the row when it dies, so stat takes the earliest."""
    with freeze_time("2024-01-01 12:00:00") as frozen:
        meta = MemoryKvStore(_ttl_kvset("meta", 60))
        body = MemoryKvStore(_ttl_kvset("body", 3600))
        articles = KvStore.zip_with(
            Article, meta=meta, body=body, mandatory=(True, False)
        )
        articles.set("a", Article(meta="m", body="b"))

        frozen.move_to("2024-01-01 12:00:30")
        stat = articles.stat("a")
        assert stat is not None and stat.expires_at is not None
        assert stat.expires_at.minute == 1  # meta's expiry — the row needs it

        frozen.move_to("2024-01-01 12:01:30")
        assert articles.get("a") is None  # mandatory part gone -> row gone


def test_zip_stat_never_expires_when_a_present_part_never_expires() -> None:
    with freeze_time("2024-01-01 12:00:00"):
        meta = MemoryKvStore(_ttl_kvset("meta", 60))
        forever = MemoryKvStore(
            KvSet.with_str_keys(
                "forever",
                key_serde=SerDe.identity(str),
                value_serde=SerDe.utf8_bytes(),
            )
        )
        articles = KvStore.zip_with(Article, meta=meta, body=forever)
        articles.set("a", Article(meta="m", body="b"))

        stat = articles.stat("a")
        assert stat is not None
        assert stat.expires_at is None
