"""Shared assertions for KvStore backends.

Do not call ``assert_ttl_expiry_gc`` against Redis hash stores: a hash-mode
store with a finite ``KvSet.ttl_policy`` is unconstructable by design.

``assert_ttl_expiry_gc`` uses freezegun; it fits Memory (and other clock-driven
TTL leaves). Filesystem default ``MTIME`` TTL needs ``os.utime`` instead — see
``tests/test_filesystem.py`` (``test_filesystem_ttl_via_mtime`` /
``test_filesystem_expiry_gc_hide``).
"""

from __future__ import annotations

from collections.abc import Callable

from freezegun import freeze_time

from kvtsjl.store import KvStore
from kvtsjl.store.schema.ttl import ExpiryGc, TtlPolicy

StoreFactory = Callable[[], KvStore[str, str]]


def assert_basic_crud(store: KvStore[str, str]) -> None:
    assert store.get("missing") is None
    store.set("a", "one")
    assert store.get("a") == "one"
    store.set("a", "two")
    assert store.get("a") == "two"
    assert store.delete("a") is True
    assert store.get("a") is None
    assert store.delete("a") is False


def assert_batch_ops(store: KvStore[str, str]) -> None:
    store.batch_set({"k1": "v1", "k2": "v2", "k3": "v3"})
    assert store.batch_get(["k1", "k2", "missing"]) == {"k1": "v1", "k2": "v2"}
    assert store.batch_delete(["k1", "k3", "missing"]) == 2
    assert store.get("k2") == "v2"
    assert store.get("k1") is None


def assert_batch_return_counts(store: KvStore[str, str]) -> None:
    assert store.batch_get([]) == {}
    store.batch_set({})
    assert store.batch_delete([]) == 0
    store.batch_set({"a": "1", "b": "2"})
    assert store.batch_delete(["a", "missing", "a"]) == 1
    assert store.get("b") == "2"
    assert store.batch_delete(["b", "missing"]) == 1


def assert_scan_and_scope(store: KvStore[str, str]) -> None:
    store.set("alpha", "1")
    store.set("beta", "2")
    assert sorted(store.list()) == ["alpha", "beta"]
    assert dict(store.scan(include_values=True)) == {"alpha": "1", "beta": "2"}

    scoped = store.scoped(region="us")
    scoped.set("alpha", "scoped")
    assert scoped.get("alpha") == "scoped"
    assert store.get("alpha") == "1"
    assert sorted(scoped.list()) == ["alpha"]


def assert_scope_isolation(store: KvStore[str, str]) -> None:
    acme = store.scoped(tenant="acme")
    zeta = store.scoped(tenant="zeta")
    acme.set("doc1", "acme-secret")
    zeta.set("doc1", "zeta-secret")
    assert acme.get("doc1") == "acme-secret"
    assert zeta.get("doc1") == "zeta-secret"
    assert store.get("doc1") is None

    # ``/`` in a scope id must not forge segment boundaries (same as store wire).
    nested = store.scoped(tenant="a").scoped(x="b")
    slash_id = store.scoped(**{"tenant": "a/x/b"})
    nested.set("doc1", "nested-secret")
    slash_id.set("doc1", "slash-secret")
    assert nested.get("doc1") == "nested-secret"
    assert slash_id.get("doc1") == "slash-secret"


def assert_ttl_expiry_gc(
    make_store: Callable[[ExpiryGc], KvStore[str, str]],
) -> None:
    """TTL + ExpiryGc for leaves that honour lazy expiry.

    ``make_store`` must build a store whose ``KvSet`` uses a short TTL.
    Not for Redis hash mode (unconstructable with finite collection TTL).

    Discriminator (modes share ``get() -> None`` on expiry): after the expired
    read, HIDE still has an entry for ``gc_expired``; LAZY_DELETE already reaped.
    """
    with freeze_time("2024-01-01 12:00:00") as frozen:
        hide = make_store(ExpiryGc.HIDE)
        hide.set("h", "live")
        lazy = make_store(ExpiryGc.LAZY_DELETE)
        lazy.set("l", "live")
        frozen.move_to("2024-01-01 12:01:00")
        assert hide.get("h") is None
        assert lazy.get("l") is None
        assert hide.gc_expired(max_entries=10) == 1
        assert lazy.gc_expired(max_entries=10) == 0


def assert_gc_expired_cap(store: KvStore[str, str]) -> None:
    """``gc_expired(max_entries=N)`` deletes at most N expired keys.

    Store must use a short collection TTL; caller seeds via ``set`` under freeze.
    Requires per-write ``ttl=TtlPolicy.none()`` support (raises on S3/GCS/Azure/
    filesystem default TTL modes — same class of exclusion as Redis hash).
    """
    with freeze_time("2024-01-01 12:00:00") as frozen:
        store.set("a", "1")
        store.set("b", "2")
        store.set("c", "3", ttl=TtlPolicy.none())
        frozen.move_to("2024-01-01 12:01:00")
        assert store.gc_expired(max_entries=1) == 1
        assert store.get("c") == "3"
        # Cap held: exactly one of a/b remains for the follow-up sweep.
        assert store.gc_expired(max_entries=10) == 1
        assert store.get("a") is None
        assert store.get("b") is None
        assert store.get("c") == "3"


def assert_exists_ttl_expiry_gc(
    make_store: Callable[[ExpiryGc], KvStore[str, str]],
) -> None:
    """exists × TTL × ExpiryGc — must fail if ExpiryGc is ignored.

    After expiry, ``exists`` is False for both modes. Discriminator: HIDE still
    has a row for ``gc_expired``; LAZY_DELETE already reaped on the exists probe.
    """
    with freeze_time("2024-01-01 12:00:00") as frozen:
        hide = make_store(ExpiryGc.HIDE)
        hide.set("h", "live")
        lazy = make_store(ExpiryGc.LAZY_DELETE)
        lazy.set("l", "live")
        assert hide.exists("h") is True
        assert lazy.exists("l") is True
        frozen.move_to("2024-01-01 12:01:00")
        assert hide.exists("h") is False
        assert lazy.exists("l") is False
        assert hide.gc_expired(max_entries=10) == 1
        assert lazy.gc_expired(max_entries=10) == 0


def assert_batch_exists_and_stat(store: KvStore[str, str]) -> None:
    store.set("a", "1")
    store.set("b", "2")
    assert store.batch_exists(["a", "missing", "b"]) == {"a", "b"}
    stats = store.batch_stat(["a", "missing", "b"])
    assert set(stats) == {"a", "b"}
    assert store.stat("missing") is None
    assert store.exists("missing") is False


def assert_mirror_created_at_preserves_expiry(
    primary: KvStore[str, str],
    secondary: KvStore[str, str],
) -> None:
    """Capable mirror pair: secondary expiry stays aligned with primary (≤1s).

    Uses a ``created_at`` *before* frozen now so a secondary that ignores the
    write clock diverges (same pattern as the OBJECT_TIME residual test).
    """
    from datetime import UTC, datetime

    from kvtsjl import MirrorKvStore

    assert primary.supports_write_clock and secondary.supports_write_clock
    mirrored = MirrorKvStore(primary, secondary)
    # 45s before frozen now — dropping created_at on secondary yields ~45s drift.
    created = datetime(2024, 1, 1, 11, 59, 15, tzinfo=UTC)
    with freeze_time("2024-01-01 12:00:00"):
        mirrored.set("k", "v", created_at=created)
        p_stat = primary.stat("k")
        s_stat = secondary.stat("k")
        assert p_stat is not None and s_stat is not None
        assert p_stat.expires_at is not None and s_stat.expires_at is not None
        drift = abs((p_stat.expires_at - s_stat.expires_at).total_seconds())
        assert drift <= 1.0


def assert_promote_preserves_expiry(
    primary: KvStore[str, str],
    secondary: KvStore[str, str],
) -> None:
    """Capable coalesce promote: primary keeps secondary's write clock (≤1s).

    Advances time by half the remaining TTL after a past ``created_at`` write so
    the secondary is still alive for ``get``, and short-TTL fixtures fail with an
    explicit remaining-life assertion instead of a silent miss.
    """
    from datetime import UTC, datetime, timedelta

    from kvtsjl import FallbackReadKvStore

    assert primary.supports_write_clock and secondary.supports_write_clock
    # 45s before frozen now — clock-drop on promote yields large expiry drift.
    created = datetime(2024, 1, 1, 11, 59, 15, tzinfo=UTC)
    noon = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
    with freeze_time(noon) as frozen:
        secondary.set("k", "v", created_at=created)
        s0 = secondary.stat("k")
        assert s0 is not None and s0.expires_at is not None, (
            "secondary missing/expired right after set with a 45s-past created_at; "
            "collection TTL must exceed that lookback (use >= 60s)"
        )
        remaining = (s0.expires_at - noon).total_seconds()
        assert remaining > 5, (
            f"promote helper needs >5s remaining TTL after past created_at, "
            f"got {remaining}s — use a longer collection TTL"
        )
        advance = min(30.0, remaining / 2)
        store = FallbackReadKvStore(primary, secondary, promote=True)
        frozen.move_to(noon + timedelta(seconds=advance))
        assert store.get("k") == "v", (
            f"secondary expired before promote probe "
            f"(advanced {advance}s with {remaining}s remaining)"
        )
        p_stat = primary.stat("k")
        s_stat = secondary.stat("k")
        assert p_stat is not None and s_stat is not None
        assert p_stat.expires_at is not None and s_stat.expires_at is not None
        drift = abs((p_stat.expires_at - s_stat.expires_at).total_seconds())
        assert drift <= 1.0
