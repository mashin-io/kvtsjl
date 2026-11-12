"""``beneath()``: a store's entries in every scope beneath its own, keyed with their scope.

The laws, on every medium and through compositions:

1. ``v.beneath().<op>(Scoped(s, k))`` is ``v.scoped(**s).<op>(k)``.
2. ``v.beneath().scoped(**a)`` and ``v.scoped(**a).beneath()`` address the same entries.
3. A composition's ``beneath()`` is that composition over its children's ``beneath()``.

And a batch is a batch: one round of I/O across every scope it names, as the
medium allows, split by the medium's limits.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
import sqlite3
from typing import Any

from freezegun import freeze_time
import pytest

from kvtsjl import (
    CopyClock,
    Keep,
    KvSet,
    MemoryKvStore,
    Put,
    Remove,
    Scope,
    Scoped,
    SerDe,
    TtlPolicy,
)
from kvtsjl.backends.sql.sqlite import SqliteSqlDbClientAdapter
from kvtsjl.backends.sql.store import SqlDbKvStore
from kvtsjl.exceptions import KvStoreError, KvStoreScopeError
from kvtsjl.store.logical import KvStore
from kvtsjl.store.schema.layout import KeyLayout
from kvtsjl.store.schema.scan import KeyPrefix, ValueWhere
from tests.media import MEDIA, VERSIONED_MEDIA, make_medium
from tests.sql_helpers import ensure_sql_table, make_sql_kvset

SCHEMA = ("user", "slug")


def at(slug: str, key: str) -> Scoped[str]:
    return Scoped(Scope.of(slug=slug), key)


def _seeded(name: str, request: pytest.FixtureRequest) -> Any:
    store = make_medium(name, request, scope_schema=SCHEMA)
    store.scoped(user="u", slug="a").set("h1", "A1")
    store.scoped(user="u", slug="b").set("h2", "B2")
    store.scoped(user="v", slug="a").set("h1", "not yours")
    return store


# --- law 1: every operation is the scoped store's -----------------------------


@pytest.mark.parametrize("name", MEDIA)
def test_reads_are_the_scoped_store_s(name: str, request: pytest.FixtureRequest) -> None:
    store = _seeded(name, request)
    mine = store.scoped(user="u").beneath()
    keys = [at("a", "h1"), at("b", "h2"), at("c", "h1"), at("a", "h2")]

    assert [mine.get(k) for k in keys] == [
        store.scoped(user="u", slug=k.scope.segments[0].id).get(k.key) for k in keys
    ]
    assert mine.batch_get(keys) == {at("a", "h1"): "A1", at("b", "h2"): "B2"}
    assert mine.batch_exists(keys) == {at("a", "h1"), at("b", "h2")}
    assert set(mine.batch_stat(keys)) == {at("a", "h1"), at("b", "h2")}
    assert mine.exists(at("a", "h1")) and not mine.exists(at("c", "h1"))
    assert mine.stat(at("a", "h1")) is not None


@pytest.mark.parametrize("name", MEDIA)
def test_writes_land_where_the_scoped_store_would_put_them(
    name: str, request: pytest.FixtureRequest
) -> None:
    store = _seeded(name, request)
    mine = store.scoped(user="u").beneath()

    mine.set(at("c", "h3"), "C3")
    mine.batch_set({at("d", "h4"): "D4", at("e", "h5"): "E5"})
    assert store.scoped(user="u", slug="c").get("h3") == "C3"
    assert store.scoped(user="u", slug="e").get("h5") == "E5"

    assert mine.delete(at("c", "h3")) is True
    assert mine.batch_delete([at("d", "h4"), at("e", "h5"), at("z", "h9")]) == 2
    assert mine.unset(at("b", "h2")) is True
    assert store.scoped(user="u").beneath().batch_get([at("d", "h4"), at("b", "h2")]) == {}
    assert store.scoped(user="v", slug="a").get("h1") == "not yours"


@pytest.mark.parametrize("name", VERSIONED_MEDIA)
def test_updates_go_through_across_scopes(name: str, request: pytest.FixtureRequest) -> None:
    store = _seeded(name, request)
    mine = store.scoped(user="u").beneath()

    held = mine.batch_update(
        [at("a", "h1"), at("b", "h2"), at("c", "h3")],
        lambda cur: {
            at("a", "h1"): Put(f"{cur[at('a', 'h1')]}+"),
            at("b", "h2"): Remove(),
            at("c", "h3"): Put("new"),
        },
    )
    assert held == {at("a", "h1"): "A1+", at("c", "h3"): "new"}
    assert store.scoped(user="u", slug="a").get("h1") == "A1+"
    assert store.scoped(user="u", slug="b").get("h2") is None
    assert mine.set_if_absent(at("c", "h3"), "again") is False
    assert mine.update(at("c", "h3"), lambda v: Keep()) == "new"


# --- containment: a key cannot leave the view ---------------------------------


@pytest.mark.parametrize(
    "scope",
    [Scope.empty(), Scope.of(user="v", slug="a"), Scope.of(slug="a", extra="x"), Scope.of(other="a")],
    ids=["none", "another-user", "too-many", "wrong-kind"],
)
def test_a_key_names_exactly_the_kinds_beneath(scope: Scope, request: pytest.FixtureRequest) -> None:
    mine = _seeded("memory", request).scoped(user="u").beneath()
    with pytest.raises(KvStoreScopeError, match="slug"):
        mine.get(Scoped(scope, "h1"))
    with pytest.raises(KvStoreScopeError):
        mine.batch_get([at("a", "h1"), Scoped(scope, "h1")])


def test_a_view_that_is_not_a_prefix_of_the_schema_is_refused(request: pytest.FixtureRequest) -> None:
    store = make_medium("memory", request, scope_schema=SCHEMA)
    with pytest.raises(KvStoreScopeError):
        store.scoped(slug="a").beneath()


# --- law 2: beneath() and scoped() commute ------------------------------------


@pytest.mark.parametrize("name", MEDIA)
def test_beneath_and_scoped_commute(name: str, request: pytest.FixtureRequest) -> None:
    store = _seeded(name, request)
    empty = Scoped(Scope.empty(), "h1")

    assert store.scoped(user="u").beneath().scoped(slug="a").get(empty) == "A1"
    assert store.scoped(user="u", slug="a").beneath().get(empty) == "A1"
    assert store.beneath().scoped(user="u").get(at("a", "h1")) == "A1"
    assert store.beneath().get(Scoped(Scope.of(user="v", slug="a"), "h1")) == "not yours"


def test_beneath_of_beneath_carries_an_empty_scope(request: pytest.FixtureRequest) -> None:
    mine = _seeded("memory", request).scoped(user="u").beneath()
    twice = mine.beneath()
    assert twice.get(Scoped(Scope.empty(), at("a", "h1"))) == "A1"
    with pytest.raises(KvStoreScopeError):
        twice.get(Scoped(Scope.of(slug="a"), at("a", "h1")))


# --- scans: every key comes back with its scope -------------------------------


@pytest.mark.parametrize("name", MEDIA)
def test_a_scan_lists_every_scope_beneath_and_nothing_else(
    name: str, request: pytest.FixtureRequest
) -> None:
    mine = _seeded(name, request).scoped(user="u").beneath()
    assert sorted(mine.scan(include_values=True), key=repr) == sorted(
        [(at("a", "h1"), "A1"), (at("b", "h2"), "B2")], key=repr
    )
    assert list(mine.scan(at("b", "h"))) == [at("b", "h2")]


@pytest.mark.parametrize(
    "kvset",
    [
        dict(scope_schema=None),
        dict(scope_schema=SCHEMA, key_layout=KeyLayout.HASHED),
    ],
    ids=["no-schema", "hashed"],
)
def test_a_scan_that_cannot_recover_scopes_refuses_before_any_row(kvset: dict[str, Any]) -> None:
    """Keyed operations still work there: each key carries its scope."""
    store = MemoryKvStore(
        KvSet.with_str_keys(
            "x", key_serde=SerDe.identity(str), value_serde=SerDe.identity(str), **kvset
        )
    )
    store.scoped(user="u", slug="a").set("h1", "A1")
    mine = store.scoped(user="u").beneath()
    rows = mine.scan()
    with pytest.raises(KvStoreError, match="cannot recover scopes"):
        next(rows)
    assert mine.get(at("a", "h1")) == "A1"


@pytest.mark.parametrize("name", MEDIA)
def test_scan_scoped_is_beneath_scan_with_whole_scopes(name: str, request: pytest.FixtureRequest) -> None:
    store = _seeded(name, request)
    listed = sorted(
        (scope.path_display(), leaf, value)
        for scope, leaf, value in store.scoped(user="u").scan_scoped(include_values=True)
    )
    assert listed == [("user/u/slug/a", "h1", "A1"), ("user/u/slug/b", "h2", "B2")]
    assert [leaf for _, leaf, _ in store.scan_scoped(prefix="h1")] == ["h1", "h1"]
    kept = store.scan_scoped(where=ValueWhere[str](lambda v: v.startswith("B")), include_values=True)
    assert [v for _, _, v in kept] == ["B2"]
    assert len(list(store.scan_scoped(where=KeyPrefix[str]("h"), limit=2))) == 2


@pytest.mark.parametrize("name", ["memory", "sql"])
def test_expired_entries_beneath_are_listed_and_collected_with_their_scope(
    name: str, request: pytest.FixtureRequest
) -> None:
    with freeze_time("2024-01-01 12:00:00") as clock:
        store = make_medium(
            name, request, TtlPolicy(ttl_duration=timedelta(hours=1)), scope_schema=SCHEMA
        )
        store.scoped(user="u", slug="a").set("old", "x")
        store.scoped(user="v", slug="a").set("old", "theirs")
        clock.tick(timedelta(hours=2))
        store.scoped(user="u", slug="b").set("new", "y")
        mine = store.scoped(user="u").beneath()

        assert mine.expired_keys(limit=10) == [at("a", "old")]
        assert mine.gc_expired_keys(max_entries=10) == [at("a", "old")]
        assert mine.expired_keys(limit=10) == []
        assert store.scoped(user="v").beneath().expired_keys(limit=10) == [at("a", "old")]


# --- a batch is one round of I/O across scopes, split by the medium's limits -----


class _CountingSql(SqliteSqlDbClientAdapter):
    def __init__(self, conn: sqlite3.Connection) -> None:
        super().__init__(conn)
        self.calls: list[tuple[str, int]] = []

    def _count(self, name: str, n: int) -> None:
        self.calls.append((name, n))

    def fetch_by_keys(self, **kw: Any) -> Any:
        self._count("fetch", len(kw["keys"]))
        return super().fetch_by_keys(**kw)

    def exists_by_keys(self, **kw: Any) -> Any:
        self._count("exists", len(kw["keys"]))
        return super().exists_by_keys(**kw)

    def delete_by_keys(self, **kw: Any) -> Any:
        self._count("delete", len(kw["keys"]))
        return super().delete_by_keys(**kw)

    def upsert_rows(self, **kw: Any) -> Any:
        self._count("upsert", len(kw["rows"]))
        return super().upsert_rows(**kw)

    def insert_each_if_absent(self, **kw: Any) -> Any:
        self._count("insert", len(kw["rows"]))
        return super().insert_each_if_absent(**kw)


def _counted_sql(batch_size: int = 500) -> tuple[Any, _CountingSql]:
    sql_kvset = make_sql_kvset("bodies", scope_fields=SCHEMA, scope_schema=SCHEMA)
    conn = sqlite3.connect(":memory:")
    ensure_sql_table(conn, sql_kvset)
    adapter = _CountingSql(conn)
    store = SqlDbKvStore(sql_kvset, adapter=adapter, batch_size=batch_size)
    return store, adapter


def test_sql_reads_and_writes_many_scopes_in_one_statement_each() -> None:
    store, adapter = _counted_sql()
    mine = store.scoped(user="u").beneath()
    keys = [at(s, f"h{i}") for i, s in enumerate("abcde")]

    mine.batch_set({k: "v" for k in keys})
    assert mine.batch_get(keys) == dict.fromkeys(keys, "v")
    assert mine.batch_exists(keys) == set(keys)
    assert mine.batch_delete(keys) == 5
    assert adapter.calls == [("upsert", 5), ("fetch", 5), ("exists", 5), ("delete", 5)]


def test_sql_splits_a_batch_by_its_limit() -> None:
    store, adapter = _counted_sql(batch_size=2)
    mine = store.scoped(user="u").beneath()
    keys = [at(s, "h") for s in "abcde"]

    mine.batch_update(keys, lambda cur: {k: Put("v") for k in cur})

    assert adapter.calls == [("fetch", 2), ("fetch", 2), ("fetch", 1), ("insert", 2), ("insert", 2), ("insert", 1)]


def test_redis_reads_many_scopes_in_one_mget(request: pytest.FixtureRequest) -> None:
    store = _seeded("redis", request)
    client = store.backend._client
    calls: list[int] = []
    mget = client.mget

    def counted(keys: Any, *a: Any) -> Any:
        calls.append(len(keys))
        return mget(keys, *a)

    client.mget = counted
    store.backend.batch_size = 2
    mine = store.scoped(user="u").beneath()
    got = mine.batch_get([at("a", "h1"), at("b", "h2"), at("c", "h3")])

    assert got == {at("a", "h1"): "A1", at("b", "h2"): "B2"}
    assert calls == [2, 1], "one MGET per batch_size keys, across scopes"


@pytest.mark.parametrize("name", ["s3", "gcs", "azure", "filesystem"])
def test_an_object_store_reads_many_scopes_as_one_concurrent_batch(
    name: str, request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _seeded(name, request)
    batches = _spy(store, "batch_get_across", monkeypatch)
    mine = store.scoped(user="u").beneath()
    assert mine.batch_get([at("a", "h1"), at("b", "h2"), at("c", "h3")]) == {
        at("a", "h1"): "A1",
        at("b", "h2"): "B2",
    }
    assert batches == [3], "the scopes went out as separate batches"


# --- law 3: compositions are rebuilt over their children's beneath() ------------


def _memory(name: str, ttl: timedelta | None = None) -> Any:
    return MemoryKvStore(
        KvSet.with_str_keys(
            name,
            key_serde=SerDe.identity(str),
            value_serde=SerDe.identity(str),
            ttl_policy=TtlPolicy(ttl_duration=ttl),
            scope_schema=SCHEMA,
        )
    )


def _spy(store: Any, method: str, monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """The size of each call of ``method`` on ``store``'s backend and every clone of it.

    A scoped view clones the backend, so the spy sits on the class and keeps
    only calls on this collection.
    """
    seen: list[int] = []
    cls = type(store.backend)
    original = getattr(cls, method)
    name = store.kvset.name

    def counted(self: Any, items: Any, *a: Any, **kw: Any) -> Any:
        if self.kvset.name == name:
            seen.append(len(items))
        return original(self, items, *a, **kw)

    monkeypatch.setattr(cls, method, counted)
    return seen


def test_a_cache_in_front_of_a_durable_store_batches_and_fills_once_across_scopes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2024, 1, 10, 12, tzinfo=UTC)
    with freeze_time(now):
        cache, durable = _memory("cache", timedelta(days=1)), _memory("durable")
        for slug in "abc":
            durable.scoped(user="u", slug=slug).set("h", slug.upper(), created_at=now - timedelta(days=3))
        cache.scoped(user="u", slug="a").set("h", "A")
        reads, fills = _spy(durable, "batch_get_across", monkeypatch), _spy(cache, "batch_write_if_unchanged_across", monkeypatch)
        cache_reads, stats = _spy(cache, "batch_get_across", monkeypatch), _spy(durable, "batch_stat_across", monkeypatch)
        keys = [at(s, "h") for s in "abc"]

        mine = cache.fallback_read(durable, copy_clock=CopyClock.COPY).scoped(user="u").beneath()
        assert mine.batch_get(keys) == {at("a", "h"): "A", at("b", "h"): "B", at("c", "h"): "C"}

        assert (cache_reads, reads, fills) == ([3], [2], [2]), "one batch per target, across scopes"
        assert stats == [], "a COPY fill asked the source for a clock"
        held = cache.scoped(user="u", slug="b").stat("h")
        assert held is not None and held.expires_at == now + timedelta(days=1)


def test_a_mirror_writes_each_side_in_one_batch_across_scopes(monkeypatch: pytest.MonkeyPatch) -> None:
    primary, copy = _memory("p"), _memory("c")
    sets = _spy(copy, "batch_set_across", monkeypatch)
    mirrored = primary.mirror(copy).scoped(user="u").beneath()
    mirrored.batch_set({at("a", "h"): "1", at("b", "h"): "2"})
    assert sets == [2]
    assert copy.scoped(user="u", slug="b").get("h") == "2"


class _Turn:
    def __init__(self, core: str | None, receipt: str | None) -> None:
        self.core, self.receipt = core, receipt

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _Turn) and (self.core, self.receipt) == (other.core, other.receipt)

    def __repr__(self) -> str:
        return f"_Turn({self.core!r}, {self.receipt!r})"


def test_a_record_with_a_coarser_part_reads_the_same_through_beneath(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Receipts are keyed by user alone; each key's scope narrows for them."""
    core = _memory("core")
    receipts = MemoryKvStore(
        KvSet.with_str_keys(
            "receipts", key_serde=SerDe.identity(str), value_serde=SerDe.identity(str),
            scope_schema=("user",),
        )
    )
    turns = KvStore.zip_with(
        _Turn, commit_part="core", after_commit=("receipt",), core=core,
        receipt=receipts.scope_from("user"),
    )
    turns.scoped(user="u", slug="a").set("t1", _Turn("C1", "R1"))
    turns.scoped(user="u", slug="b").set("t2", _Turn("C2", "R2"))
    reads = _spy(core, "batch_get_across", monkeypatch)

    mine = turns.scoped(user="u").beneath()
    keys = [at("a", "t1"), at("b", "t2"), at("c", "t3")]
    assert mine.batch_get(keys) == {k: turns.scoped(user="u", slug=k.scope.segments[0].id).get(k.key) for k in keys[:2]}
    assert reads == [3], "the commit part was read once across scopes"
    assert receipts.scoped(user="u").get("t2") == "R2"


@pytest.mark.parametrize(
    "view",
    [
        lambda s: s.imap(str.upper, str.lower),
        lambda s: s.imap_keys(lambda k: f"k:{k}", lambda sk: sk[2:]),
    ],
    ids=["imap", "imap_keys"],
)
def test_views_are_rebuilt_over_the_store_beneath(
    view: Callable[[Any], Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _memory("s")
    gets = _spy(store, "batch_get_across", monkeypatch)
    mine = view(store).scoped(user="u").beneath()
    mine.batch_set({at("a", "h"): "x", at("b", "h"): "y"})
    got = mine.batch_get([at("a", "h"), at("b", "h")])

    assert got == {k: view(store).scoped(user="u", slug=k.scope.segments[0].id).get("h") for k in got}
    assert gets == [2]
    assert sorted(k.scope.segments[0].id for k in mine.scan()) == ["a", "b"]


def test_a_view_only_narrows(request: pytest.FixtureRequest) -> None:
    """``clone_with_scope`` to another user would move the view out from under its keys."""
    mine = _seeded("memory", request).scoped(user="u").beneath()
    with pytest.raises(KvStoreScopeError, match="only narrows"):
        mine.clone_with_scope(Scope.of(user="v"))
    assert mine.clone_with_scope(Scope.of(user="u", slug="a")).get(Scoped(Scope.empty(), "h1")) == "A1"


@pytest.mark.parametrize("name", [m for m in MEDIA if m != "redis_hash"])  # no per-entry TTL
def test_stats_across_scopes_carry_each_entry_s_expiry(
    name: str, request: pytest.FixtureRequest
) -> None:
    store = make_medium(name, request, TtlPolicy(ttl_duration=timedelta(hours=1)), scope_schema=SCHEMA)
    store.scoped(user="u", slug="a").set("h1", "A1")
    store.scoped(user="u", slug="b").set("h2", "B2")
    keys = [at("a", "h1"), at("b", "h2")]
    stats = store.scoped(user="u").beneath().batch_stat(keys)
    for key in keys:
        alone = store.scoped(user="u", slug=key.scope.segments[0].id).stat(key.key)
        assert alone is not None and stats[key].persistent == alone.persistent
        if alone.expires_at is not None:
            got = stats[key].expires_at
            assert got is not None and abs((got - alone.expires_at).total_seconds()) < 2


def test_a_scoped_prefix_matches_only_its_own_scope() -> None:
    from kvtsjl.store.schema.scan import ScanRow

    prefix = KeyPrefix[Scoped[str]](at("b", "h"))
    assert prefix.evaluate(ScanRow(key=at("b", "h2")))
    assert not prefix.evaluate(ScanRow(key=at("a", "h2"))), "another scope matched"
    assert not prefix.evaluate(ScanRow(key=at("b", "x2")))
