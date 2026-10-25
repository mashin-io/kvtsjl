"""``update``: decide what to do with an entry from what it holds, atomically.

``decide`` sees the current value and returns ``Keep()``, ``Put(v)`` or
``Remove()``; the decision is written only if the entry is still what was
seen, and is retaken when a concurrent writer got there first. Every medium
with a compare-and-write supports it, and every composition derives it.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
import sqlite3
from typing import Any

from freezegun import freeze_time
import pytest

from kvtsjl import Keep, KvSet, MemoryKvStore, Put, Remove, SerDe, Versioned
from kvtsjl.backends.sql.schema import SqlDbField, SqlDbKvSet
from kvtsjl.backends.sql.sqlite import SqliteSqlDbClientAdapter
from kvtsjl.backends.sql.store import SqlDbKvStore
from kvtsjl.exceptions import (
    KvStoreConflict,
    KvStoreError,
    KvStoreUnsupported,
    UncommittedWriteError,
)
from kvtsjl.keymap import KeyMap
from kvtsjl.keymap_algebra.dict_map import DictKeyMap
from kvtsjl.store.logical import KvStore
from kvtsjl.store.schema.ttl import ExpiryGc
from tests.media import TTL, VERSIONED_MEDIA, expect_emulator_gap, make_medium
from tests.sql_helpers import ensure_sql_table, make_sql_kvset
from tests.test_clear_part import Digest, _column, _digest_view, _doc, _wide_table


def _bump(v: str | None) -> Keep | Put[str] | Remove:
    return Put("1") if v is None else Put(str(int(v) + 1))


def _racing(store: Any, *, times: int, value: str | None = None) -> list[str | None]:
    """Let another writer land right after each of the next ``times`` reads.

    It writes ``other-<n>``, or ``value`` each time when given.
    """
    seen: list[str | None] = []
    read = store.batch_read_versioned

    def racing_read(keys: Any) -> Any:
        versioned = read(keys)
        for key in keys:
            seen.append(versioned[key].value)
            if len(seen) <= times:
                store.set(key, value if value is not None else f"other-{len(seen)}")
        return versioned

    store.batch_read_versioned = racing_read
    return seen


# --- every medium with a compare-and-write ----------------------------------


@pytest.fixture(params=[*VERSIONED_MEDIA, "dict"])
def store(request: pytest.FixtureRequest) -> Any:
    if request.param == "dict":
        return DictKeyMap[str, str]({})
    return make_medium(request.param, request)


def test_it_inserts_replaces_and_removes(store: Any) -> None:
    assert store.update("k", _bump) == "1"
    assert store.update("k", _bump) == "2"
    assert store.get("k") == "2"
    assert store.update("k", lambda v: Keep()) == "2"
    assert store.update("k", lambda v: Remove()) is None
    assert store.get("k") is None


def test_a_concurrent_write_is_seen_and_the_decision_retaken(store: Any) -> None:
    store.set("k", "10")
    seen = _racing(store, times=1)

    result = store.update("k", lambda v: Put(f"{v}+mine"))

    assert seen == ["10", "other-1"], "the decision was not retaken on what changed"
    assert result == "other-1+mine"
    assert store.get("k") == "other-1+mine", "the concurrent write was overwritten"


def test_a_removal_that_loses_is_retaken(store: Any, request: pytest.FixtureRequest) -> None:
    expect_emulator_gap(request, request.node.callspec.params["store"], "conditional delete")
    store.set("k", "10")
    _racing(store, times=1)

    assert store.update("k", lambda v: Remove() if v == "10" else Keep()) == "other-1"
    assert store.get("k") == "other-1", "a removal decided on a stale value went through"


def test_removing_nothing_writes_nothing(store: Any) -> None:
    assert store.write_if_unchanged("k", Versioned.absent(), Remove()) is False
    assert store.update("k", lambda v: Remove()) is None


def test_an_update_that_always_loses_gives_up_having_written_nothing(store: Any) -> None:
    store.set("k", "10")
    _racing(store, times=99)

    with pytest.raises(KvStoreConflict):
        store.update("k", lambda v: Put("mine"), attempts=3)
    assert store.get("k") == "other-3"


def test_an_insert_that_loses_sees_the_winner(store: Any) -> None:
    calls: list[str | None] = []

    def decide(v: str | None) -> Keep | Put[str]:
        calls.append(v)
        return Keep() if v is not None else Put("mine")

    _racing(store, times=1)
    assert store.update("k", decide) == "other-1"
    assert calls == [None, "other-1"]


@pytest.mark.parametrize("name", ["memory", "sql", "s3", "gcs", "azure"])
def test_an_expired_entry_reads_as_none_and_is_replaced(
    name: str, request: pytest.FixtureRequest
) -> None:
    """set_if_absent refuses a dead entry; update replaces it, so it never sticks."""
    with freeze_time("2024-01-01 12:00:00") as frozen:
        store = make_medium(name, request, TTL, expiry_gc=ExpiryGc.HIDE)
        store.set("k", "old")
        frozen.tick(timedelta(hours=2))

        assert store.get("k") is None
        assert store.set_if_absent("k", "fresh") is False
        assert store.update("k", lambda v: Put("fresh") if v is None else Keep()) == "fresh"
        assert store.get("k") == "fresh"

        store.set("j", "old")
        frozen.tick(timedelta(hours=2))
        assert store.batch_get_or_set(["j"], lambda ks: {k: "new" for k in ks}) == {"j": "new"}
        assert store.get("j") == "new"


def test_a_medium_that_cannot_replace_refuses_only_a_replace(
    request: pytest.FixtureRequest,
) -> None:
    """The filesystem inserts conditionally but has no compare-and-replace."""
    files = make_medium("filesystem", request)
    assert files.update("k", _bump) == "1"
    assert files.get_or_set("k", lambda: "mine") == "1"
    assert files.update("k", lambda v: Keep()) == "1"
    with pytest.raises(KvStoreUnsupported):
        files.update("k", _bump)
    assert files.get("k") == "1"


def test_a_view_of_a_shared_row_updates_and_removes_only_its_columns() -> None:
    conn = sqlite3.connect(":memory:")
    _wide_table(conn)
    core, impact = _column(conn, "core", owns_rows=True), _column(conn, "impact")
    core.set("d", "C")

    assert impact.update("d", lambda v: Put("I") if v is None else Keep()) == "I"
    assert impact.update("d", lambda v: Put(f"{v}2")) == "I2"
    assert impact.update("d", lambda v: Remove()) is None
    assert _doc(conn, "d") == {"core": "C", "text": None, "score": None, "impact": None}


def test_a_version_the_table_did_not_give_out_is_refused() -> None:
    store, _ = _counted_sql()
    store.set("k", "v")
    with pytest.raises(KvStoreError, match="not a version"):
        store.write_if_unchanged("k", Versioned("v", "an-etag"), Put("w"))
    assert store.get("k") == "v"


@pytest.mark.parametrize("name", ["redis", "redis_hash"])
def test_a_redis_batch_is_sent_batch_size_keys_at_a_time(
    name: str, request: pytest.FixtureRequest
) -> None:
    store = make_medium(name, request)
    store.backend.batch_size = 2
    client = store.backend._client
    pipelines: list[object] = []
    pipeline = client.pipeline

    def counted(*a: Any, **kw: Any) -> Any:
        pipelines.append(kw.get("transaction", True))
        return pipeline(*a, **kw)

    client.pipeline = counted
    keys = ["a", "b", "c"]
    store.batch_update(keys, lambda cur: {k: Put("1") for k in cur})
    store.batch_update(keys, lambda cur: {k: Put("2") for k in cur})

    assert pipelines == [False, False, True, True], "a batch went out in one request"
    assert store.batch_get(keys) == dict.fromkeys(keys, "2")


def test_a_view_whose_columns_are_unset_shows_none() -> None:
    """Not a value made of NULLs: decide must see that there is nothing yet."""
    conn = sqlite3.connect(":memory:")
    _wide_table(conn)
    core, digest = _column(conn, "core", owns_rows=True), _digest_view(conn)
    core.set("d", "C")

    made = digest.update("d", lambda v: Put(Digest("T", "S")) if v is None else Keep())
    assert made == Digest("T", "S")


def test_a_view_with_no_columns_of_its_own_refuses_to_remove() -> None:
    """The only thing left to remove is a row its siblings share."""
    conn = sqlite3.connect(":memory:")
    _wide_table(conn)
    core = _column(conn, "core", owns_rows=True)
    reader = SqlDbKvStore(
        SqlDbKvSet.create(
            core.kvset,
            fields=(SqlDbField("core", read_only=True),),
            key_field="id",
            shares_table=True,
        ),
        adapter=SqliteSqlDbClientAdapter(conn),
    )
    core.set("d", "C")
    with pytest.raises(KvStoreUnsupported, match="no columns of its own"):
        reader.update("d", lambda v: Remove())
    assert core.get("d") == "C"


def test_a_view_that_does_not_own_rows_refuses_to_create_one() -> None:
    conn = sqlite3.connect(":memory:")
    _wide_table(conn)
    impact = _column(conn, "impact")
    with pytest.raises(KvStoreUnsupported, match="does not own its rows"):
        impact.update("missing", lambda v: Put("I"))
    assert _doc(conn, "missing") == {}


def test_an_owner_s_removal_deletes_the_row() -> None:
    conn = sqlite3.connect(":memory:")
    _wide_table(conn)
    core = _column(conn, "core", owns_rows=True)
    core.set("d", "C")
    assert core.update("d", lambda v: Remove()) is None
    assert _doc(conn, "d") == {}


def test_an_adapter_without_compare_and_write_refuses_to_replace() -> None:
    class NoCompare(SqliteSqlDbClientAdapter):
        def supports_compare_and_write(self) -> bool:
            return False

    sql_kvset = make_sql_kvset("plain")
    conn = sqlite3.connect(":memory:")
    ensure_sql_table(conn, sql_kvset)
    store = SqlDbKvStore(sql_kvset, adapter=NoCompare(conn))
    assert store.update("k", _bump) == "1", "inserting needs no compare-and-write"
    with pytest.raises(KvStoreUnsupported, match="compare-and-write"):
        store.update("k", _bump)


def test_attempts_must_be_positive(store: Any) -> None:
    with pytest.raises(ValueError, match="attempts"):
        store.update("k", _bump, attempts=0)


def test_an_update_keeps_the_entry_s_expiry(request: pytest.FixtureRequest) -> None:
    for name in (m for m in VERSIONED_MEDIA if m != "redis_hash"):  # no per-entry TTL
        store = make_medium(name, request, TTL)
        store.update("k", _bump)
        store.update("k", _bump)
        stat = store.stat("k")
        assert stat is not None and stat.expires_at is not None, f"{name} dropped the TTL"


@pytest.mark.parametrize("name", ["memory", "sql", "gcs", "azure"])
def test_a_live_rewrite_of_an_expired_value_is_a_change(
    name: str, request: pytest.FixtureRequest
) -> None:
    """The entry looked expired when read; a writer made it live with the same bytes.

    A decision taken on "absent" must not then overwrite it. (S3's ETag is a
    hash of the bytes, so S3 cannot see this one; Redis expires natively.)
    """
    with freeze_time("2024-01-01 12:00:00") as frozen:
        store = make_medium(name, request, TTL, expiry_gc=ExpiryGc.HIDE)
        store.set("k", "old")
        frozen.tick(timedelta(hours=2))
        seen = _racing(store, times=1, value="old")

        assert store.update("k", lambda v: Put("fresh") if v is None else Keep()) == "old"
        assert seen == [None, "old"]


def test_batch_update_decides_together_and_retakes_only_the_losers(store: Any) -> None:
    store.set("b", "10")
    store.set("c", "20")
    shown: list[dict[str, str | None]] = []

    def decide(current: Any) -> dict[str, Any]:
        shown.append(dict(current))
        return {
            "a": Put("new"),
            "b": Put(f"{current['b']}+1") if "b" in current else Keep(),
            "c": Remove(),
        }

    seen = _racing(store, times=2)  # the first two keys read are a, then b
    held = store.batch_update(["a", "b", "c"], decide)

    assert shown[0] == {"a": None, "b": "10", "c": "20"}
    assert shown[1] == {"a": "other-1", "b": "other-2"}, "a key that won was decided again"
    assert held == {"a": "new", "b": "other-2+1"}
    assert (store.get("a"), store.get("b"), store.get("c")) == ("new", "other-2+1", None)
    assert len(seen) == 5


def test_batch_update_names_the_keys_that_lost_and_keeps_the_rest(store: Any) -> None:
    raced: list[str] = []

    def always_race(keys: Any) -> Any:
        versioned = read(keys)
        if "lose" in keys:
            raced.append(f"theirs-{len(raced)}")
            store.set("lose", raced[-1])
        return versioned

    read = store.batch_read_versioned
    store.batch_read_versioned = always_race
    with pytest.raises(KvStoreConflict) as caught:
        store.batch_update(["win", "lose"], lambda cur: {k: Put("mine") for k in cur}, attempts=2)
    assert (caught.value.lost, caught.value.held) == (("lose",), {"win": "mine"})
    assert (store.get("win"), store.get("lose")) == ("mine", raced[-1])


# --- get_or_set, made an update ---------------------------------------------


def test_get_or_set_returns_the_winner_and_computes_once(store: Any) -> None:
    if not isinstance(store, KvStore):
        pytest.skip("get_or_set is a store operation")
    computed: list[str] = []

    def compute() -> str:
        computed.append("mine")
        return "mine"

    _racing(store, times=1)
    assert store.get_or_set("k", compute) == "other-1"
    assert computed == ["mine"]
    assert store.get("k") == "other-1", "the winner was overwritten"


def test_batch_get_or_set_keeps_what_another_writer_stored(store: Any) -> None:
    if not isinstance(store, KvStore):
        pytest.skip("batch_get_or_set is a store operation")

    def compute(keys: Any) -> dict[str, str]:
        store.set("b", "theirs")   # lands while ours is being computed
        return {k: f"mine-{k}" for k in keys}

    assert store.batch_get_or_set(["a", "b"], compute) == {"a": "mine-a", "b": "theirs"}
    assert (store.get("a"), store.get("b")) == ("mine-a", "theirs")


# --- batches are batches -----------------------------------------------------


class _Counting(SqliteSqlDbClientAdapter):
    """Counts the statements' callers, not the statements: one call per batch."""

    calls: list[str]

    def __init__(self, conn: sqlite3.Connection) -> None:
        super().__init__(conn)
        self.calls = []

    def fetch_by_keys(self, **kw: Any) -> Any:
        self.calls.append("fetch")
        return super().fetch_by_keys(**kw)

    def insert_each_if_absent(self, **kw: Any) -> Any:
        self.calls.append("insert")
        return super().insert_each_if_absent(**kw)

    def replace_each_if(self, **kw: Any) -> Any:
        self.calls.append("replace")
        return super().replace_each_if(**kw)

    def delete_each_if(self, **kw: Any) -> Any:
        self.calls.append("delete")
        return super().delete_each_if(**kw)


def _counted_sql() -> tuple[Any, _Counting]:
    sql_kvset = make_sql_kvset("counted")
    conn = sqlite3.connect(":memory:")
    ensure_sql_table(conn, sql_kvset)
    adapter = _Counting(conn)
    return SqlDbKvStore(sql_kvset, adapter=adapter), adapter


def test_a_sql_batch_update_is_one_fetch_and_one_write_per_kind() -> None:
    store, adapter = _counted_sql()
    keys = ["a", "b", "c"]

    store.batch_update(keys, lambda cur: {k: Put("1") for k in cur})
    store.batch_update(keys, lambda cur: {k: Put("2") for k in cur})
    store.batch_update(keys, lambda cur: {k: Remove() for k in cur})

    assert adapter.calls == ["fetch", "insert", "fetch", "replace", "fetch", "delete"]
    assert store.batch_get(keys) == {}


def test_batch_get_or_set_computes_once_and_writes_once() -> None:
    store, adapter = _counted_sql()
    store.set("a", "held")
    adapter.calls.clear()
    asked: list[list[str]] = []

    def compute(keys: Any) -> dict[str, str]:
        asked.append(list(keys))
        return {k: f"v-{k}" for k in keys}

    got = store.batch_get_or_set(["a", "b", "c"], compute)

    assert got == {"a": "held", "b": "v-b", "c": "v-c"}
    assert asked == [["b", "c"]]
    assert adapter.calls == ["fetch", "fetch", "insert"], "a hit was not served by the read"


def test_a_hit_costs_get_or_set_one_read() -> None:
    store, adapter = _counted_sql()
    store.set("a", "held")
    adapter.calls.clear()
    assert store.get_or_set("a", lambda: "unused") == "held"
    assert store.batch_get_or_set(["a"], lambda ks: {}) == {"a": "held"}
    assert adapter.calls == ["fetch", "fetch"]


def test_a_record_s_commit_part_is_read_once_per_round() -> None:
    row, body = _leaf("r"), _leaf("b")
    reads: list[str] = []
    get, versioned = row.batch_get, row.batch_read_versioned

    def counted_get(keys: Any) -> Any:
        reads.append("get")
        return get(keys)

    def counted_versioned(keys: Any) -> Any:
        reads.append("versioned")
        return versioned(keys)

    row.batch_get, row.batch_read_versioned = counted_get, counted_versioned
    records = KvStore.zip_with(Rec, commit_part="row", after_commit=("body",), row=row, body=body)
    records.set("k", Rec("R", "B"))
    reads.clear()

    records.update("k", lambda v: Put(Rec("R2", "B2")))
    assert reads == ["versioned"], "the commit part was read again to assemble the record"


def test_a_hit_never_reads_versions() -> None:
    """A hit is served by a read, so a cache in front of the store can serve it."""
    store = _leaf("s")
    store.set("a", "held")

    def refuse(_keys: Any) -> Any:
        raise AssertionError("a hit read versions")

    store.batch_read_versioned = refuse
    assert store.get_or_set("a", lambda: "unused") == "held"
    assert store.batch_get_or_set(["a"], lambda ks: {}) == {"a": "held"}


def test_a_composition_sends_each_target_one_batch() -> None:
    primary, copy = _leaf("p"), _leaf("c")
    calls: list[str] = []
    for name, store, method in [
        ("read", primary, "batch_read_versioned"),
        ("cond", primary, "batch_write_if_unchanged"),
        ("copy", copy, "batch_set"),
    ]:
        original = getattr(store, method)

        def counted(*a: Any, _o: Any = original, _n: str = name, **kw: Any) -> Any:
            calls.append(_n)
            return _o(*a, **kw)

        setattr(store, method, counted)

    primary.mirror(copy).batch_update(["a", "b", "c"], lambda cur: {k: Put("v") for k in cur})

    assert calls == ["read", "cond", "copy"]
    assert copy.batch_get(["a", "b", "c"]) == {"a": "v", "b": "v", "c": "v"}


def test_fill_missing_copies_a_page_in_one_batch() -> None:
    cold = _leaf("cold")
    cold.batch_set({k: k for k in "abcde"})
    primary = _leaf("p")
    batches: list[int] = []
    write = primary.batch_write_if_unchanged

    def counted(writes: Any, **kw: Any) -> Any:
        batches.append(len(writes))
        return write(writes, **kw)

    primary.batch_write_if_unchanged = counted
    stats = primary.fallback_read(cold, promote=False).fill_missing(page_size=10, limit=4)

    assert batches == [4]
    assert (stats.written, primary.get("e")) == (4, None)


# --- every composition ------------------------------------------------------


def _leaf(name: str) -> Any:
    return MemoryKvStore(
        KvSet.with_str_keys(name, key_serde=SerDe.identity(str), value_serde=SerDe.identity(str))
    )


def test_a_mirror_copies_only_a_write_that_happened() -> None:
    primary, copy = _leaf("p"), _leaf("c")
    mirrored = primary.mirror(copy)
    assert mirrored.update("k", _bump) == "1"
    assert copy.get("k") == "1"

    _racing(mirrored, times=99)
    with pytest.raises(KvStoreConflict):
        mirrored.update("k", lambda v: Put("mine"), attempts=2)
    assert copy.get("k") != "mine", "a losing update was copied"
    with pytest.raises(KvStoreConflict):
        mirrored.update("k", lambda v: Remove(), attempts=2)
    assert copy.get("k") is not None, "a losing removal was copied"


def test_an_index_follows_an_update() -> None:
    synced: list[tuple[str, str]] = []

    class SpyIndex:
        name = "spy"
        sync_on_write = True
        sync_policy = None

        def bind_store(self, store: Any) -> None: ...
        def sync(self, key: str, value: str) -> None:
            synced.append((key, value))

        def batch_sync(self, items: Any) -> None:
            synced.extend(items.items())

        def delete(self, key: str) -> None:
            synced.append((key, "deleted"))

        def batch_delete(self, keys: Any) -> None:
            synced.extend((k, "deleted") for k in keys)
        def clone_with_scope(self, scope: Any) -> Any:
            return self

    indexed = _leaf("p").indexed(SpyIndex())
    indexed.update("k", _bump)
    indexed.update("k", lambda v: Remove())
    assert synced == [("k", "1"), ("k", "deleted")]


def test_a_fallback_s_value_is_seen_and_promoted_once() -> None:
    primary, cold = _leaf("p"), _leaf("c")
    cold.set("k", "5")
    tiered = primary.fallback_read(cold, promote=False)

    assert tiered.update("k", _bump) == "6"
    assert (primary.get("k"), cold.get("k")) == ("6", "5")


@dataclass(frozen=True)
class Rec:
    row: str | None
    body: str | None


def test_a_record_whose_parts_follow_its_commit_part_updates_atomically() -> None:
    row, note = _leaf("r"), _leaf("n")
    records = KvStore.zip_with(Rec, commit_part="row", after_commit=("body",), row=row, body=note)

    records.update("k", lambda v: Put(Rec("R", "B")) if v is None else Keep())
    records.update("k", lambda v: Put(Rec(v.row, "B2")) if v is not None else Keep())

    assert records.get("k") == Rec("R", "B2")


def test_a_record_with_parts_before_its_commit_point_refuses_to_replace() -> None:
    """They would replace the live record's parts before knowing it had not changed."""
    records = KvStore.zip_with(Rec, commit_part="row", row=_leaf("r"), body=_leaf("b"))
    records.update("k", lambda v: Put(Rec("R", "B")) if v is None else Keep())

    with pytest.raises(KvStoreUnsupported, match="before the commit"):
        records.update("k", lambda v: Put(Rec("R2", "B2")))
    assert records.get("k") == Rec("R", "B")


def test_a_commit_part_cannot_be_put_as_none() -> None:
    records = KvStore.zip_with(Rec, commit_part="row", after_commit=("body",), row=_leaf("r"), body=_leaf("b"))
    with pytest.raises(UncommittedWriteError) as caught:
        records.update("k", lambda v: Put(Rec(None, "B")))
    assert "decides existence" in str(caught.value.__cause__)
    assert records.get("k") is None


def test_views_update_through_their_mapping() -> None:
    inner = _leaf("i")
    upper = inner.imap(str.upper, str.lower)
    assert upper.update("k", lambda v: Put("ABC") if v is None else Keep()) == "ABC"
    assert upper.update("k", lambda v: Put(f"{v}D")) == "ABCD", "decide saw the stored form"
    assert inner.get("k") == "abcd"

    renamed = inner.imap_keys(lambda k: f"n:{k}", lambda sk: sk[2:])
    renamed.update("j", lambda v: Put("x"))
    assert inner.get("n:j") == "x"

    scoped = MemoryKvStore(
        KvSet.with_str_keys(
            "s", key_serde=SerDe.identity(str), value_serde=SerDe.identity(str),
            scope_schema=("user",),
        )
    )
    scoped.scope_from("user").scoped(user="u", session="s").update("k", _bump)
    assert scoped.scoped(user="u").get("k") == "1"


def test_a_group_refuses_to_be_updated_as_one() -> None:
    rows = MemoryKvStore(
        KvSet.with_str_keys(
            "rows", key_serde=SerDe.identity(str), value_serde=SerDe.identity(str),
            scope_schema=("parent",),
        )
    )
    with pytest.raises(KvStoreUnsupported, match="after_commit"):
        rows.group_by("parent").update("p", lambda v: Put({"a": "1"}))
    with pytest.raises(KvStoreUnsupported, match="after_commit"):
        rows.group_by("parent").read_versioned("p")


def test_a_plain_keymap_updates_too() -> None:
    counts: KeyMap[str, str] = DictKeyMap({})
    bump: Callable[[str | None], Any] = _bump
    for _ in range(3):
        counts.update("k", bump)
    assert counts.get("k") == "3"
