"""A record spread over several targets, described by roles.

The role a target plays decides how every operation orders it. These tests
cover the rules directly against recording targets, then show they hold
through real compositions and nested trees, and then build a composition
kvtsjl does not ship from the mixin alone — the evidence that the abstraction
is not shaped around the compositions that use it today.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import sqlite3
from typing import Any

from freezegun import freeze_time
import pytest

from kvtsjl import KvSet, MemoryKvStore, SerDe, TtlPolicy
from kvtsjl.backends.sql.sqlite import SqliteSqlDbClientAdapter
from kvtsjl.backends.sql.store import SqlDbKvStore
from kvtsjl.exceptions import (
    KvStoreError,
    KvStoreUnsupported,
    PartialSyncError,
    RecordWriteError,
    UncommittedWriteError,
)
from kvtsjl.keymap import KeyMap
from kvtsjl.keymap_algebra.dict_map import DictKeyMap
from kvtsjl.keymap_algebra.topology import (
    DependentSyncs,
    Lookup,
    Parts,
    RecordTopologyMixin,
    Role,
    Target,
    Topology,
    WriteOptions,
)
from kvtsjl.store.compose.delegating import DelegatingKvStore
from kvtsjl.store.compose.topology import StoreTopologyMixin
from kvtsjl.store.schema.layout import ScanQuery
from kvtsjl.store.schema.policy import SyncPolicy
from kvtsjl.store.schema.scan import KeyPrefix
from kvtsjl.store.schema.stat import RecordStat
from kvtsjl.store.schema.ttl import ExpiryGc
from tests.sql_helpers import ensure_sql_table, make_sql_kvset

OPTS = WriteOptions()
CLOCK = datetime(2024, 1, 1, tzinfo=UTC)


def _now() -> DependentSyncs:
    return DependentSyncs()


def _leaf(name: str, *, ttl: int | None = None) -> Any:
    return MemoryKvStore(
        KvSet.with_str_keys(
            name,
            key_serde=SerDe.identity(str),
            value_serde=SerDe.identity(str),
            ttl_policy=(
                TtlPolicy(ttl_duration=timedelta(seconds=ttl)) if ttl is not None else None
            ),
        ),
        expiry_gc=ExpiryGc.HIDE,
    )


def _medium(store: Any) -> list[str]:
    """Keys still on the medium, bypassing lazy expiry reaping."""
    return sorted(store.backend._bucket().keys())


class _Spy(Target[str, str, str]):
    """A target that records each call and holds its entries in a dict."""

    def __init__(
        self,
        log: list[str],
        name: str,
        role: Role,
        *,
        fail: bool = False,
        wrote: bool = True,
        held: Mapping[str, str] | None = None,
        clock: bool = False,
        deferred: bool = False,
        expired: Sequence[str] = (),
        lookup: Lookup | None = None,
        fill: bool = False,
        policy: SyncPolicy = SyncPolicy.REQUIRED,
    ) -> None:
        super().__init__(name, role, lookup=lookup, fill=fill, policy=policy)
        self.log = log
        self.fail = fail
        self.wrote = wrote
        self.held = dict(held or {})
        self.clock = clock
        self.deferred = deferred
        self.expired = list(expired)
        self.written: list[WriteOptions] = []

    def _note(self, op: str) -> None:
        self.log.append(f"{op}:{self.name}")
        if self.fail:
            raise RuntimeError(f"{self.name} down")

    def write(self, key: str, value: str, options: WriteOptions) -> None:
        self._note("write")
        self.held[key] = value
        self.written.append(options)

    def write_if_absent(self, key: str, value: str, options: WriteOptions) -> bool:
        self._note("cond")
        if not self.wrote or key in self.held:
            return False
        self.held[key] = value
        self.written.append(options)
        return True

    def batch_write(self, items: Mapping[str, str], options: WriteOptions) -> None:
        self._note("batch_write")
        self.held.update(items)

    def delete(self, key: str) -> bool:
        self._note("delete")
        return self.held.pop(key, None) is not None

    def batch_delete(self, keys: Sequence[str]) -> int:
        self._note("batch_delete")
        return sum(self.held.pop(k, None) is not None for k in keys)

    def read(self, key: str) -> str | None:
        self._note("read")
        return self.held.get(key)

    def batch_read(self, keys: Sequence[str]) -> dict[str, str]:
        self._note(f"batch_read{sorted(keys)}")
        return {k: self.held[k] for k in keys if k in self.held}

    def exists(self, key: str) -> bool:
        self._note("exists")
        return key in self.held

    def stat(self, key: str) -> RecordStat | None:
        return RecordStat(created_at=CLOCK) if key in self.held else None

    def batch_stat(self, keys: Sequence[str]) -> dict[str, RecordStat]:
        return {k: RecordStat(created_at=CLOCK) for k in keys if k in self.held}

    def read_stream(self, key: str) -> Iterator[Any] | None:
        self._note("read_stream")
        value = self.held.get(key)
        return None if value is None else iter([value])

    def write_stream(
        self, key: str, source: Any, options: WriteOptions, size: int | None
    ) -> None:
        self._note("write_stream")
        self.held[key] = "".join(source)
        self.written.append(options)

    @property
    def supports_write_clock(self) -> bool:
        return self.clock

    @property
    def writes_are_deferred(self) -> bool:
        return self.deferred

    def gc(self, max_entries: int) -> list[str]:
        self._note("gc")
        return self.expired


class _Unwritable(_Spy):
    """Reads fine; every write fails."""

    def write(self, key: str, value: str, options: WriteOptions) -> None:
        raise RuntimeError(f"{self.name} refuses writes")


def _sealed(
    log: list[str], *, failing: str | None = None, wrote: bool = True
) -> Topology[str, str]:
    """A row that refers to a body, indexed: one target of each written role.

    The row is declared before the body on purpose, so declaration order alone
    would write the commit point first.
    """
    row = _Spy(log, "row", Role.AUTHORITY, fail=failing == "row", wrote=wrote)
    body = _Spy(log, "body", Role.COMPONENT, fail=failing == "body")
    index = _Spy(log, "index", Role.DEPENDENT, fail=failing == "index")
    return Topology((index,), parts=(row, body), assemble=lambda held: held[row])


# --- writes ---------------------------------------------------------------


def test_roles_decide_the_write_order_not_the_declaration() -> None:
    """Declared dependent-first, commit point before its component: reordered."""
    log: list[str] = []
    _sealed(log).write("k", "v", OPTS, _now())
    assert log == ["write:body", "write:row", "write:index"]


def test_batch_writes_take_the_same_order() -> None:
    log: list[str] = []
    _sealed(log).batch_write({"k": "v"}, OPTS, _now())
    assert log == ["batch_write:body", "batch_write:row", "batch_write:index"]


def test_a_refused_authority_writes_no_dependent() -> None:
    log: list[str] = []
    wrote = _sealed(log, wrote=False).write_if_absent("k", "v", OPTS, _now())
    assert wrote is False
    assert log == ["write:body", "cond:row"], "a dependent of someone else's record ran"


def test_the_write_clock_reaches_only_targets_that_keep_one() -> None:
    """A dependent without a clock gets the write without ``created_at``."""
    log: list[str] = []
    primary = _Spy(log, "primary", Role.AUTHORITY, clock=True)
    plain = _Spy(log, "plain", Role.DEPENDENT, clock=False)
    Topology((primary, plain)).write("k", "v", WriteOptions(created_at=CLOCK), _now())
    assert primary.written[0].created_at == CLOCK
    assert plain.written[0].created_at is None


def test_a_store_refuses_a_clock_its_authority_cannot_keep() -> None:
    """Refused before any target is written, not half-way through."""
    secondary = _leaf("secondary")

    class NoClock(MemoryKvStore[Any, Any, Any, Any]):
        @property
        def supports_write_clock(self) -> bool:
            return False

    primary = NoClock(_leaf("primary").kvset)
    mirrored: Any = primary.mirror(secondary)
    with pytest.raises(Exception, match="created_at"):
        mirrored.set("k", "v", created_at=CLOCK)
    assert secondary.get("k") is None


# --- deletes --------------------------------------------------------------


def test_deletes_take_the_authority_first_and_visit_everything() -> None:
    """The record vanishes at once; nothing is left behind by stopping early."""
    log: list[str] = []
    _sealed(log).delete("k", _now())
    assert log == ["delete:row", "delete:body", "delete:index"]


def test_batch_deletes_take_the_same_order_and_count_the_authority() -> None:
    log: list[str] = []
    removed = Topology(
        (
            _Spy(log, "copy", Role.DEPENDENT, held={"a": "1", "b": "2"}),
            _Spy(log, "primary", Role.AUTHORITY, held={"a": "1"}),
            _Spy(log, "cold", Role.FALLBACK, held={"a": "1", "b": "2"}),
        )
    ).batch_delete(["a", "b", "a"], _now())
    assert removed == 1, "counted the copy rather than the record"
    assert log == ["batch_delete:primary", "batch_delete:copy"]


# --- reads ----------------------------------------------------------------


def test_exists_asks_the_authority_then_fallbacks_in_order() -> None:
    log: list[str] = []
    topology = Topology(
        (
            _Spy(log, "hot", Role.AUTHORITY),
            _Spy(log, "warm", Role.FALLBACK, held={"k": "w"}),
            _Spy(log, "cold", Role.FALLBACK, held={"k": "c"}),
        )
    )
    assert topology.exists("k") is True
    assert log == ["exists:hot", "exists:warm"], "kept asking after a fallback answered"


def test_a_read_follows_the_lookup_order_and_stops_at_the_first_hit() -> None:
    """Declared after, cache first: ``lookup`` places it, not the declaration."""
    log: list[str] = []
    topology = Topology(
        (
            _Spy(log, "cold", Role.FALLBACK, held={"k": "cold"}),
            _Spy(log, "primary", Role.AUTHORITY),
            _Spy(log, "cache", Role.DEPENDENT, lookup=Lookup.BEFORE_AUTHORITY),
            _Spy(log, "copy", Role.DEPENDENT, held={"k": "copy"}),
        )
    )
    assert topology.read("k") == "cold"
    assert log == ["read:cache", "read:primary", "read:cold"], "read a target never looked up"


def test_a_hit_fills_the_targets_consulted_before_it_that_ask_for_it() -> None:
    log: list[str] = []
    cache = _Spy(log, "cache", Role.DEPENDENT, lookup=Lookup.BEFORE_AUTHORITY, fill=True)
    primary = _Spy(log, "primary", Role.AUTHORITY, clock=True)
    topology = Topology((cache, primary, _Spy(log, "cold", Role.FALLBACK, held={"k": "v"})))

    assert topology.read("k") == "v"
    assert "k" not in primary.held, "filled a target that did not ask"
    assert cache.held["k"] == "v"


def test_a_fill_carries_the_clock_so_the_value_keeps_its_remaining_life() -> None:
    log: list[str] = []
    primary = _Spy(log, "primary", Role.AUTHORITY, clock=True, fill=True)
    topology = Topology((primary, _Spy(log, "cold", Role.FALLBACK, held={"k": "v"})))
    topology.read("k")
    assert primary.written[0].created_at == CLOCK


def test_a_failed_fill_still_returns_the_value() -> None:
    log: list[str] = []
    primary = _Unwritable(log, "primary", Role.AUTHORITY, fill=True)
    cold = _Spy(log, "cold", Role.FALLBACK, held={"k": "v"})
    assert Topology((primary, cold)).read("k") == "v"


class _Raced(_Spy):
    """Another writer lands right after this target misses."""

    def read(self, key: str) -> str | None:
        found = super().read(key)
        self.held[key] = "newer"
        return found

    def batch_read(self, keys: Sequence[str]) -> dict[str, str]:
        found = super().batch_read(keys)
        self.held.update(dict.fromkeys(keys, "newer"))
        return found


def test_a_fill_never_replaces_a_value_written_since_the_miss() -> None:
    """A promotion that overwrites would replace the newer write with the copy."""
    log: list[str] = []
    primary = _Raced(log, "primary", Role.AUTHORITY, fill=True)
    cold = _Spy(log, "cold", Role.FALLBACK, held={"k": "older"})
    assert Topology((primary, cold)).read("k") == "older"
    assert primary.held["k"] == "newer", "the fill replaced a newer write"


def test_a_batch_fill_never_replaces_one_either() -> None:
    log: list[str] = []
    primary = _Raced(log, "primary", Role.AUTHORITY, fill=True, clock=True)
    cold = _Spy(log, "cold", Role.FALLBACK, held={"k": "older"})
    assert Topology((primary, cold)).batch_read(["k"]) == {"k": "older"}
    assert primary.held["k"] == "newer", "the batch fill replaced a newer write"


def test_values_flow_downstream_only() -> None:
    """A lagging replica must not write a record back into what it copies."""
    log: list[str] = []
    cache = _Spy(log, "cache", Role.DEPENDENT, lookup=Lookup.BEFORE_AUTHORITY, fill=True)
    primary = _Spy(log, "primary", Role.AUTHORITY, fill=True)
    replica = _Spy(
        log, "replica", Role.DEPENDENT, lookup=Lookup.AFTER_AUTHORITY, held={"k": "stale"}
    )
    assert Topology((cache, primary, replica)).read("k") == "stale"
    assert "k" not in primary.held, "a copy was written back into the authority"
    assert "k" not in cache.held, "a copy fed a sibling copy"


def test_a_fill_the_target_cannot_do_is_raised_not_logged() -> None:
    """Best-effort tolerates a failing medium, not a composition that cannot work."""

    class NoConditionalWrite(DictKeyMap[str, str]):
        def set_if_absent(self, key: str, value: str, **_kw: Any) -> bool:
            return KeyMap.set_if_absent(self, key, value)

    hot = NoConditionalWrite({})
    with pytest.raises(KvStoreUnsupported):
        hot.coalesce(DictKeyMap({"k": "v"})).get("k")


def test_a_streamed_fill_the_target_cannot_take_is_raised() -> None:
    class NoStreams(_Spy):
        def write_stream(self, *_a: Any, **_kw: Any) -> None:
            raise KvStoreUnsupported("this target cannot take a stream")

    log: list[str] = []
    primary = NoStreams(log, "primary", Role.AUTHORITY, fill=True)
    cold = _Spy(log, "cold", Role.FALLBACK, held={"k": "v"})
    with pytest.raises(KvStoreUnsupported):
        Topology((primary, cold)).read_stream("k")


def test_best_effort_never_hides_an_unsupported_dependent() -> None:
    class Unsupported(_Spy):
        def write(self, key: str, value: str, options: WriteOptions) -> None:
            raise KvStoreUnsupported("this copy cannot take that write")

    log: list[str] = []
    primary = _Spy(log, "primary", Role.AUTHORITY)
    copy = Unsupported(log, "copy", Role.DEPENDENT, policy=SyncPolicy.BEST_EFFORT)
    with pytest.raises(PartialSyncError) as caught:
        Topology((primary, copy)).write("k", "v", OPTS, _now())
    assert isinstance(caught.value.failures["copy"], KvStoreUnsupported)
    assert caught.value.committed is True


def test_a_batch_read_asks_each_target_only_for_what_is_still_missing() -> None:
    log: list[str] = []
    primary = _Spy(log, "primary", Role.AUTHORITY, held={"a": "1"}, fill=True)
    cold = _Spy(log, "cold", Role.FALLBACK, held={"a": "x", "b": "2"})
    found = Topology((primary, cold)).batch_read(["a", "b", "c"])
    assert found == {"a": "1", "b": "2"}
    assert log[:2] == ["batch_read['a', 'b', 'c']:primary", "batch_read['b', 'c']:cold"]
    assert primary.held["b"] == "2", "a batch hit was not filled"


def test_metadata_comes_from_the_first_target_that_has_the_key() -> None:
    log: list[str] = []
    topology = Topology(
        (
            _Spy(log, "primary", Role.AUTHORITY),
            _Spy(log, "cold", Role.FALLBACK, held={"k": "v"}),
        )
    )
    assert topology.stat("k") == RecordStat(created_at=CLOCK)
    assert topology.batch_stat(["k", "nope"]) == {"k": RecordStat(created_at=CLOCK)}


def test_a_streamed_fill_serves_from_the_target_it_filled() -> None:
    log: list[str] = []
    primary = _Spy(log, "primary", Role.AUTHORITY, fill=True)
    cold = _Spy(log, "cold", Role.FALLBACK, held={"k": "v"})
    chunks = Topology((primary, cold)).read_stream("k")
    assert chunks is not None and "".join(chunks) == "v"
    assert log == [
        "read_stream:primary",
        "read_stream:cold",
        "write_stream:primary",
        "read_stream:primary",
    ]


def test_an_assembled_record_is_absent_when_its_authority_is() -> None:
    """A written body with no row is an orphan, not a record."""
    row, body = _leaf("row"), _leaf("body")
    zipped = KeyMap.zip_with(Seal, commit_part="row", row=row, body=body)
    body.set("orphan", "B")
    assert zipped.get("orphan") is None
    assert zipped.batch_get(["orphan"]) == {}


def test_an_assembled_record_exists_only_where_it_reads() -> None:
    """The authority is necessary, not sufficient: assemble has the last word.

    A committed row whose mandatory body is gone does not read back, so it
    must not exist or be listed either.
    """
    row, body = _leaf("row"), _leaf("body")
    zipped = KeyMap.zip_with(
        Seal, commit_part="row", mandatory=[False, True], row=row, body=body
    )
    row.set("k", "R")
    assert zipped.get("k") is None
    assert zipped.exists("k") is False
    assert zipped.batch_exists(["k"]) == set()
    assert list(zipped.list()) == [], "listed a record get cannot return"
    assert list(zipped.scan(include_values=True)) == []
    assert list(zipped.scan_entries(ScanQuery[str](include_values=True))) == []


def test_a_part_alone_is_not_listed_when_another_is_mandatory() -> None:
    """With no authority the keys come from every part; each is confirmed."""
    row, body = _leaf("row"), _leaf("body")
    zipped = KeyMap.zip_with(Seal, mandatory=[True, False], row=row, body=body)
    body.set("orphan", "B")
    zipped.set("whole", Seal(row="R", body="B"))
    assert sorted(zipped.list()) == ["whole"]
    assert zipped.exists("orphan") is False


# --- the shape of a record ------------------------------------------------


def test_a_record_has_one_authority_and_a_fallback_needs_one() -> None:
    log: list[str] = []
    with pytest.raises(ValueError, match="one authority"):
        Topology((_Spy(log, "a", Role.AUTHORITY), _Spy(log, "b", Role.AUTHORITY)))
    with pytest.raises(ValueError, match="fallback"):
        Topology(
            (_Spy(log, "cold", Role.FALLBACK),),
            parts=(_Spy(log, "part", Role.COMPONENT),),
            assemble=lambda held: None,
        )
    with pytest.raises(ValueError, match="authority or components"):
        Topology((_Spy(log, "copy", Role.DEPENDENT),))


def test_components_are_parts_assembled_and_take_no_lookups() -> None:
    log: list[str] = []
    part = _Spy(log, "part", Role.COMPONENT)
    with pytest.raises(ValueError, match="assemble go together"):
        Topology(parts=(part,))
    with pytest.raises(ValueError, match="assemble go together"):
        Topology((_Spy(log, "primary", Role.AUTHORITY),), assemble=lambda held: None)
    with pytest.raises(ValueError, match="holds a part"):
        Topology((part,))
    with pytest.raises(ValueError, match="comes after a commit point"):
        Topology(parts=(part, _Spy(log, "copy", Role.DEPENDENT)), assemble=lambda held: None)
    with pytest.raises(ValueError, match="read whole"):
        Topology(
            parts=(_Spy(log, "row", Role.AUTHORITY), _Spy(log, "cold", Role.FALLBACK)),
            assemble=lambda held: None,
        )
    with pytest.raises(ValueError, match="read from its parts"):
        Topology(
            (_Spy(log, "cache", Role.DEPENDENT, lookup=Lookup.BEFORE_AUTHORITY),),
            parts=(part,),
            assemble=lambda held: None,
        )


def test_roles_that_cannot_be_looked_up_or_filled_say_so() -> None:
    log: list[str] = []
    with pytest.raises(ValueError, match="always read"):
        _Spy(log, "primary", Role.AUTHORITY, lookup=Lookup.AFTER_AUTHORITY)
    with pytest.raises(ValueError, match="written can be filled"):
        _Spy(log, "cold", Role.FALLBACK, fill=True)


# --- failures -------------------------------------------------------------


@pytest.mark.parametrize(
    ("failing", "error", "committed"),
    [
        ("body", UncommittedWriteError, False),
        ("row", UncommittedWriteError, False),
        ("index", PartialSyncError, True),
    ],
)
def test_a_failure_says_which_side_of_the_commit_point_it_is_on(
    failing: str, error: type[RecordWriteError], committed: bool
) -> None:
    log: list[str] = []
    with pytest.raises(error) as caught:
        _sealed(log, failing=failing).write("k", "v", OPTS, _now())
    assert caught.value.committed is committed
    assert isinstance(caught.value.failures[failing], RuntimeError)


def test_a_best_effort_dependent_logs_rather_than_failing_the_write() -> None:
    log: list[str] = []
    primary = _Spy(log, "primary", Role.AUTHORITY)
    copy = _Spy(log, "copy", Role.DEPENDENT, fail=True, policy=SyncPolicy.BEST_EFFORT)
    Topology((primary, copy)).write("k", "v", OPTS, _now())
    assert primary.held == {"k": "v"}


# --- deferred commits -----------------------------------------------------


def test_a_dependent_durable_at_once_waits_for_the_commit() -> None:
    log: list[str] = []
    syncs = _now()
    topology = Topology(
        (
            _Spy(log, "primary", Role.AUTHORITY, deferred=True),
            _Spy(log, "index", Role.DEPENDENT),
            _Spy(log, "same_txn", Role.DEPENDENT, deferred=True),
        )
    )
    topology.write("k", "v", OPTS, syncs)
    assert log == ["write:primary", "write:same_txn"], "ran before the caller committed"
    assert syncs.pending == 1

    syncs.flush()
    assert log[-1] == "write:index"
    assert syncs.pending == 0


def test_a_removal_after_a_deferred_commit_point_waits_for_the_commit() -> None:
    """A component deleted before a rollback would leave the record pointing at nothing."""
    log: list[str] = []
    syncs = _now()
    row = _Spy(log, "row", Role.AUTHORITY, deferred=True, held={"k": "r"})
    body = _Spy(log, "body", Role.COMPONENT, held={"k": "b"})
    same_txn = _Spy(log, "same_txn", Role.COMPONENT, deferred=True, held={"k": "s"})
    topology = Topology(parts=(row, body, same_txn), assemble=lambda held: held[row])

    assert topology.delete("k", syncs) is True
    assert log == ["delete:row", "delete:same_txn"], "a removal outran the caller's commit"
    assert syncs.pending == 1

    syncs.flush()
    assert log[-1] == "delete:body"


def test_a_part_left_behind_by_a_delete_reports_the_record_gone() -> None:
    """The authority went first, so the record is removed; the part is stale."""
    log: list[str] = []
    row = _Spy(log, "row", Role.AUTHORITY, held={"k": "r"})
    body = _Spy(log, "body", Role.COMPONENT, fail=True)
    topology = Topology(parts=(row, body), assemble=lambda held: held[row])
    with pytest.raises(PartialSyncError) as caught:
        topology.delete("k", _now())
    assert caught.value.committed is True
    assert "k" not in row.held


def test_with_no_commit_point_every_part_is_removed_at_once() -> None:
    log: list[str] = []
    syncs = _now()
    a = _Spy(log, "a", Role.COMPONENT, deferred=True, held={"k": "1"})
    b = _Spy(log, "b", Role.COMPONENT, held={"k": "2"})
    Topology(parts=(a, b), assemble=lambda held: held[a]).delete("k", syncs)
    assert log == ["delete:a", "delete:b"]
    assert syncs.pending == 0


def test_a_record_deleted_in_a_rolled_back_transaction_keeps_its_body(
    sqlite_conn: sqlite3.Connection,
) -> None:
    sql_kvset = make_sql_kvset("rows")
    ensure_sql_table(sqlite_conn, sql_kvset)
    base = SqlDbKvStore(sql_kvset, adapter=SqliteSqlDbClientAdapter(sqlite_conn))
    rows = base.rebind(SqliteSqlDbClientAdapter(sqlite_conn, autocommit=False))
    bodies = _leaf("bodies")
    records = KeyMap.zip_with(Seal, commit_part="row", row=rows, body=bodies)
    sqlite_conn.execute("BEGIN IMMEDIATE")
    records.set("t", Seal(row="R", body="B"))
    sqlite_conn.commit()

    sqlite_conn.execute("BEGIN IMMEDIATE")
    records.delete("t")
    assert bodies.get("t") == "B", "the body went before the commit"
    sqlite_conn.rollback()
    records.discard_index_syncs()
    assert records.get("t") == Seal(row="R", body="B")

    sqlite_conn.execute("BEGIN IMMEDIATE")
    records.delete("t")
    sqlite_conn.commit()
    records.flush_index_syncs()
    assert records.get("t") is None
    assert bodies.get("t") is None, "the body outlived its committed delete"


# --- garbage collection ---------------------------------------------------


def test_mirror_gc_does_not_starve_the_secondary() -> None:
    """The secondary got only what the primary left of the budget.

    With as many expired entries in the primary as the budget, the secondary
    was never visited and the call still reported success.
    """
    with freeze_time("2024-01-01 12:00:00") as frozen:
        primary, secondary = _leaf("p", ttl=60), _leaf("s", ttl=60)
        mirrored: Any = primary.mirror(secondary)
        for index in range(3):
            primary.set(f"p{index}", "v")
            secondary.set(f"s{index}", "v")
        frozen.tick(timedelta(seconds=120))
        collected = mirrored.gc_expired_keys(max_entries=3)

    assert _medium(secondary) == [], "the secondary's expired entries were left behind"
    assert sorted(collected) == ["p0", "p1", "p2", "s0", "s1", "s2"]


def test_dependents_forget_what_the_authority_collected() -> None:
    """The record expired, so a copy that outlives it is stale."""
    with freeze_time("2024-01-01 12:00:00") as frozen:
        primary, secondary = _leaf("p", ttl=60), _leaf("s")
        mirrored: Any = primary.mirror(secondary)
        mirrored.set("k", "v")
        frozen.tick(timedelta(seconds=120))
        mirrored.gc_expired_keys(max_entries=10)
    assert secondary.get("k") is None


def test_a_fallback_is_never_collected() -> None:
    """It is never written, so its lifecycle is not ours."""
    with freeze_time("2024-01-01 12:00:00") as frozen:
        hot, cold = _leaf("hot", ttl=60), _leaf("cold", ttl=60)
        cold.set("k", "v")
        frozen.tick(timedelta(seconds=120))
        hot.fallback_read(cold).gc_expired_keys(max_entries=10)
    assert _medium(cold) == ["k"]


def test_a_failing_dependent_does_not_fail_a_collection() -> None:
    log: list[str] = []
    primary = _Spy(log, "primary", Role.AUTHORITY, expired=["k"])
    copy = _Spy(log, "copy", Role.DEPENDENT, fail=True)
    assert Topology((primary, copy)).gc(5, _now()) == ["k"]
    assert log == ["gc:primary", "batch_delete:copy"]


# --- nested trees ---------------------------------------------------------


@dataclass(frozen=True)
class Seal:
    row: str | None
    body: str | None


def _failing_set(store: Any) -> Any:
    def boom(*_a: Any, **_kw: Any) -> None:
        raise RuntimeError("backup down")

    store.set = boom
    return store


def test_a_dependent_failing_deep_inside_the_authority_reports_committed() -> None:
    """zip ⟶ row=mirror ⟶ backup fails after the mirror's primary committed.

    The mirror's commit point is the zip's commit point, so the record exists
    and the caller is told so — not handed a raw failure to interpret.
    """
    backup = _failing_set(_leaf("backup"))
    tree = KeyMap.zip_with(
        Seal,
        commit_part="row",
        row=_leaf("primary").mirror(backup, sync_policy=SyncPolicy.REQUIRED),
        body=_leaf("body"),
    )
    with pytest.raises(PartialSyncError) as caught:
        tree.set("t", Seal(row="R", body="B"))
    assert caught.value.committed is True
    assert tree.exists("t") is True


def test_the_same_failure_inside_a_component_reports_uncommitted() -> None:
    """Same mirror, same failure — but here it is a component, before the commit.

    The mirror says "my record committed"; the zip knows its own record did
    not, because its commit part was never reached.
    """
    backup = _failing_set(_leaf("backup"))
    row = _leaf("row")
    tree = KeyMap.zip_with(
        Seal,
        commit_part="row",
        row=row,
        body=_leaf("body").mirror(backup, sync_policy=SyncPolicy.REQUIRED),
    )
    with pytest.raises(UncommittedWriteError) as caught:
        tree.set("t", Seal(row="R", body="B"))
    assert caught.value.committed is False
    assert row.get("t") is None
    assert tree.exists("t") is False


def test_deleting_from_a_zip_removes_every_part() -> None:
    """Deletion used ``any()`` over a generator and stopped at the first hit."""
    a, b = _leaf("a"), _leaf("b")
    zipped = KeyMap.zip_with(Seal, row=a, body=b)
    zipped.set("k", Seal(row="R", body="B"))

    assert zipped.delete("k") is True
    assert a.get("k") is None
    assert b.get("k") is None, "a later part survived the delete"


# --- deferred index syncs reach the whole tree ----------------------------


class _SpyIndex:
    name = "spy"
    sync_on_write = True
    sync_policy = None

    def __init__(self, *, fail: bool = False) -> None:
        self.synced: list[str] = []
        self._fail = fail

    def bind_store(self, store: Any) -> None:
        self._store = store

    def sync(self, key: str, value: Any) -> None:
        if self._fail:
            raise RuntimeError("index down")
        self.synced.append(key)

    def batch_sync(self, items: Any) -> None: ...
    def delete(self, key: str) -> None: ...
    def batch_delete(self, keys: Any) -> None: ...
    def clone_with_scope(self, scope: Any) -> Any:
        return self


def _deferred_indexed(
    conn: sqlite3.Connection, name: str, index: _SpyIndex
) -> Any:
    sql_kvset = make_sql_kvset(name)
    ensure_sql_table(conn, sql_kvset)
    base = SqlDbKvStore(sql_kvset, adapter=SqliteSqlDbClientAdapter(conn))
    return base.rebind(SqliteSqlDbClientAdapter(conn, autocommit=False)).indexed(index)


def test_a_caller_holding_only_the_root_can_flush_every_index(
    sqlite_conn: sqlite3.Connection,
) -> None:
    index = _SpyIndex()
    tree = KeyMap.zip_with(
        Seal,
        commit_part="row",
        row=_deferred_indexed(sqlite_conn, "rows", index),
        body=_leaf("body"),
    )
    sqlite_conn.execute("BEGIN IMMEDIATE")
    tree.set("t", Seal(row="R", body="B"))
    assert tree.pending_index_syncs == 1, "the root cannot see the nested queue"
    sqlite_conn.commit()

    tree.flush_index_syncs()
    assert index.synced == ["t"]
    assert tree.pending_index_syncs == 0


def test_one_failing_index_does_not_strand_the_others(
    sqlite_conn: sqlite3.Connection,
) -> None:
    good, bad = _SpyIndex(), _SpyIndex(fail=True)
    tree = _deferred_indexed(sqlite_conn, "left", bad).mirror(
        _deferred_indexed(sqlite_conn, "right", good)
    )
    sqlite_conn.execute("BEGIN IMMEDIATE")
    tree.set("t", "v")
    sqlite_conn.commit()

    with pytest.raises(PartialSyncError):
        tree.flush_index_syncs()
    assert good.synced == ["t"], "a failure elsewhere stranded this index's work"
    assert tree.pending_index_syncs == 0


def test_discarding_reaches_the_whole_tree(sqlite_conn: sqlite3.Connection) -> None:
    index = _SpyIndex()
    tree = KeyMap.zip_with(
        Seal,
        commit_part="row",
        row=_deferred_indexed(sqlite_conn, "rows", index),
        body=_leaf("body"),
    )
    sqlite_conn.execute("BEGIN IMMEDIATE")
    tree.set("t", Seal(row="R", body="B"))
    sqlite_conn.rollback()

    tree.discard_index_syncs()
    assert tree.pending_index_syncs == 0
    assert index.synced == []


# --- a composition kvtsjl does not ship -----------------------------------


class WriteThroughCache[K, V](StoreTopologyMixin[K, V], DelegatingKvStore[K, V]):
    """A cache in front of a store, written through. Not part of kvtsjl.

    The cache is a dependent looked up before the store and filled on a miss.
    Every operation follows from those three declarations.
    """

    def __init__(self, store: Any, cache: Any) -> None:
        super().__init__(store)
        self._store, self._cache = store, cache

    def topology(self) -> Topology[K, V]:
        return Topology(
            (
                self._target("store", Role.AUTHORITY, self._store),
                self._target(
                    "cache",
                    Role.DEPENDENT,
                    self._cache,
                    lookup=Lookup.BEFORE_AUTHORITY,
                    fill=True,
                ),
            )
        )


def test_a_new_composition_needs_nothing_but_its_roles() -> None:
    store, cache = _leaf("store"), _leaf("cache")
    composed: Any = WriteThroughCache(store, cache)

    composed.set("k", "v")
    assert (store.get("k"), cache.get("k")) == ("v", "v")

    assert composed.set_if_absent("k", "other") is False
    assert cache.get("k") == "v", "a lost race was copied into the cache"

    assert composed.exists("k") is True
    assert composed.delete("k") is True
    assert (store.get("k"), cache.get("k")) == (None, None)


def test_its_reads_come_from_the_cache_first_and_fill_it() -> None:
    store, cache = _leaf("store"), _leaf("cache")
    composed: Any = WriteThroughCache(store, cache)
    store.set("k", "v")

    assert composed.get("k") == "v"
    assert cache.get("k") == "v", "a miss did not fill the cache"

    store.delete("k")
    assert composed.get("k") == "v", "the cache was not read first"
    assert sorted(composed.list()) == [], "the scan did not follow the authority"


def test_its_scan_is_the_authority_s_own_not_a_cache_s_copy() -> None:
    """A scan reads what decides existence; a stale cache must not leak in."""
    store, cache = _leaf("store"), _leaf("cache")
    composed: Any = WriteThroughCache(store, cache)
    store.set("k", "new")
    cache.set("k", "stale")
    assert list(composed.scan(include_values=True)) == [("k", "new")]
    query = ScanQuery[str](include_values=True)
    assert list(composed.scan_entries(query)) == [("k", "new")]


def test_delete_where_can_override_a_dependent_s_policy() -> None:
    secondary = _leaf("secondary")

    def down(*_a: Any, **_kw: Any) -> int:
        raise RuntimeError("secondary down")

    secondary.batch_delete = down
    mirrored: Any = _leaf("primary").mirror(secondary, sync_policy=SyncPolicy.REQUIRED)
    mirrored.set("k1", "v")
    mirrored.set("k2", "v")

    with pytest.raises(PartialSyncError):
        mirrored.delete_where(KeyPrefix("k1"))
    assert mirrored.delete_where(KeyPrefix("k2"), sync_policy=SyncPolicy.BEST_EFFORT) == 1


@dataclass(frozen=True)
class Doc:
    title: str
    views: int


class Docs(RecordTopologyMixin[str, Doc]):
    """A record split over parts of different types. Not part of kvtsjl."""

    def __init__(self, titles: KeyMap[str, str], views: KeyMap[str, int]) -> None:
        self._titles, self._views = titles, views

    def topology(self) -> Topology[str, Doc]:
        title = self._part("title", Role.AUTHORITY, self._titles, lambda d: d.title)
        views = self._part("views", Role.COMPONENT, self._views, lambda d: d.views)

        def assemble(held: Parts[str, Doc]) -> Doc | None:
            found = held[title]
            return None if found is None else Doc(found, held[views] or 0)

        return Topology(parts=(title, views), assemble=assemble)


def test_parts_are_read_back_by_the_part_that_holds_them() -> None:
    titles: KeyMap[str, str] = DictKeyMap({})
    views: KeyMap[str, int] = DictKeyMap({})
    docs = Docs(titles, views)

    docs.set("d", Doc("Hello", 3))
    assert (titles.get("d"), views.get("d")) == ("Hello", 3)
    assert docs.get("d") == Doc("Hello", 3)
    assert docs.batch_get(["d", "nope"]) == {"d": Doc("Hello", 3)}

    views.set("orphan", 9)
    assert docs.get("orphan") is None, "a part without its authority made a record"


def test_a_composition_whose_parts_all_share_existence_reads_it_back() -> None:
    """With no authority, existence is whether the assembled record reads."""
    a, b = _leaf("a"), _leaf("b")
    zipped = KeyMap.zip_with(Seal, row=a, body=b)
    zipped.set("k", Seal(row="R", body="B"))
    assert zipped.exists("k") is True
    assert zipped.exists("missing") is False
    with pytest.raises(KvStoreError, match="commit_part"):
        zipped.set_if_absent("k", Seal(row="R", body="B"))


def test_one_failing_operation_does_not_strand_the_rest_of_a_queue(
    sqlite_conn: sqlite3.Connection,
) -> None:
    """Within one node: an early queued sync failing must not drop later ones."""

    class FailsOn(_SpyIndex):
        def __init__(self, bad: str) -> None:
            super().__init__()
            self._bad = bad

        def sync(self, key: str, value: Any) -> None:
            if key == self._bad:
                raise RuntimeError("index down for this key")
            self.synced.append(key)

    index = FailsOn("first")
    store = _deferred_indexed(sqlite_conn, "queue", index)
    sqlite_conn.execute("BEGIN IMMEDIATE")
    store.set("first", "v")
    store.set("second", "v")
    sqlite_conn.commit()

    with pytest.raises(PartialSyncError) as caught:
        store.flush_index_syncs()
    assert index.synced == ["second"], "a failure stranded the operation after it"
    assert len(caught.value.failures) == 1
    assert store.pending_index_syncs == 0
