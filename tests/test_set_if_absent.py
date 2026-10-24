"""R15: insert without overwriting, as one operation.

A writer that must not replace an existing value had to ``get`` then ``set``,
leaving a window in which a concurrent writer's value is lost. This is the
medium's own conditional write instead, and no backend fakes it.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import sqlite3
import sys
import time
from typing import Any

import pytest

from kvtsjl import KvSet, MemoryKvStore, SerDe
from kvtsjl.backends.sql.sqlite import SqliteSqlDbClientAdapter
from kvtsjl.backends.sql.store import SqlDbKvStore
from kvtsjl.exceptions import KvStoreError
from kvtsjl.store.schema.ttl import ExpiryGc, TtlPolicy

sys.path.insert(0, "tests")
from tests.sql_helpers import ensure_sql_table, make_sql_kvset


def _memory(ttl: TtlPolicy | None = None) -> Any:
    return MemoryKvStore(
        KvSet.with_str_keys(
            "m",
            key_serde=SerDe.identity(str),
            value_serde=SerDe.identity(str),
            ttl_policy=ttl,
        )
    )


def _sql(conn: sqlite3.Connection, ttl: TtlPolicy | None = None, **kw: Any) -> Any:
    sql_kvset = make_sql_kvset("sia", ttl_policy=ttl)
    ensure_sql_table(conn, sql_kvset)
    return SqlDbKvStore(sql_kvset, adapter=SqliteSqlDbClientAdapter(conn), **kw)


def test_memory_inserts_once_and_never_replaces() -> None:
    store = _memory()
    assert store.set_if_absent("k", "first") is True
    assert store.set_if_absent("k", "second") is False
    assert store.get("k") == "first", "an existing value was replaced"


def test_sql_inserts_once_and_never_replaces(sqlite_conn: sqlite3.Connection) -> None:
    store = _sql(sqlite_conn)
    assert store.set_if_absent("k", "first") is True
    assert store.set_if_absent("k", "second") is False
    assert store.get("k") == "first"


def test_only_one_of_two_racing_writers_wins(sqlite_conn: sqlite3.Connection) -> None:
    """Exactly one call reports the write, and its value is the one stored."""
    store = _sql(sqlite_conn)
    results = [store.set_if_absent("k", f"writer-{i}") for i in range(5)]

    assert results.count(True) == 1, f"{results.count(True)} writers each claimed the key"
    winner = results.index(True)
    assert store.get("k") == f"writer-{winner}"


@pytest.mark.parametrize("gc", [ExpiryGc.LAZY_DELETE, ExpiryGc.HIDE])
def test_an_expired_entry_still_blocks_the_insert(
    sqlite_conn: sqlite3.Connection, gc: ExpiryGc
) -> None:
    """"Absent" means absent to the medium, which has no view of kvtsjl's TTL.

    So this returns False for a key ``get`` reports as missing. Reconciling the
    two would need a read-modify-write — the race the method exists to remove.
    A caller that wants expired entries treated as absent runs
    ``gc_expired_keys`` first.
    """
    sql_kvset = make_sql_kvset("exp", ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=10)))
    ensure_sql_table(sqlite_conn, sql_kvset)
    store = SqlDbKvStore(
        sql_kvset, adapter=SqliteSqlDbClientAdapter(sqlite_conn), expiry_gc=gc
    )
    store.set("k", "old")
    sqlite_conn.execute(
        f'UPDATE "{sql_kvset.table_name}" SET "{sql_kvset.updated_at_field}"=?, '
        f'"{sql_kvset.expires_at_field}"=?',
        (time.time() - 999, time.time() - 900),
    )
    sqlite_conn.commit()

    assert store.set_if_absent("k", "new") is False
    # ...and the documented escape hatch works
    store.gc_expired_keys(max_entries=10)
    assert store.set_if_absent("k", "new") is True
    assert store.get("k") == "new"


def test_a_backend_without_the_capability_raises_rather_than_racing() -> None:
    """Never a get-then-set: that is the race, not an implementation of it."""

    class NoPrimitive(SqliteSqlDbClientAdapter):
        def supports_insert_if_absent(self) -> bool:
            return False

    conn = sqlite3.connect(":memory:")
    sql_kvset = make_sql_kvset("nope")
    ensure_sql_table(conn, sql_kvset)
    store = SqlDbKvStore(sql_kvset, adapter=NoPrimitive(conn))

    with pytest.raises(KvStoreError, match="set_if_absent"):
        store.set_if_absent("k", "v")
    assert store.get("k") is None, "the refused write still landed"


def test_the_default_is_to_refuse() -> None:
    """A store that never implements it must raise, not silently race."""
    from kvtsjl.keymap import KeyMap

    with pytest.raises(KvStoreError, match="set_if_absent"):
        KeyMap.set_if_absent(object(), "k", "v")  # type: ignore[arg-type]


def test_it_respects_the_bound_scope() -> None:
    store = MemoryKvStore(
        KvSet.with_str_keys(
            "s",
            key_serde=SerDe.identity(str),
            value_serde=SerDe.identity(str),
            scope_schema=("user",),
        )
    )
    assert store.scoped(user="a").set_if_absent("k", "A") is True
    assert store.scoped(user="b").set_if_absent("k", "B") is True, "scopes collided"
    assert store.scoped(user="a").get("k") == "A"


# --- the capability must survive composition -------------------------------


def _mem_pair() -> tuple[Any, Any]:
    return _memory(), _memory()


def test_it_survives_mirror_and_the_secondary_only_copies_a_real_write() -> None:
    """A losing writer must not plant its value in the secondary."""
    primary, secondary = _mem_pair()
    store = primary.mirror(secondary)

    assert store.set_if_absent("k", "first") is True
    assert secondary.get("k") == "first"

    assert store.set_if_absent("k", "second") is False
    assert primary.get("k") == "first"
    assert secondary.get("k") == "first", "the losing write reached the secondary"


def test_it_survives_fallback_read() -> None:
    primary, source = _mem_pair()
    store = primary.fallback_read(source)

    assert store.set_if_absent("k", "v") is True
    assert store.set_if_absent("k", "other") is False
    assert primary.get("k") == "v"


def test_it_survives_a_mapped_view() -> None:
    inner = _memory()
    view = inner.imap(lambda s: s.upper(), lambda s: s.lower())

    assert view.set_if_absent("k", "ABC") is True
    assert inner.get("k") == "abc", "the inverse was not applied"
    assert view.set_if_absent("k", "XYZ") is False
    assert view.get("k") == "ABC"


def test_readonly_refuses_it_as_a_write() -> None:
    from kvtsjl.exceptions import KvStoreReadOnlyError

    store = _memory()
    store.set_if_absent("k", "v")
    with pytest.raises(KvStoreReadOnlyError, match="set_if_absent"):
        store.readonly().set_if_absent("j", "v")


def test_an_indexed_store_indexes_only_a_write_that_happened() -> None:
    """A losing writer must not sync: the stored value is someone else's.

    Indexing our value would describe a record that is not there — search
    would return a hit whose hydrated value is the winner's, or nothing.
    """
    synced: list[tuple[str, str]] = []

    class SpyIndex:
        name = "spy"
        sync_on_write = True
        sync_policy = None

        def bind_store(self, store: Any) -> None:
            self._store = store

        def sync(self, key: str, value: str) -> None:
            synced.append((key, value))

        def batch_sync(self, items: Any) -> None: ...
        def delete(self, key: str) -> None: ...
        def batch_delete(self, keys: Any) -> None: ...
        def clone_with_scope(self, scope: Any) -> Any:
            return self

    store = _memory().indexed(SpyIndex())

    assert store.set_if_absent("k", "first") is True
    assert synced == [("k", "first")]

    assert store.set_if_absent("k", "second") is False
    assert synced == [("k", "first")], "indexed a write that never happened"


# --- every medium has the primitive ----------------------------------------
#
# A conditional write is one request the medium makes conditional, never a
# read followed by a write. Each leaf is checked on its own medium (or the
# fake that enforces the medium's precondition) — the same three facts.

_MEDIA = ["memory", "sql", "redis", "redis_hash", "s3", "gcs", "azure", "filesystem"]
# The modes that keep expiry in a header, sidecar or metadata of their own: a
# conditional write that dropped its headers would pass in the default modes.
_HEADER_MODES = ["s3_expires", "gcs_custom_time", "azure_metadata", "filesystem_sidecar"]
_TTL = TtlPolicy(ttl_duration=timedelta(hours=1))


def _kvset(ttl: TtlPolicy | None) -> Any:
    return KvSet.with_str_keys(
        "media",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        ttl_policy=ttl,
    )


def _medium(name: str, request: pytest.FixtureRequest, ttl: TtlPolicy | None) -> Any:
    import fakeredis

    from kvtsjl.backends.azure import AzureBlobKvStore, AzureTtlMode
    from kvtsjl.backends.filesystem import FilesystemKvStore, FilesystemTtlMode
    from kvtsjl.backends.gcs import GcsKvStore, GcsTtlMode
    from kvtsjl.backends.redis import RedisKvStore
    from kvtsjl.backends.s3 import S3KvStore, S3TtlMode
    from tests.fake_azure import FakeAzureContainer
    from tests.fake_gcs import FakeGcsBucket

    kvset = _kvset(ttl)
    if name == "memory":
        return MemoryKvStore(kvset)
    if name == "sql":
        return _sql(request.getfixturevalue("sqlite_conn"), ttl)
    if name == "redis":
        return RedisKvStore.flat(kvset, fakeredis.FakeRedis())
    if name == "redis_hash":
        return RedisKvStore.hash_collection(kvset, fakeredis.FakeRedis())
    if name in ("s3", "s3_expires"):
        client = request.getfixturevalue("s3_client")
        bucket = request.getfixturevalue("s3_bucket")
        mode = S3TtlMode.EXPIRES if name == "s3_expires" else S3TtlMode.OBJECT_TIME
        return S3KvStore(
            kvset, client=client, bucket=bucket, key_prefix="app/", ttl_mode=mode
        )
    if name in ("gcs", "gcs_custom_time"):
        mode = GcsTtlMode.CUSTOM_TIME if name == "gcs_custom_time" else GcsTtlMode.OBJECT_TIME
        return GcsKvStore(kvset, bucket=FakeGcsBucket(), key_prefix="app/", ttl_mode=mode)  # type: ignore[arg-type]
    if name in ("azure", "azure_metadata"):
        mode = AzureTtlMode.METADATA if name == "azure_metadata" else AzureTtlMode.OBJECT_TIME
        return AzureBlobKvStore(
            kvset, container=FakeAzureContainer(), key_prefix="app/", ttl_mode=mode  # type: ignore[arg-type]
        )
    if name in ("filesystem", "filesystem_sidecar"):
        mode = (
            FilesystemTtlMode.SIDECAR
            if name == "filesystem_sidecar"
            else FilesystemTtlMode.MTIME
        )
        return FilesystemKvStore(
            kvset, root=request.getfixturevalue("tmp_path"), ttl_mode=mode
        )
    raise AssertionError(name)


@pytest.mark.parametrize("medium", _MEDIA)
def test_every_medium_inserts_once_and_never_replaces(
    medium: str, request: pytest.FixtureRequest
) -> None:
    store = _medium(medium, request, None)
    assert store.set_if_absent("k", "first") is True
    assert store.set_if_absent("k", "second") is False
    assert store.get("k") == "first", "the refused write replaced the value"
    assert store.set_if_absent("j", "other") is True, "a different key was refused"
    assert sorted(store.list()) == ["j", "k"]


@pytest.mark.parametrize("medium", [m for m in _MEDIA if m != "redis_hash"] + _HEADER_MODES)
def test_every_medium_expires_a_conditional_write_like_a_plain_one(
    medium: str, request: pytest.FixtureRequest
) -> None:
    """Same TTL headers either way: an inserted value must not outlive its policy.

    A hash collection expires as a whole and cannot take a TTL policy, so it
    is left out, as everywhere else.
    """
    store = _medium(medium, request, _TTL)
    store.set("plain", "v")
    assert store.set_if_absent("conditional", "v") is True
    plain, conditional = store.stat("plain"), store.stat("conditional")
    assert plain is not None and plain.expires_at is not None
    assert conditional is not None and conditional.expires_at is not None, (
        "the conditional write dropped the expiry"
    )
    assert abs((conditional.expires_at - plain.expires_at).total_seconds()) < 5


@pytest.mark.parametrize("medium", [m for m in _MEDIA if m != "redis_hash"] + _HEADER_MODES)
def test_every_medium_with_a_write_clock_keeps_it_on_a_conditional_write(
    medium: str, request: pytest.FixtureRequest
) -> None:
    """A copied value keeps its remaining life only if the clock survives."""
    store = _medium(medium, request, _TTL)
    if not store.supports_write_clock:
        pytest.skip(f"{medium} has no write clock in this mode")
    clock = datetime.now(UTC) - timedelta(minutes=30)
    store.set("plain", "v", created_at=clock)
    assert store.set_if_absent("conditional", "v", created_at=clock) is True
    plain, conditional = store.stat("plain"), store.stat("conditional")
    assert plain is not None and conditional is not None
    assert conditional.expires_at is not None and plain.expires_at is not None
    # Media that report expiry from the time left drift by microseconds
    # between two reads; a dropped clock is off by the 30 minutes above.
    drift = abs((conditional.expires_at - plain.expires_at).total_seconds())
    assert drift < 1, "the clock was dropped"


@pytest.mark.parametrize("medium", [m for m in _MEDIA if m != "redis_hash"] + _HEADER_MODES)
def test_every_medium_applies_a_per_write_ttl_to_a_conditional_write(
    medium: str, request: pytest.FixtureRequest
) -> None:
    """Header modes write their own expiry only for a per-write ``ttl=``."""
    from kvtsjl.exceptions import KvStoreTtlUnsupported

    store = _medium(medium, request, _TTL)
    longer = TtlPolicy(ttl_duration=timedelta(hours=5))
    try:
        store.set("plain", "v", ttl=longer)
    except KvStoreTtlUnsupported:
        pytest.skip(f"{medium} takes no per-write TTL in this mode")
    assert store.set_if_absent("conditional", "v", ttl=longer) is True
    plain, conditional = store.stat("plain"), store.stat("conditional")
    assert plain is not None and plain.expires_at is not None
    assert conditional is not None and conditional.expires_at is not None
    drift = abs((conditional.expires_at - plain.expires_at).total_seconds())
    assert drift < 1, "the per-write TTL was dropped for the policy's"


def test_s3_asks_again_while_another_conditional_put_is_in_flight() -> None:
    """A 409 means the outcome is someone else's write; it is asked again."""
    from botocore.exceptions import ClientError

    from kvtsjl.backends.object.s3_driver import S3BlobDriver

    def error(code: str, status: int) -> ClientError:
        return ClientError(
            {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}},
            "PutObject",
        )

    class Conflicted:
        def __init__(self, answers: list[ClientError | None]) -> None:
            self.answers, self.calls = answers, 0

        def put_object(self, **kwargs: Any) -> None:
            assert kwargs["IfNoneMatch"] == "*"
            self.calls += 1
            answer = self.answers.pop(0)
            if answer is not None:
                raise answer

    lost = Conflicted([error("ConditionalRequestConflict", 409), error("PreconditionFailed", 412)])
    assert S3BlobDriver(lost, "b").write_if_absent("k", b"v") is False  # type: ignore[arg-type]
    assert lost.calls == 2

    won = Conflicted([error("ConditionalRequestConflict", 409), None])
    assert S3BlobDriver(won, "b").write_if_absent("k", b"v") is True  # type: ignore[arg-type]

    stuck = Conflicted([error("ConditionalRequestConflict", 409)] * 3)
    with pytest.raises(ClientError, match="ConditionalRequestConflict"):
        S3BlobDriver(stuck, "b").write_if_absent("k", b"v")  # type: ignore[arg-type]
    assert stuck.calls == 3, "gave up before the attempts ran out"


# --- plain keymaps -----------------------------------------------------------


def test_a_dict_keymap_inserts_once() -> None:
    from kvtsjl.keymap_algebra.dict_map import DictKeyMap

    keymap: DictKeyMap[str, str] = DictKeyMap({})
    assert keymap.set_if_absent("k", "first") is True
    assert keymap.set_if_absent("k", "second") is False
    assert keymap.get("k") == "first"


def test_keymap_views_forward_it_through_their_mapping() -> None:
    from kvtsjl.keymap_algebra.dict_map import DictKeyMap

    inner: DictKeyMap[str, str] = DictKeyMap({})
    upper = inner.imap(lambda s: s.upper(), lambda s: s.lower())
    assert upper.set_if_absent("k", "ABC") is True
    assert inner.get("k") == "abc", "the inverse was not applied"
    assert upper.set_if_absent("k", "XYZ") is False

    renamed = inner.imap_keys(lambda k: f"n:{k}", lambda k: k[2:])
    assert renamed.set_if_absent("j", "v") is True
    assert inner.get("n:j") == "v", "the key was not translated"
    assert renamed.set_if_absent("j", "w") is False
