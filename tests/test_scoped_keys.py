"""R9: scope ids cannot forge a key boundary.

``encode_in_key`` joined kind/id/leaf with ``:`` and escaped nothing, so an id
containing ``:`` produced a key another scope could also produce — and the two
scopes then shared a row.
"""

from __future__ import annotations

import pytest

from kvtsjl import KvSet, MemoryKvStore, SerDe
from kvtsjl.exceptions import KvStoreScopeError
from kvtsjl.scope import Scope


def kvset(*, schema: tuple[str, ...] | None = ("user", "slug")) -> KvSet[str, str, bytes, bytes]:
    return KvSet.with_str_keys(
        "ltm",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.identity(str),
        scope_schema=schema,
    )


def test_two_scopes_no_longer_share_a_row() -> None:
    """The reported bug, end to end through a real store.

    ``user="a:slug:b", slug="c"`` and ``user="a", slug="b:slug:c"`` both used
    to encode to ``user:a:slug:b:slug:c:sha``.
    """
    store = MemoryKvStore(kvset())
    victim = store.scoped(user="a", slug="b:slug:c")
    other = store.scoped(user="a:slug:b", slug="c")

    victim.set("sha", "VICTIM")
    other.set("sha", "OTHER")

    assert victim.get("sha") == "VICTIM", "another scope overwrote this row"
    assert other.get("sha") == "OTHER"


def test_scope_ids_are_injective() -> None:
    kv = kvset()
    pairs = [
        (("a:slug:b", "c"), "sha"),
        (("a", "b:slug:c"), "sha"),
        (("a%3Ab", "c"), "sha"),
        (("a", "b"), "sha"),
        (("a", "b"), "other"),
        (("%", ":"), "sha"),
    ]
    keys = [kv.encode_in_key(Scope.of(user=u, slug=s), leaf) for (u, s), leaf in pairs]
    assert len(set(keys)) == len(keys), f"collision among {keys}"


@pytest.mark.parametrize(
    "user,slug,leaf",
    [
        ("alice", "notes", "sha"),
        ("a:b", "c%d", "x:y"),
        ("ünï", "sl ug", "ß:1"),
        ("a", "", "k"),
        ("%3A", "%25", "%"),
    ],
)
def test_round_trip(user: str, slug: str, leaf: str) -> None:
    kv = kvset()
    scope = Scope.of(user=user, slug=slug)
    assert kv.decode_in_key(kv.encode_in_key(scope, leaf)) == (scope, leaf)


@pytest.mark.parametrize("leaf", ["plain", "http://x/y", "2026-10-20T12:00:00Z", "ns:id:3"])
def test_leaves_and_plain_ids_are_byte_identical(leaf: str) -> None:
    """Existing data must still read: the leaf is never escaped under a schema."""
    kv = kvset()
    encoded = kv.encode_in_key(Scope.of(user="alice", slug="notes"), leaf)
    assert encoded == f"user:alice:slug:notes:{leaf}"


def test_a_write_needs_the_full_scope() -> None:
    """Arity must be fixed, or a missing segment is indistinguishable from the leaf.

    ``scope(user="a") + leaf "slug:x:sha"`` and ``scope(user="a", slug="x") +
    leaf "sha"`` would otherwise be the same key.
    """
    kv = kvset()
    with pytest.raises(KvStoreScopeError, match="full scope"):
        kv.encode_in_key(Scope.of(user="a"), "slug:x:sha")
    with pytest.raises(KvStoreScopeError, match="full scope"):
        MemoryKvStore(kv).scoped(user="a").set("sha", "v")


def test_a_prefix_scope_is_still_valid_for_reads_and_scans() -> None:
    kv = kvset()
    assert kv.encode_in_key(Scope.of(user="a"), None) == "user:a"
    store = MemoryKvStore(kv)
    store.scoped(user="a", slug="b").set("sha", "v")
    # The bound scope only strips its own prefix, so the rest of the in-key
    # comes back as the leaf. Recovering (scope, leaf) here is R8's job.
    assert list(store.scoped(user="a").list()) == ["slug:b:sha"]


@pytest.mark.parametrize(
    "foreign", ["other:a:thing:b:sha", "user:a:sha", "nonsense", ""]
)
def test_foreign_keys_decode_to_none(foreign: str) -> None:
    assert kvset().decode_in_key(foreign) is None


def test_a_schema_less_scoped_kvset_escapes_the_leaf_instead() -> None:
    """Without a declared arity the leaf carries the escaping, and round-trips."""
    kv = kvset(schema=None)
    store = MemoryKvStore(kv).scoped(tenant="t1")
    store.set("ns:id:3", "V")
    store.set("plain", "W")
    assert store.get("ns:id:3") == "V"
    assert sorted(store.list()) == ["ns:id:3", "plain"]


def test_a_schema_less_unscoped_kvset_leaves_the_leaf_alone() -> None:
    kv = kvset(schema=None)
    assert kv.encode_in_key(Scope.empty(), "ns:id:3") == "ns:id:3"


def test_a_schema_less_kvset_does_not_collide_across_arities() -> None:
    """The reason the schema-less leaf is escaped at all.

    Without a declared arity, ``scope(a=1) + leaf "b:2:x"`` and
    ``scope(a=1, b=2) + leaf "x"`` would both encode to ``a:1:b:2:x``. A
    round-trip test cannot see this — both halves cancel out — so it takes two
    scopes writing through one store.
    """
    store = MemoryKvStore(kvset(schema=None))
    one = store.scoped(a="1")
    two = store.scoped(a="1", b="2")

    one.set("b:2:x", "ONE")
    two.set("x", "TWO")

    assert one.get("b:2:x") == "ONE", "the deeper scope overwrote this row"
    assert two.get("x") == "TWO"


# --- R8: recover the scope a key was written under ---------------------------


def _seeded() -> MemoryKvStore[str, str, str, str]:
    store = MemoryKvStore(kvset())
    store.scoped(user="alice", slug="notes").set("sha1", "A")
    store.scoped(user="alice", slug="a:b").set("sha2", "B")
    store.scoped(user="bob", slug="notes").set("sha3", "C")
    return store


def test_scan_scoped_yields_every_entry_with_its_full_scope() -> None:
    got = {
        (s.segments[0].id, s.segments[1].id, leaf)
        for s, leaf, _ in _seeded().scan_scoped()
    }
    assert got == {
        ("alice", "notes", "sha1"),
        ("alice", "a:b", "sha2"),   # separator inside an id survives
        ("bob", "notes", "sha3"),
    }


def test_scan_scoped_on_a_partial_scope_keeps_the_filter_and_fills_the_scope() -> None:
    got = {
        (s.segments[0].id, s.segments[1].id, leaf)
        for s, leaf, _ in _seeded().scoped(user="alice").scan_scoped()
    }
    assert got == {("alice", "notes", "sha1"), ("alice", "a:b", "sha2")}


def test_scan_scoped_carries_values_when_asked() -> None:
    rows = sorted(
        (leaf, value) for _, leaf, value in _seeded().scan_scoped(include_values=True)
    )
    assert rows == [("sha1", "A"), ("sha2", "B"), ("sha3", "C")]
    assert all(v is None for _, _, v in _seeded().scan_scoped())


def test_scan_scoped_pages_without_dropping_or_duplicating() -> None:
    store = MemoryKvStore(kvset())
    for i in range(25):
        store.scoped(user="u", slug=f"s{i:02d}").set(f"k{i:02d}", "v")
    full = sorted(leaf for _, leaf, _ in store.scan_scoped())
    for page in (1, 2, 7, 25, 100):
        paged = sorted(leaf for _, leaf, _ in store.scan_scoped(page_size=page))
        assert paged == full, f"page_size={page}"
    assert len(full) == 25


def test_scan_scoped_refuses_a_hashed_layout() -> None:
    from kvtsjl.exceptions import KvStoreError
    from kvtsjl.store.schema.layout import KeyLayout

    hashed = KvSet.with_str_keys(
        "h",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.identity(str),
        scope_schema=("user",),
        key_layout=KeyLayout.HASHED,
    )
    store = MemoryKvStore(hashed)
    assert store.supports_scoped_scan() is False
    with pytest.raises(KvStoreError, match="cannot recover scopes"):
        list(store.scan_scoped())


def test_scan_scoped_refuses_a_kvset_without_a_schema() -> None:
    from kvtsjl.exceptions import KvStoreError

    store = MemoryKvStore(kvset(schema=None))
    assert store.supports_scoped_scan() is False
    with pytest.raises(KvStoreError, match="cannot recover scopes"):
        list(store.scan_scoped())


def test_plain_scan_is_unchanged() -> None:
    """R8 adds a method; it must not alter what scan() returns."""
    store = _seeded()
    assert sorted(store.scoped(user="bob", slug="notes").list()) == ["sha3"]
    assert sorted(store.scoped(user="alice").list()) == [
        "slug:a%3Ab:sha2",
        "slug:notes:sha1",
    ]
