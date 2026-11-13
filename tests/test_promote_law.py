"""A promote never writes a value back into the store it was read from.

``cache.mirror(durable).fallback_read(durable)`` promotes through the mirror,
so every cold read rewrote ``durable`` with what it had just read. Built that
way with ``promote=True``, the composition refuses; the right shapes —
``cache.fallback_read(durable)`` to read, ``durable.mirror(cache)`` to write —
and ``promote=False`` are allowed. "The same store" is the same object: two
scopes of one table are two stores, and copying between them is a migration.
"""

from __future__ import annotations

from typing import Any

import pytest

from kvtsjl import KvSet, MemoryKvStore, SerDe
from kvtsjl.exceptions import KvStoreComposeError
from kvtsjl.store.logical import KvStore


def _store(name: str, *, scoped: bool = False) -> KvStore[str, str]:
    return MemoryKvStore(
        KvSet.with_str_keys(
            name,
            key_serde=SerDe.identity(str),
            value_serde=SerDe.identity(str),
            scope_schema=("tenant",) if scoped else None,
        )
    )


def test_a_promote_through_a_mirror_into_its_fallback_refuses() -> None:
    cache, durable = _store("cache"), _store("durable")
    with pytest.raises(KvStoreComposeError, match=r"durable\.mirror\(cache\)"):
        cache.mirror(durable).fallback_read(durable)


def test_the_shared_store_is_found_however_deep_it_sits() -> None:
    cache, durable = _store("cache"), _store("durable")
    same: Any = durable.imap(lambda v: v, lambda v: v)
    with pytest.raises(KvStoreComposeError):
        cache.mirror(durable).fallback_read(same)
    with pytest.raises(KvStoreComposeError):
        durable.fallback_read(cache.mirror(durable))


def test_a_shared_store_in_any_part_of_a_composition_is_found() -> None:
    """The durable store is the first of the mirror's parts, not the last."""
    cache, durable = _store("cache"), _store("durable")
    with pytest.raises(KvStoreComposeError):
        durable.mirror(cache).fallback_read(durable)


def test_without_promote_the_same_shape_reads_through() -> None:
    cache, durable = _store("cache"), _store("durable")
    durable.set("a", "1")
    reads = cache.mirror(durable).fallback_read(durable, promote=False)
    assert reads.get("a") == "1"
    assert cache.get("a") is None


def test_the_right_shapes_are_allowed() -> None:
    cache, durable = _store("cache"), _store("durable")
    reads = cache.fallback_read(durable)
    writes = durable.mirror(cache)
    writes.set("a", "1")
    cache.delete("a")
    assert reads.get("a") == "1"
    assert cache.get("a") == "1", "the read did not promote"


def test_two_scopes_of_one_store_are_two_stores() -> None:
    table = _store("table", scoped=True)
    old, new = table.scoped(tenant="old"), table.scoped(tenant="new")
    old.set("a", "1")
    assert new.fallback_read(old).get("a") == "1"
    assert new.get("a") == "1"
