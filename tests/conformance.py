"""Shared assertions for KvStore backends."""

from __future__ import annotations

from collections.abc import Callable

from kvtsjl.store import KvStore

StoreFactory = Callable[[], KvStore[str, str, str, bytes, object]]


def assert_basic_crud(store: KvStore[str, str, str, bytes, object]) -> None:
    assert store.get("missing") is None
    store.set("a", "one")
    assert store.get("a") == "one"
    store.set("a", "two")
    assert store.get("a") == "two"
    assert store.delete("a") is True
    assert store.get("a") is None
    assert store.delete("a") is False


def assert_batch_ops(store: KvStore[str, str, str, bytes, object]) -> None:
    store.batch_set({"k1": "v1", "k2": "v2", "k3": "v3"})
    assert store.batch_get(["k1", "k2", "missing"]) == {"k1": "v1", "k2": "v2"}
    assert store.batch_delete(["k1", "k3", "missing"]) == 2
    assert store.get("k2") == "v2"
    assert store.get("k1") is None


def assert_scan_and_scope(store: KvStore[str, str, str, bytes, object]) -> None:
    store.set("alpha", "1")
    store.set("beta", "2")
    assert sorted(store.list()) == ["alpha", "beta"]
    assert dict(store.scan(include_values=True)) == {"alpha": "1", "beta": "2"}

    scoped = store.scoped(region="us")
    scoped.set("alpha", "scoped")
    assert scoped.get("alpha") == "scoped"
    assert store.get("alpha") == "1"
    assert sorted(scoped.list()) == ["alpha"]
