"""FilesystemKvStore tests."""

from __future__ import annotations

from datetime import timedelta
import os
from pathlib import Path
import time

from freezegun import freeze_time
import pytest

from kvtsjl import ExpiryGc, FilesystemKvStore, KeyLayout, KvSet, SerDe, TtlPolicy
from kvtsjl.backends.filesystem import FilesystemTtlMode
from kvtsjl.exceptions import KvStoreScanUnsupported, KvStoreTtlUnsupported
from tests.conformance import assert_basic_crud, assert_batch_ops, assert_scan_and_scope


@pytest.fixture
def store(
    str_bytes_kvset: KvSet[str, str, str, bytes], tmp_path: Path
) -> FilesystemKvStore[str, str, str]:
    return FilesystemKvStore(str_bytes_kvset, root=tmp_path)


def test_filesystem_crud(store: FilesystemKvStore[str, str, str]) -> None:
    assert_basic_crud(store)


def test_filesystem_batch(store: FilesystemKvStore[str, str, str]) -> None:
    assert_batch_ops(store)


def test_filesystem_batch_get_matches_point_get(
    str_bytes_kvset: KvSet[str, str, str, bytes], tmp_path: Path
) -> None:
    store = FilesystemKvStore(str_bytes_kvset, root=tmp_path, io_concurrency=4)
    store.set("a", "va")
    store.set("c", "vc")
    keys = ["a", "b", "c", "missing"]
    batched = store.batch_get(keys)
    for key in keys:
        assert batched.get(key) == store.get(key)
    assert "b" not in batched
    assert "missing" not in batched
    assert store.batch_get([]) == {}


def test_filesystem_batch_set_concurrent(
    str_bytes_kvset: KvSet[str, str, str, bytes], tmp_path: Path
) -> None:
    store = FilesystemKvStore(str_bytes_kvset, root=tmp_path, io_concurrency=4)
    store.batch_set({})
    items = {f"k{i}": f"v{i}" for i in range(20)}
    store.batch_set(items)
    assert store.batch_get(list(items)) == items


def test_filesystem_batch_delete_concurrent(
    str_bytes_kvset: KvSet[str, str, str, bytes], tmp_path: Path
) -> None:
    store = FilesystemKvStore(str_bytes_kvset, root=tmp_path, io_concurrency=4)
    store.batch_set({f"k{i}": f"v{i}" for i in range(10)})
    assert store.batch_delete([f"k{i}" for i in range(10)] + ["missing"]) == 10
    assert store.batch_get([f"k{i}" for i in range(10)]) == {}


def test_filesystem_batch_get_concurrent_lazy_delete_idempotent(tmp_path: Path) -> None:
    kvset = KvSet.with_str_keys(
        "ttl",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=30)),
    )
    store = FilesystemKvStore(kvset, root=tmp_path, io_concurrency=8)
    store.set("expired", "gone")
    path = next(p for p in tmp_path.rglob("*") if p.is_file())
    old = time.time() - 120
    os.utime(path, (old, old))
    # Duplicate keys force overlapping workers on the same expired object.
    assert store.batch_get(["expired"] * 16) == {}
    assert store.get("expired") is None
    assert not path.exists()


def test_filesystem_io_concurrency_rejects_zero(
    str_bytes_kvset: KvSet[str, str, str, bytes], tmp_path: Path
) -> None:
    with pytest.raises(ValueError, match="io_concurrency"):
        FilesystemKvStore(str_bytes_kvset, root=tmp_path, io_concurrency=0)


def test_filesystem_scan_and_scope(store: FilesystemKvStore[str, str, str]) -> None:
    assert_scan_and_scope(store)


def test_filesystem_no_meta_sidecars(
    store: FilesystemKvStore[str, str, str], tmp_path: Path
) -> None:
    store.set("a", "v")
    files = [p for p in tmp_path.rglob("*") if p.is_file()]
    assert files
    assert not any(p.name.endswith(".meta.json") for p in files)


def test_filesystem_ttl_via_mtime(tmp_path: Path) -> None:
    kvset = KvSet.with_str_keys(
        "ttl",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=30)),
    )
    store = FilesystemKvStore(kvset, root=tmp_path)
    store.set("a", "live")
    assert store.get("a") == "live"
    path = next(p for p in tmp_path.rglob("*") if p.is_file())
    old = time.time() - 120
    os.utime(path, (old, old))
    assert store.get("a") is None
    assert not path.exists()


def test_filesystem_expiry_gc_hide(tmp_path: Path) -> None:
    kvset = KvSet.with_str_keys(
        "ttl",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=30)),
    )
    store = FilesystemKvStore(kvset, root=tmp_path, expiry_gc=ExpiryGc.HIDE)
    store.set("a", "live")
    path = next(p for p in tmp_path.rglob("*") if p.is_file())
    old = time.time() - 120
    os.utime(path, (old, old))
    assert store.get("a") is None
    assert path.exists()


def test_filesystem_explicit_ttl_raises_by_default(tmp_path: Path) -> None:
    kvset = KvSet.with_str_keys(
        "ttl",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        ttl_policy=TtlPolicy.hourly(),
    )
    store = FilesystemKvStore(kvset, root=tmp_path)
    with pytest.raises(KvStoreTtlUnsupported):
        store.set("a", "v", ttl=TtlPolicy.hourly())


def test_filesystem_sidecar_ttl_and_none(tmp_path: Path) -> None:
    kvset = KvSet.with_str_keys(
        "ttl",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        ttl_policy=TtlPolicy.hourly(),
    )
    store = FilesystemKvStore(kvset, root=tmp_path, ttl_mode=FilesystemTtlMode.SIDECAR)
    with freeze_time("2024-01-01 12:00:00") as frozen:
        store.set("short", "b", ttl=TtlPolicy(ttl_duration=timedelta(seconds=30)))
        store.set("pinned", "c", ttl=TtlPolicy.none())
        store.set("plain", "d")
        data_files = [
            p for p in tmp_path.rglob("*") if p.is_file() and not str(p).endswith(".expires")
        ]
        assert any(Path(str(p) + ".expires").is_file() for p in data_files)
        frozen.move_to("2024-01-01 12:01:00")
        assert store.get("short") is None
        assert store.get("pinned") == "c"
        assert store.get("plain") == "d"
        store.delete("pinned")
        assert not any(str(p).endswith(".expires") for p in tmp_path.rglob("*") if p.is_file())


def test_filesystem_hashed_scan_unsupported(tmp_path: Path) -> None:
    kvset = KvSet.with_str_keys(
        "hashed",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        key_layout=KeyLayout.HASHED,
    )
    store = FilesystemKvStore(kvset, root=tmp_path)
    store.set("k", "v")
    assert store.get("k") == "v"
    with pytest.raises(KvStoreScanUnsupported):
        list(store.scan())
