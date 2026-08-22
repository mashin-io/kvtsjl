"""FilesystemKvStore tests."""

from __future__ import annotations

import os
from pathlib import Path
import time

import pytest

from kvtsjl import FilesystemKvStore, KeyLayout, KvSet, SerDe, TtlPolicy
from kvtsjl.exceptions import KvStoreScanUnsupported
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
    from datetime import timedelta

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
