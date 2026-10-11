"""P7 Scan v2 integration gates."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any

from freezegun import freeze_time
import pytest

from kvtsjl import (
    ExpiryGc,
    FilesystemKvStore,
    KvSet,
    KvStore,
    MemoryKeyIndex,
    MemoryKvStore,
    PartialSyncError,
    SerDe,
    SyncPolicy,
    TtlPolicy,
)
from kvtsjl.exceptions import KvStoreScanUnsupported
from kvtsjl.store.schema.scan import (
    Expired,
    KeyPrefix,
    KeyRange,
    Projection,
    Pushdown,
    ScanCursor,
    ScanOrder,
    ScanSpec,
    ValueWhere,
)


def _mem(
    *,
    ttl_policy: TtlPolicy | None = None,
    expiry_gc: ExpiryGc = ExpiryGc.LAZY_DELETE,
) -> MemoryKvStore[str, str, str, bytes]:
    kwargs: dict[str, Any] = {}
    if ttl_policy is not None:
        kwargs["ttl_policy"] = ttl_policy
    kvset = KvSet.with_str_keys(
        "p7",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        **kwargs,
    )
    return MemoryKvStore(kvset, expiry_gc=expiry_gc)


def test_scan_sugar_prefix_and_where() -> None:
    store = _mem()
    store.batch_set({"inv/a": "1", "inv/b": "2", "other": "3"})
    assert sorted(store.scan(prefix="inv/")) == ["inv/a", "inv/b"]
    assert sorted(store.scan(where=KeyPrefix("inv/"))) == ["inv/a", "inv/b"]
    assert store.count(where=KeyPrefix("inv/")) == 2


def test_limit_and_key_range() -> None:
    store = _mem()
    store.batch_set({k: k for k in ("a", "b", "c", "d")})
    assert list(store.scan(where=KeyRange(start="b", end="d"), limit=2)) == [
        "b",
        "c",
    ]


def test_memory_cursor_start_after() -> None:
    store = _mem()
    store.batch_set({k: k for k in ("a", "b", "c", "d")})
    first = list(
        store.scan_rows(
            ScanSpec(project=Projection.KEYS, limit=2, order=ScanOrder.UNORDERED)
        )
    )
    assert len(first) == 2
    # Resume after first page's last key (memory seek).
    last = first[-1].key
    rest = list(
        store.scan(
            spec=ScanSpec(
                project=Projection.KEYS,
                cursor=ScanCursor(token=last),
            )
        )
    )
    assert last not in rest
    assert set(rest) | {r.key for r in first} == {"a", "b", "c", "d"}


def test_strict_raises_on_residual() -> None:
    store = _mem()
    store.set("a", "1")
    with pytest.raises(KvStoreScanUnsupported, match="residual"):
        list(
            store.scan(
                where=ValueWhere(lambda v: True),
                on_unsupported=Pushdown.STRICT,
            )
        )


def test_filesystem_keyprefix_listing_narrower(
    tmp_path: Path, str_bytes_kvset: KvSet[str, str, str, bytes]
) -> None:
    store = FilesystemKvStore(str_bytes_kvset, root=tmp_path)
    store.batch_set(
        {
            "al": "0",
            "alp": "0",
            "alpha": "0",
            "inv": "0",
            "inv/a": "1",
            "inv/b": "2",
            "other/x": "3",
            "zzz": "4",
        }
    )
    # Result set vs brute-force first (silent-empty is the failure mode).
    assert sorted(store.scan(prefix="al")) == ["al", "alp", "alpha"]
    assert sorted(store.scan(where=KeyPrefix("inv"))) == ["inv", "inv/a", "inv/b"]
    assert sorted(store.scan(where=KeyPrefix("inv/"))) == ["inv/a", "inv/b"]

    listed_after: list[str] = []
    driver = store.backend._driver
    orig_list = driver.list
    collection = store.backend._collection_prefix

    def counting_list(*, prefix: str):
        keys = list(orig_list(prefix=prefix))
        if prefix != collection:
            listed_after.extend(s.key for s in keys)
        return iter(keys)

    wide = list(orig_list(prefix=collection))
    assert len(wide) >= 8

    driver.list = counting_list  # type: ignore[method-assign]
    rows = list(store.scan(where=KeyPrefix("inv/")))
    assert sorted(rows) == ["inv/a", "inv/b"]
    assert listed_after, "KeyPrefix should push a narrower listing prefix"
    assert len(listed_after) < len(wide)

    plan = str(store.explain(where=KeyPrefix("inv/")))
    assert "KeyPrefix" in plan


def test_filesystem_decode_if_prefix_safety_net(
    tmp_path: Path, str_bytes_kvset: KvSet[str, str, str, bytes]
) -> None:
    """Mismatched listing prefix must not leak out-of-prefix keys."""
    store = FilesystemKvStore(str_bytes_kvset, root=tmp_path).scoped(tenant="t1")
    store.batch_set({"inv/a": "1", "inv/b": "2", "other": "3"})
    # Force a too-wide listing prefix while asking for inv/ via decode_if_prefix.
    backend = store.backend

    def wide_listing(key_prefix: str | None) -> str:
        return backend._collection_prefix  # type: ignore[attr-defined]

    backend._key_codec.listing_prefix = wide_listing  # type: ignore[method-assign]
    assert sorted(store.scan(where=KeyPrefix("inv/"))) == ["inv/a", "inv/b"]


def test_filesystem_key_only_zero_body_reads(
    tmp_path: Path, str_bytes_kvset: KvSet[str, str, str, bytes]
) -> None:
    store = FilesystemKvStore(str_bytes_kvset, root=tmp_path)
    store.batch_set({"a": "1", "b": "2"})
    reads = {"n": 0}
    orig = store.backend._read_live  # type: ignore[attr-defined]

    def counting(key: str) -> bytes | None:
        reads["n"] += 1
        return orig(key)

    store.backend._read_live = counting  # type: ignore[method-assign]
    assert sorted(store.scan()) == ["a", "b"]
    assert reads["n"] == 0


def test_zip_forwards_keyprefix() -> None:
    left = _mem()
    right = _mem()
    left.batch_set({"inv/a": "1", "other": "x"})
    right.batch_set({"inv/a": "2", "inv/b": "3"})
    zipped = KvStore.zip(left, right)

    seen_prefixes: list[str | None] = []
    for part in (left, right):
        orig = part.compile_scan

        def wrapping(spec: ScanSpec[str], *, _orig=orig, _seen=seen_prefixes):
            from kvtsjl.store.schema.scan import extract_key_prefix

            _seen.append(extract_key_prefix(spec.where))
            return _orig(spec)

        part.compile_scan = wrapping  # type: ignore[method-assign]

    rows = list(zipped.scan(where=KeyPrefix("inv/"), include_values=True))
    assert {k for k, _ in rows} == {"inv/a", "inv/b"}
    assert seen_prefixes == ["inv/", "inv/"]


def test_delete_where_expired_best_effort_vs_prefix_raises() -> None:
    store = _mem(
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=30)),
        expiry_gc=ExpiryGc.HIDE,
    )
    index = MemoryKeyIndex[str, str]()
    indexed = store.indexed(index, sync_policy=SyncPolicy.REQUIRED)

    with freeze_time("2024-01-01 12:00:00") as frozen:
        indexed.set("gone", "v")
        indexed.set("live", "v2", ttl=TtlPolicy.none())
        frozen.move_to("2024-01-01 12:01:00")

        # Break index sync for deletes.
        def boom(*_a: Any, **_k: Any) -> None:
            raise RuntimeError("index down")

        for idx in indexed.indexes:
            idx.batch_delete = boom  # type: ignore[method-assign]
            idx.delete = boom  # type: ignore[method-assign]

        # (a) Expired / GC: completes without raising.
        n = indexed.delete_where(Expired(), limit=10)
        assert n >= 1
        assert indexed.get("live") == "v2"

        # (b) non-Expired under REQUIRED: raises on index sync failure.
        with pytest.raises(PartialSyncError):
            indexed.delete_where(KeyPrefix("l"), limit=10)


def test_gc_expired_folds_to_delete_where() -> None:
    store = _mem(
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=30)),
        expiry_gc=ExpiryGc.HIDE,
    )
    with freeze_time("2024-01-01 12:00:00") as frozen:
        store.set("a", "1")
        frozen.move_to("2024-01-01 12:01:00")
        assert store.gc_expired(max_entries=10) == 1


def test_zip_rejects_ordered_scan() -> None:
    zipped = KvStore.zip(_mem(), _mem())
    with pytest.raises(KvStoreScanUnsupported, match="UNORDERED"):
        list(zipped.scan(spec=ScanSpec(order=ScanOrder.KEY_ASC)))
