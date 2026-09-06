"""Algebraic KeyMap / KvStore operators."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib

import pytest

from kvtsjl import (
    IndexSet,
    KeyMap,
    KvSet,
    KvStore,
    MemoryKvStore,
    MemoryVectorIndex,
    SerDe,
    TtlPolicy,
    VectorQuery,
)
from kvtsjl.exceptions import KvStoreReadOnlyError, KvStoreScanUnsupported
from kvtsjl.keymap_algebra import DictKeyMap


def _str_store(name: str = "s") -> MemoryKvStore[str, str, str, bytes]:
    kvset = KvSet.with_str_keys(
        name,
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
    )
    return MemoryKvStore(kvset)


def test_imap_roundtrip() -> None:
    store = _str_store()
    store.set("a", "hello")
    wired = store.imap(lambda s: s.upper(), lambda s: s.lower())
    assert wired.get("a") == "HELLO"
    wired.set("b", "WORLD")
    assert store.get("b") == "world"
    wired.set("c", "PIN", ttl=TtlPolicy.none())
    assert store.get("c") == "pin"


def test_mirror_forwards_ttl() -> None:
    from datetime import timedelta

    from freezegun import freeze_time

    kvset = KvSet.with_str_keys(
        "ttl",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        ttl_policy=TtlPolicy.hourly(),
    )
    primary = MemoryKvStore(kvset)
    secondary = MemoryKvStore(kvset)
    store = primary.mirror(secondary)
    with freeze_time("2024-01-01 12:00:00") as frozen:
        store.set("k", "v", ttl=TtlPolicy(ttl_duration=timedelta(seconds=30)))
        frozen.move_to("2024-01-01 12:01:00")
        assert primary.get("k") is None
        assert secondary.get("k") is None


def test_map_readonly() -> None:
    store = _str_store()
    store.set("a", "x")
    view = store.map(lambda s: len(s))
    assert view.get("a") == 1
    with pytest.raises(KvStoreReadOnlyError):
        view.set("a", 2)


def test_imap_keys_hash() -> None:
    underlying = _str_store("hash")
    to_store = lambda k: hashlib.sha256(k.encode()).hexdigest()
    hashed = underlying.imap_keys(to_store, None)
    hashed.set("alice", "doc")
    assert underlying.get(to_store("alice")) == "doc"
    assert hashed.get("alice") == "doc"
    with pytest.raises(KvStoreScanUnsupported):
        list(hashed.scan())


def test_imap_keys_with_inverse_scan() -> None:
    underlying = _str_store("rev")
    table = {"a": "1", "b": "2"}
    inv = {v: k for k, v in table.items()}
    remapped = underlying.imap_keys(lambda k: table[k], lambda sk: inv[sk])
    remapped.set("a", "va")
    remapped.set("b", "vb")
    assert set(remapped.list()) == {"a", "b"}


def test_zip_with_optional_parts() -> None:
    meta_s = _str_store("meta2")
    body_s = _str_store("body2")

    @dataclass
    class Doc:
        meta: str | None
        body: str | None

    articles = KvStore.zip_with(Doc, meta=meta_s, body=body_s)
    articles.set("a", Doc(meta="m1", body="b1"))
    articles.set("b", Doc(meta="m2", body=None))
    assert articles.get("a") == Doc(meta="m1", body="b1")
    assert articles.get("b") == Doc(meta="m2", body=None)
    assert body_s.get("b") is None
    assert set(articles.list()) == {"a", "b"}


def test_zip_as_bundle() -> None:
    meta_s = _str_store("meta3")
    body_s = _str_store("body3")

    @dataclass
    class Doc:
        meta: str | None
        body: str | None

    @dataclass
    class DocParts:
        meta: MemoryKvStore[str, str, str, bytes]
        body: MemoryKvStore[str, str, str, bytes]

    articles = KvStore.zip_as(Doc, DocParts(meta=meta_s, body=body_s))
    articles.set("a", Doc(meta="m1", body="b1"))
    assert articles.get("a") == Doc(meta="m1", body="b1")
    assert set(articles.list()) == {"a"}


def test_zip_as_keymap_bundle() -> None:
    meta_m = DictKeyMap[str, str]({"k": "m"})
    body_m = DictKeyMap[str, str]({"k": "b"})

    @dataclass
    class Doc:
        meta: str | None
        body: str | None

    @dataclass
    class DocParts:
        meta: DictKeyMap[str, str]
        body: DictKeyMap[str, str]

    articles = KeyMap.zip_as(Doc, DocParts(meta=meta_m, body=body_m))
    assert articles.get("k") == Doc(meta="m", body="b")


def test_zip_as_rejects_non_dataclass() -> None:
    with pytest.raises(TypeError, match="dataclass instance"):
        KvStore.zip_as(object, {"meta": _str_store()})  # type: ignore[arg-type]


def test_zip_tuple() -> None:
    a = _str_store("za")
    b = _str_store("zb")
    a.set("k", "1")
    zipped = KvStore.zip(a, b)
    assert zipped.get("k") == ("1", None)
    zipped.set("k", ("1", "2"))
    assert b.get("k") == "2"


def test_zip_with_batch_get_fans_out_once() -> None:
    from unittest.mock import MagicMock

    meta_s = _str_store("meta_bg")
    body_s = _str_store("body_bg")
    meta_s.set("k1", "m1")
    body_s.set("k1", "b1")
    meta_s.set("k2", "m2")

    meta = MagicMock(wraps=meta_s)
    body = MagicMock(wraps=body_s)

    @dataclass
    class Doc:
        meta: str | None
        body: str | None

    articles = KvStore.zip_with(Doc, meta=meta, body=body)
    keys = ["k1", "k2", "k3"]
    assert articles.batch_get(keys) == {
        "k1": Doc(meta="m1", body="b1"),
        "k2": Doc(meta="m2", body=None),
    }
    meta.batch_get.assert_called_once_with(keys)
    body.batch_get.assert_called_once_with(keys)
    assert meta.get.call_count == 0
    assert body.get.call_count == 0

    assert articles.batch_get([]) == {}
    assert meta.batch_get.call_count == 1
    assert body.batch_get.call_count == 1

    articles.batch_get(["k1", "k1", "k2"])
    meta.batch_get.assert_called_with(["k1", "k2"])
    body.batch_get.assert_called_with(["k1", "k2"])


def test_zip_with_batch_set_fans_out_once() -> None:
    from unittest.mock import MagicMock

    meta_s = _str_store("meta_bs")
    body_s = _str_store("body_bs")
    meta = MagicMock(wraps=meta_s)
    body = MagicMock(wraps=body_s)

    @dataclass
    class Doc:
        meta: str | None
        body: str | None

    articles = KvStore.zip_with(Doc, meta=meta, body=body)
    items = {
        "a": Doc(meta="m1", body="b1"),
        "b": Doc(meta="m2", body=None),
        "c": Doc(meta=None, body="b3"),
    }
    articles.batch_set(items)
    meta.batch_set.assert_called_once_with({"a": "m1", "b": "m2"}, ttl=None)
    body.batch_set.assert_called_once_with({"a": "b1", "c": "b3"}, ttl=None)
    meta.batch_delete.assert_called_once_with(["c"])
    body.batch_delete.assert_called_once_with(["b"])
    assert meta.set.call_count == 0
    assert body.set.call_count == 0
    assert articles.batch_get(["a", "b", "c"]) == {
        "a": Doc(meta="m1", body="b1"),
        "b": Doc(meta="m2", body=None),
        "c": Doc(meta=None, body="b3"),
    }
    articles.batch_set({})
    assert meta.batch_set.call_count == 1
    assert body.batch_set.call_count == 1


def test_zip_tuple_batch_set_fans_out_once() -> None:
    from unittest.mock import MagicMock

    a_s = _str_store("za_bs")
    b_s = _str_store("zb_bs")
    a = MagicMock(wraps=a_s)
    b = MagicMock(wraps=b_s)
    zipped = KvStore.zip(a, b)
    zipped.batch_set({"k1": ("1", None), "k2": (None, "2")})
    a.batch_set.assert_called_once_with({"k1": "1"}, ttl=None)
    b.batch_set.assert_called_once_with({"k2": "2"}, ttl=None)
    a.batch_delete.assert_called_once_with(["k2"])
    b.batch_delete.assert_called_once_with(["k1"])
    assert zipped.batch_get(["k1", "k2"]) == {
        "k1": ("1", None),
        "k2": (None, "2"),
    }


def test_zip_keymap_batch_set_fans_out_once() -> None:
    from unittest.mock import MagicMock

    meta_m = DictKeyMap[str, str]({})
    body_m = DictKeyMap[str, str]({})
    meta = MagicMock(wraps=meta_m)
    body = MagicMock(wraps=body_m)

    @dataclass
    class Doc:
        meta: str | None
        body: str | None

    articles = KeyMap.zip_with(Doc, meta=meta, body=body)
    articles.batch_set(
        {
            "a": Doc(meta="m1", body="b1"),
            "b": Doc(meta="m2", body=None),
        }
    )
    meta.batch_set.assert_called_once_with({"a": "m1", "b": "m2"})
    body.batch_set.assert_called_once_with({"a": "b1"})
    body.batch_delete.assert_called_once_with(["b"])
    assert meta.batch_delete.call_count == 0


def test_zip_with_batch_get_mixed_presence() -> None:
    meta_s = _str_store("meta_mix")
    body_s = _str_store("body_mix")

    @dataclass
    class Doc:
        meta: str | None
        body: str | None

    articles = KvStore.zip_with(Doc, meta=meta_s, body=body_s)
    meta_s.set("meta_only", "m")
    body_s.set("body_only", "b")
    articles.set("both", Doc(meta="m2", body="b2"))

    keys = ["meta_only", "body_only", "both", "neither"]
    batched = articles.batch_get(keys)
    for key in keys:
        assert batched.get(key) == articles.get(key)
    assert "neither" not in batched


def test_zip_tuple_batch_get_fans_out_once() -> None:
    from unittest.mock import MagicMock

    a_s = _str_store("za_bg")
    b_s = _str_store("zb_bg")
    a_s.set("k1", "1")
    b_s.set("k2", "2")
    a = MagicMock(wraps=a_s)
    b = MagicMock(wraps=b_s)
    zipped = KvStore.zip(a, b)
    keys = ["k1", "k2", "k3"]
    assert zipped.batch_get(keys) == {
        "k1": ("1", None),
        "k2": (None, "2"),
    }
    a.batch_get.assert_called_once_with(keys)
    b.batch_get.assert_called_once_with(keys)
    assert a.get.call_count == 0
    assert b.get.call_count == 0


def test_zip_keymap_batch_get_fans_out_once() -> None:
    from unittest.mock import MagicMock

    meta_m = DictKeyMap[str, str]({"k1": "m1", "k2": "m2"})
    body_m = DictKeyMap[str, str]({"k1": "b1"})
    meta = MagicMock(wraps=meta_m)
    body = MagicMock(wraps=body_m)

    @dataclass
    class Doc:
        meta: str | None
        body: str | None

    articles = KeyMap.zip_with(Doc, meta=meta, body=body)
    keys = ["k1", "k2", "k3"]
    assert articles.batch_get(keys) == {
        "k1": Doc(meta="m1", body="b1"),
        "k2": Doc(meta="m2", body=None),
    }
    meta.batch_get.assert_called_once_with(keys)
    body.batch_get.assert_called_once_with(keys)


def test_fallback_batch_get_promotes() -> None:
    primary = _str_store("fp_bg")
    secondary = _str_store("fs_bg")
    secondary.set("x", "from-s")
    secondary.set("y", "from-s2")
    primary.set("z", "from-p")
    hot = primary.coalesce(secondary, promote=True)
    assert hot.batch_get(["x", "y", "z", "missing"]) == {
        "x": "from-s",
        "y": "from-s2",
        "z": "from-p",
    }
    assert primary.get("x") == "from-s"
    assert primary.get("y") == "from-s2"


def test_coalesce_alias() -> None:
    primary = _str_store("p")
    secondary = _str_store("s")
    secondary.set("x", "from-s")
    hot = primary.coalesce(secondary, promote=True)
    assert hot.get("x") == "from-s"
    assert primary.get("x") == "from-s"


def test_then_fk() -> None:
    orders = _str_store("orders")
    users = _str_store("users")
    orders.set("o1", "u1")
    users.set("u1", "alice")
    joined = orders.then(users)
    assert joined.get("o1") == "alice"
    assert joined.get("missing") is None
    with pytest.raises(KvStoreReadOnlyError):
        joined.set("o1", "bob")


def test_then_with() -> None:
    docs = _str_store("docs")
    authors = _str_store("authors")
    docs.set("d1", "a1|Hello")
    authors.set("a1", "Alice")
    joined = docs.then_with(lambda _k, v: v.split("|", 1)[0], authors)
    assert joined.get("d1") == "Alice"


def test_expand_and_fold() -> None:
    users = _str_store("users")
    users.set("u1", "alice")
    users.set("u2", "bob")

    def emails_of(user_id: str, _name: str) -> dict[str, bool]:
        if user_id == "u1":
            return {"a@x.com": True, "a@y.com": True}
        return {"b@x.com": True}

    per_user = users.expand(emails_of)
    col = per_user.get("u1")
    assert col is not None
    assert isinstance(col, DictKeyMap)
    assert col.get("a@x.com") is True
    assert len(col.keys()) == 2

    summaries = users.expand_map(
        emails_of,
        lambda _k, name, c: f"{name}:{len(c.keys()) if isinstance(c, DictKeyMap) else 0}",
    )
    assert summaries.get("u1") == "alice:2"
    assert summaries.get("u2") == "bob:1"


def test_zip_indexed_vector() -> None:
    meta_s = _str_store("vm")
    body_s = _str_store("vb")

    @dataclass
    class Doc:
        meta: str | None
        body: str | None

    articles = KvStore.zip_with(Doc, meta=meta_s, body=body_s)
    index_set = IndexSet.with_str_ids(
        "vec",
        id_serde=SerDe.identity(str),
        meta_serde=SerDe.identity(dict),
        embedding_of=lambda _k, v: [float(len(v.body or ""))],
        document_of=lambda _k, v: v.body or "",
    )
    vec = MemoryVectorIndex(
        index_set=index_set,
        merge_data_fn=lambda _k, _v, prev: prev or {"n": 0},
        embed_content=lambda text: [float(len(text))],
    )
    store = articles.indexed(vec)
    store.set("a", Doc(meta="t", body="hello"))
    store.set("b", Doc(meta="t", body="hi"))
    hits = store.search_hits(vec, VectorQuery(embedding=[5.0]))
    assert hits[0].key == "a"


def test_dict_keymap_zip() -> None:
    m = DictKeyMap({"a": 1})
    assert m.get("a") == 1
    m.set("b", 2)
    assert m.delete("a")
    assert KeyMap.zip(m, DictKeyMap({"b": 3})).get("b") == (2, 3)
