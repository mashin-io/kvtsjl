"""KeyCodec round-trip tests."""

from __future__ import annotations

from kvtsjl import KvSet, Scope, SerDe
from kvtsjl.bind import NativeCollectionBinder
from kvtsjl.bind.namespace import resolve_collection_binding
from kvtsjl.schema.ref import WireRef
from kvtsjl.scope import ScopeSegment
from kvtsjl.store.schema.key_codec import BindingLayoutKeyCodec


def test_binding_layout_key_codec_round_trip() -> None:
    kvset = KvSet.with_str_keys(
        "codec",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
    )
    binding = resolve_collection_binding(
        WireRef.from_kvset(kvset),
        binder=NativeCollectionBinder[str, str](
            collection_formatter=lambda ref: f"{ref.name}/{ref.version_label()}/"
        ),
    )
    assert binding.collection is not None
    scope = Scope((ScopeSegment("tenant", "acme"),))
    codec: BindingLayoutKeyCodec[str, str] = BindingLayoutKeyCodec(
        kvset,
        binding,
        scope=scope,
        collection_prefix=binding.collection,
    )
    physical = codec.encode_physical("doc1")
    assert codec.decode_physical(physical) == "doc1"
    assert codec.decode_physical("other/prefix/x") is None
    assert codec.matches_prefix(physical, None) is True
    assert codec.matches_prefix(physical, "doc") is True
    assert codec.matches_prefix(physical, "other") is False
    assert codec.decode_if_prefix(physical, "doc") == "doc1"
    assert codec.decode_if_prefix(physical, "other") is None
