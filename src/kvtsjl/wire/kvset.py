"""KvSet descriptor and in-key (scope + leaf) helpers."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from kvtsjl.wire.blob_ops import BlobOps, BytesBlobOps, StrBlobOps
from kvtsjl.exceptions import KvStoreScopeError
from kvtsjl.wire.layout import KeyLayout
from kvtsjl.wire.schema import PhysicalSchema
from kvtsjl.scope import Scope
from kvtsjl.serde import SerDe
from kvtsjl.wire.ttl import TtlPolicy


@dataclass(frozen=True, slots=True)
class KvSet[K, V, KBLOB, VBLOB](PhysicalSchema[K, V, KBLOB, VBLOB]):
    """Document collection wire schema — not a store."""

    str_serde: SerDe[str, KBLOB]
    blob_ops: BlobOps[KBLOB]
    ttl_policy: TtlPolicy = TtlPolicy.none()
    key_layout: KeyLayout = KeyLayout.LITERAL
    scope_schema: tuple[str, ...] | None = None

    @property
    def value_serde(self) -> SerDe[V, VBLOB]:
        return self.data_serde

    @classmethod
    def create(
        cls,
        name: str,
        *,
        version: int | str = 1,
        key_serde: SerDe[K, KBLOB],
        value_serde: SerDe[V, VBLOB],
        str_serde: SerDe[str, KBLOB],
        blob_ops: BlobOps[KBLOB],
        ttl_policy: TtlPolicy | None = None,
        key_layout: KeyLayout = KeyLayout.LITERAL,
        scope_schema: tuple[str, ...] | None = None,
    ) -> KvSet[K, V, KBLOB, VBLOB]:
        """Build a KvSet with explicit str_serde / blob_ops."""
        return cls(
            name=name,
            version=version,
            key_serde=key_serde,
            data_serde=value_serde,
            str_serde=str_serde,
            blob_ops=blob_ops,
            ttl_policy=ttl_policy or TtlPolicy.none(),
            key_layout=key_layout,
            scope_schema=scope_schema,
        )

    @staticmethod
    def with_str_keys[KK, VV, VVBLOB](
        name: str,
        *,
        version: int | str = 1,
        key_serde: SerDe[KK, str],
        value_serde: SerDe[VV, VVBLOB],
        ttl_policy: TtlPolicy | None = None,
        key_layout: KeyLayout = KeyLayout.LITERAL,
        scope_schema: tuple[str, ...] | None = None,
    ) -> KvSet[KK, VV, str, VVBLOB]:
        return KvSet(
            name=name,
            version=version,
            key_serde=key_serde,
            data_serde=value_serde,
            str_serde=SerDe.safe_str(),
            blob_ops=StrBlobOps(),
            ttl_policy=ttl_policy or TtlPolicy.none(),
            key_layout=key_layout,
            scope_schema=scope_schema,
        )

    @staticmethod
    def with_bytes_keys[KK, VV, VVBLOB](
        name: str,
        *,
        version: int | str = 1,
        key_serde: SerDe[KK, bytes],
        value_serde: SerDe[VV, VVBLOB],
        ttl_policy: TtlPolicy | None = None,
        key_layout: KeyLayout = KeyLayout.LITERAL,
        scope_schema: tuple[str, ...] | None = None,
    ) -> KvSet[KK, VV, bytes, VVBLOB]:
        return KvSet(
            name=name,
            version=version,
            key_serde=key_serde,
            data_serde=value_serde,
            str_serde=SerDe.utf8_bytes(),
            blob_ops=BytesBlobOps(),
            ttl_policy=ttl_policy or TtlPolicy.none(),
            key_layout=key_layout,
            scope_schema=scope_schema,
        )

    def validate_scope(self, scope: Scope) -> None:
        schema = self.scope_schema
        if schema is None:
            return
        kinds = tuple(s.kind for s in scope.segments)
        if len(kinds) > len(schema):
            raise KvStoreScopeError(f"scope kinds {kinds!r} exceed schema {schema!r}")
        if kinds != schema[: len(kinds)]:
            raise KvStoreScopeError(
                f"scope kinds {kinds!r} do not match schema prefix of {schema!r}"
            )

    def encode_scope_parts(self, scope: Scope) -> list[KBLOB]:
        parts: list[KBLOB] = []
        for seg in scope.segments:
            parts.append(self.str_serde.serialize(seg.kind))
            parts.append(self.str_serde.serialize(seg.id))
        return parts

    def encode_in_key(self, scope: Scope, key: K | None = None) -> KBLOB:
        """Compose scope (+ optional leaf) into an in-key KBLOB."""
        parts = self.encode_scope_parts(scope)
        if key is not None:
            parts.append(self.key_serde.serialize(key))
        if not parts:
            return self.blob_ops.concat()
        return self.blob_ops.join(parts)

    def encode_scan_prefix(self, scope: Scope, key_prefix: K | None = None) -> KBLOB:
        return self.encode_in_key(scope, key_prefix)

    def decode_leaf_from_in_key(self, scope: Scope, in_key: KBLOB) -> K | None:
        """Strip scope prefix from ``in_key`` and deserialize the leaf."""
        scope_prefix = self.encode_in_key(scope, None)
        ops = self.blob_ops
        if not scope.segments:
            return self.key_serde.deserialize(in_key)
        if not ops.startswith(in_key, scope_prefix):
            return None
        rest = ops.strip_prefix(in_key, scope_prefix)
        if rest is None:
            return None
        sep = ops.separator
        if ops.len(rest) >= ops.len(sep) and ops.startswith(rest, sep):
            stripped = ops.strip_prefix(rest, sep)
            if stripped is None:
                return None
            rest = stripped
        if ops.len(rest) == 0:
            return None
        return self.key_serde.deserialize(rest)

    def same_schema_as(self, other: KvSet[K, V, KBLOB, VBLOB]) -> bool:
        return (
            self.same_physical_as(other)
            and self.key_layout == other.key_layout
            and self.scope_schema == other.scope_schema
            and self.str_serde is other.str_serde
            and self.blob_ops is other.blob_ops
        )


def join_blobs[KBLOB](ops: BlobOps[KBLOB], parts: Sequence[KBLOB]) -> KBLOB:
    return ops.join(list(parts))
