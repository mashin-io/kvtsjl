"""KvSet descriptor and in-key (scope + leaf) helpers."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import cast

from kvtsjl.exceptions import KvStoreScopeError
from kvtsjl.schema.blob_ops import (
    BlobOps,
    BytesBlobOps,
    StrBlobOps,
    escape_segment,
    unescape_segment,
)
from kvtsjl.schema.physical import PhysicalSchema
from kvtsjl.scope import Scope
from kvtsjl.serde import CompressionCodec, SerDe
from kvtsjl.store.schema.layout import KeyLayout
from kvtsjl.store.schema.ttl import TtlPolicy


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

    def with_compressed_value(
        self, codec: CompressionCodec
    ) -> KvSet[K, V, KBLOB, bytes]:
        """Return a copy whose value serde compresses inner wire bytes with ``codec``."""
        inner = self.value_serde
        if inner.blob_type is not bytes:
            raise ValueError(
                f"with_compressed_value requires bytes value serde, got {inner.blob_type!r}"
            )
        byte_inner = cast(SerDe[V, bytes], inner)
        return cast(
            KvSet[K, V, KBLOB, bytes],
            replace(self, data_serde=SerDe.compressed(codec, byte_inner)),
        )

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

    def _escape_segment(self, text: str) -> KBLOB:
        """Percent-escape the separator and the escape character in a segment.

        Only ``%`` and the separator are escaped, so any value containing
        neither encodes to exactly the bytes it did before.
        """
        return self.str_serde.serialize(
            escape_segment(text, self.blob_ops.separator_str)
        )

    def _unescape_segment(self, text: str) -> str:
        return unescape_segment(text, self.blob_ops.separator_str)

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

    def leaf_needs_escaping(self, scope: Scope) -> bool:
        """A key is only unambiguous when its scope arity is fixed.

        Escaping the kinds and ids stops a separator inside a *value* from
        forging a boundary, but it cannot fix a varying number of segments:
        with ``scope_schema=("user", "slug")``,

            scope(user="a")           + leaf "slug:x:sha"  ->  user:a:slug:x:sha
            scope(user="a", slug="x") + leaf "sha"         ->  user:a:slug:x:sha

        are still the same key. Fixing the arity on writes removes that, and
        lets the leaf stay unescaped, so every key whose leaf contains ``:``
        keeps the bytes it has today.

        Prefix scopes remain valid for reads, scans and prefix listing — they
        encode no leaf, so there is nothing to confuse.

        Returns whether the leaf must also be escaped, which is only the case
        for a scoped kvset that declared no ``scope_schema``.
        """
        schema = self.scope_schema
        if schema is None:
            # No declared arity, so the boundary between the last scope id and
            # the leaf is not fixed by the schema. Escaping the leaf restores
            # it. Declaring a scope_schema is better: it keeps the leaf's bytes
            # unchanged.
            return bool(scope.segments)
        if len(scope.segments) != len(schema):
            kinds = tuple(s.kind for s in scope.segments)
            raise KvStoreScopeError(
                f"writing a key needs the full scope {schema!r}, got {kinds!r}; "
                "a partial scope is ambiguous because the missing segments "
                "cannot be told apart from the leaf"
            )
        return False

    def encode_in_key(self, scope: Scope, key: K | None = None) -> KBLOB:
        """Compose scope (+ optional leaf) into an in-key KBLOB.

        Segment kinds and ids are percent-escaped so a ``:`` inside a value
        cannot be read as a separator. The leaf is left as-is: it is the last
        part, and with the scope arity fixed the remainder of the key is the
        leaf whatever it contains.
        """
        escape_leaf = False
        if key is not None:
            escape_leaf = self.leaf_needs_escaping(scope)
        parts: list[KBLOB] = []
        for seg in scope.segments:
            parts.append(self._escape_segment(seg.kind))
            parts.append(self._escape_segment(seg.id))
        if key is not None:
            leaf_blob = self.key_serde.serialize(key)
            parts.append(self.blob_ops.escape(leaf_blob) if escape_leaf else leaf_blob)
        if not parts:
            return self.blob_ops.concat()
        return self.blob_ops.join(parts)

    def decode_in_key(self, in_key: KBLOB) -> tuple[Scope, K] | None:
        """Inverse of ``encode_in_key``: recover the full scope and the leaf.

        Returns ``None`` when ``in_key`` does not belong to this schema — too
        few segments, kinds that do not match, or a leaf this kvset's serde
        cannot read. Never raises on a foreign key.

        A key written before the escaping landed may still decode, to the
        wrong scope, when an id contained the separator: such a key is
        byte-identical to a legitimate one whose leaf contains it. That is the
        ambiguity being removed, and it cannot be detected after the fact.
        """
        ops = self.blob_ops
        schema = self.scope_schema
        sep = ops.separator_str
        text = self._as_text(in_key)
        if schema is None:
            leaf = self._leaf_or_none(in_key)
            return (Scope.empty(), leaf) if leaf is not None else None

        tokens = text.split(sep)
        if len(tokens) < 2 * len(schema) + 1:
            return None
        segments: list[tuple[str, str]] = []
        for index, kind in enumerate(schema):
            raw_kind = self._unescape_segment(tokens[2 * index])
            if raw_kind != kind:
                return None
            segments.append((kind, self._unescape_segment(tokens[2 * index + 1])))
        # The leaf is whatever is left, separators and all.
        leaf_text = sep.join(tokens[2 * len(schema) :])
        leaf = self._leaf_or_none(self._as_blob(leaf_text))
        if leaf is None:
            return None
        return Scope.of(**dict(segments)), leaf

    def _maybe_unescape(self, leaf_blob: KBLOB, scope: Scope) -> KBLOB:
        """Undo the write-side leaf escaping, where it applies."""
        if self.scope_schema is None and scope.segments:
            return self.blob_ops.unescape(leaf_blob)
        return leaf_blob

    def _leaf_or_none(self, blob: KBLOB) -> K | None:
        try:
            return self.key_serde.deserialize(blob)
        except Exception:
            return None

    def _as_text(self, blob: KBLOB) -> str:
        return blob.decode("utf-8") if isinstance(blob, bytes) else cast(str, blob)

    def _as_blob(self, text: str) -> KBLOB:
        sample = self.blob_ops.separator
        out = text.encode("utf-8") if isinstance(sample, bytes) else text
        return cast(KBLOB, out)

    def decode_leaf_from_in_key(self, scope: Scope, in_key: KBLOB) -> K | None:
        """Strip scope prefix from ``in_key`` and deserialize the leaf.

        Unescapes the leaf when this kvset escapes it on write, so a scan
        returns the key the caller passed to ``set``.
        """
        scope_prefix = self.encode_in_key(scope, None)
        ops = self.blob_ops
        if not scope.segments:
            return self._leaf_or_none(self._maybe_unescape(in_key, scope))
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
        return self._leaf_or_none(self._maybe_unescape(rest, scope))

    def same_schema_as(self, other: KvSet[K, V, KBLOB, VBLOB]) -> bool:
        return (
            self.same_physical_as(other)
            and self.key_layout == other.key_layout
            and self.scope_schema == other.scope_schema
            and self.str_serde is other.str_serde
            and self.blob_ops is other.blob_ops
        )
