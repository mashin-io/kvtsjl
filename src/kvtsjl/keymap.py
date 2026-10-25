"""Readable and writable keyed maps plus algebraic composition."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any, overload

from kvtsjl.exceptions import KvStoreUnsupported
from kvtsjl.store.schema.stat import RecordStat
from kvtsjl.store.schema.ttl import TtlPolicy

if TYPE_CHECKING:
    from kvtsjl.keymap_algebra.bundle import ZipPartsBundle
    from kvtsjl.store.logical import KvStore
    from kvtsjl.store.readable import ReadableKvStore


class ReadableKeyMap[K, T](ABC):
    """Read-only keyed entries ``K → T`` (get / exists / stat + algebra)."""

    @abstractmethod
    def get(self, key: K) -> T | None:
        """Return the value for ``key``, or ``None`` if absent."""

    def batch_get(self, keys: Sequence[K]) -> dict[K, T]:
        """Return ``{key: value}`` for keys that exist."""
        out: dict[K, T] = {}
        for key in keys:
            value = self.get(key)
            if value is not None:
                out[key] = value
        return out

    def exists(self, key: K) -> bool:
        """Whether ``key`` is logically present (honours expiry like ``get``)."""
        return self.get(key) is not None

    def batch_exists(self, keys: Sequence[K]) -> set[K]:
        """Subset of ``keys`` that are logically present."""
        return set(self.batch_get(keys).keys())

    def stat(self, key: K) -> RecordStat | None:
        """Metadata for ``key``, or ``None`` if absent / expired."""
        if not self.exists(key):
            return None
        return RecordStat()

    def batch_stat(self, keys: Sequence[K]) -> dict[K, RecordStat]:
        """``{key: RecordStat}`` for keys that are logically present."""
        return {key: RecordStat() for key in self.batch_exists(keys)}

    def get_with_stat(self, key: K) -> tuple[T, RecordStat] | None:
        """Value and stat together, or ``None`` if absent / expired."""
        value = self.get(key)
        if value is None:
            return None
        meta = self.stat(key)
        return value, meta if meta is not None else RecordStat()

    def batch_get_with_stat(
        self, keys: Sequence[K]
    ) -> dict[K, tuple[T, RecordStat]]:
        """``{key: (value, RecordStat)}`` for keys that exist."""
        values = self.batch_get(keys)
        stats = self.batch_stat(list(values.keys()))
        return {
            key: (value, stats.get(key, RecordStat()))
            for key, value in values.items()
        }

    # --- read-only algebra (Readable* return) ---

    @overload
    def map[U](
        self: KvStore[K, T], forward: Callable[[T], U]
    ) -> ReadableKvStore[K, U]: ...

    @overload
    def map[U](
        self: ReadableKvStore[K, T], forward: Callable[[T], U]
    ) -> ReadableKvStore[K, U]: ...

    @overload
    def map[U](
        self, forward: Callable[[T], U]
    ) -> ReadableKeyMap[K, U]: ...

    def map[U](self, forward: Callable[[T], U]) -> ReadableKeyMap[K, U]:
        """Read-only value view."""
        from kvtsjl.keymap_algebra.build import build_map

        return build_map(self, forward)

    @overload
    def then[V](self: KvStore[K, T], other: KvStore[T, V]) -> ReadableKvStore[K, V]: ...

    @overload
    def then[V](
        self: ReadableKvStore[K, T], other: ReadableKvStore[T, V]
    ) -> ReadableKvStore[K, V]: ...

    @overload
    def then[V](self, other: ReadableKeyMap[T, V]) -> ReadableKeyMap[K, V]: ...

    def then[V](self, other: ReadableKeyMap[T, V]) -> ReadableKeyMap[K, V]:
        """``other[self[k]]`` — compose partial maps (read-only view)."""
        from kvtsjl.keymap_algebra.build import build_then

        return build_then(self, other)

    @overload
    def then_with[J, V](
        self: KvStore[K, T],
        key_of: Callable[[K, T], J],
        other: KvStore[J, V],
    ) -> ReadableKvStore[K, V]: ...

    @overload
    def then_with[J, V](
        self: ReadableKvStore[K, T],
        key_of: Callable[[K, T], J],
        other: ReadableKvStore[J, V],
    ) -> ReadableKvStore[K, V]: ...

    @overload
    def then_with[J, V](
        self,
        key_of: Callable[[K, T], J],
        other: ReadableKeyMap[J, V],
    ) -> ReadableKeyMap[K, V]: ...

    def then_with[J, V](
        self,
        key_of: Callable[[K, T], J],
        other: ReadableKeyMap[J, V],
    ) -> ReadableKeyMap[K, V]:
        """``other[key_of(k, self[k])]`` (read-only view)."""
        from kvtsjl.keymap_algebra.build import build_then_with

        return build_then_with(self, key_of, other)

    @overload
    def expand[SK, SV](
        self: KvStore[K, T],
        expander: Callable[
            [K, T], ReadableKeyMap[SK, SV] | Mapping[SK, SV] | Sequence[tuple[SK, SV]]
        ],
    ) -> ReadableKvStore[K, ReadableKeyMap[SK, SV]]: ...

    @overload
    def expand[SK, SV](
        self: ReadableKvStore[K, T],
        expander: Callable[
            [K, T], ReadableKeyMap[SK, SV] | Mapping[SK, SV] | Sequence[tuple[SK, SV]]
        ],
    ) -> ReadableKvStore[K, ReadableKeyMap[SK, SV]]: ...

    @overload
    def expand[SK, SV](
        self,
        expander: Callable[
            [K, T], ReadableKeyMap[SK, SV] | Mapping[SK, SV] | Sequence[tuple[SK, SV]]
        ],
    ) -> ReadableKeyMap[K, ReadableKeyMap[SK, SV]]: ...

    def expand[SK, SV](
        self,
        expander: Callable[
            [K, T], ReadableKeyMap[SK, SV] | Mapping[SK, SV] | Sequence[tuple[SK, SV]]
        ],
    ) -> ReadableKeyMap[K, ReadableKeyMap[SK, SV]]:
        """Nest a collection under each key (same outer ``K``; read-only)."""
        from kvtsjl.keymap_algebra.build import build_expand

        return build_expand(self, expander)

    @overload
    def expand_map[SK, SV, U](
        self: KvStore[K, T],
        expander: Callable[
            [K, T], ReadableKeyMap[SK, SV] | Mapping[SK, SV] | Sequence[tuple[SK, SV]]
        ],
        aggregate: Callable[[K, T, ReadableKeyMap[SK, SV]], U],
    ) -> ReadableKvStore[K, U]: ...

    @overload
    def expand_map[SK, SV, U](
        self: ReadableKvStore[K, T],
        expander: Callable[
            [K, T], ReadableKeyMap[SK, SV] | Mapping[SK, SV] | Sequence[tuple[SK, SV]]
        ],
        aggregate: Callable[[K, T, ReadableKeyMap[SK, SV]], U],
    ) -> ReadableKvStore[K, U]: ...

    @overload
    def expand_map[SK, SV, U](
        self,
        expander: Callable[
            [K, T], ReadableKeyMap[SK, SV] | Mapping[SK, SV] | Sequence[tuple[SK, SV]]
        ],
        aggregate: Callable[[K, T, ReadableKeyMap[SK, SV]], U],
    ) -> ReadableKeyMap[K, U]: ...

    def expand_map[SK, SV, U](
        self,
        expander: Callable[
            [K, T], ReadableKeyMap[SK, SV] | Mapping[SK, SV] | Sequence[tuple[SK, SV]]
        ],
        aggregate: Callable[[K, T, ReadableKeyMap[SK, SV]], U],
    ) -> ReadableKeyMap[K, U]:
        """Expand then fold ``(k, v, col) → U`` (read-only)."""
        from kvtsjl.keymap_algebra.build import build_expand_map

        return build_expand_map(self, expander, aggregate)

    # --- writability-preserving algebra (three overload levels) ---

    @overload
    def coalesce(
        self: KvStore[K, T], other: KvStore[K, T], *, promote: bool = True
    ) -> KvStore[K, T]: ...

    @overload
    def coalesce(
        self: KeyMap[K, T], other: KeyMap[K, T], *, promote: bool = True
    ) -> KeyMap[K, T]: ...

    @overload
    def coalesce(
        self: ReadableKvStore[K, T],
        other: ReadableKvStore[K, T],
        *,
        promote: bool = True,
    ) -> ReadableKvStore[K, T]: ...

    @overload
    def coalesce(
        self, other: ReadableKeyMap[K, T], *, promote: bool = True
    ) -> ReadableKeyMap[K, T]: ...

    def coalesce(
        self, other: ReadableKeyMap[K, T], *, promote: bool = True
    ) -> ReadableKeyMap[K, T]:
        """Left-biased merge: prefer ``self``, else ``other`` (writes to ``self`` when writable)."""
        from kvtsjl.keymap_algebra.build import build_coalesce

        return build_coalesce(self, other, promote=promote)

    @overload
    def imap[U](
        self: KvStore[K, T],
        forward: Callable[[T], U],
        inverse: Callable[[U], T],
    ) -> KvStore[K, U]: ...

    @overload
    def imap[U](
        self: KeyMap[K, T],
        forward: Callable[[T], U],
        inverse: Callable[[U], T],
    ) -> KeyMap[K, U]: ...

    @overload
    def imap[U](
        self: ReadableKvStore[K, T],
        forward: Callable[[T], U],
        inverse: Callable[[U], T],
    ) -> ReadableKvStore[K, U]: ...

    @overload
    def imap[U](
        self,
        forward: Callable[[T], U],
        inverse: Callable[[U], T],
    ) -> ReadableKeyMap[K, U]: ...

    def imap[U](
        self, forward: Callable[[T], U], inverse: Callable[[U], T]
    ) -> ReadableKeyMap[K, U]:
        """Invertible value codec."""
        from kvtsjl.keymap import KeyMap
        from kvtsjl.keymap_algebra.build import build_imap

        if not isinstance(self, KeyMap):
            raise TypeError(f"imap requires a writable KeyMap, got {type(self).__name__}")
        return build_imap(self, forward, inverse)

    @overload
    def imap_keys[NK](
        self: KvStore[K, T],
        to_store: Callable[[NK], K],
        from_store: Callable[[K], NK] | None = None,
    ) -> KvStore[NK, T]: ...

    @overload
    def imap_keys[NK](
        self: KeyMap[K, T],
        to_store: Callable[[NK], K],
        from_store: Callable[[K], NK] | None = None,
    ) -> KeyMap[NK, T]: ...

    @overload
    def imap_keys[NK](
        self: ReadableKvStore[K, T],
        to_store: Callable[[NK], K],
        from_store: Callable[[K], NK] | None = None,
    ) -> ReadableKvStore[NK, T]: ...

    @overload
    def imap_keys[NK](
        self,
        to_store: Callable[[NK], K],
        from_store: Callable[[K], NK] | None = None,
    ) -> ReadableKeyMap[NK, T]: ...

    def imap_keys[NK](
        self,
        to_store: Callable[[NK], K],
        from_store: Callable[[K], NK] | None = None,
    ) -> ReadableKeyMap[NK, T]:
        """Caller keys ``NK`` mapped onto this map's keys ``K``."""
        from kvtsjl.keymap import KeyMap
        from kvtsjl.keymap_algebra.build import build_imap_keys

        if not isinstance(self, KeyMap):
            raise TypeError(
                f"imap_keys requires a writable KeyMap, got {type(self).__name__}"
            )
        return build_imap_keys(self, to_store, from_store)

    @overload
    @classmethod
    def zip[ZipK, A, B](
        cls, a: KvStore[ZipK, A], b: KvStore[ZipK, B], /
    ) -> KvStore[ZipK, tuple[A | None, B | None]]: ...

    @overload
    @classmethod
    def zip[ZipK, A, B](
        cls, a: KeyMap[ZipK, A], b: KeyMap[ZipK, B], /
    ) -> KeyMap[ZipK, tuple[A | None, B | None]]: ...

    @overload
    @classmethod
    def zip[ZipK, A, B, C](
        cls, a: KvStore[ZipK, A], b: KvStore[ZipK, B], c: KvStore[ZipK, C], /
    ) -> KvStore[ZipK, tuple[A | None, B | None, C | None]]: ...

    @overload
    @classmethod
    def zip[ZipK, A, B, C](
        cls, a: KeyMap[ZipK, A], b: KeyMap[ZipK, B], c: KeyMap[ZipK, C], /
    ) -> KeyMap[ZipK, tuple[A | None, B | None, C | None]]: ...

    @overload
    @classmethod
    def zip[ZipK, A, B, C, D](
        cls,
        a: KvStore[ZipK, A],
        b: KvStore[ZipK, B],
        c: KvStore[ZipK, C],
        d: KvStore[ZipK, D],
        /,
    ) -> KvStore[ZipK, tuple[A | None, B | None, C | None, D | None]]: ...

    @overload
    @classmethod
    def zip[ZipK, A, B, C, D](
        cls,
        a: KeyMap[ZipK, A],
        b: KeyMap[ZipK, B],
        c: KeyMap[ZipK, C],
        d: KeyMap[ZipK, D],
        /,
    ) -> KeyMap[ZipK, tuple[A | None, B | None, C | None, D | None]]: ...

    @overload
    @classmethod
    def zip[ZipK, A, B, C, D, E](
        cls,
        a: KvStore[ZipK, A],
        b: KvStore[ZipK, B],
        c: KvStore[ZipK, C],
        d: KvStore[ZipK, D],
        e: KvStore[ZipK, E],
        /,
    ) -> KvStore[ZipK, tuple[A | None, B | None, C | None, D | None, E | None]]: ...

    @overload
    @classmethod
    def zip[ZipK, A, B, C, D, E](
        cls,
        a: KeyMap[ZipK, A],
        b: KeyMap[ZipK, B],
        c: KeyMap[ZipK, C],
        d: KeyMap[ZipK, D],
        e: KeyMap[ZipK, E],
        /,
    ) -> KeyMap[ZipK, tuple[A | None, B | None, C | None, D | None, E | None]]: ...

    @classmethod
    def zip(
        cls,
        *parts: ReadableKeyMap[Any, Any],
        mandatory: Sequence[bool] | None = None,
        created_at_merge: Callable[[Sequence[datetime | None]], datetime | None]
        | None = None,
    ) -> ReadableKeyMap[Any, tuple[Any, ...]]:
        """Pointwise product; missing parts are ``None`` in the tuple.

        ``mandatory``: per-part presence flags (``None`` → all False → OR).
        ``created_at_merge``: optional store ``stat`` merge (KvStore parts only).
        """
        from kvtsjl.keymap_algebra.build import build_zip

        return build_zip(
            *parts, mandatory=mandatory, created_at_merge=created_at_merge
        )

    @overload
    @classmethod
    def zip_with[ZipK, W](
        cls,
        ctor: Callable[..., W],
        *,
        mandatory: Sequence[bool] | None = None,
        created_at_merge: Callable[[Sequence[datetime | None]], datetime | None]
        | None = None,
        commit_part: str | None = None,
        after_commit: Sequence[str] = (),
        **parts: KvStore[ZipK, Any],
    ) -> KvStore[ZipK, W]: ...

    @overload
    @classmethod
    def zip_with[ZipK, W](
        cls,
        ctor: Callable[..., W],
        *,
        mandatory: Sequence[bool] | None = None,
        created_at_merge: Callable[[Sequence[datetime | None]], datetime | None]
        | None = None,
        commit_part: str | None = None,
        after_commit: Sequence[str] = (),
        **parts: KeyMap[ZipK, Any],
    ) -> KeyMap[ZipK, W]: ...

    @classmethod
    def zip_with[ZipK, W](
        cls,
        ctor: Callable[..., W],
        *,
        mandatory: Sequence[bool] | None = None,
        created_at_merge: Callable[[Sequence[datetime | None]], datetime | None]
        | None = None,
        commit_part: str | None = None,
        after_commit: Sequence[str] = (),
        **parts: ReadableKeyMap[ZipK, Any],
    ) -> ReadableKeyMap[ZipK, W]:
        """Assemble a dataclass (or constructor) from named part maps."""
        from kvtsjl.keymap_algebra.build import build_zip_with

        return build_zip_with(
            ctor,
            mandatory=mandatory,
            created_at_merge=created_at_merge,
            commit_part=commit_part,
            after_commit=after_commit,
            **parts,
        )

    @classmethod
    def zip_as[ZipK, W](
        cls,
        ctor: type[W],
        parts: ZipPartsBundle[ZipK],
        *,
        mandatory: Sequence[bool] | None = None,
        created_at_merge: Callable[[Sequence[datetime | None]], datetime | None]
        | None = None,
        commit_part: str | None = None,
        after_commit: Sequence[str] = (),
    ) -> ReadableKeyMap[ZipK, W]:
        """Assemble from a dataclass bundle of part maps (strict field typing)."""
        from kvtsjl.keymap_algebra.build import build_zip_with
        from kvtsjl.keymap_algebra.bundle import parts_from_bundle

        return build_zip_with(
            ctor,
            mandatory=mandatory,
            created_at_merge=created_at_merge,
            commit_part=commit_part,
            after_commit=after_commit,
            **parts_from_bundle(parts),
        )

class KeyMap[K, T](ReadableKeyMap[K, T], ABC):
    """Writable keyed entries ``K → T`` (``ReadableKeyMap`` + mutations).

    ``KvStore`` uses ``T = V`` (documents). ``Index`` uses ``T = M`` (per-key
    metadata; ``set`` is metadata-only and typically errors if the key is not
    already indexed). Default ``batch_get`` / ``batch_set`` / ``batch_delete``
    may loop over point ops; overrides should prefer native bulk. Composed maps
    (``zip``, ``fallback``, ``mirror``, …) must delegate to each part's
    ``batch_get`` (etc.), not reimplement ``N×get``.

    ``ttl`` / ``created_at`` are accepted on the ABC so ``KvStore`` (TTL writes)
    and plain ``KeyMap`` share one signature; non-store maps ignore them.
    """

    def set_if_absent(
        self,
        key: K,
        value: T,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> bool:
        """Write ``key`` only if it is absent. ``True`` if this call wrote it.

        First of the ``set_if_*`` family: a write the medium performs only when
        a condition holds, reporting whether it did. Compare-and-set joins it
        later as ``set_if_matches``, with its own capability — a backend can
        have one primitive and not the other.

        **"Absent" means absent to the medium.** Where kvtsjl emulates TTL
        rather than the medium enforcing it, an expired entry is still a row
        or an object, so the native primitive refuses and this returns
        ``False`` even though ``get`` returns ``None``. A caller that wants
        expired entries treated as absent runs ``gc_expired_keys`` first.
        Reconciling the two silently would mean a read-modify-write, which is
        the race this method exists to avoid.
        """
        raise KvStoreUnsupported(
            f"{type(self).__name__} does not support set_if_absent"
        )

    @abstractmethod
    def set(
        self,
        key: K,
        value: T,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        """Insert or replace the value for ``key``."""

    @abstractmethod
    def delete(self, key: K) -> bool:
        """Remove ``key`` if present. Return whether it was present."""

    def batch_set(
        self,
        items: Mapping[K, T],
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        """Insert or replace many entries."""
        for key, value in items.items():
            self.set(key, value, ttl=ttl, created_at=created_at)

    def batch_delete(self, keys: Sequence[K]) -> int:
        """Remove many keys. Return how many were present."""
        n = 0
        for key in keys:
            if self.delete(key):
                n += 1
        return n

    def unset(self, key: K) -> bool:
        """Remove this map's value for ``key``, and nothing it shares with another.

        For a map that owns its entries — almost every one — that is
        ``delete``. It differs where an entry is shared: a view of a row that
        other views also write clears only its own part of it. So a
        composition removing *one part* of a record — a zip writing a ``None``
        slot — unsets that part, and removing the record deletes.
        """
        return self.delete(key)

    def batch_unset(self, keys: Sequence[K]) -> int:
        """``unset`` many keys. Return how many held a value."""
        return sum(1 for key in keys if self.unset(key))
