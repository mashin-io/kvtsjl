"""Readable and writable keyed maps plus algebraic composition."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any, cast, overload

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
        from kvtsjl.store.logical import KvStore

        if isinstance(self, KvStore):
            from kvtsjl.store.compose.algebra_map import MappedKvStore

            return MappedKvStore(self, forward)
        from kvtsjl.keymap_algebra.map import MappedKeyMap

        return MappedKeyMap(self, forward)

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
        from kvtsjl.store.logical import KvStore

        if isinstance(self, KvStore) and isinstance(other, KvStore):
            from kvtsjl.store.compose.algebra_then import ThenKvStore

            return ThenKvStore(self, other)
        from kvtsjl.keymap_algebra.then import ThenKeyMap

        return ThenKeyMap(self, other)

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
        from kvtsjl.store.logical import KvStore

        if isinstance(self, KvStore) and isinstance(other, KvStore):
            from kvtsjl.store.compose.algebra_then import ThenWithKvStore

            return ThenWithKvStore(self, key_of, other)
        from kvtsjl.keymap_algebra.then import ThenWithKeyMap

        return ThenWithKeyMap(self, key_of, other)

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
        from kvtsjl.store.logical import KvStore

        if isinstance(self, KvStore):
            from kvtsjl.store.compose.algebra_expand import ExpandKvStore

            return ExpandKvStore(self, expander)
        from kvtsjl.keymap_algebra.expand import ExpandKeyMap

        return ExpandKeyMap(self, expander)

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
        from kvtsjl.store.logical import KvStore

        if isinstance(self, KvStore):
            from kvtsjl.store.compose.algebra_expand import ExpandMapKvStore

            return ExpandMapKvStore(self, expander, aggregate)
        from kvtsjl.keymap_algebra.expand import ExpandMapKeyMap

        return ExpandMapKeyMap(self, expander, aggregate)

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
        from kvtsjl.store.logical import KvStore

        if isinstance(self, KvStore) and isinstance(other, KvStore):
            return self.fallback_read(other, promote=promote)
        if isinstance(self, KeyMap) and isinstance(other, KeyMap):
            from kvtsjl.keymap_algebra.coalesce import CoalescedKeyMap

            return CoalescedKeyMap(self, other, promote=promote)
        raise TypeError(
            f"coalesce requires writable KeyMap or KvStore parts, got "
            f"{type(self).__name__} and {type(other).__name__}"
        )

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
        from kvtsjl.store.logical import KvStore

        if isinstance(self, KvStore):
            from kvtsjl.store.compose.algebra_map import IMappedKvStore

            return IMappedKvStore(self, forward, inverse)
        if isinstance(self, KeyMap):
            from kvtsjl.keymap_algebra.map import IMappedKeyMap

            return IMappedKeyMap(self, forward, inverse)
        raise TypeError(f"imap requires a writable KeyMap, got {type(self).__name__}")

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
        from kvtsjl.store.logical import KvStore

        if isinstance(self, KvStore):
            from kvtsjl.store.compose.algebra_map import IMappedKeysKvStore

            return IMappedKeysKvStore(self, to_store, from_store)
        if isinstance(self, KeyMap):
            from kvtsjl.keymap_algebra.map import IMappedKeysKeyMap

            return IMappedKeysKeyMap(self, to_store, from_store)
        raise TypeError(
            f"imap_keys requires a writable KeyMap, got {type(self).__name__}"
        )

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
    def zip(cls, *parts: ReadableKeyMap[Any, Any]) -> ReadableKeyMap[Any, tuple[Any, ...]]:
        """Pointwise product; missing parts are ``None`` in the tuple."""
        from kvtsjl.store.logical import KvStore

        if parts and all(isinstance(p, KvStore) for p in parts):
            from kvtsjl.store.compose.algebra_zip import ZippedKvStore

            return ZippedKvStore(cast(Sequence[KvStore[Any, Any]], parts))
        from kvtsjl.keymap_algebra.zip import ZippedKeyMap

        return ZippedKeyMap(cast(Sequence[KeyMap[Any, Any]], parts))

    @overload
    @classmethod
    def zip_with[ZipK, W](
        cls,
        ctor: Callable[..., W],
        **parts: KvStore[ZipK, Any],
    ) -> KvStore[ZipK, W]: ...

    @overload
    @classmethod
    def zip_with[ZipK, W](
        cls,
        ctor: Callable[..., W],
        **parts: KeyMap[ZipK, Any],
    ) -> KeyMap[ZipK, W]: ...

    @classmethod
    def zip_with[ZipK, W](
        cls,
        ctor: Callable[..., W],
        **parts: ReadableKeyMap[ZipK, Any],
    ) -> ReadableKeyMap[ZipK, W]:
        """Assemble a dataclass (or constructor) from named part maps."""
        from kvtsjl.store.logical import KvStore

        if parts and all(isinstance(p, KvStore) for p in parts.values()):
            from kvtsjl.store.compose.algebra_zip import ZipWithKvStore

            return ZipWithKvStore(ctor, cast(Mapping[str, KvStore[ZipK, Any]], parts))
        from kvtsjl.keymap_algebra.zip import ZipWithKeyMap

        return ZipWithKeyMap(ctor, cast(Mapping[str, KeyMap[ZipK, Any]], parts))

    @classmethod
    def zip_as[ZipK, W](
        cls,
        ctor: type[W],
        parts: ZipPartsBundle[ZipK],
    ) -> ReadableKeyMap[ZipK, W]:
        """Assemble from a dataclass bundle of part maps (strict field typing)."""
        from kvtsjl.keymap_algebra.bundle import parts_from_bundle
        from kvtsjl.store.logical import KvStore

        mapped = parts_from_bundle(parts)
        if all(isinstance(p, KvStore) for p in mapped.values()):
            from kvtsjl.store.compose.algebra_zip import ZipWithKvStore

            return ZipWithKvStore(ctor, cast(Mapping[str, KvStore[ZipK, Any]], mapped))
        from kvtsjl.keymap_algebra.zip import ZipWithKeyMap

        return ZipWithKeyMap(ctor, mapped)


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
