"""Minimal keyed map: get/set/delete plus algebraic composition."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, overload

if TYPE_CHECKING:
    from kvtsjl.keymap_algebra.bundle import ZipPartsBundle


class KeyMap[K, T](ABC):
    """Keyed entries ``K → T`` with single- and batch-form mutations.

    ``KvStore`` uses ``T = V`` (documents). ``Index`` uses ``T = M`` (per-key
    metadata; ``set`` is metadata-only and typically errors if the key is not
    already indexed). Batch methods default to looping; override for native
    bulk APIs.

    Algebraic operators (``map``, ``zip``, ``then``, ``expand``, …) return
    derived ``KeyMap`` views; see ``kvtsjl.keymap_algebra``.
    """

    @abstractmethod
    def get(self, key: K) -> T | None:
        """Return the value for ``key``, or ``None`` if absent."""

    @abstractmethod
    def set(self, key: K, value: T) -> None:
        """Insert or replace the value for ``key``."""

    @abstractmethod
    def delete(self, key: K) -> bool:
        """Remove ``key`` if present. Return whether it was present."""

    def batch_get(self, keys: Sequence[K]) -> dict[K, T]:
        """Return ``{key: value}`` for keys that exist."""
        out: dict[K, T] = {}
        for key in keys:
            value = self.get(key)
            if value is not None:
                out[key] = value
        return out

    def batch_set(self, items: Mapping[K, T]) -> None:
        """Insert or replace many entries."""
        for key, value in items.items():
            self.set(key, value)

    def batch_delete(self, keys: Sequence[K]) -> int:
        """Remove many keys. Return how many were present."""
        n = 0
        for key in keys:
            if self.delete(key):
                n += 1
        return n

    def map[U](self, forward: Callable[[T], U]) -> KeyMap[K, U]:
        """Read-only value view."""
        from kvtsjl.keymap_algebra.map import MappedKeyMap

        return MappedKeyMap(self, forward)

    def imap[U](
        self, forward: Callable[[T], U], inverse: Callable[[U], T]
    ) -> KeyMap[K, U]:
        """Invertible value codec."""
        from kvtsjl.keymap_algebra.map import IMappedKeyMap

        return IMappedKeyMap(self, forward, inverse)

    def imap_keys[NK](
        self,
        to_store: Callable[[NK], K],
        from_store: Callable[[K], NK] | None = None,
    ) -> KeyMap[NK, T]:
        """Caller keys ``NK`` mapped onto this map's keys ``K``."""
        from kvtsjl.keymap_algebra.map import IMappedKeysKeyMap

        return IMappedKeysKeyMap(self, to_store, from_store)

    @overload
    @classmethod
    def zip[ZipK, A, B](
        cls, a: KeyMap[ZipK, A], b: KeyMap[ZipK, B], /
    ) -> KeyMap[ZipK, tuple[A | None, B | None]]: ...

    @overload
    @classmethod
    def zip[ZipK, A, B, C](
        cls, a: KeyMap[ZipK, A], b: KeyMap[ZipK, B], c: KeyMap[ZipK, C], /
    ) -> KeyMap[ZipK, tuple[A | None, B | None, C | None]]: ...

    @overload
    @classmethod
    def zip[ZipK, A, B, C, D](
        cls,
        a: KeyMap[ZipK, A],
        b: KeyMap[ZipK, B],
        c: KeyMap[ZipK, C],
        d: KeyMap[ZipK, D],
        /
    ) -> KeyMap[ZipK, tuple[A | None, B | None, C | None, D | None]]: ...

    @overload
    @classmethod
    def zip[ZipK, A, B, C, D, E](
        cls,
        a: KeyMap[ZipK, A],
        b: KeyMap[ZipK, B],
        c: KeyMap[ZipK, C],
        d: KeyMap[ZipK, D],
        e: KeyMap[ZipK, E],
        /
    ) -> KeyMap[ZipK, tuple[A | None, B | None, C | None, D | None, E | None]]: ...

    @classmethod
    def zip(cls, *parts: KeyMap[Any, Any]) -> KeyMap[Any, tuple[Any, ...]]:
        """Pointwise product; missing parts are ``None`` in the tuple."""
        from kvtsjl.keymap_algebra.zip import ZippedKeyMap

        return ZippedKeyMap(parts)

    @classmethod
    def zip_with[ZipK, W](
        cls,
        ctor: Callable[..., W],
        **parts: KeyMap[ZipK, Any],
    ) -> KeyMap[ZipK, W]:
        """Assemble a dataclass (or constructor) from named part maps.

        For strict per-part value types use ``zip_as`` or annotate a ``TypedDict``
        and call ``zip_with(ctor, **parts)``.
        """
        from kvtsjl.keymap_algebra.zip import ZipWithKeyMap

        return ZipWithKeyMap(ctor, parts)

    @classmethod
    def zip_as[ZipK, W](
        cls,
        ctor: type[W],
        parts: ZipPartsBundle[ZipK],
    ) -> KeyMap[ZipK, W]:
        """Assemble from a dataclass bundle of part maps (strict field typing).

        Prefer over ``zip_with`` when each part store has a distinct value type::

            @dataclass
            class DocParts:
                meta: KvStore[str, str]
                body: KvStore[str, str]

            articles = KvStore.zip_as(Doc, DocParts(meta=meta_s, body=body_s))

        ``TypedDict`` users can stay on ``zip_with`` with an annotated dict::

            parts: DocPartsTD = {"meta": meta_s, "body": body_s}
            articles = KvStore.zip_with(Doc, **parts)
        """
        from kvtsjl.keymap_algebra.bundle import parts_from_bundle
        from kvtsjl.keymap_algebra.zip import ZipWithKeyMap

        return ZipWithKeyMap(ctor, parts_from_bundle(parts))

    def then[V](self, other: KeyMap[T, V]) -> KeyMap[K, V]:
        """``other[self[k]]`` — compose partial maps (read-only view)."""
        from kvtsjl.keymap_algebra.then import ThenKeyMap

        return ThenKeyMap(self, other)

    def then_with[J, V](
        self,
        key_of: Callable[[K, T], J],
        other: KeyMap[J, V],
    ) -> KeyMap[K, V]:
        """``other[key_of(k, self[k])]`` (read-only view)."""
        from kvtsjl.keymap_algebra.then import ThenWithKeyMap

        return ThenWithKeyMap(self, key_of, other)

    def expand[SK, SV](
        self,
        expander: Callable[
            [K, T], KeyMap[SK, SV] | Mapping[SK, SV] | Sequence[tuple[SK, SV]]
        ],
    ) -> KeyMap[K, KeyMap[SK, SV]]:
        """Nest a collection under each key (same outer ``K``; read-only)."""
        from kvtsjl.keymap_algebra.expand import ExpandKeyMap

        return ExpandKeyMap(self, expander)

    def expand_map[SK, SV, U](
        self,
        expander: Callable[
            [K, T], KeyMap[SK, SV] | Mapping[SK, SV] | Sequence[tuple[SK, SV]]
        ],
        aggregate: Callable[[K, T, KeyMap[SK, SV]], U],
    ) -> KeyMap[K, U]:
        """Expand then fold ``(k, v, col) → U`` (read-only)."""
        from kvtsjl.keymap_algebra.expand import ExpandMapKeyMap

        return ExpandMapKeyMap(self, expander, aggregate)

    def coalesce(
        self, other: KeyMap[K, T], *, promote: bool = True
    ) -> KeyMap[K, T]:
        """Left-biased merge: prefer ``self``, else ``other`` (writes to ``self``)."""
        from kvtsjl.keymap_algebra.coalesce import CoalescedKeyMap

        return CoalescedKeyMap(self, other, promote=promote)
