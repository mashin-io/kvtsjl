"""Builders: KeyMap core alone, or Core + fan-out mixins when parts are stores."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from typing import Any, cast

from kvtsjl.keymap import KeyMap, ReadableKeyMap
from kvtsjl.keymap_algebra.expand import ExpandKeyMap, ExpandMapKeyMap
from kvtsjl.keymap_algebra.map import IMappedKeyMap, IMappedKeysKeyMap, MappedKeyMap
from kvtsjl.keymap_algebra.then import ThenKeyMap, ThenWithKeyMap
from kvtsjl.keymap_algebra.zip import ZippedKeyMap, ZipWithKeyMap
from kvtsjl.store.logical import KvStore


def build_map[K, T, U](
    underlying: ReadableKeyMap[K, T], forward: Callable[[T], U]
) -> ReadableKeyMap[K, U]:
    if isinstance(underlying, KvStore):
        from kvtsjl.store.compose.mapped import MappedKvStore

        return MappedKvStore(underlying, forward)
    return MappedKeyMap(underlying, forward)


def build_imap[K, T, U](
    underlying: KeyMap[K, T],
    forward: Callable[[T], U],
    inverse: Callable[[U], T],
) -> KeyMap[K, U]:
    if isinstance(underlying, KvStore):
        from kvtsjl.store.compose.mapped import IMappedKvStore

        return IMappedKvStore(underlying, forward, inverse)
    return IMappedKeyMap(underlying, forward, inverse)


def build_imap_keys[K, SK, T](
    underlying: KeyMap[SK, T],
    to_store: Callable[[K], SK],
    from_store: Callable[[SK], K] | None = None,
) -> KeyMap[K, T]:
    if isinstance(underlying, KvStore):
        from kvtsjl.store.compose.mapped import IMappedKeysKvStore

        return IMappedKeysKvStore(underlying, to_store, from_store)
    return IMappedKeysKeyMap(underlying, to_store, from_store)


def build_then[K, J, V](
    left: ReadableKeyMap[K, J], right: ReadableKeyMap[J, V]
) -> ReadableKeyMap[K, V]:
    if isinstance(left, KvStore) and isinstance(right, KvStore):
        from kvtsjl.store.compose.then import ThenKvStore

        return ThenKvStore(left, right)
    return ThenKeyMap(left, right)


def build_then_with[K, T, J, V](
    left: ReadableKeyMap[K, T],
    key_of: Callable[[K, T], J],
    right: ReadableKeyMap[J, V],
) -> ReadableKeyMap[K, V]:
    if isinstance(left, KvStore) and isinstance(right, KvStore):
        from kvtsjl.store.compose.then import ThenWithKvStore

        return ThenWithKvStore(left, key_of, right)
    return ThenWithKeyMap(left, key_of, right)


def build_expand[K, T, SK, SV](
    underlying: ReadableKeyMap[K, T],
    expander: Callable[
        [K, T], ReadableKeyMap[SK, SV] | Mapping[SK, SV] | Sequence[tuple[SK, SV]]
    ],
) -> ReadableKeyMap[K, ReadableKeyMap[SK, SV]]:
    if isinstance(underlying, KvStore):
        from kvtsjl.store.compose.expand import ExpandKvStore

        return ExpandKvStore(underlying, expander)
    return ExpandKeyMap(underlying, expander)


def build_expand_map[K, T, SK, SV, U](
    underlying: ReadableKeyMap[K, T],
    expander: Callable[
        [K, T], ReadableKeyMap[SK, SV] | Mapping[SK, SV] | Sequence[tuple[SK, SV]]
    ],
    aggregate: Callable[[K, T, ReadableKeyMap[SK, SV]], U],
) -> ReadableKeyMap[K, U]:
    if isinstance(underlying, KvStore):
        from kvtsjl.store.compose.expand import ExpandMapKvStore

        return ExpandMapKvStore(underlying, expander, aggregate)
    return ExpandMapKeyMap(underlying, expander, aggregate)


def build_zip(
    *parts: ReadableKeyMap[Any, Any],
    mandatory: Sequence[bool] | None = None,
    created_at_merge: Callable[[Sequence[datetime | None]], datetime | None]
    | None = None,
) -> ReadableKeyMap[Any, tuple[Any, ...]]:
    if parts and all(isinstance(p, KvStore) for p in parts):
        from kvtsjl.store.compose.zipped import ZippedKvStore

        return ZippedKvStore(
            cast(Sequence[KvStore[Any, Any]], parts),
            mandatory=mandatory,
            created_at_merge=created_at_merge,
        )
    if created_at_merge is not None:
        raise TypeError("created_at_merge requires KvStore parts")
    return ZippedKeyMap(cast(Sequence[KeyMap[Any, Any]], parts), mandatory=mandatory)


def build_zip_with[ZipK, W](
    ctor: Callable[..., W],
    *,
    mandatory: Sequence[bool] | None = None,
    created_at_merge: Callable[[Sequence[datetime | None]], datetime | None]
    | None = None,
    commit_part: str | None = None,
    after_commit: Sequence[str] = (),
    **parts: ReadableKeyMap[ZipK, Any],
) -> ReadableKeyMap[ZipK, W]:
    if parts and all(isinstance(p, KvStore) for p in parts.values()):
        from kvtsjl.store.compose.zipped import ZipWithKvStore

        return ZipWithKvStore(
            ctor,
            cast(Mapping[str, KvStore[ZipK, Any]], parts),
            commit_part=commit_part,
            after_commit=after_commit,
            mandatory=mandatory,
            created_at_merge=created_at_merge,
        )
    if created_at_merge is not None:
        raise TypeError("created_at_merge requires KvStore parts")
    return ZipWithKeyMap(
        ctor,
        cast(Mapping[str, KeyMap[ZipK, Any]], parts),
        mandatory=mandatory,
        commit_part=commit_part,
        after_commit=after_commit,
    )


def build_coalesce[K, T](
    left: ReadableKeyMap[K, T],
    right: ReadableKeyMap[K, T],
    *,
    promote: bool = True,
) -> ReadableKeyMap[K, T]:
    """Left-biased merge; KvStore parts get scan/GC fan-out (scan never promotes)."""
    from kvtsjl.keymap_algebra.coalesce import CoalescedKeyMap

    if isinstance(left, KvStore) and isinstance(right, KvStore):
        from kvtsjl.store.compose.fallback import CoalescedKvStore

        return CoalescedKvStore(left, right, promote=promote)
    if isinstance(left, KeyMap) and isinstance(right, KeyMap):
        return CoalescedKeyMap(left, right, promote=promote)
    raise TypeError(
        f"coalesce requires writable KeyMap or KvStore parts, got "
        f"{type(left).__name__} and {type(right).__name__}"
    )
