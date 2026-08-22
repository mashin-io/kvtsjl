"""Collect part ``KeyMap`` / ``KvStore`` fields from a dataclass bundle."""

from __future__ import annotations

from dataclasses import fields, is_dataclass
from typing import TYPE_CHECKING, Any, Protocol

from kvtsjl.keymap import KeyMap

if TYPE_CHECKING:
    from kvtsjl.store.logical import KvStore


class ZipPartsBundle[ZipK](Protocol):
    """Dataclass bundle whose fields are ``KeyMap[ZipK, *]`` part maps."""

    pass


def parts_from_bundle(bundle: object) -> dict[str, KeyMap[Any, Any]]:
    """Collect ``KeyMap`` fields from a dataclass instance (top-level only)."""
    if not is_dataclass(bundle) or isinstance(bundle, type):
        raise TypeError("zip_as expects a dataclass instance")
    parts: dict[str, KeyMap[Any, Any]] = {}
    for f in fields(bundle):
        value = getattr(bundle, f.name)
        if not isinstance(value, KeyMap):
            raise TypeError(
                f"zip_as bundle field {f.name!r} must be a KeyMap, got {type(value)!r}"
            )
        parts[f.name] = value
    if not parts:
        raise ValueError("zip_as bundle has no KeyMap fields")
    return parts


def stores_from_bundle(bundle: object) -> dict[str, KvStore[Any, Any]]:
    """Like ``parts_from_bundle`` but requires ``KvStore`` fields."""
    from kvtsjl.store.logical import KvStore

    parts = parts_from_bundle(bundle)
    stores: dict[str, KvStore[Any, Any]] = {}
    for name, part in parts.items():
        if not isinstance(part, KvStore):
            raise TypeError(
                f"zip_as bundle field {name!r} must be a KvStore, got {type(part)!r}"
            )
        stores[name] = part
    return stores
