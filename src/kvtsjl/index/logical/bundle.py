"""Collect indexes from a dataclass bundle."""

from __future__ import annotations

from dataclasses import fields, is_dataclass
from typing import Any, cast

from kvtsjl.index.logical.abc import Index


def indexes_from_bundle(bundle: object) -> tuple[Index[Any, Any, Any, Any], ...]:
    """Collect ``Index`` fields from a dataclass instance (top-level only).

    Index fields must be ``init=True`` so ``IndexedKvStore`` can remap them when
    scoping (``dataclasses.replace`` / reconstruct cannot set ``init=False``).
    """
    if not is_dataclass(bundle) or isinstance(bundle, type):
        raise TypeError("indexed_as expects a dataclass instance")
    found: list[Index[Any, Any, Any, Any]] = []
    for f in fields(bundle):
        value = getattr(bundle, f.name)
        if isinstance(value, Index):
            if not f.init:
                raise TypeError(
                    f"Index field {f.name!r} must be an init field "
                    "(scoped remapping cannot set init=False fields)"
                )
            found.append(cast(Index[Any, Any, Any, Any], value))
    if not found:
        raise ValueError("dataclass bundle has no Index fields")
    return tuple(found)
