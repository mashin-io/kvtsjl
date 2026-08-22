"""Collect indexes from a dataclass bundle."""

from __future__ import annotations

from dataclasses import fields, is_dataclass
from typing import Any

from kvtsjl.index.abc import Index


def indexes_from_bundle(bundle: object) -> tuple[Index[Any, Any, Any, Any], ...]:
    """Collect ``Index`` fields from a dataclass instance (top-level only)."""
    if not is_dataclass(bundle) or isinstance(bundle, type):
        raise TypeError("indexed_as expects a dataclass instance")
    found: list[Index[Any, Any, Any, Any]] = []
    for f in fields(bundle):
        value = getattr(bundle, f.name)
        if isinstance(value, Index):
            found.append(value)
    if not found:
        raise ValueError("dataclass bundle has no Index fields")
    return tuple(found)
