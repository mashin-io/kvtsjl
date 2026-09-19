"""Helpers shared by KeyMap algebra wrappers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import fields, is_dataclass
from typing import Any

from kvtsjl.keymap import ReadableKeyMap
from kvtsjl.keymap_algebra.dict_map import DictKeyMap


def as_keymap[SK, SV](
    col: ReadableKeyMap[SK, SV] | Mapping[SK, SV] | Sequence[tuple[SK, SV]],
) -> ReadableKeyMap[SK, SV]:
    """Normalize expander output to a ``ReadableKeyMap``."""
    if isinstance(col, ReadableKeyMap):
        return col
    if isinstance(col, Mapping):
        return DictKeyMap(col)
    return DictKeyMap.from_pairs(col)


def dataclass_field_names(cls: type[Any]) -> tuple[str, ...]:
    if not is_dataclass(cls):
        raise TypeError(f"zip_with expects a dataclass type, got {cls!r}")
    return tuple(f.name for f in fields(cls))
