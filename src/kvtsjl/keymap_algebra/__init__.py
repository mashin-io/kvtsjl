"""Algebraic operators on ``KeyMap`` (functor / zip / then / expand / coalesce)."""

from __future__ import annotations

from kvtsjl.keymap_algebra.bundle import ZipPartsBundle
from kvtsjl.keymap_algebra.coalesce import CoalescedKeyMap
from kvtsjl.keymap_algebra.dict_map import DictKeyMap
from kvtsjl.keymap_algebra.expand import ExpandKeyMap, ExpandMapKeyMap
from kvtsjl.keymap_algebra.map import IMappedKeyMap, IMappedKeysKeyMap, MappedKeyMap
from kvtsjl.keymap_algebra.then import ThenKeyMap, ThenWithKeyMap
from kvtsjl.keymap_algebra.zip import ZippedKeyMap, ZipWithKeyMap

__all__ = [
    "CoalescedKeyMap",
    "DictKeyMap",
    "ExpandKeyMap",
    "ExpandMapKeyMap",
    "IMappedKeyMap",
    "IMappedKeysKeyMap",
    "MappedKeyMap",
    "ThenKeyMap",
    "ThenWithKeyMap",
    "ZipPartsBundle",
    "ZipWithKeyMap",
    "ZippedKeyMap",
]
