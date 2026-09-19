"""Pointwise ``zip`` / ``zip_with`` (optional parts)."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import fields, is_dataclass
from typing import Any

from kvtsjl.keymap import KeyMap


class ZippedKeyMap[K](KeyMap[K, tuple[Any, ...]]):
    """Pointwise product: row if any part present; missing parts are ``None``."""

    def __init__(self, parts: Sequence[KeyMap[K, Any]]) -> None:
        if len(parts) < 2:
            raise ValueError("zip requires at least two KeyMaps")
        self._parts = tuple(parts)

    def get(self, key: K) -> tuple[Any, ...] | None:
        values = tuple(part.get(key) for part in self._parts)
        if all(v is None for v in values):
            return None
        return values

    def set(self, key: K, value: tuple[Any, ...]) -> None:
        if len(value) != len(self._parts):
            raise ValueError(
                f"zip set expects {len(self._parts)}-tuple, got {len(value)}"
            )
        for part, item in zip(self._parts, value, strict=True):
            if item is None:
                part.delete(key)
            else:
                part.set(key, item)

    def delete(self, key: K) -> bool:
        return any(part.delete(key) for part in self._parts)

    def batch_get(self, keys: Sequence[K]) -> dict[K, tuple[Any, ...]]:
        """Fan out to each part's ``batch_get`` once; assemble like point ``get``.

        Empty ``keys`` returns ``{}`` without calling parts. Duplicate keys are
        collapsed (order-preserving) before part calls; the result is a dict so
        duplicates would last-win either way.
        """
        if not keys:
            return {}
        unique = list(dict.fromkeys(keys))
        part_maps = [part.batch_get(unique) for part in self._parts]
        out: dict[K, tuple[Any, ...]] = {}
        for key in unique:
            values = tuple(pm.get(key) for pm in part_maps)
            if not all(v is None for v in values):
                out[key] = values
        return out

    def batch_set(self, items: Mapping[K, tuple[Any, ...]]) -> None:
        """Fan out to each part's ``batch_set`` / ``batch_delete`` once.

        Per part: non-``None`` tuple slots are ``batch_set``; ``None`` slots are
        ``batch_delete`` (same as point ``set``).
        """
        if not items:
            return
        n = len(self._parts)
        for value in items.values():
            if len(value) != n:
                raise ValueError(f"zip set expects {n}-tuple, got {len(value)}")
        for index, part in enumerate(self._parts):
            to_set: dict[K, Any] = {}
            to_delete: list[K] = []
            for key, value in items.items():
                item = value[index]
                if item is None:
                    to_delete.append(key)
                else:
                    to_set[key] = item
            if to_set:
                part.batch_set(to_set)
            if to_delete:
                part.batch_delete(to_delete)

    def batch_delete(self, keys: Sequence[K]) -> int:
        """Fan out to each part's ``batch_delete`` once; count like point ``delete``.

        A key counts as present if any part has it (probed via part ``batch_get``
        before deletes). Duplicate keys in ``keys`` count at most once.
        """
        if not keys:
            return 0
        unique = list(dict.fromkeys(keys))
        present: set[K] = set()
        for part in self._parts:
            present.update(part.batch_get(unique).keys())
        for part in self._parts:
            part.batch_delete(unique)
        n = 0
        seen: set[K] = set()
        for key in keys:
            if key in present and key not in seen:
                n += 1
                seen.add(key)
        return n


class ZipWithKeyMap[K, V](KeyMap[K, V]):
    """``zip`` assembled via a dataclass (or constructor) with optional fields."""

    def __init__(
        self,
        ctor: Callable[..., V],
        parts: Mapping[str, KeyMap[K, Any]],
        *,
        field_names: Sequence[str] | None = None,
    ) -> None:
        if not parts:
            raise ValueError("zip_with requires at least one part")
        self._ctor = ctor
        self._parts = dict(parts)
        if field_names is not None:
            self.field_names = tuple(field_names)
        elif is_dataclass(ctor):
            self.field_names = tuple(f.name for f in fields(ctor))
        else:
            self.field_names = tuple(parts.keys())
        missing = [n for n in self.field_names if n not in self._parts]
        if missing:
            raise ValueError(f"zip_with missing part stores for fields: {missing}")
        extra = [n for n in self._parts if n not in self.field_names]
        if extra:
            raise ValueError(f"zip_with unknown part names: {extra}")

    def get(self, key: K) -> V | None:
        kwargs = {name: self._parts[name].get(key) for name in self.field_names}
        if all(v is None for v in kwargs.values()):
            return None
        return self._ctor(**kwargs)

    def set(self, key: K, value: V) -> None:
        for name in self.field_names:
            item = getattr(value, name)
            part = self._parts[name]
            if item is None:
                part.delete(key)
            else:
                part.set(key, item)

    def delete(self, key: K) -> bool:
        return any(part.delete(key) for part in self._parts.values())

    def batch_get(self, keys: Sequence[K]) -> dict[K, V]:
        """Fan out to each part's ``batch_get`` once; assemble like point ``get``.

        Empty ``keys`` returns ``{}`` without calling parts. Duplicate keys are
        collapsed (order-preserving) before part calls; the result is a dict so
        duplicates would last-win either way.
        """
        if not keys:
            return {}
        unique = list(dict.fromkeys(keys))
        part_found = {
            name: part.batch_get(unique) for name, part in self._parts.items()
        }
        out: dict[K, V] = {}
        for key in unique:
            kwargs = {name: part_found[name].get(key) for name in self.field_names}
            if not all(v is None for v in kwargs.values()):
                out[key] = self._ctor(**kwargs)
        return out

    def batch_set(self, items: Mapping[K, V]) -> None:
        """Fan out to each part's ``batch_set`` / ``batch_delete`` once.

        Per field: non-``None`` values are ``batch_set``; ``None`` values are
        ``batch_delete`` (same as point ``set``).
        """
        if not items:
            return
        for name in self.field_names:
            part = self._parts[name]
            to_set: dict[K, Any] = {}
            to_delete: list[K] = []
            for key, value in items.items():
                item = getattr(value, name)
                if item is None:
                    to_delete.append(key)
                else:
                    to_set[key] = item
            if to_set:
                part.batch_set(to_set)
            if to_delete:
                part.batch_delete(to_delete)

    def batch_delete(self, keys: Sequence[K]) -> int:
        """Fan out to each part's ``batch_delete`` once; count like point ``delete``.

        A key counts as present if any part has it (probed via part ``batch_get``
        before deletes). Duplicate keys in ``keys`` count at most once.
        """
        if not keys:
            return 0
        unique = list(dict.fromkeys(keys))
        present: set[K] = set()
        for part in self._parts.values():
            present.update(part.batch_get(unique).keys())
        for part in self._parts.values():
            part.batch_delete(unique)
        n = 0
        seen: set[K] = set()
        for key in keys:
            if key in present and key not in seen:
                n += 1
                seen.add(key)
        return n
