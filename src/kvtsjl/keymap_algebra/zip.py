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
            self._field_names = tuple(field_names)
        elif is_dataclass(ctor):
            self._field_names = tuple(f.name for f in fields(ctor))
        else:
            self._field_names = tuple(parts.keys())
        missing = [n for n in self._field_names if n not in self._parts]
        if missing:
            raise ValueError(f"zip_with missing part stores for fields: {missing}")
        extra = [n for n in self._parts if n not in self._field_names]
        if extra:
            raise ValueError(f"zip_with unknown part names: {extra}")

    def get(self, key: K) -> V | None:
        kwargs = {name: self._parts[name].get(key) for name in self._field_names}
        if all(v is None for v in kwargs.values()):
            return None
        return self._ctor(**kwargs)

    def set(self, key: K, value: V) -> None:
        for name in self._field_names:
            item = getattr(value, name)
            part = self._parts[name]
            if item is None:
                part.delete(key)
            else:
                part.set(key, item)

    def delete(self, key: K) -> bool:
        return any(part.delete(key) for part in self._parts.values())
