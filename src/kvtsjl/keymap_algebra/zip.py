"""Pointwise ``zip`` / ``zip_with`` (optional parts; configurable presence)."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import fields, is_dataclass
from datetime import datetime
from typing import Any

from kvtsjl.exceptions import KvStoreError
from kvtsjl.keymap import KeyMap
from kvtsjl.keymap_algebra.topology import RecordTopologyMixin, Role, Topology
from kvtsjl.store.schema.ttl import TtlPolicy


def normalize_mandatory(
    n: int, mandatory: Sequence[bool] | None
) -> tuple[bool, ...]:
    """``None`` → all False (OR). Length must match ``n`` when provided."""
    if mandatory is None:
        return tuple(False for _ in range(n))
    if len(mandatory) != n:
        raise ValueError(
            f"mandatory length {len(mandatory)} must match number of parts ({n})"
        )
    return tuple(bool(m) for m in mandatory)


def row_present(values: Sequence[Any | None], mandatory: Sequence[bool]) -> bool:
    """Row exists iff every mandatory slot is present and not all values are None."""
    if all(v is None for v in values):
        return False
    for value, required in zip(values, mandatory, strict=True):
        if required and value is None:
            return False
    return True


class ZippedKeyMap[K](RecordTopologyMixin[K, tuple[Any, ...]], KeyMap[K, tuple[Any, ...]]):
    """Pointwise product; presence controlled by ``mandatory`` (default OR)."""

    def __init__(
        self,
        parts: Sequence[KeyMap[K, Any]],
        *,
        mandatory: Sequence[bool] | None = None,
    ) -> None:
        if len(parts) < 2:
            raise ValueError("zip requires at least two KeyMaps")
        self._parts = tuple(parts)
        self._mandatory = normalize_mandatory(len(parts), mandatory)

    def topology(self) -> Topology[K, tuple[Any, ...]]:
        """Every part a component: no part alone decides the row exists."""
        parts = tuple(
            self._part(f"part[{index}]", Role.COMPONENT, part, _slot(index))
            for index, part in enumerate(self._parts)
        )
        return Topology(
            parts=parts,
            assemble=lambda held: self._row(tuple(held[part] for part in parts)),
        )

    def _row(self, values: tuple[Any, ...]) -> tuple[Any, ...] | None:
        return values if row_present(values, self._mandatory) else None

    def _check(self, values: Iterable[tuple[Any, ...]]) -> None:
        for value in values:
            if len(value) != len(self._parts):
                raise ValueError(
                    f"zip set expects {len(self._parts)}-tuple, got {len(value)}"
                )

    def set(
        self,
        key: K,
        value: tuple[Any, ...],
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        self._check([value])
        super().set(key, value, ttl=ttl, created_at=created_at)

    def batch_set(
        self,
        items: Mapping[K, tuple[Any, ...]],
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> None:
        self._check(items.values())
        super().batch_set(items, ttl=ttl, created_at=created_at)


class ZipWithKeyMap[K, V](RecordTopologyMixin[K, V], KeyMap[K, V]):
    """``zip`` assembled via a dataclass (or constructor) with optional fields."""

    def __init__(
        self,
        ctor: Callable[..., V],
        parts: Mapping[str, KeyMap[K, Any]],
        *,
        field_names: Sequence[str] | None = None,
        mandatory: Sequence[bool] | None = None,
        commit_part: str | None = None,
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
        self._mandatory = normalize_mandatory(len(self.field_names), mandatory)
        if commit_part is not None and commit_part not in self._parts:
            raise ValueError(
                f"commit_part {commit_part!r} is not one of {tuple(self._parts)}"
            )
        self._commit_part = commit_part

    def topology(self) -> Topology[K, V]:
        """The commit part is the authority; every other part a component.

        Without a commit part no part alone decides the record exists, so
        every part is a component and presence stays with ``mandatory``.
        """
        parts = tuple(
            self._part(
                name,
                Role.AUTHORITY if name == self._commit_part else Role.COMPONENT,
                self._parts[name],
                _field(name),
            )
            for name in self.field_names
        )
        return Topology(
            parts=parts,
            assemble=lambda held: self._record(tuple(held[part] for part in parts)),
        )

    def _record(self, values: tuple[Any, ...]) -> V | None:
        if not row_present(values, self._mandatory):
            return None
        return self._ctor(**dict(zip(self.field_names, values, strict=True)))

    def set_if_absent(
        self,
        key: K,
        value: V,
        *,
        ttl: TtlPolicy | None = None,
        created_at: datetime | None = None,
    ) -> bool:
        """Write the other parts, then commit only if the record is absent.

        Two writers may both write the non-commit parts — nothing can see them
        while the commit part is absent — and only one commits. Where those
        parts are keyed by the record key, the winner's commit can end up
        beside the loser's body; key them by content to rule that out.
        """
        if self._commit_part is None:
            raise KvStoreError(
                "set_if_absent on a zip needs a commit_part: without one no "
                "part decides whether the record exists"
            )
        return super().set_if_absent(key, value, ttl=ttl, created_at=created_at)


def _slot(index: int) -> Callable[[tuple[Any, ...]], Any]:
    return lambda value: value[index]


def _field(name: str) -> Callable[[object], Any]:
    return lambda value: getattr(value, name)
