"""Logical key-prefix scopes (kind/id segments) for a KvSet."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ScopeSegment:
    """One ``kind`` / ``id`` pair in a logical key prefix."""

    kind: str
    id: str


@dataclass(frozen=True, slots=True)
class Scope:
    """Ordered kind/id segments — a **logical** in-key prefix under the collection binding.

    Scope is not a second addressing axis: it is ergonomic key composition. The
    leaf ``K`` and these segments together form the logical in-key; backends /
    ``KeyLayout`` choose how that material is physicalized (flat string, HASH
    field, nested directories, …).

    Build free-form prefixes with ``Scope.of(...)``, ``extend``, ``child``, or ``/``.
    """

    segments: tuple[ScopeSegment, ...] = ()

    @staticmethod
    def of(**kinds_to_ids: str) -> Scope:
        """Build a scope preserving kwargs insertion order."""
        return Scope(
            segments=tuple(
                ScopeSegment(kind=k, id=str(v)) for k, v in kinds_to_ids.items()
            )
        )

    @classmethod
    def empty(cls) -> Scope:
        return cls()

    def extend(self, kind: str, id: str) -> Scope:
        """Append one kind/id segment."""
        return Scope(segments=self.segments + (ScopeSegment(kind=kind, id=str(id)),))

    def child(self, **kinds_to_ids: str) -> Scope:
        """Append segments from kwargs (insertion order)."""
        extra = tuple(ScopeSegment(kind=k, id=str(v)) for k, v in kinds_to_ids.items())
        return Scope(segments=self.segments + extra)

    def __truediv__(
        self, other: ScopeSegment | tuple[str, str] | Scope
    ) -> Scope:
        """Path-like append: ``scope / ("kind", "id")`` or ``scope / other_scope``."""
        if isinstance(other, Scope):
            return Scope(segments=self.segments + other.segments)
        if isinstance(other, ScopeSegment):
            return self.extend(other.kind, other.id)
        kind, seg_id = other
        return self.extend(kind, seg_id)

    def path_display(self) -> str:
        """Human-readable ``kind/id/...`` view (not a storage path)."""
        if not self.segments:
            return ""
        return "/".join(f"{s.kind}/{s.id}" for s in self.segments)


# Alias for callers who think in prefixes rather than "scope".
KeyPrefix = Scope
