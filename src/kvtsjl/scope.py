"""Orthogonal scope partitions for a KvSet."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ScopeSegment:
    kind: str
    id: str


@dataclass(frozen=True, slots=True)
class Scope:
    """Ordered named segments — physical in-key prefix under a collection binding."""

    segments: tuple[ScopeSegment, ...] = ()

    @staticmethod
    def of(**kinds_to_ids: str) -> Scope:
        """Build a scope preserving kwargs insertion order."""
        return Scope(
            segments=tuple(
                ScopeSegment(kind=k, id=str(v)) for k, v in kinds_to_ids.items()
            )
        )

    def extend(self, kind: str, id: str) -> Scope:
        return Scope(segments=self.segments + (ScopeSegment(kind=kind, id=str(id)),))

    def child(self, **kinds_to_ids: str) -> Scope:
        extra = tuple(ScopeSegment(kind=k, id=str(v)) for k, v in kinds_to_ids.items())
        return Scope(segments=self.segments + extra)

    def path_display(self) -> str:
        if not self.segments:
            return ""
        return "/".join(f"{s.kind}/{s.id}" for s in self.segments)

    @classmethod
    def empty(cls) -> Scope:
        return cls()

    @classmethod
    def tenant(cls, tenant_id: str) -> Scope:
        return cls.of(tenant=tenant_id)

    @classmethod
    def user(cls, tenant_id: str, user_id: str) -> Scope:
        return cls.of(tenant=tenant_id, user=user_id)

    @classmethod
    def session(cls, tenant_id: str, user_id: str, session_id: str) -> Scope:
        return cls.of(tenant=tenant_id, user=user_id, session=session_id)

    @classmethod
    def turn(cls, tenant_id: str, user_id: str, session_id: str, turn_id: str) -> Scope:
        return cls.of(tenant=tenant_id, user=user_id, session=session_id, turn=turn_id)
