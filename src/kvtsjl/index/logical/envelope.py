"""Index envelope types for physical index backends."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class EmptyEnvelope:
    """No envelope fields beyond wired ``D`` (``M`` may still wrap ``D`` structurally)."""


@dataclass(frozen=True, slots=True)
class VectorEnvelope:
    """Search / denorm extras on ``VectorRecord`` not round-tripped as ``D`` on wire."""

    document: str | None = None
    embedding: tuple[float, ...] | None = None
    score: float | None = None  # search-only; semantics are backend-defined
