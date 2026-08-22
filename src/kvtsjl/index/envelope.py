"""Index envelope types for ``IndexBackend``."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class EmptyEnvelope:
    """No envelope fields beyond wired ``D`` (``M`` may still wrap ``D`` structurally)."""
