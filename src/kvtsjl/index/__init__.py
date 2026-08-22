"""Logical search indexes over document stores."""

from __future__ import annotations

from kvtsjl.index.abc import Index, IndexHit
from kvtsjl.index.bundle import indexes_from_bundle
from kvtsjl.index.envelope import EmptyEnvelope

__all__ = [
    "EmptyEnvelope",
    "Index",
    "IndexHit",
    "indexes_from_bundle",
]
