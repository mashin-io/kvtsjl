"""Index store leaf backends (``Index`` implementations on a medium or in-process)."""

from __future__ import annotations

from kvtsjl.backends.index.memory import MemoryKeyIndex, MemoryTermIndex

__all__ = [
    "MemoryKeyIndex",
    "MemoryTermIndex",
]
