"""KV store backends.

Optional backends (e.g. Redis) are imported from their modules after installing
the matching extra::

    from kvtsjl.backends.redis import RedisKvStore
"""

from __future__ import annotations

from kvtsjl.backends.filesystem import FilesystemKvStore
from kvtsjl.backends.memory import MemoryKvStore

__all__ = [
    "FilesystemKvStore",
    "MemoryKvStore",
]
