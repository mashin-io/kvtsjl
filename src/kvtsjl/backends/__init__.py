"""KV store backends."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from kvtsjl.backends.filesystem import FilesystemKvStore
from kvtsjl.backends.memory import MemoryKvStore

if TYPE_CHECKING:
    from kvtsjl.backends.redis import RedisKvStore as RedisKvStore

__all__ = [
    "FilesystemKvStore",
    "MemoryKvStore",
    "RedisKvStore",
]


def __getattr__(name: str) -> Any:
    if name == "RedisKvStore":
        from kvtsjl.backends.redis import RedisKvStore as _RedisKvStore

        return _RedisKvStore
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
