"""KV store backends.

Optional backends are imported from their modules after installing the matching
extra::

    from kvtsjl.backends.redis import RedisKvStore
    from kvtsjl.backends.s3 import S3KvStore
    from kvtsjl.backends.gcs import GcsKvStore
"""

from __future__ import annotations

from kvtsjl.backends.filesystem import FilesystemKvStore
from kvtsjl.backends.memory import MemoryKvStore

__all__ = [
    "FilesystemKvStore",
    "MemoryKvStore",
]
