"""KvStore leaf backends (``KvBackend`` implementations).

Optional document backends are imported from their modules after installing the
matching extra::

    from kvtsjl.backends.redis import RedisKvStore
    from kvtsjl.backends.s3 import S3KvStore
    from kvtsjl.backends.gcs import GcsKvStore
    from kvtsjl.backends.azure import AzureBlobKvStore
    from kvtsjl.backends.sql import SqlDbKvStore, SqliteSqlDbClientAdapter

Index store leaf backends live under ``kvtsjl.backends.index``::

    from kvtsjl.backends.index import MemoryKeyIndex, MemoryTermIndex
    from kvtsjl.backends.index.chroma import ChromaVectorIndex  # kvtsjl[chroma]
"""

from __future__ import annotations

from kvtsjl.backends.filesystem import FilesystemKvStore, FilesystemTtlMode
from kvtsjl.backends.memory import MemoryKvStore

__all__ = [
    "FilesystemKvStore",
    "FilesystemTtlMode",
    "MemoryKvStore",
]
