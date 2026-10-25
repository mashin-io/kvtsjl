"""Every leaf medium, built the same way, for tests that hold across all of them.

Each is the real medium or the fake that enforces the medium's own
preconditions: moto for S3, fakeredis, an in-memory SQLite connection, and
the GCS and Azure fakes in this directory.

The fakes enforce what this suite believes the services do. To check that
belief against the services' own emulators, set ``KVTSJL_AZURITE`` to an
Azurite connection string and ``KVTSJL_FAKE_GCS`` to a fake-gcs-server
endpoint: the emulated media then join every list below, and every test
over the media runs against them too.
"""

from __future__ import annotations

from datetime import timedelta
import os
from typing import Any

import pytest

from kvtsjl import KvSet, MemoryKvStore, SerDe, TtlPolicy
from kvtsjl.backends.sql.sqlite import SqliteSqlDbClientAdapter
from kvtsjl.backends.sql.store import SqlDbKvStore
from kvtsjl.store.schema.ttl import ExpiryGc
from tests.sql_helpers import ensure_sql_table, make_sql_kvset

_AZURITE = "KVTSJL_AZURITE" in os.environ
_FAKE_GCS = "KVTSJL_FAKE_GCS" in os.environ

MEDIA = [
    "memory", "sql", "redis", "redis_hash", "s3", "gcs", "azure", "filesystem",
    *(["azurite"] if _AZURITE else []),
    *(["fake_gcs"] if _FAKE_GCS else []),
]
# The modes that keep expiry in a header, sidecar or metadata of their own: a
# conditional write that dropped its headers would pass in the default modes.
HEADER_MODES = [
    "s3_expires", "gcs_custom_time", "azure_metadata", "filesystem_sidecar",
    *(["azurite_metadata"] if _AZURITE else []),
    *(["fake_gcs_custom_time"] if _FAKE_GCS else []),
]
# The media with a compare-and-write: the filesystem inserts conditionally but
# cannot replace conditionally, so it is left out of these.
VERSIONED_MEDIA = [m for m in MEDIA if m != "filesystem"]
TTL = TtlPolicy(ttl_duration=timedelta(hours=1))

# What an emulator does not do that its service does. Each is checked against
# the request the SDK sends, so the gap is the emulator's, not kvtsjl's; the
# test is expected to fail there, strictly, so a fixed emulator is noticed.
EMULATOR_GAPS = {
    ("fake_gcs", "conditional delete"): (
        "fake-gcs-server ignores ifGenerationMatch on DELETE; the SDK sends it "
        "and GCS honours it"
    ),
}


def expect_emulator_gap(request: pytest.FixtureRequest, medium: str, feature: str) -> None:
    """Expect this test to fail on ``medium`` if its emulator lacks ``feature``."""
    reason = EMULATOR_GAPS.get((medium, feature))
    if reason is not None:
        request.applymarker(pytest.mark.xfail(strict=True, reason=reason))


def _kvset(ttl: TtlPolicy | None) -> Any:
    return KvSet.with_str_keys(
        "media",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        ttl_policy=ttl,
    )


def make_medium(
    name: str,
    request: pytest.FixtureRequest,
    ttl: TtlPolicy | None = None,
    *,
    expiry_gc: ExpiryGc = ExpiryGc.LAZY_DELETE,
) -> Any:
    """The medium ``name``, holding ``str`` values under ``str`` keys."""
    import fakeredis

    from kvtsjl.backends.azure import AzureBlobKvStore, AzureTtlMode
    from kvtsjl.backends.filesystem import FilesystemKvStore, FilesystemTtlMode
    from kvtsjl.backends.gcs import GcsKvStore, GcsTtlMode
    from kvtsjl.backends.redis import RedisKvStore
    from kvtsjl.backends.s3 import S3KvStore, S3TtlMode
    from tests.fake_azure import FakeAzureContainer
    from tests.fake_gcs import FakeGcsBucket

    kvset = _kvset(ttl)
    if name == "memory":
        return MemoryKvStore(kvset, expiry_gc=expiry_gc)
    if name == "sql":
        conn = request.getfixturevalue("sqlite_conn")
        sql_kvset = make_sql_kvset("media", ttl_policy=ttl)
        ensure_sql_table(conn, sql_kvset)
        return SqlDbKvStore(
            sql_kvset, adapter=SqliteSqlDbClientAdapter(conn), expiry_gc=expiry_gc
        )
    if name == "redis":
        return RedisKvStore.flat(kvset, fakeredis.FakeRedis())
    if name == "redis_hash":
        return RedisKvStore.hash_collection(kvset, fakeredis.FakeRedis())
    if name in ("s3", "s3_expires"):
        client = request.getfixturevalue("s3_client")
        bucket = request.getfixturevalue("s3_bucket")
        mode = S3TtlMode.EXPIRES if name == "s3_expires" else S3TtlMode.OBJECT_TIME
        return S3KvStore(
            kvset, client=client, bucket=bucket, key_prefix="app/", ttl_mode=mode,
            expiry_gc=expiry_gc,
        )
    if name in ("gcs", "gcs_custom_time", "fake_gcs", "fake_gcs_custom_time"):
        mode = GcsTtlMode.CUSTOM_TIME if name.endswith("custom_time") else GcsTtlMode.OBJECT_TIME
        bucket = (
            request.getfixturevalue("gcs_emulator_bucket") if name.startswith("fake_") else FakeGcsBucket()
        )
        return GcsKvStore(
            kvset, bucket=bucket, key_prefix="app/", ttl_mode=mode,
            expiry_gc=expiry_gc,
        )
    if name in ("azure", "azure_metadata", "azurite", "azurite_metadata"):
        mode = AzureTtlMode.METADATA if name.endswith("metadata") else AzureTtlMode.OBJECT_TIME
        container = (
            request.getfixturevalue("azurite_container")
            if name.startswith("azurite")
            else FakeAzureContainer()
        )
        return AzureBlobKvStore(
            kvset, container=container, key_prefix="app/", ttl_mode=mode,
            expiry_gc=expiry_gc,
        )
    if name in ("filesystem", "filesystem_sidecar"):
        mode = FilesystemTtlMode.SIDECAR if name == "filesystem_sidecar" else FilesystemTtlMode.MTIME
        return FilesystemKvStore(
            kvset, root=request.getfixturevalue("tmp_path"), ttl_mode=mode, expiry_gc=expiry_gc
        )
    raise AssertionError(name)
