"""Shared fixtures and KvSet helpers for tests."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import timedelta

import boto3
import fakeredis
from moto import mock_aws
import pytest

from kvtsjl import KvSet, SerDe, TtlPolicy
from kvtsjl.backends.azure import AzureBlobKvStore
from kvtsjl.backends.gcs import GcsKvStore
from kvtsjl.backends.redis import RedisKvStore
from kvtsjl.backends.s3 import S3KvStore
from tests.fake_azure import FakeAzureContainer
from tests.fake_gcs import FakeGcsBucket


@pytest.fixture
def str_bytes_kvset() -> KvSet[str, str, str, bytes]:
    return KvSet.with_str_keys(
        "test",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
    )


@pytest.fixture
def str_bytes_kvset_ttl() -> KvSet[str, str, str, bytes]:
    return KvSet.with_str_keys(
        "test-ttl",
        key_serde=SerDe.identity(str),
        value_serde=SerDe.utf8_bytes(),
        ttl_policy=TtlPolicy(ttl_duration=timedelta(seconds=60)),
    )


@pytest.fixture
def fake_redis() -> fakeredis.FakeRedis:
    return fakeredis.FakeRedis(decode_responses=False)


@pytest.fixture
def redis_flat_store(
    str_bytes_kvset: KvSet[str, str, str, bytes],
    fake_redis: fakeredis.FakeRedis,
) -> RedisKvStore[str, str, str, bytes, None]:
    return RedisKvStore.flat(str_bytes_kvset, fake_redis)


@pytest.fixture
def redis_hash_store(
    str_bytes_kvset: KvSet[str, str, str, bytes],
    fake_redis: fakeredis.FakeRedis,
) -> RedisKvStore[str, str, str, bytes, str]:
    return RedisKvStore.hash_collection(str_bytes_kvset, fake_redis)


@pytest.fixture
def s3_bucket() -> str:
    return "kvtsjl-test-bucket"


@pytest.fixture
def s3_client(s3_bucket: str) -> Iterator[object]:
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=s3_bucket)
        yield client


@pytest.fixture
def s3_store(
    str_bytes_kvset: KvSet[str, str, str, bytes],
    s3_client: object,
    s3_bucket: str,
) -> S3KvStore[str, str, str]:
    return S3KvStore(
        str_bytes_kvset,
        client=s3_client,  # type: ignore[arg-type]
        bucket=s3_bucket,
        key_prefix="app/",
    )


@pytest.fixture
def fake_gcs_bucket() -> FakeGcsBucket:
    return FakeGcsBucket()


@pytest.fixture
def gcs_store(
    str_bytes_kvset: KvSet[str, str, str, bytes],
    fake_gcs_bucket: FakeGcsBucket,
) -> GcsKvStore[str, str, str]:
    # Test double; production passes google.cloud.storage.Bucket.
    return GcsKvStore(
        str_bytes_kvset,
        bucket=fake_gcs_bucket,  # type: ignore[arg-type]
        key_prefix="app/",
    )


@pytest.fixture
def fake_azure_container() -> FakeAzureContainer:
    return FakeAzureContainer()


@pytest.fixture
def azure_store(
    str_bytes_kvset: KvSet[str, str, str, bytes],
    fake_azure_container: FakeAzureContainer,
) -> AzureBlobKvStore[str, str, str]:
    # Test double; production passes azure.storage.blob.ContainerClient.
    return AzureBlobKvStore(
        str_bytes_kvset,
        container=fake_azure_container,  # type: ignore[arg-type]
        key_prefix="app/",
    )
