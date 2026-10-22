"""R13: batch_sync reads the previous records in one call, not one per key.

``meta_of`` needs the previous record for every key it is about to write — to
reuse an unchanged embedding, and to merge metadata. Asking for them one at a
time is a round trip per key, on the path used for bulk passes and rebuilds.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from kvtsjl.backends.index.memory_vector import MemoryVectorIndex
from kvtsjl.index.schema.index_set import IndexSet
from kvtsjl.serde import SerDe


class CountingIndex(MemoryVectorIndex[Any, Any, Any, Any, Any, Any]):
    """Counts reads, separating single gets from batched ones."""

    def __init__(self, *a: Any, **kw: Any) -> None:
        super().__init__(*a, **kw)
        self.gets = 0
        self.batch_gets = 0

    def get(self, key: Any) -> Any:
        self.gets += 1
        return super().get(key)

    def batch_get(self, keys: Sequence[Any]) -> Mapping[Any, Any]:
        self.batch_gets += 1
        return super().batch_get(keys)


def _index() -> CountingIndex:
    return CountingIndex(
        IndexSet.with_str_ids(
            "batch",
            id_serde=SerDe.identity(str),
            meta_serde=SerDe.identity(dict),
            document_of=lambda k, v: v.get("text"),
            embedding_of=lambda k, v: v.get("vec"),
        ),
        lambda k, v, previous: dict(previous or {}) | dict(v),
    )


def test_batch_sync_reads_the_previous_records_once() -> None:
    index = _index()
    items = {f"k{i}": {"text": f"doc {i}", "vec": [float(i)]} for i in range(10)}

    index.batch_sync(items)

    assert index.batch_gets == 1, f"read the batch {index.batch_gets} times"


def test_a_backend_that_batches_is_not_bypassed() -> None:
    """A backend overriding batch_get must see the whole batch, not per-key gets."""
    seen: list[int] = []

    class OneCall(CountingIndex):
        def batch_get(self, keys: Sequence[Any]) -> Mapping[Any, Any]:
            seen.append(len(keys))
            return {}          # a real backend's single round trip

        def get(self, key: Any) -> Any:
            raise AssertionError("batch_sync fell back to per-key reads")

    index = OneCall(
        IndexSet.with_str_ids(
            "batch",
            id_serde=SerDe.identity(str),
            meta_serde=SerDe.identity(dict),
            document_of=lambda k, v: v.get("text"),
            embedding_of=lambda k, v: v.get("vec"),
        ),
        lambda k, v, previous: dict(previous or {}) | dict(v),
    )
    index.batch_sync({f"k{i}": {"text": "d", "vec": [1.0]} for i in range(8)})

    assert seen == [8], f"expected one call for 8 keys, got {seen}"


def test_batch_sync_still_sees_each_previous_record() -> None:
    """Batching must not cost the reuse it exists to serve."""
    index = _index()
    index.batch_sync({"k": {"text": "hello", "vec": [1.0]}})
    first = index.get("k")

    index.batch_sync({"k": {"text": "hello", "extra": "x", "vec": [9.0]}})
    second = index.get("k")

    assert second.embedding == first.embedding, "unchanged document was re-embedded"
    assert second.data["extra"] == "x"
    assert second.data["text"] == "hello", "previous metadata was lost"
