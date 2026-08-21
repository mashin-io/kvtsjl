"""Pydantic SerDe helpers.

Install with::

    pip install 'kvtsjl[pydantic]'

Then::

    from kvtsjl.serde.pydantic import for_pydantic, for_pydantic_bytes
"""

from __future__ import annotations

import json

from pydantic import BaseModel

from kvtsjl.serde import SerDe

__all__ = ["for_pydantic", "for_pydantic_bytes"]


def for_pydantic[M: BaseModel](model: type[M]) -> SerDe[M, str]:
    """Round-trip a Pydantic model as a JSON string blob."""

    def _ser(value: M) -> str:
        return json.dumps(value.model_dump(mode="json"))

    def _de(blob: str) -> M:
        if isinstance(blob, model):
            return blob
        return model.model_validate(json.loads(blob))

    return SerDe(serializer=_ser, deserializer=_de, blob_type=str)


def for_pydantic_bytes[M: BaseModel](model: type[M]) -> SerDe[M, bytes]:
    """Round-trip a Pydantic model as UTF-8 JSON bytes."""

    def _ser(value: M) -> bytes:
        return json.dumps(value.model_dump(mode="json")).encode("utf-8")

    def _de(blob: bytes) -> M:
        if isinstance(blob, model):
            return blob
        return model.model_validate(json.loads(blob.decode("utf-8")))

    return SerDe(serializer=_ser, deserializer=_de, blob_type=bytes)
