"""Shared object-store collection binding helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from kvtsjl.bind import NativeCollectionBinder
from kvtsjl.schema.ref import WireRef


def normalize_key_prefix(prefix: str) -> str:
    prefix = prefix.strip().lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return prefix


def safe_collection_name(name: str) -> str:
    return (name or "_").replace("/", "_").replace("\\", "_").replace("..", "_").replace("\0", "")


def object_collection_binder(
    key_prefix: str = "",
) -> NativeCollectionBinder[Any, str]:
    """Cloud object-store binder: ``{prefix}{safe_name}/v{version}/``."""
    base = normalize_key_prefix(key_prefix)

    def format_collection(ref: WireRef[Any]) -> str:
        return f"{base}{safe_collection_name(ref.name)}/{ref.version_label()}/"

    return NativeCollectionBinder(collection_formatter=format_collection)


def filesystem_collection_binder(
    root: str | Path,
) -> NativeCollectionBinder[Any, str]:
    """Filesystem binder: ``{root}/{safe_name}/{version}/`` (posix, trailing slash)."""
    root_path = Path(root)

    def format_collection(ref: WireRef[Any]) -> str:
        return (
            root_path / safe_collection_name(ref.name) / ref.version_label()
        ).as_posix() + "/"

    return NativeCollectionBinder(collection_formatter=format_collection)
