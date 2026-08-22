"""Human-readable ``repr`` helpers for stores and indexes."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from kvtsjl.scope import Scope


def physical_label(schema: Any) -> str:
    """``name@v1`` label for ``KvSet`` / ``IndexSet`` descriptors."""
    return f"{schema.name}@{schema.version_label()}"


def callable_label(fn: Callable[..., Any]) -> str:
    return getattr(fn, "__qualname__", repr(fn))


def compose_repr(class_name: str, **fields: Any) -> str:
    parts: list[str] = []
    for key, value in fields.items():
        if _omit_field(key, value):
            continue
        parts.append(f"{key}={value!r}")
    return f"{class_name}({', '.join(parts)})"


def _omit_field(key: str, value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, Scope) and not value.segments:
        return True
    if key == "promote" and value is True:
        return True
    if key == "sync_on_write" and value is True:
        return True
    return False


def backend_repr(backend: Any) -> str:
    """``repr`` for leaf ``KvBackend`` instances."""
    cls = type(backend).__name__
    fields: dict[str, Any] = {"kvset": physical_label(backend.kvset)}
    if backend.scope.segments:
        fields["scope"] = backend.scope.path_display()
    collection = backend._binding.collection
    if collection is not None and collection != "":
        fields["collection"] = collection
    return compose_repr(cls, **fields)


def index_backend_repr(index: Any, **extra: Any) -> str:
    """``repr`` for leaf ``IndexBackend`` instances."""
    cls = type(index).__name__
    fields: dict[str, Any] = {"index": physical_label(index.index_set)}
    if index.scope.segments:
        fields["scope"] = index.scope.path_display()
    fields.update(extra)
    return compose_repr(cls, **fields)


def stores_repr(parts: Sequence[Any]) -> list[str]:
    return [repr(store) for store in parts]


def stores_mapping_repr(parts: Mapping[str, Any]) -> str:
    return ", ".join(f"{name}={store!r}" for name, store in parts.items())
