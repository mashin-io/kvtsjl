"""Scope unit tests."""

from __future__ import annotations

from kvtsjl import KeyPrefix, Scope, ScopeSegment


def test_of_preserves_order() -> None:
    scope = Scope.of(org="acme", env="prod")
    assert [(s.kind, s.id) for s in scope.segments] == [
        ("org", "acme"),
        ("env", "prod"),
    ]


def test_truediv_append() -> None:
    scope = Scope.empty() / ("org", "acme") / ScopeSegment("env", "prod")
    assert scope.path_display() == "org/acme/env/prod"
    nested = Scope.of(a="1") / Scope.of(b="2")
    assert [s.kind for s in nested.segments] == ["a", "b"]


def test_key_prefix_alias() -> None:
    assert KeyPrefix is Scope


def test_no_app_specific_factories() -> None:
    assert not hasattr(Scope, "tenant")
    assert not hasattr(Scope, "user")
    assert not hasattr(Scope, "session")
    assert not hasattr(Scope, "turn")
