"""P7 ScanSpec / Predicate unit tests."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

from kvtsjl.store.schema.scan import (
    AlwaysTrue,
    And,
    Expired,
    KeyPrefix,
    KeyRange,
    ModifiedBefore,
    Needs,
    ScanRow,
    ScanStat,
    ValueWhere,
    apply_cascade,
    is_gc_predicate,
    partition_by_needs,
    split_key_predicates,
)


def test_split_key_predicates_and() -> None:
    where = KeyPrefix("inv/") & ModifiedBefore(datetime.now(timezone.utc))
    key_w, resid = split_key_predicates(where)
    assert isinstance(key_w, KeyPrefix)
    assert key_w.prefix == "inv/"
    assert isinstance(resid, ModifiedBefore)


def test_split_key_only_and() -> None:
    where = KeyPrefix("a") & KeyPrefix("ab")
    key_w, resid = split_key_predicates(where)
    assert isinstance(key_w, And)
    assert isinstance(resid, AlwaysTrue)


def test_split_value_stays_residual() -> None:
    where = KeyPrefix("x") & ValueWhere(lambda v: v == 1)
    key_w, resid = split_key_predicates(where)
    assert isinstance(key_w, KeyPrefix)
    assert isinstance(resid, ValueWhere)


def test_expired_is_gc_predicate() -> None:
    assert is_gc_predicate(Expired())
    assert is_gc_predicate(Expired() & Expired())
    assert not is_gc_predicate(KeyPrefix("x"))
    assert not is_gc_predicate(Expired() & KeyPrefix("x"))


def test_cascade_stat_before_batch_get() -> None:
    """STAT reject must not call batch_get."""
    store = MagicMock()
    store.batch_get = MagicMock(return_value={})
    t0 = datetime(2020, 1, 1, tzinfo=timezone.utc)
    t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    rows = iter(
        [
            ScanRow("a", stat=ScanStat(modified_at=t1)),
            ScanRow("b", stat=ScanStat(modified_at=t0)),
        ]
    )
    residual = ModifiedBefore(datetime(2022, 1, 1, tzinfo=timezone.utc)) & ValueWhere(
        lambda v: True
    )
    out = list(apply_cascade(rows, residual=residual, store=store))
    assert [r.key for r in out] == []
    # Only "b" survives STAT; one batch_get for that survivor (or empty if flush
    # never ran because we need values — b passes STAT so batch_get once).
    assert store.batch_get.call_count == 1
    assert store.batch_get.call_args[0][0] == ["b"]


def test_partition_by_needs() -> None:
    residual = KeyPrefix("a") & ModifiedBefore(datetime.now(timezone.utc)) & ValueWhere(
        lambda v: True
    )
    key_p, stat_p, value_p = partition_by_needs(residual)
    assert len(key_p) == 1 and key_p[0].needs() == Needs.KEY
    assert len(stat_p) == 1 and stat_p[0].needs() == Needs.STAT
    assert len(value_p) == 1 and value_p[0].needs() == Needs.VALUE


def test_key_range_evaluate() -> None:
    pred = KeyRange(start="b", end="d")
    assert pred.evaluate(ScanRow("b"))
    assert pred.evaluate(ScanRow("c"))
    assert not pred.evaluate(ScanRow("a"))
    assert not pred.evaluate(ScanRow("d"))
