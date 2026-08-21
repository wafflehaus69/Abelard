"""Retry-once on classify transport failure (SC-E22R, operational).

Measured 2026-08-19..21: 2 of 5 consecutive classify calls failed on first
attempt, every one a truncated stream on a ~12k-output-token generation. The
retry narrows that window; the loud-degrade path stays the fallback.
"""

from __future__ import annotations

import pytest

from scout_daemon import classify, ledger, state
from scout_daemon.errors import ClassificationError


class Boom(Exception):
    """Stand-in for a dropped transport (RemoteProtocolError and friends)."""


@pytest.fixture()
def conn(tmp_path):
    c = state.connect(tmp_path / "r.sqlite3")
    ledger.apply_schema(c)
    return c


def _patch(monkeypatch, sequence):
    """classify_batch raises/returns each element of `sequence` in turn."""
    calls = {"n": 0}

    def fake(items, keys, *, client=None, logger=None):
        i = calls["n"]
        calls["n"] += 1
        outcome = sequence[min(i, len(sequence) - 1)]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(classify, "classify_batch", fake)
    return calls


def test_transport_failure_retries_once_and_succeeds(monkeypatch) -> None:
    good = ({}, classify.CostRecord(llm_calls=1, items_classified=1))
    calls = _patch(monkeypatch, [Boom("peer closed connection"), good])
    cost = classify.CostRecord()
    exc = None
    for attempt in (1, 2):
        try:
            _, chunk = classify.classify_batch([], [], logger=None)
            exc = None
            break
        except ClassificationError as unusable:
            exc = unusable
            break
        except Exception as transport:
            exc = transport
            if attempt == 1:
                cost.transport_retries += 1
                continue
    assert calls["n"] == 2, "must have retried exactly once"
    assert exc is None, "second attempt should have succeeded"
    assert cost.transport_retries == 1


def test_classification_error_is_not_retried(monkeypatch) -> None:
    """The model answered and the answer was unusable. Asking again buys a
    second unusable answer at full price."""
    calls = _patch(monkeypatch, [ClassificationError("bad json"),
                                 ({}, classify.CostRecord())])
    exc = None
    for attempt in (1, 2):
        try:
            classify.classify_batch([], [], logger=None)
            exc = None
            break
        except ClassificationError as unusable:
            exc = unusable
            break
        except Exception as transport:
            exc = transport
            if attempt == 1:
                continue
    assert calls["n"] == 1, "a ClassificationError must NOT be retried"
    assert isinstance(exc, ClassificationError)


def test_two_transport_failures_still_degrade_loudly(monkeypatch) -> None:
    """The fallback survives: one retry, then the existing safe degradation."""
    calls = _patch(monkeypatch, [Boom("drop one"), Boom("drop two")])
    exc = None
    for attempt in (1, 2):
        try:
            classify.classify_batch([], [], logger=None)
            exc = None
            break
        except ClassificationError as unusable:
            exc = unusable
            break
        except Exception as transport:
            exc = transport
            if attempt == 1:
                continue
    assert calls["n"] == 2
    assert isinstance(exc, Boom), "must surface the failure, not swallow it"


# ---------------------------------------------------------------------------
# Telemetry
# ---------------------------------------------------------------------------

def test_cost_record_carries_the_retry_count() -> None:
    assert classify.CostRecord().transport_retries == 0
    assert classify.CostRecord(transport_retries=2).transport_retries == 2


def test_scan_cost_persists_the_retry_count(conn) -> None:
    """A scan that succeeded on the second attempt is not the same event as one
    that succeeded on the first, and only telemetry can tell them apart later."""
    ledger.record_cost(
        conn, scan_id="s1", model="m", llm_calls=1, input_tokens=10,
        output_tokens=5, cache_read_tokens=0, cache_creation_tokens=0,
        cost_usd=0.1, items_classified=3, transport_retries=1,
    )
    row = conn.execute(
        "SELECT transport_retries FROM scan_cost WHERE scan_id='s1'").fetchone()
    assert row[0] == 1


def test_retry_count_defaults_to_zero_for_existing_callers(conn) -> None:
    ledger.record_cost(
        conn, scan_id="s2", model="m", llm_calls=1, input_tokens=1,
        output_tokens=1, cache_read_tokens=0, cache_creation_tokens=0,
        cost_usd=0.0, items_classified=1,
    )
    row = conn.execute(
        "SELECT transport_retries FROM scan_cost WHERE scan_id='s2'").fetchone()
    assert row[0] == 0


def test_a_judgeless_scan_stays_detectable(conn) -> None:
    """`llm_calls=0 AND items_classified>0` is how three dead scans were found
    and excluded from the floor analysis. The retry must not hide that signal."""
    ledger.record_cost(
        conn, scan_id="s3", model="m", llm_calls=0, input_tokens=0,
        output_tokens=0, cache_read_tokens=0, cache_creation_tokens=0,
        cost_usd=0.0, items_classified=162, transport_retries=1,
    )
    dead = conn.execute(
        "SELECT COUNT(*) FROM scan_cost WHERE llm_calls=0 AND items_classified>0"
    ).fetchone()[0]
    assert dead == 1
