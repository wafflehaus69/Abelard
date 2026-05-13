"""Envelope shape tests — the schema is the contract."""

from __future__ import annotations

import pytest

from research_daemon.envelope import build_error, build_ok, make_warning


ENVELOPE_KEYS = {
    "status",
    "data_completeness",
    "data",
    "source",
    "timestamp",
    "error_detail",
    "warnings",
}


def test_build_ok_shape():
    env = build_ok({"x": 1}, source="finnhub")
    assert set(env.keys()) == ENVELOPE_KEYS
    assert env["status"] == "ok"
    assert env["data_completeness"] == "complete"
    assert env["data"] == {"x": 1}
    assert env["source"] == "finnhub"
    assert env["error_detail"] is None
    assert env["warnings"] == []
    assert env["timestamp"].endswith("Z")


def test_build_ok_accepts_partial_completeness():
    env = build_ok({"x": 1}, source="finnhub", data_completeness="partial")
    assert env["data_completeness"] == "partial"


def test_build_ok_accepts_metadata_only():
    env = build_ok({"x": 1}, source="edgar", data_completeness="metadata_only")
    assert env["data_completeness"] == "metadata_only"


def test_build_ok_rejects_none_completeness():
    with pytest.raises(ValueError):
        build_ok({"x": 1}, source="finnhub", data_completeness="none")


def test_build_ok_warnings_attached():
    w = make_warning(field="volume", reason="not_available_on_free_tier", source="finnhub")
    env = build_ok({"x": 1}, source="finnhub", warnings=[w])
    assert env["warnings"] == [w]


def test_build_error_shape():
    env = build_error(status="error", source="edgar", detail="boom")
    assert set(env.keys()) == ENVELOPE_KEYS
    assert env["status"] == "error"
    assert env["data_completeness"] == "none"
    assert env["data"] is None
    assert env["source"] == "edgar"
    assert env["error_detail"] == "boom"
    assert env["warnings"] == []


def test_build_error_rejects_ok_status():
    with pytest.raises(ValueError):
        build_error(status="ok", source="finnhub", detail="x")  # type: ignore[arg-type]


def test_build_error_carries_optional_data():
    env = build_error(
        status="rate_limited", source="finnhub", detail="hit", data={"retry_after": 5}
    )
    assert env["data"] == {"retry_after": 5}


def test_make_warning_required_fields():
    w = make_warning(field="volume", reason="rate_limited", source="finnhub")
    assert w == {"field": "volume", "reason": "rate_limited", "source": "finnhub"}
    assert "suggestion" not in w


def test_make_warning_includes_suggestion():
    w = make_warning(
        field="volume",
        reason="not_available_on_free_tier",
        source="finnhub",
        suggestion="upgrade Finnhub",
    )
    assert w["suggestion"] == "upgrade Finnhub"
