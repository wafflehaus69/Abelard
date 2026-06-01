"""Pure-callable tests for scrape_cycle + ScrapeCycleResult.

Per Full Brief Stage 2a-i sub-step B (Abelard 2026-05-29). Focused on
shape + discriminator invariants of the pure callable; the CLI envelope
behavior is already covered by existing scrape-side tests.
"""

from __future__ import annotations

import sqlite3
from dataclasses import fields
from unittest.mock import patch

import pytest

from news_watch_daemon.scrape.orchestrator import (
    PerSourceResult,
    ScrapeCycleResult,
    ScrapeResult,
    scrape_cycle,
)


# ---------- ScrapeCycleResult shape pins ----------


def test_scrape_cycle_result_is_frozen_dataclass():
    sr = ScrapeCycleResult(status="ok", started_at_unix=100)
    with pytest.raises(Exception):
        sr.status = "scrape_failed"   # type: ignore[misc]


def test_scrape_cycle_result_required_fields():
    """status + started_at_unix are required."""
    field_names = {f.name for f in fields(ScrapeCycleResult)}
    assert "status" in field_names
    assert "started_at_unix" in field_names


def test_scrape_cycle_result_mirrors_synthesize_result_shape():
    """Mando Stage 2a-i discipline: both *Result types share the same
    structural shape — status discriminator + reason + structured payload —
    so orchestrator composition is uniform across them."""
    field_names = {f.name for f in fields(ScrapeCycleResult)}
    # Required common fields per Stage 2a-i mirror discipline:
    assert "status" in field_names
    assert "reason" in field_names
    # Result-specific payload field:
    assert "scrape_result" in field_names


# ---------- scrape_cycle exception path ----------


def _empty_scrape_result(started: int = 100) -> ScrapeResult:
    return ScrapeResult(
        started_at_unix=started,
        started_at="2026-05-29T14:30:00Z",
        duration_ms=10,
        sources_attempted=0,
        sources_succeeded=0,
        sources_failed=0,
        sources_skipped=0,
        per_source=[],
        headlines_inserted_total=0,
        theme_tags_inserted_total=0,
        themes_active=[],
    )


def test_scrape_cycle_catches_run_scrape_exception_returns_scrape_failed():
    """run_scrape raises -> scrape_cycle captures, writes error heartbeat,
    returns status='scrape_failed' with reason populated. Doesn't propagate."""
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE daemon_heartbeat (component TEXT, status TEXT, "
                 "updated_at_unix INTEGER, duration_ms INTEGER, error_detail TEXT)")
    try:
        with patch(
            "news_watch_daemon.scrape.orchestrator.run_scrape",
            side_effect=RuntimeError("simulated DB unreachable mid-sweep"),
        ):
            result = scrape_cycle(
                conn=conn,
                sources=[],
                themes=[],
            )
        assert result.status == "scrape_failed"
        assert "simulated DB unreachable mid-sweep" in (result.reason or "")
        assert result.scrape_result is None
        assert result.attention_outcome is None
    finally:
        conn.close()


def test_scrape_cycle_returns_ok_on_successful_run_scrape():
    """run_scrape returns ScrapeResult cleanly -> scrape_cycle returns
    status='ok' with the underlying scrape_result populated."""
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE daemon_heartbeat (component TEXT, status TEXT, "
                 "updated_at_unix INTEGER, duration_ms INTEGER, error_detail TEXT)")
    try:
        with patch(
            "news_watch_daemon.scrape.orchestrator.run_scrape",
            return_value=_empty_scrape_result(),
        ):
            result = scrape_cycle(
                conn=conn,
                sources=[],
                themes=[],
            )
        assert result.status == "ok"
        assert result.scrape_result is not None
        assert result.scrape_result.sources_attempted == 0
        assert result.attention_outcome is None   # no callback provided
        assert result.reason is None
    finally:
        conn.close()


def test_scrape_cycle_attention_callback_runs_when_provided():
    """attention_callback gets invoked after successful run_scrape; its
    return value populates result.attention_outcome."""
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE daemon_heartbeat (component TEXT, status TEXT, "
                 "updated_at_unix INTEGER, duration_ms INTEGER, error_detail TEXT)")
    callback_called = []
    def _attn():
        callback_called.append(True)
        return {"status": "ok", "crossings_evaluated": 3}
    try:
        with patch(
            "news_watch_daemon.scrape.orchestrator.run_scrape",
            return_value=_empty_scrape_result(),
        ):
            result = scrape_cycle(
                conn=conn, sources=[], themes=[],
                attention_callback=_attn,
            )
        assert result.status == "ok"
        assert callback_called == [True]
        assert result.attention_outcome == {"status": "ok", "crossings_evaluated": 3}
    finally:
        conn.close()


def test_scrape_cycle_attention_exception_captured_not_propagated():
    """attention_callback raising -> captured into attention_outcome dict
    with status='error'. Per existing _handle_scrape attention-never-kills-
    scrape discipline; scrape_cycle preserves that semantic."""
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE daemon_heartbeat (component TEXT, status TEXT, "
                 "updated_at_unix INTEGER, duration_ms INTEGER, error_detail TEXT)")
    def _attn():
        raise ValueError("attention exploded")
    try:
        with patch(
            "news_watch_daemon.scrape.orchestrator.run_scrape",
            return_value=_empty_scrape_result(),
        ):
            result = scrape_cycle(
                conn=conn, sources=[], themes=[],
                attention_callback=_attn,
            )
        # scrape_cycle still returns ok — attention failure doesn't propagate
        assert result.status == "ok"
        assert result.scrape_result is not None
        assert result.attention_outcome is not None
        assert result.attention_outcome["status"] == "error"
        assert "ValueError" in result.attention_outcome["reason"]
        assert "attention exploded" in result.attention_outcome["reason"]
    finally:
        conn.close()
