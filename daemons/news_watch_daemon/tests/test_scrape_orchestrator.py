"""Orchestrator tests — sources mocked at the SourcePlugin level.

The orchestrator's collaborators:
  - SourcePlugin instances (injected → mocked here as canned FetchResults)
  - sqlite3.Connection (real, against a fresh in-memory or temp DB)
  - ThemeConfig list (real, built inline)

HTTP is not exercised at this layer — http_client and source plugins
own those edge cases. These tests exercise dedup, theme tagging,
source_health bookkeeping, and the result-shape contract.
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Iterable
from unittest.mock import MagicMock

import pytest

from news_watch_daemon.db import connect, init_db
from news_watch_daemon.scrape.dedup import compute_dedupe_hash
from news_watch_daemon.scrape.orchestrator import (
    DEDUP_WINDOW_S,
    DEFAULT_SINCE_LOOKBACK_S,
    PerSourceResult,
    ScrapeResult,
    run_scrape,
)
from news_watch_daemon.sources.base import FetchedItem, FetchResult, SourcePlugin
from news_watch_daemon.theme_config import ThemeConfig


REPO_ROOT = Path(__file__).resolve().parent.parent
SEED_THEME = REPO_ROOT / "themes" / "us_iran_escalation.yaml"
FIXED_NOW = 1_800_000_000  # deterministic timestamp for tests


# ---------- helpers ----------


class _FakeSource(SourcePlugin):
    """Real SourcePlugin subclass; `fetch` is a MagicMock for assertions."""

    def __init__(
        self,
        *,
        name: str,
        items: Iterable[FetchedItem] = (),
        status: str = "ok",
        error_detail: str | None = None,
        cadence_minutes: int | None = None,
    ) -> None:
        self._name = name
        self._cadence_minutes = cadence_minutes
        self._fetch_result = FetchResult(
            source=name,
            fetched_at_unix=FIXED_NOW,
            items=list(items),
            status=status,
            error_detail=error_detail,
        )
        # Instance attribute shadows the class method, giving us
        # MagicMock's assert_called* helpers without losing ABC compliance.
        self.fetch = MagicMock(return_value=self._fetch_result)

    @property
    def name(self) -> str:
        return self._name

    @property
    def cadence_minutes(self) -> int | None:
        return self._cadence_minutes

    def fetch(self, since_unix: int) -> FetchResult:  # noqa: D401, F811 — abstract satisfier
        return self._fetch_result  # pragma: no cover — instance attr shadows

    def rate_limit_budget_remaining(self) -> float:
        return 1.0


def _fake_source(
    name: str,
    items: Iterable[FetchedItem] = (),
    status: str = "ok",
    error_detail: str | None = None,
) -> _FakeSource:
    return _FakeSource(
        name=name, items=items, status=status, error_detail=error_detail,
    )


def _item(headline: str, *, published: int = FIXED_NOW - 3600, source_id: str = "x") -> FetchedItem:
    return FetchedItem(
        source_item_id=source_id,
        headline=headline,
        url=f"https://example.com/{source_id}",
        published_at_unix=published,
        raw_source="TestWire",
        tickers=[],
        raw_body=None,
    )


def _seed_theme() -> ThemeConfig:
    from news_watch_daemon.theme_config import load_theme
    return load_theme(SEED_THEME)


@pytest.fixture
def conn(tmp_path):
    c = connect(tmp_path / "state.db")
    init_db(c)
    # Insert the seed theme into the registry table so FK constraints on
    # headline_theme_tags pass when the orchestrator inserts tag rows.
    theme = _seed_theme()
    c.execute(
        "INSERT INTO themes (theme_id, display_name, status, config_hash, "
        "loaded_at_unix, loaded_at) VALUES (?, ?, ?, ?, ?, ?)",
        (theme.theme_id, theme.display_name, theme.status, theme.config_hash(), 0, "t"),
    )
    yield c
    c.close()


# ---------- single-source happy path ----------


def test_single_source_inserts_headlines(conn):
    theme = _seed_theme()
    src = _fake_source("finnhub:general", items=[
        _item("Iran tests new ballistic missile", source_id="a"),
        _item("Stock market opens flat", source_id="b"),
    ])
    result = run_scrape(conn, [src], [theme], now_unix=FIXED_NOW)
    assert result.sources_attempted == 1
    assert result.sources_succeeded == 1
    assert result.sources_failed == 0
    assert result.headlines_inserted_total == 2
    assert result.per_source[0].name == "finnhub:general"
    assert result.per_source[0].items_inserted == 2


def test_primary_keyword_match_creates_primary_tag(conn):
    theme = _seed_theme()
    src = _fake_source("finnhub:general", items=[
        _item("Iran tests new ballistic missile"),
    ])
    run_scrape(conn, [src], [theme], now_unix=FIXED_NOW)
    rows = conn.execute(
        "SELECT theme_id, confidence FROM headline_theme_tags"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["theme_id"] == "us_iran_escalation"
    assert rows[0]["confidence"] == "primary"


def test_secondary_only_match_creates_secondary_tag(conn):
    theme = _seed_theme()
    # CENTCOM is in secondary; no primary keyword present.
    src = _fake_source("finnhub:general", items=[_item("CENTCOM announces new exercises")])
    run_scrape(conn, [src], [theme], now_unix=FIXED_NOW)
    rows = conn.execute("SELECT confidence FROM headline_theme_tags").fetchall()
    assert len(rows) == 1
    assert rows[0]["confidence"] == "secondary"


def test_exclusion_match_skips_tag(conn):
    theme = _seed_theme()
    # "Iranian cuisine" is in exclusions.
    src = _fake_source("finnhub:general", items=[_item("Iranian cuisine flourishes in New York")])
    result = run_scrape(conn, [src], [theme], now_unix=FIXED_NOW)
    assert result.headlines_inserted_total == 1  # headline is stored
    assert result.theme_tags_inserted_total == 0  # but not tagged to theme
    rows = conn.execute("SELECT COUNT(*) AS n FROM headline_theme_tags").fetchone()
    assert rows["n"] == 0


def test_no_match_skips_tag(conn):
    theme = _seed_theme()
    src = _fake_source("finnhub:general", items=[_item("Stock market opens flat")])
    result = run_scrape(conn, [src], [theme], now_unix=FIXED_NOW)
    assert result.headlines_inserted_total == 1
    assert result.theme_tags_inserted_total == 0


# ---------- dedup ----------


def test_same_headline_in_same_sweep_deduped(conn):
    theme = _seed_theme()
    same = "Iran tests new ballistic missile"
    src = _fake_source("finnhub:general", items=[
        _item(same, source_id="a"),
        _item(same, source_id="b"),  # exact duplicate
    ])
    result = run_scrape(conn, [src], [theme], now_unix=FIXED_NOW)
    assert result.per_source[0].items_fetched == 2
    assert result.per_source[0].items_after_dedup == 1
    assert result.headlines_inserted_total == 1


def test_dedup_across_sources_in_same_sweep(conn):
    """Two sources returning the same headline → one inserted, one deduped."""
    theme = _seed_theme()
    same = "Iran tests new ballistic missile"
    finnhub = _fake_source("finnhub:general", items=[_item(same, source_id="f1")])
    rss = _fake_source("rss:example", items=[_item(same, source_id="r1")])
    result = run_scrape(conn, [finnhub, rss], [theme], now_unix=FIXED_NOW)
    assert result.headlines_inserted_total == 1
    # One source got to insert; the other saw items_after_dedup=0
    inserts = [p.items_inserted for p in result.per_source]
    assert sorted(inserts) == [0, 1]


def test_dedup_against_prior_run_within_window(conn):
    theme = _seed_theme()
    src = _fake_source("finnhub:general", items=[_item("Iran tests new missile")])
    run_scrape(conn, [src], [theme], now_unix=FIXED_NOW)
    # Second sweep, 1 hour later — should dedup against the first.
    result2 = run_scrape(conn, [src], [theme], now_unix=FIXED_NOW + 3600)
    assert result2.headlines_inserted_total == 0
    assert result2.per_source[0].items_after_dedup == 0


def test_dedup_window_is_72_hours_across_sources(conn):
    """A near-duplicate from a DIFFERENT source after 72h re-enters.

    Same source+same headline collides on headline_id (PK) forever and
    is filtered by the dedup_hash check within the window. After the
    window expires, a *different* source emitting the same headline
    passes both checks and inserts cleanly — that's the intent of the
    72h window (cross-source dedup, not eternal blocklist).
    """
    theme = _seed_theme()
    src_a = _fake_source("finnhub:general", items=[_item("Iran tests new missile")])
    run_scrape(conn, [src_a], [theme], now_unix=FIXED_NOW)
    # 73 hours later, a different source emits the same story.
    later = FIXED_NOW + (73 * 3600)
    src_b = _fake_source("rss:other", items=[_item("Iran tests new missile")])
    result2 = run_scrape(conn, [src_b], [theme], now_unix=later)
    assert result2.headlines_inserted_total == 1


def test_same_source_same_headline_after_72h_is_pk_filtered(conn):
    """Same source + same headline is single-occurrence-ever (headline_id PK)."""
    theme = _seed_theme()
    src = _fake_source("finnhub:general", items=[_item("Iran tests new missile")])
    run_scrape(conn, [src], [theme], now_unix=FIXED_NOW)
    later = FIXED_NOW + (73 * 3600)
    src_again = _fake_source("finnhub:general", items=[_item("Iran tests new missile")])
    result2 = run_scrape(conn, [src_again], [theme], now_unix=later)
    # dedup_hash window check passes (>72h old); PK constraint catches it.
    # Net effect: zero inserts, no crash.
    assert result2.headlines_inserted_total == 0


# ---------- source failures ----------


def test_source_failure_does_not_block_other_sources(conn):
    theme = _seed_theme()
    failing = _fake_source("finnhub:general", items=[], status="error", error_detail="http_5xx: 503")
    working = _fake_source("rss:example", items=[_item("Iran tests new missile")])
    result = run_scrape(conn, [failing, working], [theme], now_unix=FIXED_NOW)
    assert result.sources_attempted == 2
    assert result.sources_failed == 1
    assert result.sources_succeeded == 1
    assert result.headlines_inserted_total == 1


def test_all_sources_failing_still_returns_result(conn):
    theme = _seed_theme()
    a = _fake_source("finnhub:general", status="error", error_detail="boom")
    b = _fake_source("rss:x", status="error", error_detail="boom")
    result = run_scrape(conn, [a, b], [theme], now_unix=FIXED_NOW)
    assert result.sources_failed == 2
    assert result.sources_succeeded == 0
    assert result.headlines_inserted_total == 0


def test_partial_status_inserts_what_it_got(conn):
    theme = _seed_theme()
    src = _fake_source(
        "finnhub:general",
        items=[_item("Iran tests new missile")],
        status="partial",
        error_detail="dropped 1 malformed item",
    )
    result = run_scrape(conn, [src], [theme], now_unix=FIXED_NOW)
    assert result.headlines_inserted_total == 1
    assert result.per_source[0].status == "partial"
    assert result.per_source[0].error_detail == "dropped 1 malformed item"


# ---------- source_health bookkeeping ----------


def test_source_health_ok_resets_counter_and_advances_last_success(conn):
    theme = _seed_theme()
    src = _fake_source("finnhub:general", items=[_item("Iran tests new missile")])
    run_scrape(conn, [src], [theme], now_unix=FIXED_NOW)
    row = conn.execute(
        "SELECT * FROM source_health WHERE source = 'finnhub:general'"
    ).fetchone()
    assert row["last_status"] == "ok"
    assert row["last_successful_fetch_unix"] == FIXED_NOW
    assert row["last_attempt_unix"] == FIXED_NOW
    assert row["consecutive_failure_count"] == 0


def test_source_health_error_increments_counter_preserves_prior_success(conn):
    theme = _seed_theme()
    ok_src = _fake_source("finnhub:general", items=[_item("Iran tests new missile")])
    run_scrape(conn, [ok_src], [theme], now_unix=FIXED_NOW)
    err_src = _fake_source("finnhub:general", status="error", error_detail="boom")
    run_scrape(conn, [err_src], [theme], now_unix=FIXED_NOW + 100)
    row = conn.execute(
        "SELECT * FROM source_health WHERE source = 'finnhub:general'"
    ).fetchone()
    assert row["last_status"] == "error"
    assert row["consecutive_failure_count"] == 1
    # Prior success timestamp preserved (advancing only on status=ok).
    assert row["last_successful_fetch_unix"] == FIXED_NOW
    assert row["last_attempt_unix"] == FIXED_NOW + 100


def test_source_health_partial_resets_counter_but_no_last_success(conn):
    theme = _seed_theme()
    err = _fake_source("finnhub:general", status="error", error_detail="boom")
    run_scrape(conn, [err], [theme], now_unix=FIXED_NOW)
    partial = _fake_source(
        "finnhub:general",
        items=[_item("Iran tests new missile")],
        status="partial",
        error_detail="dropped 1",
    )
    run_scrape(conn, [partial], [theme], now_unix=FIXED_NOW + 100)
    row = conn.execute(
        "SELECT * FROM source_health WHERE source = 'finnhub:general'"
    ).fetchone()
    assert row["last_status"] == "partial"
    assert row["consecutive_failure_count"] == 0
    # No prior success → last_successful stays NULL
    assert row["last_successful_fetch_unix"] is None


def test_source_health_rate_limited_increments_counter(conn):
    theme = _seed_theme()
    a = _fake_source("finnhub:general", status="rate_limited", error_detail="retry_after_seconds=30")
    b = _fake_source("finnhub:general", status="rate_limited", error_detail="retry_after_seconds=30")
    run_scrape(conn, [a], [theme], now_unix=FIXED_NOW)
    run_scrape(conn, [b], [theme], now_unix=FIXED_NOW + 100)
    row = conn.execute("SELECT * FROM source_health WHERE source = 'finnhub:general'").fetchone()
    assert row["consecutive_failure_count"] == 2


# ---------- since_unix sourcing ----------


def test_since_unix_defaults_to_7_days_for_new_source(conn):
    theme = _seed_theme()
    src = _fake_source("finnhub:general", items=[])
    run_scrape(conn, [src], [theme], now_unix=FIXED_NOW)
    src.fetch.assert_called_once_with(FIXED_NOW - DEFAULT_SINCE_LOOKBACK_S)


def test_since_unix_uses_last_successful_when_known(conn):
    theme = _seed_theme()
    ok = _fake_source("finnhub:general", items=[_item("Iran tests new missile")])
    run_scrape(conn, [ok], [theme], now_unix=FIXED_NOW)
    again = _fake_source("finnhub:general", items=[])
    run_scrape(conn, [again], [theme], now_unix=FIXED_NOW + 3600)
    # Second call should use FIXED_NOW (the last_successful_fetch_unix from first run)
    again.fetch.assert_called_once_with(FIXED_NOW)


# ---------- result-shape contract ----------


def test_result_includes_themes_active_sorted(conn):
    theme = _seed_theme()
    src = _fake_source("finnhub:general", items=[])
    result = run_scrape(conn, [src], [theme], now_unix=FIXED_NOW)
    assert result.themes_active == ["us_iran_escalation"]


def test_result_started_at_iso_matches_unix(conn):
    theme = _seed_theme()
    src = _fake_source("finnhub:general", items=[])
    result = run_scrape(conn, [src], [theme], now_unix=FIXED_NOW)
    assert result.started_at_unix == FIXED_NOW
    assert result.started_at.endswith("Z")


def test_no_sources_yields_zero_attempts_no_error(conn):
    """Empty source list is unusual but not an error at the orchestrator layer."""
    theme = _seed_theme()
    result = run_scrape(conn, [], [theme], now_unix=FIXED_NOW)
    assert result.sources_attempted == 0
    assert result.sources_succeeded == 0
    assert result.sources_failed == 0
    assert result.sources_skipped == 0
    assert result.headlines_inserted_total == 0


# ---------- per-source cadence (Pass B Artifact 3) --------------------


def _cadenced_source(name: str, cadence_minutes: int | None) -> _FakeSource:
    return _FakeSource(name=name, cadence_minutes=cadence_minutes)


def test_cadence_skips_source_within_window(conn):
    """A source with cadence_minutes=15 whose last attempt was 10 min ago is skipped."""
    theme = _seed_theme()
    # Seed source_health so last_attempt_unix exists.
    conn.execute(
        "INSERT INTO source_health (source, last_attempt_unix, last_attempt, last_status) "
        "VALUES (?, ?, ?, ?)",
        ("telegram:cig", FIXED_NOW - 600, "ten-min-ago", "ok"),
    )
    src = _cadenced_source("telegram:cig", cadence_minutes=15)
    result = run_scrape(conn, [src], [theme], now_unix=FIXED_NOW)
    assert result.sources_skipped == 1
    assert result.sources_attempted == 0
    assert result.per_source[0].status == "skipped"
    assert "cadence_throttled" in result.per_source[0].error_detail
    # next_eligible = last_attempt + cadence*60 = FIXED_NOW - 600 + 900
    assert f"next_eligible_at_unix={FIXED_NOW - 600 + 900}" in result.per_source[0].error_detail
    src.fetch.assert_not_called()


def test_cadence_does_not_skip_after_window(conn):
    """Same cadence, last attempt 16 min ago — fetched."""
    theme = _seed_theme()
    conn.execute(
        "INSERT INTO source_health (source, last_attempt_unix, last_attempt, last_status) "
        "VALUES (?, ?, ?, ?)",
        ("telegram:cig", FIXED_NOW - 960, "sixteen-min-ago", "ok"),
    )
    src = _cadenced_source("telegram:cig", cadence_minutes=15)
    result = run_scrape(conn, [src], [theme], now_unix=FIXED_NOW)
    assert result.sources_skipped == 0
    assert result.sources_attempted == 1
    src.fetch.assert_called_once()


def test_cadence_none_always_runs(conn):
    """cadence_minutes=None means run every cycle regardless of timing."""
    theme = _seed_theme()
    conn.execute(
        "INSERT INTO source_health (source, last_attempt_unix, last_attempt, last_status) "
        "VALUES (?, ?, ?, ?)",
        ("finnhub:general", FIXED_NOW - 60, "one-min-ago", "ok"),
    )
    src = _cadenced_source("finnhub:general", cadence_minutes=None)
    result = run_scrape(conn, [src], [theme], now_unix=FIXED_NOW)
    assert result.sources_attempted == 1
    src.fetch.assert_called_once()


def test_cadence_brand_new_source_runs_even_with_cadence(conn):
    """No source_health row at all → never throttled (first appearance)."""
    theme = _seed_theme()
    src = _cadenced_source("telegram:new", cadence_minutes=60)
    result = run_scrape(conn, [src], [theme], now_unix=FIXED_NOW)
    assert result.sources_skipped == 0
    assert result.sources_attempted == 1
    src.fetch.assert_called_once()


def test_skipped_source_does_not_update_source_health(conn):
    """Skipped sources leave source_health untouched."""
    theme = _seed_theme()
    conn.execute(
        "INSERT INTO source_health (source, last_attempt_unix, last_attempt, last_status, "
        "last_error_detail, consecutive_failure_count) VALUES (?, ?, ?, ?, ?, ?)",
        ("telegram:cig", FIXED_NOW - 600, "ten-min-ago", "ok", None, 0),
    )
    src = _cadenced_source("telegram:cig", cadence_minutes=15)
    run_scrape(conn, [src], [theme], now_unix=FIXED_NOW)
    row = conn.execute(
        "SELECT last_attempt_unix, last_status FROM source_health WHERE source = ?",
        ("telegram:cig",),
    ).fetchone()
    # Unchanged from what we seeded
    assert row["last_attempt_unix"] == FIXED_NOW - 600
    assert row["last_status"] == "ok"


def test_skip_does_not_count_as_failed_or_succeeded(conn):
    """Mixed sweep: 1 ok, 1 skipped — sources_failed=0, sources_succeeded=1."""
    theme = _seed_theme()
    conn.execute(
        "INSERT INTO source_health (source, last_attempt_unix, last_attempt, last_status) "
        "VALUES (?, ?, ?, ?)",
        ("telegram:cig", FIXED_NOW - 60, "recent", "ok"),
    )
    ok_src = _cadenced_source("finnhub:general", cadence_minutes=None)
    ok_src.fetch.return_value = FetchResult(
        source="finnhub:general", fetched_at_unix=FIXED_NOW,
        items=[_item("Iran tests new missile")], status="ok",
    )
    skipped_src = _cadenced_source("telegram:cig", cadence_minutes=15)
    result = run_scrape(conn, [ok_src, skipped_src], [theme], now_unix=FIXED_NOW)
    assert result.sources_succeeded == 1
    assert result.sources_failed == 0
    assert result.sources_skipped == 1
    assert result.sources_attempted == 1
