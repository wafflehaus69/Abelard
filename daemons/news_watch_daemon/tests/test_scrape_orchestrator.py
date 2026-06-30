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


# ---------- word-boundary regex (Pass C Step 1, MiCA fix) ----------


def test_word_boundary_substring_does_not_falsely_tag(conn):
    """The MiCA-in-chemical class of false positive must not occur.

    Loads the real tokenized_finance_infrastructure theme (which has
    `MiCA` as a primary keyword) and runs against the exact headline
    that triggered the false positive in Pass B smoke. With word
    boundaries enabled, `chemical` must not match `MiCA`.
    """
    from pathlib import Path
    from news_watch_daemon.theme_config import load_theme
    repo = Path(__file__).resolve().parent.parent
    tokfi = load_theme(repo / "themes" / "tokenized_finance_infrastructure.yaml")
    src = _fake_source(
        "finnhub:general",
        items=[_item(
            "Europe's chemical makers catch a break as Iran war hits Asian rivals",
        )],
    )
    # Insert the tokfi theme into the registry so FK constraints pass
    conn.execute(
        "INSERT INTO themes (theme_id, display_name, status, config_hash, "
        "loaded_at_unix, loaded_at) VALUES (?, ?, ?, ?, ?, ?)",
        (tokfi.theme_id, tokfi.display_name, tokfi.status, tokfi.config_hash(), 0, "t"),
    )
    result = run_scrape(conn, [src], [tokfi], now_unix=FIXED_NOW)
    # The headline must NOT be tagged to tokenized_finance_infrastructure
    rows = conn.execute(
        "SELECT 1 FROM headline_theme_tags WHERE theme_id = ?",
        (tokfi.theme_id,),
    ).fetchall()
    assert rows == [], (
        "MiCA matched substring 'mica' inside 'chemical' — word-boundary fix failed"
    )
    assert result.theme_tags_inserted_total == 0


def test_word_boundary_standalone_keyword_still_matches(conn):
    """`MiCA regulation` must still match the MiCA primary keyword.

    Word boundaries don't break the legitimate case; they only block
    substring collisions inside unrelated words.
    """
    from pathlib import Path
    from news_watch_daemon.theme_config import load_theme
    repo = Path(__file__).resolve().parent.parent
    tokfi = load_theme(repo / "themes" / "tokenized_finance_infrastructure.yaml")
    src = _fake_source(
        "finnhub:general",
        items=[_item("EU MiCA regulation phases in next quarter, issuers prepare")],
    )
    conn.execute(
        "INSERT INTO themes (theme_id, display_name, status, config_hash, "
        "loaded_at_unix, loaded_at) VALUES (?, ?, ?, ?, ?, ?)",
        (tokfi.theme_id, tokfi.display_name, tokfi.status, tokfi.config_hash(), 0, "t"),
    )
    result = run_scrape(conn, [src], [tokfi], now_unix=FIXED_NOW)
    rows = conn.execute(
        "SELECT confidence FROM headline_theme_tags WHERE theme_id = ?",
        (tokfi.theme_id,),
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["confidence"] == "primary"


def test_word_boundary_apostrophe_s_edge_case(conn):
    """`\\bIran\\b` must still match `Iran` in `Iran's nuclear program`.

    The apostrophe is a non-word character, so it provides the right
    word boundary. This is the most likely surprise in the fix; verify
    explicitly. Uses the real us_iran_escalation seed theme.
    """
    theme = _seed_theme()
    src = _fake_source(
        "finnhub:general",
        items=[_item("Iran's nuclear program restart raises stakes")],
    )
    result = run_scrape(conn, [src], [theme], now_unix=FIXED_NOW)
    rows = conn.execute(
        "SELECT confidence FROM headline_theme_tags WHERE theme_id = ?",
        (theme.theme_id,),
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["confidence"] == "primary"
    assert result.theme_tags_inserted_total == 1


def test_pluralization_sweep_rate_cuts_matches_fed_policy_path(conn):
    """Post-pluralization-sweep: `rate cuts` (plural) is a separate
    keyword from `rate cut` (singular) in fed_policy_path's primary list.

    With word boundaries, `\\brate cut\\b` does NOT match "rate cuts"
    (the trailing `s` is a word char), so both forms must be in the
    keyword list for both forms to tag. Audit-trail test for the
    "pluralization sweep post-MiCA-fix" commit.
    """
    from pathlib import Path
    from news_watch_daemon.theme_config import load_theme
    repo = Path(__file__).resolve().parent.parent
    fed = load_theme(repo / "themes" / "fed_policy_path.yaml")
    src = _fake_source(
        "finnhub:general",
        items=[_item("Fed signals two more rate cuts this year")],
    )
    conn.execute(
        "INSERT INTO themes (theme_id, display_name, status, config_hash, "
        "loaded_at_unix, loaded_at) VALUES (?, ?, ?, ?, ?, ?)",
        (fed.theme_id, fed.display_name, fed.status, fed.config_hash(), 0, "t"),
    )
    result = run_scrape(conn, [src], [fed], now_unix=FIXED_NOW)
    rows = conn.execute(
        "SELECT confidence FROM headline_theme_tags WHERE theme_id = ?",
        (fed.theme_id,),
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["confidence"] == "primary"
    assert result.theme_tags_inserted_total == 1


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


# ---------- cross-source observation log (Pass D foundation 2026-05-17) ----


def test_cross_source_log_within_sweep_writes_observation(conn, tmp_path):
    """Two sources sending the same headline in one sweep → second
    observation lands in cross_source_log; first inserts as headline."""
    from news_watch_daemon.scrape.cross_source_log import read_observations

    theme = _seed_theme()
    log_path = tmp_path / "cross.jsonl"
    same = "Iran tests new ballistic missile"
    finnhub = _fake_source("finnhub:general", items=[_item(same, source_id="f1")])
    rss = _fake_source("rss:example", items=[_item(same, source_id="r1")])
    run_scrape(
        conn, [finnhub, rss], [theme],
        now_unix=FIXED_NOW, cross_source_log_path=log_path,
    )
    records = read_observations(log_path)
    assert len(records) == 1
    rec = records[0]
    assert rec["first_source"] == "finnhub:general"
    assert rec["second_source"] == "rss:example"
    assert rec["dedupe_hash"] == compute_dedupe_hash(same)
    assert rec["latency_seconds"] == 0   # same-sweep, same fetched_at


def test_cross_source_log_window_dup_writes_observation(conn, tmp_path):
    """Same headline from different source on a LATER sweep within the
    dedup window → cross_source_log records both observations with
    correct latency."""
    from news_watch_daemon.scrape.cross_source_log import read_observations

    theme = _seed_theme()
    log_path = tmp_path / "cross.jsonl"
    same = "Iran tests new ballistic missile"

    finnhub = _fake_source("finnhub:general", items=[_item(same, source_id="f1")])
    run_scrape(
        conn, [finnhub], [theme], now_unix=FIXED_NOW,
        cross_source_log_path=log_path,
    )
    later = FIXED_NOW + 1800  # 30 minutes later
    rss = _fake_source("rss:example", items=[_item(same, source_id="r1")])
    run_scrape(
        conn, [rss], [theme], now_unix=later,
        cross_source_log_path=log_path,
    )
    records = read_observations(log_path)
    assert len(records) == 1
    rec = records[0]
    assert rec["first_source"] == "finnhub:general"
    assert rec["second_source"] == "rss:example"
    assert rec["latency_seconds"] == 1800


def test_cross_source_log_same_source_dup_NOT_logged(conn, tmp_path):
    """Same headline from the SAME source twice (within-sweep or
    re-fetch) is dedup-skipped but NOT logged — that's noise for the
    cross-source question."""
    from news_watch_daemon.scrape.cross_source_log import read_observations

    theme = _seed_theme()
    log_path = tmp_path / "cross.jsonl"
    same = "Iran tests new ballistic missile"
    src = _fake_source("finnhub:general", items=[
        _item(same, source_id="a"),
        _item(same, source_id="b"),  # within-sweep same-source dup
    ])
    run_scrape(
        conn, [src], [theme], now_unix=FIXED_NOW,
        cross_source_log_path=log_path,
    )
    # File may not even exist if no cross-source dup ever fired.
    records = read_observations(log_path)
    assert records == []


def test_cross_source_log_disabled_when_path_is_none(conn, tmp_path):
    """No log_path → no log file → no behavior change vs Pass A/B.
    The headline still gets dedup-skipped; the cross-source observation
    is silently lost (current behavior, preserved for backward compat)."""
    from news_watch_daemon.scrape.cross_source_log import read_observations

    theme = _seed_theme()
    log_path = tmp_path / "cross.jsonl"  # will NOT be written
    same = "Iran tests new ballistic missile"
    finnhub = _fake_source("finnhub:general", items=[_item(same, source_id="f1")])
    rss = _fake_source("rss:example", items=[_item(same, source_id="r1")])
    # No cross_source_log_path arg.
    run_scrape(conn, [finnhub, rss], [theme], now_unix=FIXED_NOW)
    assert not log_path.exists()
    assert read_observations(log_path) == []


def test_cross_source_log_three_sources_chain(conn, tmp_path):
    """A, B, C all observe the same headline. The chain (A->B, A->C)
    should yield two log entries — second and third observations both
    paired with the FIRST observer."""
    from news_watch_daemon.scrape.cross_source_log import read_observations

    theme = _seed_theme()
    log_path = tmp_path / "cross.jsonl"
    same = "Iran tests new ballistic missile"
    a = _fake_source("source:a", items=[_item(same, source_id="1")])
    b = _fake_source("source:b", items=[_item(same, source_id="2")])
    c = _fake_source("source:c", items=[_item(same, source_id="3")])
    run_scrape(
        conn, [a, b, c], [theme], now_unix=FIXED_NOW,
        cross_source_log_path=log_path,
    )
    records = read_observations(log_path)
    assert len(records) == 2
    # Both records pair against source:a as first observer.
    assert all(r["first_source"] == "source:a" for r in records)
    second_sources = sorted(r["second_source"] for r in records)
    assert second_sources == ["source:b", "source:c"]


def test_cross_source_log_io_failure_does_not_abort_scrape(conn, tmp_path, caplog):
    """If the cross_source_log write raises OSError (disk full,
    permission denied), the scrape continues. The observation is
    lost but synthesis/alerting pipeline integrity is preserved."""
    import logging
    from unittest.mock import patch

    theme = _seed_theme()
    log_path = tmp_path / "cross.jsonl"
    same = "Iran tests new ballistic missile"
    finnhub = _fake_source("finnhub:general", items=[_item(same, source_id="f1")])
    rss = _fake_source("rss:example", items=[_item(same, source_id="r1")])

    with patch(
        "news_watch_daemon.scrape.orchestrator.write_cross_source_observation",
        side_effect=OSError("simulated disk full"),
    ):
        with caplog.at_level(logging.WARNING, logger="news_watch_daemon.scrape"):
            result = run_scrape(
                conn, [finnhub, rss], [theme],
                now_unix=FIXED_NOW, cross_source_log_path=log_path,
            )
    # Scrape completed.
    assert result.headlines_inserted_total == 1
    # And surfaced the I/O failure as a WARN.
    assert any(
        "cross_source_log append failed" in record.message
        for record in caplog.records
    )


# ---------- Pass D follow-on: per-keyword case-sensitivity for acronym keywords ----------
#
# Rule: any keyword containing 2+ consecutive uppercase letters compiles
# case-sensitively via the (?-i:...) inline modifier. Common-word keywords
# (no 2-cap run) stay case-insensitive. Solves SWIFT-vs-swift, MiCA-vs-mica,
# etc. without a YAML schema change. Audit (2026-05-24) verified no
# load-bearing IGNORECASE keyword has 2+ caps.


def test_has_consecutive_uppercase():
    """Pure function: returns True iff keyword contains 2+ consecutive uppercase."""
    from news_watch_daemon.scrape.orchestrator import _has_consecutive_uppercase
    # Case-sensitive class (acronyms / acronym-containing phrases)
    assert _has_consecutive_uppercase("SWIFT") is True
    assert _has_consecutive_uppercase("USDC") is True
    assert _has_consecutive_uppercase("MiCA") is True       # "CA" is 2 consecutive
    assert _has_consecutive_uppercase("GENIUS Act") is True
    assert _has_consecutive_uppercase("FIT21") is True
    assert _has_consecutive_uppercase("BTC price") is True
    # Case-insensitive class (no 2-cap run anywhere)
    assert _has_consecutive_uppercase("stablecoin") is False
    assert _has_consecutive_uppercase("tokenized") is False
    assert _has_consecutive_uppercase("Circle Internet") is False
    assert _has_consecutive_uppercase("Federal Reserve") is False
    assert _has_consecutive_uppercase("rate cut") is False
    assert _has_consecutive_uppercase("Iran") is False     # single cap


def _compile_keywords(keywords: list[str]):
    """Test helper: compile a keyword list via the production code paths."""
    import re
    from news_watch_daemon.scrape.orchestrator import _join_keywords_wb
    return re.compile(_join_keywords_wb(keywords), re.IGNORECASE)


def test_swift_case_sensitive_matches_swift_not_swift_lowercase():
    """SWIFT keyword matches the all-caps financial-system acronym but not
    the English adverb 'swift' under any case variant."""
    pat = _compile_keywords(["SWIFT"])
    assert pat.search("SWIFT messaging system disruption") is not None
    assert pat.search("the swift and professional action") is None
    assert pat.search("Swift on-chain settlement pilot") is None  # capitalized differs


def test_stablecoin_case_insensitive_preserved():
    """Common-word keywords keep IGNORECASE behavior (no 2-cap run → no opt-out)."""
    pat = _compile_keywords(["stablecoin"])
    assert pat.search("stablecoin float hit a new high") is not None
    assert pat.search("Stablecoin issuer earnings call") is not None
    assert pat.search("STABLECOIN ADOPTION GROWS") is not None


def test_mica_case_sensitive():
    """MiCA matches its exact mixed-case form; other capitalizations don't."""
    pat = _compile_keywords(["MiCA"])
    assert pat.search("MiCA enforcement action in Europe") is not None
    assert pat.search("the mica industry uses pegmatite ore") is None
    assert pat.search("Mica regulation in chemistry contexts") is None


def test_swift_real_positive_still_works():
    """Tokenized-finance theme's secondary keyword set still tags genuine
    SWIFT mentions after the case-sensitivity rule applies."""
    from news_watch_daemon.theme_config import load_theme
    from news_watch_daemon.scrape.orchestrator import _compile_theme_regexes, _tag_for_theme
    theme = load_theme(REPO_ROOT / "themes" / "tokenized_finance_infrastructure.yaml")
    [regs] = _compile_theme_regexes([theme])
    text = "DTCC and SWIFT pilot tokenized treasury settlement on Chainlink CCIP"
    # Multiple primary + secondary keywords match — assert tagged at all.
    assert _tag_for_theme(text, regs) is not None


def test_gunman_post_no_tokenized_finance_false_fire():
    """Integration regression: the @real_DonaldJTrump /19285 gunman post must NOT
    tag tokenized_finance_infrastructure. Pre-fix, 'swift and professional action'
    fired the SWIFT secondary keyword via IGNORECASE matching."""
    from news_watch_daemon.theme_config import load_theme
    from news_watch_daemon.scrape.orchestrator import _compile_theme_regexes, _tag_for_theme
    theme = load_theme(REPO_ROOT / "themes" / "tokenized_finance_infrastructure.yaml")
    [regs] = _compile_theme_regexes([theme])
    gunman_post = (
        "Thank you to our great Secret Service and Law Enforcement for the swift "
        "and professional action taken this evening against a gunman near the White "
        "House, who had a violent history and possible obsession with our Country's "
        "most cherished structure. The gunman is dead after an exch"
    )
    assert _tag_for_theme(gunman_post, regs) is None


def test_ai_exclusion_cluster_self_consistency():
    """Mixed-convention regression-guard for the audit observation.

    The ai_capex_cycle theme has 'DRAM' (secondary) AND 'AI hype'
    (exclusion) — both containing 2+ CONSECUTIVE caps, both case-sensitive
    under the acronym rule. On a synthetic post with 'DRAM' in its canonical
    case + 'ai hype' in lowercase, and no primary keywords:

      - 'DRAM' matches case-sensitively → secondary tag fires
      - 'AI hype' does NOT match 'ai hype' (lowercase) → exclusion misses
      - Result: secondary tag wins

    Self-consistency check: same text all-lowercase fires neither secondary
    nor exclusion (both case-sensitive). The theme correctly stays silent on
    casual-source 'ai hype' content rather than half-tagging.

    (Note: 'AI bubble' used to be the exclusion example here, but the
    2026-06-30 recall widen moved it to a lowercase inclusion as an AI-trade
    signal, so this guard now uses 'AI hype', which remains a case-sensitive
    exclusion. The acronym rule keys on 2+ CONSECUTIVE caps, so 'DRAM' is
    case-sensitive but a single-internal-cap name like 'CoreWeave' is not —
    'DRAM' is used precisely because it is. The lowercase test text
    deliberately contains no admitted capital-cycle phrase, so the
    all-lowercase case still fires nothing.)

    If someone later modifies the rule and breaks this self-consistency
    (e.g. exclusion goes case-insensitive while secondary stays case-sensitive),
    primary content from professional sources would silently get excluded by
    matching lowercase noise — this test catches that regression.
    """
    from news_watch_daemon.theme_config import load_theme
    from news_watch_daemon.scrape.orchestrator import _compile_theme_regexes, _tag_for_theme
    theme = load_theme(REPO_ROOT / "themes" / "ai_capex_cycle.yaml")
    [regs] = _compile_theme_regexes([theme])

    # Mixed-case post, no primary keywords present (no hyperscaler/capex/datacenter/etc.):
    mixed_case = (
        "DRAM pricing firms across many regions; "
        "ai hype commentary dominates retail blogs"
    )
    assert _tag_for_theme(mixed_case, regs) == "secondary"

    # Same text all-lowercase: 'DRAM' case-sensitive secondary doesn't fire,
    # 'AI hype' case-sensitive exclusion doesn't fire either — internally consistent.
    assert _tag_for_theme(mixed_case.lower(), regs) is None


# ---------- Task 2: language column populated at ingest ----------


def test_orchestrator_populates_language_column_at_insertion(conn):
    """End-to-end: a fetch carrying multilingual items results in headlines
    rows with non-null `language` populated by the orchestrator-level
    classifier inside _insert_headline_and_tags.

    Locks the single-call-site invariant: the orchestrator is the ONE
    place that classifies (not the source plugins). Adding sources in
    the future automatically inherits classification.

    Note: this test uses three items with distinct dedupe-hash
    normalizations. Edge cases like emoji-only headlines are covered
    by the classifier unit tests in test_lang_classifier.py — those
    inputs normalize to empty under scrape.dedup._DROP_CHARS_RE (ASCII-
    only) and would collide on dedupe_hash with the Cyrillic-only item
    here, masking the language assertion. The dedup behavior for non-
    Latin scripts is its own concern (Pass F follow-up).
    """
    theme = _seed_theme()
    src = _fake_source("telegram:Ateobreaking", items=[
        _item("Российские военные провели учения", source_id="ru1"),
        _item("Iran tests new ballistic missile", source_id="en1"),
        _item(("Р" * 30) + ("a" * 70), source_id="mx1"),  # 0.30 cyr → mixed
    ])
    run_scrape(conn, [src], [theme], now_unix=FIXED_NOW)

    # The fixture URL is `https://example.com/<source_id>`; pull source_id
    # back out as the row key.
    by_source_id = {
        r["url"].rsplit("/", 1)[-1]: r["language"]
        for r in conn.execute("SELECT url, language FROM headlines").fetchall()
    }
    assert by_source_id == {
        "ru1": "ru",
        "en1": "en",
        "mx1": "mixed",
    }
    # Belt-and-suspenders: no NULL language rows at all post-orchestrator.
    null_count = conn.execute(
        "SELECT COUNT(*) FROM headlines WHERE language IS NULL"
    ).fetchone()[0]
    assert null_count == 0
