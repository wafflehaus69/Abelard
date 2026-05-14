"""CLI tests for `briefs list|show` + the real `headlines recent`."""

from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

from news_watch_daemon.cli import main
from news_watch_daemon.synthesize.archive import write_brief
from news_watch_daemon.synthesize.brief import (
    Brief,
    Dispatch,
    Event,
    SynthesisMetadata,
    Trigger,
    TriggerWindow,
)


REPO_ROOT = Path(__file__).resolve().parent.parent
SEED_THEME_DIR = REPO_ROOT / "themes"


# ---------- fixtures ----------


def _seed_schema(db_path: Path) -> None:
    """Apply v1+v2 schema directly via init_db."""
    from news_watch_daemon.db import connect, init_db
    conn = connect(db_path)
    try:
        init_db(conn)
    finally:
        conn.close()


@pytest.fixture
def env(monkeypatch, tmp_path):
    db_path = tmp_path / "state.db"
    archive_path = tmp_path / "briefs"
    archive_path.mkdir()
    proposals_path = tmp_path / "proposals"

    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(db_path))
    monkeypatch.setenv("NEWS_WATCH_THEMES_DIR", str(SEED_THEME_DIR))
    monkeypatch.setenv("NEWS_WATCH_BRIEF_ARCHIVE", str(archive_path))
    monkeypatch.setenv("NEWS_WATCH_PROPOSALS_PATH", str(proposals_path))
    monkeypatch.setenv("LOG_LEVEL", "WARNING")

    _seed_schema(db_path)
    yield (db_path, archive_path)


def _read_envelope(capsys) -> dict:
    return json.loads(capsys.readouterr().out)


def _make_brief(
    brief_id: str,
    *,
    generated_at: str,
    themes: list[str],
    events_scores: list[float] | None = None,
    alerted: bool = False,
    suppressed_reason: str | None = None,
) -> Brief:
    events_scores = events_scores or [0.7]
    events = [
        Event(
            event_id=f"evt-{i}",
            headline_summary=f"event {i}",
            themes=themes,
            source_headlines=[],
            materiality_score=s,
            thesis_links=[],
        )
        for i, s in enumerate(events_scores, start=1)
    ]
    return Brief(
        brief_id=brief_id,
        generated_at=generated_at,
        trigger=Trigger(
            type="event", reason="t",
            window=TriggerWindow(since="A", until="B"),
        ),
        themes_covered=themes,
        events=events,
        narrative="n",
        dispatch=Dispatch(
            alerted=alerted,
            suppressed_reason=suppressed_reason,
        ),
        synthesis_metadata=SynthesisMetadata(
            model_used="claude-sonnet-4-6",
            theses_doc_available=False,
        ),
    )


# ---------- briefs list ----------


def test_briefs_list_empty(env, capsys):
    rc = main(["briefs", "list"])
    payload = _read_envelope(capsys)
    assert rc == 0
    assert payload["data"]["count"] == 0
    assert payload["data"]["briefs"] == []


def test_briefs_list_returns_summaries(env, capsys):
    _, archive_path = env
    write_brief(archive_path, _make_brief(
        "nwd-2026-05-13T14-32-08Z-aaaaaaaa",
        generated_at="2026-05-13T14:32:08Z",
        themes=["us_iran_escalation"],
        events_scores=[0.8, 0.6],
    ))
    write_brief(archive_path, _make_brief(
        "nwd-2026-05-13T10-00-00Z-bbbbbbbb",
        generated_at="2026-05-13T10:00:00Z",
        themes=["fed_policy_path"],
        events_scores=[0.55],
    ))
    rc = main(["briefs", "list"])
    payload = _read_envelope(capsys)
    assert rc == 0
    assert payload["data"]["count"] == 2
    # Newest first.
    assert payload["data"]["briefs"][0]["brief_id"] == "nwd-2026-05-13T14-32-08Z-aaaaaaaa"
    # Summary shape: max_materiality_score is the high water mark.
    assert payload["data"]["briefs"][0]["max_materiality_score"] == 0.8
    assert payload["data"]["briefs"][0]["events_count"] == 2
    assert payload["data"]["briefs"][0]["alerted"] is False


def test_briefs_list_limit_respected(env, capsys):
    _, archive_path = env
    for i in range(5):
        write_brief(archive_path, _make_brief(
            f"nwd-2026-05-13T14-{i:02d}-08Z-aaaaaaaa",
            generated_at=f"2026-05-13T14:{i:02d}:08Z",
            themes=["t"],
        ))
    rc = main(["briefs", "list", "--limit", "2"])
    payload = _read_envelope(capsys)
    assert rc == 0
    assert payload["data"]["count"] == 2
    assert payload["data"]["limit"] == 2


def test_briefs_list_theme_filter(env, capsys):
    _, archive_path = env
    write_brief(archive_path, _make_brief(
        "nwd-2026-05-13T14-32-08Z-aaaaaaaa",
        generated_at="2026-05-13T14:32:08Z",
        themes=["us_iran_escalation"],
    ))
    write_brief(archive_path, _make_brief(
        "nwd-2026-05-13T10-00-00Z-bbbbbbbb",
        generated_at="2026-05-13T10:00:00Z",
        themes=["fed_policy_path"],
    ))
    rc = main(["briefs", "list", "--theme", "us_iran_escalation"])
    payload = _read_envelope(capsys)
    assert rc == 0
    assert payload["data"]["count"] == 1
    assert payload["data"]["briefs"][0]["themes_covered"] == ["us_iran_escalation"]
    assert payload["data"]["filter_theme"] == "us_iran_escalation"


def test_briefs_list_corrupt_brief_warns_but_continues(env, capsys):
    _, archive_path = env
    write_brief(archive_path, _make_brief(
        "nwd-2026-05-13T14-32-08Z-aaaaaaaa",
        generated_at="2026-05-13T14:32:08Z",
        themes=["t"],
    ))
    # Plant a corrupt brief in the same partition.
    partition = archive_path / "2026-05"
    (partition / "nwd-2026-05-13T13-32-08Z-bbbbbbbb.json").write_text(
        "{not valid json", encoding="utf-8",
    )
    rc = main(["briefs", "list"])
    payload = _read_envelope(capsys)
    # Status remains ok; one parse_error warning surfaces.
    assert rc == 0
    assert payload["data_completeness"] == "partial"
    assert payload["warnings"]
    assert payload["warnings"][0]["reason"] == "parse_error"
    # Readable brief still surfaces.
    assert payload["data"]["count"] == 1


# ---------- briefs show ----------


def test_briefs_show_returns_full_brief(env, capsys):
    _, archive_path = env
    bid = "nwd-2026-05-13T14-32-08Z-aaaaaaaa"
    write_brief(archive_path, _make_brief(
        bid, generated_at="2026-05-13T14:32:08Z", themes=["t"],
    ))
    rc = main(["briefs", "show", bid])
    payload = _read_envelope(capsys)
    assert rc == 0
    assert payload["data"]["brief"]["brief_id"] == bid
    assert payload["data"]["brief"]["narrative"] == "n"
    assert payload["data"]["brief"]["themes_covered"] == ["t"]


def test_briefs_show_missing_returns_not_found(env, capsys):
    rc = main(["briefs", "show", "nwd-2026-05-13T14-32-08Z-deadbeef"])
    payload = _read_envelope(capsys)
    assert rc == 1
    assert payload["status"] == "not_found"
    assert "not found" in payload["error_detail"]


def test_briefs_show_corrupt_returns_error(env, capsys):
    _, archive_path = env
    partition = archive_path / "2026-05"
    partition.mkdir(parents=True)
    bid = "nwd-2026-05-13T14-32-08Z-cccccccc"
    (partition / f"{bid}.json").write_text("{bad json", encoding="utf-8")
    rc = main(["briefs", "show", bid])
    payload = _read_envelope(capsys)
    assert rc == 1
    assert payload["status"] == "error"
    assert "corrupt" in payload["error_detail"]


# ---------- headlines recent ----------


def _insert_headline(
    db_path: Path,
    *,
    headline_id: str,
    headline: str,
    publisher: str,
    published_at_unix: int,
    tickers: list[str] | None = None,
    theme_tags: list[str] | None = None,
) -> None:
    """Insert a headline directly into the DB and optionally tag it.

    Bypasses scrape orchestration — we're testing the read path, not
    the ingestion path.
    """
    conn = sqlite3.connect(str(db_path))
    try:
        ts = datetime.fromtimestamp(published_at_unix, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        conn.execute(
            "INSERT INTO headlines (headline_id, source, raw_source, headline, url, "
            "published_at_unix, published_at, fetched_at_unix, fetched_at, dedupe_hash, "
            "tickers_json, entities_json) "
            "VALUES (?, 'rss:test', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                headline_id, publisher, headline, "https://x",
                published_at_unix, ts,
                published_at_unix, ts,
                headline_id,
                json.dumps(tickers or []),
                json.dumps({}),
            ),
        )
        # Tags.
        for theme_id in theme_tags or []:
            # First ensure theme exists.
            conn.execute(
                "INSERT OR IGNORE INTO themes "
                "(theme_id, display_name, status, config_hash, loaded_at_unix, loaded_at) "
                "VALUES (?, ?, 'active', 'hash', ?, ?)",
                (theme_id, theme_id, published_at_unix, ts),
            )
            conn.execute(
                "INSERT OR IGNORE INTO headline_theme_tags "
                "(headline_id, theme_id, confidence, tagged_at_unix) "
                "VALUES (?, ?, 'primary', ?)",
                (headline_id, theme_id, published_at_unix),
            )
        conn.commit()
    finally:
        conn.close()


def test_headlines_recent_empty(env, capsys):
    rc = main(["headlines", "recent"])
    payload = _read_envelope(capsys)
    assert rc == 0
    assert payload["data"]["count"] == 0


def test_headlines_recent_returns_within_window(env, capsys):
    db_path, _ = env
    now = int(time.time())
    _insert_headline(
        db_path, headline_id="h-recent", headline="Recent event",
        publisher="Reuters", published_at_unix=now - 600,  # 10 min ago
    )
    _insert_headline(
        db_path, headline_id="h-old", headline="Old event",
        publisher="AP", published_at_unix=now - 200 * 3600,  # 200 hours ago
    )
    rc = main(["headlines", "recent", "--hours", "24"])
    payload = _read_envelope(capsys)
    assert rc == 0
    ids = [h["headline_id"] for h in payload["data"]["headlines"]]
    assert ids == ["h-recent"]  # h-old is outside the 24h window


def test_headlines_recent_theme_filter(env, capsys):
    db_path, _ = env
    now = int(time.time())
    _insert_headline(
        db_path, headline_id="h-tagged", headline="Tagged event",
        publisher="Reuters", published_at_unix=now - 100,
        theme_tags=["us_iran_escalation"],
    )
    _insert_headline(
        db_path, headline_id="h-untagged", headline="Untagged event",
        publisher="AP", published_at_unix=now - 100,
    )
    rc = main(["headlines", "recent", "--theme", "us_iran_escalation"])
    payload = _read_envelope(capsys)
    assert rc == 0
    ids = [h["headline_id"] for h in payload["data"]["headlines"]]
    assert ids == ["h-tagged"]


def test_headlines_recent_ticker_filter(env, capsys):
    db_path, _ = env
    now = int(time.time())
    _insert_headline(
        db_path, headline_id="h-aapl", headline="Apple does a thing",
        publisher="Reuters", published_at_unix=now - 100,
        tickers=["AAPL"],
    )
    _insert_headline(
        db_path, headline_id="h-msft", headline="Microsoft does a thing",
        publisher="AP", published_at_unix=now - 100,
        tickers=["MSFT"],
    )
    rc = main(["headlines", "recent", "--ticker", "AAPL"])
    payload = _read_envelope(capsys)
    assert rc == 0
    ids = [h["headline_id"] for h in payload["data"]["headlines"]]
    assert ids == ["h-aapl"]


def test_headlines_recent_ticker_filter_avoids_substring_match(env, capsys):
    """`AAPL` should NOT match a headline whose tickers_json contains
    `BAAPL` or `AAPL2`. The JSON-quoted match (`"AAPL"`) avoids that."""
    db_path, _ = env
    now = int(time.time())
    _insert_headline(
        db_path, headline_id="h-baapl", headline="BAAPL doing X",
        publisher="Reuters", published_at_unix=now - 100,
        tickers=["BAAPL"],  # tickers_json contains "BAAPL" but not "AAPL"
    )
    _insert_headline(
        db_path, headline_id="h-aapl", headline="AAPL doing X",
        publisher="AP", published_at_unix=now - 100,
        tickers=["AAPL"],
    )
    rc = main(["headlines", "recent", "--ticker", "AAPL"])
    payload = _read_envelope(capsys)
    ids = [h["headline_id"] for h in payload["data"]["headlines"]]
    assert ids == ["h-aapl"]


def test_headlines_recent_returns_tickers_and_themes(env, capsys):
    db_path, _ = env
    now = int(time.time())
    _insert_headline(
        db_path, headline_id="h-full", headline="Full event",
        publisher="Reuters", published_at_unix=now - 100,
        tickers=["LHX", "NTR"], theme_tags=["us_iran_escalation"],
    )
    rc = main(["headlines", "recent"])
    payload = _read_envelope(capsys)
    h = payload["data"]["headlines"][0]
    assert h["tickers"] == ["LHX", "NTR"]
    assert "us_iran_escalation" in h["themes"]
    assert h["publisher"] == "Reuters"


def test_headlines_recent_limit_respected(env, capsys):
    db_path, _ = env
    now = int(time.time())
    for i in range(5):
        _insert_headline(
            db_path, headline_id=f"h-{i}", headline=f"event {i}",
            publisher="X", published_at_unix=now - i,
        )
    rc = main(["headlines", "recent", "--limit", "2"])
    payload = _read_envelope(capsys)
    assert payload["data"]["count"] == 2
    assert payload["data"]["limit"] == 2


def test_headlines_recent_no_db_init_errors(monkeypatch, tmp_path, capsys):
    """With NEWS_WATCH_DB_PATH set but the schema un-applied, return
    an error envelope pointing at `db init`."""
    db_path = tmp_path / "state.db"
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(db_path))
    monkeypatch.setenv("NEWS_WATCH_THEMES_DIR", str(SEED_THEME_DIR))
    monkeypatch.setenv("LOG_LEVEL", "WARNING")
    # Don't init schema.
    db_path.parent.mkdir(parents=True, exist_ok=True)
    sqlite3.connect(str(db_path)).close()  # creates empty DB file

    rc = main(["headlines", "recent"])
    payload = _read_envelope(capsys)
    assert rc == 1
    assert "db init" in payload["error_detail"]


def test_headlines_recent_orders_newest_first(env, capsys):
    db_path, _ = env
    now = int(time.time())
    _insert_headline(db_path, headline_id="older", headline="older",
                     publisher="X", published_at_unix=now - 500)
    _insert_headline(db_path, headline_id="newer", headline="newer",
                     publisher="X", published_at_unix=now - 100)
    rc = main(["headlines", "recent"])
    payload = _read_envelope(capsys)
    ids = [h["headline_id"] for h in payload["data"]["headlines"]]
    assert ids == ["newer", "older"]
