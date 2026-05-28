"""CLI subcommand tests for Pass F:
  - `translate` (manual one-shot with --source / --limit / --dry-run)
  - `db backfill-translation` (full queue with --dry-run, idempotent)

Tests use a real DB + monkey-patched run_translation_pass so the real
translation API isn't called. The patched fake returns canned
TranslationResult objects per the test's intent (happy path / partial
failure / etc.).
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

import pytest

from news_watch_daemon.cli import (
    _handle_db_backfill_translation,
    _handle_translate,
)
from news_watch_daemon.config import Config
from news_watch_daemon.db import connect, init_db
from news_watch_daemon.translation.types import TranslationResult


REPO_ROOT = Path(__file__).resolve().parent.parent


def _cfg(db_path: Path) -> Config:
    return Config(
        db_path=db_path,
        log_level="WARNING",
        telegram_api_id=12345,
        telegram_api_hash="a" * 32,
        telegram_session_string="dummy_session",
        themes_dir=REPO_ROOT / "themes",
    )


def _seed_pending(conn: sqlite3.Connection, msg_id: int, headline: str, language: str = "ru"):
    """Insert one ru/mixed row with NULL headline_en (ready for translation)."""
    conn.execute(
        "INSERT INTO headlines (headline_id, source, headline, url, "
        "published_at_unix, published_at, fetched_at_unix, fetched_at, "
        "dedupe_hash, language) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            f"hid-{msg_id}",
            "telegram:Ateobreaking",
            headline,
            f"https://t.me/Ateobreaking/{msg_id}",
            1748371200,
            "2026-05-27T17:00:00Z",
            1748371200,
            "2026-05-27T17:00:00Z",
            f"hash-{msg_id}"[:32],
            language,
        ),
    )


def _seed_themes_registry(conn: sqlite3.Connection):
    """Make at least one theme 'active' so re-tag step can run.
    Hand-rolled because load_all_themes against the real themes dir is
    fine, but we need the themes registry to mark them active."""
    from news_watch_daemon.theme_config import load_all_themes
    themes = load_all_themes(REPO_ROOT / "themes")
    for t in themes:
        if t.status != "active":
            continue
        conn.execute(
            "INSERT INTO themes (theme_id, display_name, status, config_hash, "
            "loaded_at_unix, loaded_at) VALUES (?, ?, ?, ?, ?, ?)",
            (t.theme_id, t.display_name, t.status, t.config_hash(), 0, "t"),
        )


# ---------- happy path ----------


def test_backfill_translation_happy_path(tmp_path, monkeypatch):
    """Pending row gets translated + tagged; status='ok'."""
    db = tmp_path / "state.db"
    conn = connect(db)
    try:
        init_db(conn)
        _seed_pending(conn, 170825, "Россия наступает на Украину")
        _seed_themes_registry(conn)
    finally:
        conn.close()

    # Patch run_translation_pass to return a canned ok result
    def fake_pass(**kwargs):
        return {
            ("Ateobreaking", 170825): TranslationResult(
                source_msg_id="170825",
                channel_username="Ateobreaking",
                original_text="Россия наступает на Украину",
                translated_text="Russia advances on Ukraine",
                status="ok",
                error_detail=None,
                latency_ms=100,
                attempts=1,
            ),
        }

    monkeypatch.setattr("news_watch_daemon.cli.run_translation_pass", fake_pass)

    args = argparse.Namespace(db_action="backfill-translation", dry_run=False)
    envelope = _handle_db_backfill_translation(args, _cfg(db))
    assert envelope["status"] == "ok"
    data = envelope["data"]
    assert data["rows_examined"] == 1
    assert data["rows_translated"] == 1
    assert data["by_status"] == {"ok": 1}

    # Verify the headline_en column landed
    conn = connect(db)
    try:
        row = conn.execute(
            "SELECT headline_en FROM headlines WHERE headline_id = ?",
            ("hid-170825",),
        ).fetchone()
        assert row["headline_en"] == "Russia advances on Ukraine"
    finally:
        conn.close()


def test_backfill_translation_dry_run_no_api_call(tmp_path, monkeypatch):
    """--dry-run returns a preview without calling run_translation_pass."""
    db = tmp_path / "state.db"
    conn = connect(db)
    try:
        init_db(conn)
        _seed_pending(conn, 170825, "Россия наступает")
        _seed_pending(conn, 170826, "Украина обороняется")
    finally:
        conn.close()

    call_count = {"n": 0}

    def fake_pass(**kwargs):
        call_count["n"] += 1
        return {}

    monkeypatch.setattr("news_watch_daemon.cli.run_translation_pass", fake_pass)

    args = argparse.Namespace(db_action="backfill-translation", dry_run=True)
    envelope = _handle_db_backfill_translation(args, _cfg(db))
    assert envelope["status"] == "ok"
    data = envelope["data"]
    assert data["dry_run"] is True
    assert data["rows_examined"] == 2
    assert data["rows_would_translate"] == 2
    assert data["by_source_preview"]["telegram:Ateobreaking"]["count"] == 2
    # No API call was made
    assert call_count["n"] == 0
    # No DB writes happened
    conn = connect(db)
    try:
        null_count = conn.execute(
            "SELECT COUNT(*) FROM headlines WHERE headline_en IS NULL"
        ).fetchone()[0]
        assert null_count == 2
    finally:
        conn.close()


def test_backfill_translation_idempotent(tmp_path, monkeypatch):
    """Mando's Q8 test #2: running backfill twice produces identical end state.

    Natural idempotency via the `WHERE headline_en IS NULL` filter:
    first run translates rows, sets headline_en NOT NULL; second run
    finds zero pending rows."""
    db = tmp_path / "state.db"
    conn = connect(db)
    try:
        init_db(conn)
        _seed_pending(conn, 170825, "Россия")
        _seed_themes_registry(conn)
    finally:
        conn.close()

    def fake_pass(**kwargs):
        return {
            ("Ateobreaking", 170825): TranslationResult(
                source_msg_id="170825",
                channel_username="Ateobreaking",
                original_text="Россия",
                translated_text="Russia",
                status="ok",
                error_detail=None,
                latency_ms=100,
                attempts=1,
            ),
        }

    monkeypatch.setattr("news_watch_daemon.cli.run_translation_pass", fake_pass)

    args = argparse.Namespace(db_action="backfill-translation", dry_run=False)
    # First run translates
    first = _handle_db_backfill_translation(args, _cfg(db))
    assert first["data"]["rows_translated"] == 1
    # Second run finds zero pending rows
    second = _handle_db_backfill_translation(args, _cfg(db))
    assert second["status"] == "ok"
    assert second["data"]["rows_examined"] == 0
    assert second["data"]["rows_translated"] == 0


def test_backfill_translation_per_row_failure_isolation(tmp_path, monkeypatch):
    """Mando's Q8 test #1 (CLI variant): when one row hits rate_limited,
    other rows still get translated. The rate_limited row sits in
    pending queue (headline_en stays NULL) for next-cycle retry."""
    db = tmp_path / "state.db"
    conn = connect(db)
    try:
        init_db(conn)
        _seed_pending(conn, 170825, "первое сообщение")  # ok
        _seed_pending(conn, 170826, "второе сообщение")  # rate_limited
        _seed_pending(conn, 170827, "третье сообщение")  # ok
        _seed_themes_registry(conn)
    finally:
        conn.close()

    def fake_pass(**kwargs):
        return {
            ("Ateobreaking", 170825): TranslationResult(
                source_msg_id="170825",
                channel_username="Ateobreaking",
                original_text="первое сообщение",
                translated_text="first message",
                status="ok",
                error_detail=None,
                latency_ms=100,
                attempts=1,
            ),
            ("Ateobreaking", 170826): TranslationResult(
                source_msg_id="170826",
                channel_username="Ateobreaking",
                original_text="второе сообщение",
                translated_text=None,
                status="rate_limited",
                error_detail="FloodWait retries exhausted",
                latency_ms=100,
                attempts=3,
            ),
            ("Ateobreaking", 170827): TranslationResult(
                source_msg_id="170827",
                channel_username="Ateobreaking",
                original_text="третье сообщение",
                translated_text="third message",
                status="ok",
                error_detail=None,
                latency_ms=100,
                attempts=1,
            ),
        }

    monkeypatch.setattr("news_watch_daemon.cli.run_translation_pass", fake_pass)

    args = argparse.Namespace(db_action="backfill-translation", dry_run=False)
    envelope = _handle_db_backfill_translation(args, _cfg(db))
    data = envelope["data"]
    assert data["rows_examined"] == 3
    assert data["rows_translated"] == 2   # /170825 and /170827 succeeded
    assert data["by_status"] == {"ok": 2, "rate_limited": 1}

    # DB state: /170826 still has headline_en NULL (pending queue)
    conn = connect(db)
    try:
        row_826 = conn.execute(
            "SELECT headline_en FROM headlines WHERE headline_id = ?",
            ("hid-170826",),
        ).fetchone()
        assert row_826["headline_en"] is None
        row_825 = conn.execute(
            "SELECT headline_en FROM headlines WHERE headline_id = ?",
            ("hid-170825",),
        ).fetchone()
        assert row_825["headline_en"] == "first message"
    finally:
        conn.close()


def test_backfill_translation_fails_loud_on_pre_v4_schema(tmp_path):
    """If schema_version < 4, subcommand fails loudly with `db migrate` hint."""
    db = tmp_path / "state.db"
    # Apply only v1+v2+v3 by truncating MIGRATIONS
    from news_watch_daemon.db import MIGRATIONS
    conn = connect(db)
    try:
        for version, _desc, filename in MIGRATIONS:
            if version >= 4:
                break
            sql_path = REPO_ROOT / "schema" / filename
            conn.executescript(sql_path.read_text(encoding="utf-8"))
            conn.execute(
                "INSERT INTO schema_version (version, applied_at_unix, "
                "applied_at, description) VALUES (?, ?, ?, ?)",
                (version, 0, "1970-01-01T00:00:00Z", "test"),
            )
    finally:
        conn.close()

    args = argparse.Namespace(db_action="backfill-translation", dry_run=False)
    envelope = _handle_db_backfill_translation(args, _cfg(db))
    assert envelope["status"] == "error"
    assert "schema_version=3" in envelope["error_detail"]
    assert "db migrate" in envelope["error_detail"]


def test_translate_subcommand_source_filter(tmp_path, monkeypatch):
    """`translate --source X` restricts the queue to that source only."""
    db = tmp_path / "state.db"
    conn = connect(db)
    try:
        init_db(conn)
        _seed_pending(conn, 170825, "Россия")
        # Seed a row from a different (synthetic) source
        conn.execute(
            "INSERT INTO headlines (headline_id, source, headline, url, "
            "published_at_unix, published_at, fetched_at_unix, fetched_at, "
            "dedupe_hash, language) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("hid-other", "telegram:OtherChan", "Другое", "https://t.me/OtherChan/1",
             1748371200, "2026-05-27T17:00:00Z", 1748371200, "2026-05-27T17:00:00Z",
             "hash-other", "ru"),
        )
        _seed_themes_registry(conn)
    finally:
        conn.close()

    captured = {"pending_by_channel": None}

    def fake_pass(**kwargs):
        captured["pending_by_channel"] = kwargs["pending_by_channel"]
        return {}

    monkeypatch.setattr("news_watch_daemon.cli.run_translation_pass", fake_pass)

    args = argparse.Namespace(
        source="telegram:Ateobreaking", limit=200, dry_run=False,
    )
    envelope = _handle_translate(args, _cfg(db))
    assert envelope["status"] == "ok"
    # Only Ateobreaking was passed to the translation runner
    assert list(captured["pending_by_channel"].keys()) == ["Ateobreaking"]
    assert len(captured["pending_by_channel"]["Ateobreaking"]) == 1


def test_translate_subcommand_limit_respected(tmp_path, monkeypatch):
    """`translate --limit N` caps the rows examined."""
    db = tmp_path / "state.db"
    conn = connect(db)
    try:
        init_db(conn)
        for i in range(10):
            _seed_pending(conn, 170800 + i, f"сообщение {i}")
        _seed_themes_registry(conn)
    finally:
        conn.close()

    captured = {"pending_by_channel": None}

    def fake_pass(**kwargs):
        captured["pending_by_channel"] = kwargs["pending_by_channel"]
        return {}

    monkeypatch.setattr("news_watch_daemon.cli.run_translation_pass", fake_pass)

    args = argparse.Namespace(source=None, limit=3, dry_run=False)
    envelope = _handle_translate(args, _cfg(db))
    data = envelope["data"]
    assert data["rows_examined"] == 3
    # The runner saw exactly 3 entries
    assert len(captured["pending_by_channel"]["Ateobreaking"]) == 3
