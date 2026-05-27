"""Backfill subcommand tests — migration + classifier + UPDATE flow.

The backfill is what fills in language for the rows that already exist
in the table when the v3 migration lands. Tests exercise:

  1. Backfill against a synthetic pre-migration corpus produces correct
     per-row language + by_language + by_source_language counters
  2. Pre-v3 schema is detected and the subcommand fails loudly with a
     "run db migrate first" message rather than crashing on a column
     that doesn't exist
  3. Re-running after all rows are classified is a no-op
     (rows_examined == rows_classified == 0)
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from news_watch_daemon.cli import _handle_db_backfill_language
from news_watch_daemon.config import Config
from news_watch_daemon.db import MIGRATIONS, connect, init_db


def _cfg(db_path: Path) -> Config:
    return Config(db_path=db_path, log_level="WARNING")


def _seed_row(
    conn: sqlite3.Connection,
    *,
    headline_id: str,
    source: str,
    headline: str,
) -> None:
    """Insert one synthetic pre-migration row (language column left NULL)."""
    conn.execute(
        "INSERT INTO headlines "
        "(headline_id, source, headline, url, "
        " published_at_unix, published_at, fetched_at_unix, fetched_at, "
        " dedupe_hash) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            headline_id,
            source,
            headline,
            f"https://example.com/{headline_id}",
            1748371200,
            "2026-05-27T17:00:00Z",
            1748371200,
            "2026-05-27T17:00:00Z",
            headline_id[:32],  # synthetic dedupe_hash
        ),
    )


def test_backfill_classifies_all_null_rows_with_per_source_breakdown(tmp_path):
    """End-to-end: migrate, seed multi-source pre-migration corpus, backfill,
    assert all rows have non-null language + per-source/per-language counts."""
    db = tmp_path / "state.db"
    conn = connect(db)
    try:
        init_db(conn)  # applies v1, v2, v3
        # 3 sources, mixed languages:
        #   Ateobreaking: 2 ru + 1 mixed
        #   CIG_telegram: 2 en
        #   finnhub:      1 en  (and one digits-only edge case → en)
        _seed_row(conn, headline_id="a1", source="telegram:Ateobreaking",
                  headline="Российские военные провели учения в Беларуси")
        _seed_row(conn, headline_id="a2", source="telegram:Ateobreaking",
                  headline="МИД РФ заявил о готовности к переговорам")
        _seed_row(conn, headline_id="a3", source="telegram:Ateobreaking",
                  headline=("Р" * 30) + ("a" * 70))  # 0.30 cyr_ratio → mixed
        _seed_row(conn, headline_id="c1", source="telegram:CIG_telegram",
                  headline="The U.S. tried to re-colonize the Philippines")
        _seed_row(conn, headline_id="c2", source="telegram:CIG_telegram",
                  headline="Pax Silica is structurally neocolonial")
        _seed_row(conn, headline_id="f1", source="finnhub",
                  headline="Apple reports record quarterly earnings")
        _seed_row(conn, headline_id="f2", source="finnhub",
                  headline="2026 Q2 1234567890")  # digits-only → en
    finally:
        conn.close()

    # Run backfill subcommand
    args = argparse.Namespace(command="db", db_action="backfill-language")
    envelope = _handle_db_backfill_language(args, _cfg(db))

    assert envelope["status"] == "ok"
    data = envelope["data"]
    assert data["rows_examined"] == 7
    assert data["rows_classified"] == 7
    assert data["by_language"] == {"ru": 2, "mixed": 1, "en": 4}
    assert data["by_source_language"] == {
        "telegram:Ateobreaking": {"ru": 2, "mixed": 1},
        "telegram:CIG_telegram": {"en": 2},
        "finnhub": {"en": 2},
    }

    # All rows have non-null language post-backfill
    conn = connect(db)
    try:
        null_count = conn.execute(
            "SELECT COUNT(*) FROM headlines WHERE language IS NULL"
        ).fetchone()[0]
        assert null_count == 0
        # Spot-check specific rows landed in expected buckets
        assert conn.execute(
            "SELECT language FROM headlines WHERE headline_id = ?", ("a1",)
        ).fetchone()["language"] == "ru"
        assert conn.execute(
            "SELECT language FROM headlines WHERE headline_id = ?", ("a3",)
        ).fetchone()["language"] == "mixed"
        assert conn.execute(
            "SELECT language FROM headlines WHERE headline_id = ?", ("f2",)
        ).fetchone()["language"] == "en"  # digits-only edge case
    finally:
        conn.close()


def test_backfill_idempotent_no_nulls_returns_zero(tmp_path):
    """Second invocation after all rows are classified is a no-op."""
    db = tmp_path / "state.db"
    conn = connect(db)
    try:
        init_db(conn)
        _seed_row(conn, headline_id="r1", source="finnhub",
                  headline="Test headline")
    finally:
        conn.close()

    args = argparse.Namespace(command="db", db_action="backfill-language")
    # First pass classifies the row
    first = _handle_db_backfill_language(args, _cfg(db))
    assert first["status"] == "ok"
    assert first["data"]["rows_classified"] == 1
    # Second pass examines zero rows (all already classified)
    second = _handle_db_backfill_language(args, _cfg(db))
    assert second["status"] == "ok"
    assert second["data"]["rows_examined"] == 0
    assert second["data"]["rows_classified"] == 0
    assert second["data"]["by_language"] == {}
    assert second["data"]["by_source_language"] == {}


def test_backfill_fails_loudly_on_pre_v3_schema(tmp_path):
    """If the v3 migration hasn't applied, the language column doesn't
    exist — the subcommand must detect schema_version < 3 and return a
    clear error rather than crashing on the UPDATE."""
    db = tmp_path / "state.db"
    # Apply ONLY v1 + v2 by patching MIGRATIONS down to those two.
    # Simpler approach: hand-apply v1's SQL directly so v3 is absent.
    conn = connect(db)
    try:
        # Apply only versions strictly below 3
        for version, _desc, filename in MIGRATIONS:
            if version >= 3:
                break
            sql_path = (
                Path(__file__).resolve().parent.parent / "schema" / filename
            )
            conn.executescript(sql_path.read_text(encoding="utf-8"))
            conn.execute(
                "INSERT INTO schema_version (version, applied_at_unix, "
                "applied_at, description) VALUES (?, ?, ?, ?)",
                (version, 0, "1970-01-01T00:00:00Z", "test seed"),
            )
    finally:
        conn.close()

    args = argparse.Namespace(command="db", db_action="backfill-language")
    envelope = _handle_db_backfill_language(args, _cfg(db))
    assert envelope["status"] == "error"
    assert "schema_version=2" in envelope["error_detail"]
    assert "db migrate" in envelope["error_detail"]
