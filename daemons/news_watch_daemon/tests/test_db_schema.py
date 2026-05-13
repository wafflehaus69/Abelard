"""Schema, migration, registry, and heartbeat tests for db.py."""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pytest

from news_watch_daemon import db
from news_watch_daemon.db import (
    connect,
    init_db,
    list_themes,
    read_heartbeats,
    record_heartbeat,
    schema_version,
    to_json_column,
    from_json_column,
    upsert_themes,
)
from news_watch_daemon.theme_config import load_theme


REPO_ROOT = Path(__file__).resolve().parent.parent
SEED_THEME = REPO_ROOT / "themes" / "us_iran_escalation.yaml"

EXPECTED_TABLES = {
    "schema_version",
    "themes",
    "headlines",
    "headline_theme_tags",
    "narratives",
    "alerts",
    "source_health",
    "daemon_heartbeat",
}


# ---------- fixtures ----------


@pytest.fixture
def conn(tmp_path):
    c = connect(tmp_path / "state.db")
    yield c
    c.close()


@pytest.fixture
def initialized(conn):
    init_db(conn)
    return conn


# ---------- connect ----------


def test_connect_enables_wal(conn):
    mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode.lower() == "wal"


def test_connect_enables_foreign_keys(conn):
    on = conn.execute("PRAGMA foreign_keys").fetchone()[0]
    assert on == 1


def test_connect_creates_parent_dir(tmp_path):
    nested = tmp_path / "a" / "b" / "c" / "state.db"
    c = connect(nested)
    c.close()
    assert nested.exists()


# ---------- init_db ----------


def test_init_db_creates_all_tables(initialized):
    rows = initialized.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    names = {r["name"] for r in rows}
    # sqlite_sequence is auto-created by AUTOINCREMENT — ignore it
    assert EXPECTED_TABLES.issubset(names)


def test_init_db_records_all_migrations(initialized):
    rows = initialized.execute(
        "SELECT version, description FROM schema_version ORDER BY version"
    ).fetchall()
    assert [(r["version"], r["description"]) for r in rows] == [
        (1, "initial schema"),
        (2, "headlines dedupe composite index"),
    ]
    assert schema_version(initialized) == 2


def test_init_db_is_idempotent(initialized):
    # Apply again — should be a no-op, total migration rows unchanged.
    pre_count = initialized.execute(
        "SELECT COUNT(*) AS n FROM schema_version"
    ).fetchone()["n"]
    init_db(initialized)
    post_count = initialized.execute(
        "SELECT COUNT(*) AS n FROM schema_version"
    ).fetchone()["n"]
    assert pre_count == post_count


def test_v2_composite_index_exists(initialized):
    rows = initialized.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name=?",
        ("idx_headlines_dedupe_fetched",),
    ).fetchall()
    assert len(rows) == 1


def test_v2_keeps_legacy_dedupe_index(initialized):
    rows = initialized.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name=?",
        ("idx_headlines_dedupe",),
    ).fetchall()
    assert len(rows) == 1


def test_init_db_upgrades_v1_to_v2(tmp_path, conn):
    """An older DB at v1 should pick up v2 cleanly on the next init."""
    # Roll the DB back to a pretend v1 state: apply only the initial.sql
    # by hand, stamp schema_version manually.
    schema_path = Path(__file__).resolve().parent.parent / "schema" / "initial.sql"
    conn.executescript(schema_path.read_text(encoding="utf-8"))
    conn.execute(
        "INSERT INTO schema_version (version, applied_at_unix, applied_at, description) "
        "VALUES (?, ?, ?, ?)",
        (1, 0, "1970-01-01T00:00:00Z", "initial schema"),
    )
    assert schema_version(conn) == 1
    # Now call init_db — should apply v2 without re-running v1.
    init_db(conn)
    assert schema_version(conn) == 2
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name=?",
        ("idx_headlines_dedupe_fetched",),
    ).fetchall()
    assert len(rows) == 1


def test_schema_version_zero_on_fresh_db(conn):
    assert schema_version(conn) == 0


# ---------- CHECK constraints ----------


def test_theme_status_check_constraint(initialized):
    with pytest.raises(sqlite3.IntegrityError):
        initialized.execute(
            "INSERT INTO themes (theme_id, display_name, status, config_hash, "
            "loaded_at_unix, loaded_at) VALUES (?, ?, ?, ?, ?, ?)",
            ("x", "X", "INVALID_STATUS", "h", 0, "t"),
        )


def test_narrative_velocity_check_constraint(initialized):
    initialized.execute(
        "INSERT INTO themes (theme_id, display_name, status, config_hash, "
        "loaded_at_unix, loaded_at) VALUES (?, ?, ?, ?, ?, ?)",
        ("t", "T", "active", "h", 0, "t"),
    )
    with pytest.raises(sqlite3.IntegrityError):
        initialized.execute(
            "INSERT INTO narratives "
            "(theme_id, version, synthesized_at_unix, synthesized_at, "
            "headlines_considered_count, headlines_window_start_unix, "
            "headlines_window_end_unix, thesis, evidence_json, velocity, "
            "model_used) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("t", 1, 0, "t", 0, 0, 0, "thesis", "[]", "ROCKETING", "claude"),
        )


def test_alert_type_check_constraint(initialized):
    initialized.execute(
        "INSERT INTO themes (theme_id, display_name, status, config_hash, "
        "loaded_at_unix, loaded_at) VALUES (?, ?, ?, ?, ?, ?)",
        ("t", "T", "active", "h", 0, "t"),
    )
    with pytest.raises(sqlite3.IntegrityError):
        initialized.execute(
            "INSERT INTO alerts (theme_id, alert_type, triggered_at_unix, "
            "triggered_at, message, sent_channels_json) VALUES (?, ?, ?, ?, ?, ?)",
            ("t", "INVENTED", 0, "t", "msg", "[]"),
        )


def test_source_health_status_check_constraint(initialized):
    with pytest.raises(sqlite3.IntegrityError):
        initialized.execute(
            "INSERT INTO source_health (source, last_attempt_unix, last_attempt, "
            "last_status) VALUES (?, ?, ?, ?)",
            ("finnhub", 0, "t", "FROZEN"),
        )


# ---------- foreign key enforcement ----------


def test_headline_theme_tags_fk_enforced(initialized):
    with pytest.raises(sqlite3.IntegrityError):
        initialized.execute(
            "INSERT INTO headline_theme_tags "
            "(headline_id, theme_id, confidence, tagged_at_unix) "
            "VALUES (?, ?, ?, ?)",
            ("missing_h", "missing_t", "primary", 0),
        )


# ---------- themes registry ----------


def test_upsert_themes_inserts_new(initialized):
    theme = load_theme(SEED_THEME)
    result = upsert_themes(initialized, [theme])
    assert result == {"inserted": 1, "updated": 0, "unchanged": 0}
    entries = list_themes(initialized)
    assert len(entries) == 1
    assert entries[0].theme_id == "us_iran_escalation"
    assert entries[0].config_hash == theme.config_hash()


def test_upsert_themes_unchanged_on_replay(initialized):
    theme = load_theme(SEED_THEME)
    upsert_themes(initialized, [theme])
    result = upsert_themes(initialized, [theme])
    assert result == {"inserted": 0, "updated": 0, "unchanged": 1}


def test_upsert_themes_updates_on_hash_change(initialized):
    theme = load_theme(SEED_THEME)
    upsert_themes(initialized, [theme])
    # Force a different hash by mutating an in-memory copy.
    bumped = theme.model_copy(update={"display_name": "U.S.–Iran Escalation (v2)"})
    result = upsert_themes(initialized, [bumped])
    assert result == {"inserted": 0, "updated": 1, "unchanged": 0}
    entries = list_themes(initialized)
    assert entries[0].display_name == "U.S.–Iran Escalation (v2)"


# ---------- heartbeat ----------


def test_record_and_read_heartbeats(initialized):
    record_heartbeat(
        initialized,
        component="scrape",
        status="ok",
        duration_ms=42,
    )
    record_heartbeat(
        initialized,
        component="synthesize",
        status="error",
        error_detail="not implemented",
    )
    hb = read_heartbeats(initialized)
    by_component = {row["component"]: row for row in hb}
    assert by_component["scrape"]["last_status"] == "ok"
    assert by_component["scrape"]["last_duration_ms"] == 42
    assert by_component["synthesize"]["last_status"] == "error"
    assert by_component["synthesize"]["last_error_detail"] == "not implemented"


def test_heartbeat_upsert_replaces_prior(initialized):
    record_heartbeat(initialized, component="scrape", status="ok", duration_ms=10)
    record_heartbeat(initialized, component="scrape", status="error", error_detail="boom")
    rows = read_heartbeats(initialized)
    assert len(rows) == 1
    assert rows[0]["last_status"] == "error"
    assert rows[0]["last_error_detail"] == "boom"


# ---------- JSON helpers ----------


def test_to_and_from_json_column_roundtrip():
    value = {"b": 2, "a": [1, 2, 3]}
    blob = to_json_column(value)
    assert isinstance(blob, str)
    # sorted keys for determinism
    assert blob == '{"a":[1,2,3],"b":2}'
    assert from_json_column(blob) == value


def test_json_column_handles_none():
    assert to_json_column(None) is None
    assert from_json_column(None) is None
