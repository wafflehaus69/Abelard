"""Tests for `db retag` — the retroactive tag-backfill pass (post-Phase-1 cleanup).

Covers the load-bearing properties:
  - dry-run reports would-add counts + writes NOTHING (the gate);
  - apply is additive — a previously-untagged headline that matches CURRENT
    config gets the new tag, non-matching headlines stay untagged;
  - idempotent — a second apply (and a post-apply dry-run) add zero, backed by
    the headline_theme_tags PRIMARY KEY (headline_id, theme_id) + INSERT OR IGNORE.

Uses the real seed themes so the test reflects real matcher config; assertions
are structural (a Micron headline tags ai_capex; re-run adds zero) rather than
exact totals, so they survive future config tweaks.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from news_watch_daemon.cli import main

REPO_ROOT = Path(__file__).resolve().parent.parent
SEED_THEME_DIR = REPO_ROOT / "themes"

_NOW = 1782000000


@pytest.fixture
def env(monkeypatch, tmp_path):
    db = tmp_path / "state.db"
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(db))
    monkeypatch.setenv("NEWS_WATCH_THEMES_DIR", str(SEED_THEME_DIR))
    monkeypatch.setenv("LOG_LEVEL", "WARNING")
    yield db


def _insert(db, hid, headline, *, tagged_theme=None):
    conn = sqlite3.connect(db)
    conn.execute(
        "INSERT INTO headlines (headline_id, source, headline, published_at_unix, "
        "published_at, fetched_at_unix, fetched_at, dedupe_hash) VALUES (?,?,?,?,?,?,?,?)",
        (hid, "rss:test", headline, _NOW, "2026-06-01T00:00:00Z", _NOW,
         "2026-06-01T00:00:00Z", hid),
    )
    if tagged_theme:
        conn.execute(
            "INSERT INTO headline_theme_tags (headline_id, theme_id, confidence, tagged_at_unix) "
            "VALUES (?,?,?,?)", (hid, tagged_theme, "primary", _NOW),
        )
    conn.commit()
    conn.close()


def _env(capsys):
    return json.loads(capsys.readouterr().out)


def _tags(db, hid):
    conn = sqlite3.connect(db)
    out = [r[0] for r in conn.execute(
        "SELECT theme_id FROM headline_theme_tags WHERE headline_id=?", (hid,))]
    conn.close()
    return out


def test_retag_dry_run_apply_idempotent(env, capsys):
    db = env
    assert main(["db", "init"]) == 0
    capsys.readouterr()
    assert main(["themes", "load"]) == 0
    capsys.readouterr()

    # Untagged headline that SHOULD tag ai_capex under current config (semis widen).
    _insert(db, "h_micron", "Micron Soars After AI-Fueled Sales Forecast Shatters Estimates")
    # Matches nothing.
    _insert(db, "h_none", "Local bakery wins regional baking award for sourdough")
    # Already tagged — must not be duplicated or re-touched.
    _insert(db, "h_iran", "US launches strikes on Iran nuclear facility", tagged_theme="us_iran_escalation")

    # ---- dry-run: reports, writes nothing ----
    assert main(["db", "retag", "--dry-run"]) == 0
    dry = _env(capsys)["data"]
    assert dry["dry_run"] is True
    assert dry["tags_would_add"] >= 1
    assert "ai_capex_cycle" in dry["by_theme"]
    assert _tags(db, "h_micron") == []          # dry-run did NOT write

    would_add = dry["tags_would_add"]

    # ---- apply: additive ----
    assert main(["db", "retag"]) == 0
    live = _env(capsys)["data"]
    assert live["dry_run"] is False
    assert live["tags_added"] == would_add       # apply matches the dry-run count
    assert "ai_capex_cycle" in _tags(db, "h_micron")   # newly tagged
    assert _tags(db, "h_none") == []                   # non-matching stays untagged

    # ---- idempotent: second apply + post-apply dry-run add zero ----
    assert main(["db", "retag"]) == 0
    assert _env(capsys)["data"]["tags_added"] == 0
    assert main(["db", "retag", "--dry-run"]) == 0
    assert _env(capsys)["data"]["tags_would_add"] == 0


def test_retag_does_not_duplicate_existing_tag(env, capsys):
    db = env
    assert main(["db", "init"]) == 0
    capsys.readouterr()
    assert main(["themes", "load"]) == 0
    capsys.readouterr()

    # A headline pre-tagged to a theme it also matches under config.
    _insert(db, "h_dup", "Nvidia datacenter GPU demand surges", tagged_theme="ai_capex_cycle")

    assert main(["db", "retag"]) == 0
    capsys.readouterr()
    # Still exactly one ai_capex_cycle tag — PK + INSERT OR IGNORE prevented a dup.
    conn = sqlite3.connect(db)
    n = conn.execute(
        "SELECT COUNT(*) FROM headline_theme_tags WHERE headline_id=? AND theme_id=?",
        ("h_dup", "ai_capex_cycle"),
    ).fetchone()[0]
    conn.close()
    assert n == 1


def test_retag_requires_schema(env, capsys):
    # No db init — schema absent → fail-loud error envelope, exit 1.
    rc = main(["db", "retag", "--dry-run"])
    assert rc == 1
    assert _env(capsys)["status"] == "error"
