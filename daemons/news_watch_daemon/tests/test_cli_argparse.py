"""CLI tests — argparse plumbing and real-handler smoke tests.

These cover the boundary contract: stdout/stderr discipline, exit codes,
and the leaf-path dispatch table. Every parser-advertised leaf must map to
a real handler (the not-implemented stub surface was retired 2026-07-10).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from news_watch_daemon.cli import (
    HANDLERS,
    build_parser,
    command_path,
    main,
)


REPO_ROOT = Path(__file__).resolve().parent.parent
SEED_THEME_DIR = REPO_ROOT / "themes"


ALL_LEAVES = {
    "scrape",
    "synthesize",
    "status",
    "themes list",
    "themes load",
    "headlines recent",
    "db init",
    "db migrate",
    "proposals list",
    "proposals show",
    "proposals approve",
    "proposals reject",
    "briefs list",
    "briefs show",
    "alert-sink test",
    "trigger-log tail",
    "doctor",
}


@pytest.fixture
def env(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.setenv("NEWS_WATCH_THEMES_DIR", str(SEED_THEME_DIR))
    monkeypatch.setenv("LOG_LEVEL", "WARNING")
    yield tmp_path


def _read_envelope(capsys) -> dict:
    captured = capsys.readouterr()
    return json.loads(captured.out)


# ---------- parser ----------------------------------------------------


def test_parser_has_all_top_level_commands():
    parser = build_parser()
    sub_action = next(a for a in parser._actions if a.dest == "command")
    assert set(sub_action.choices.keys()) == {
        "scrape", "synthesize", "status",
        "themes", "headlines", "db",
        "proposals", "briefs", "alert-sink", "trigger-log",
        "attention",
        # Pass F (2026-05-28): manual translation subcommand. The
        # backfill-translation variant lives under `db` subparser.
        "translate",
        # Full Brief Stage 2b-ii (2026-05-29): on-demand composite
        # artifact subcommand.
        "full-brief",
        # read-brief: reload + render a persisted Full Brief artifact.
        "read-brief",
        # Operational smoothing (2026-07-10): one-pass run cycle +
        # env/deps/DB preflight.
        "run",
        "doctor",
    }


def test_parser_requires_subcommand():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([])


def test_parser_unknown_subcommand_exits():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["not-a-real-command"])


def test_themes_requires_action():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["themes"])


def test_db_requires_action():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["db"])


@pytest.mark.parametrize("argv,expected_leaf", [
    (["scrape"], "scrape"),
    (["synthesize"], "synthesize"),
    (["synthesize", "--theme", "us_iran_escalation"], "synthesize"),
    (["status"], "status"),
    (["themes", "list"], "themes list"),
    (["themes", "load"], "themes load"),
    (["headlines", "recent"], "headlines recent"),
    (["headlines", "recent", "--theme", "x", "--hours", "12"], "headlines recent"),
    (["db", "init"], "db init"),
    (["db", "migrate"], "db migrate"),
])
def test_all_subcommands_parse_to_expected_leaf(argv, expected_leaf):
    parser = build_parser()
    args = parser.parse_args(argv)
    assert command_path(args) == expected_leaf


def test_every_known_leaf_maps_to_a_real_handler():
    """No leaf may silently fall through to a generic 'unmapped' error —
    and, since the stub surface was retired, every one must be a REAL handler."""
    for leaf in ALL_LEAVES:
        assert leaf in HANDLERS, (
            f"leaf {leaf!r} is not in HANDLERS — users would hit the "
            f"internal 'no handler mapped' error"
        )


# ---------- envelope discipline (real commands) -----------------------


def test_db_init_creates_schema(env, capsys, tmp_path):
    rc = main(["db", "init"])
    assert rc == 0
    envelope = _read_envelope(capsys)
    assert envelope["status"] == "ok"
    # schema_version is the highest applied migration; bumps with each new one.
    assert envelope["data"]["schema_version"] >= 1
    db_file = tmp_path / "state.db"
    assert db_file.exists()


def test_db_migrate_is_idempotent_after_init(env, capsys):
    assert main(["db", "init"]) == 0
    capsys.readouterr()  # clear
    rc = main(["db", "migrate"])
    assert rc == 0
    envelope = _read_envelope(capsys)
    assert envelope["status"] == "ok"
    assert envelope["data"]["schema_version"] >= 1


def test_themes_load_requires_initialized_db(env, capsys):
    rc = main(["themes", "load"])
    assert rc == 1
    envelope = _read_envelope(capsys)
    assert envelope["status"] == "error"
    assert "db init" in envelope["error_detail"]


def test_themes_load_then_list_returns_seed(env, capsys):
    """End-to-end: load all themes from the seed dir, list them back.

    Count-agnostic: as more themes land in the seed dir, the only
    invariant is that load_count == list_count == #YAMLs, and that
    `us_iran_escalation` (the canonical foundation seed) is among them.
    """
    assert main(["db", "init"]) == 0
    capsys.readouterr()

    assert main(["themes", "load"]) == 0
    load_env = _read_envelope(capsys)
    assert load_env["status"] == "ok"
    loaded_count = load_env["data"]["loaded_count"]
    assert loaded_count >= 1
    assert "us_iran_escalation" in load_env["data"]["loaded_theme_ids"]
    assert load_env["data"]["inserted"] == loaded_count

    assert main(["themes", "list"]) == 0
    list_env = _read_envelope(capsys)
    assert list_env["status"] == "ok"
    assert list_env["data"]["count"] == loaded_count
    listed_ids = [t["theme_id"] for t in list_env["data"]["themes"]]
    assert "us_iran_escalation" in listed_ids


def test_themes_load_is_idempotent(env, capsys):
    """Re-loading the same theme set produces zero inserts / updates.

    Count-agnostic: `unchanged` equals the total theme count, `inserted`
    and `updated` are both 0.
    """
    assert main(["db", "init"]) == 0
    assert main(["themes", "load"]) == 0
    capsys.readouterr()
    assert main(["themes", "load"]) == 0
    env_doc = _read_envelope(capsys)
    assert env_doc["data"]["unchanged"] >= 1
    assert env_doc["data"]["inserted"] == 0
    assert env_doc["data"]["updated"] == 0
    # All themes that exist on disk should have been re-evaluated.
    assert env_doc["data"]["unchanged"] == env_doc["data"]["loaded_count"]


def test_status_before_init_returns_partial(env, capsys):
    rc = main(["status"])
    assert rc == 0
    envelope = _read_envelope(capsys)
    assert envelope["data_completeness"] == "partial"
    assert envelope["data"]["schema_version"] == 0
    assert envelope["data"]["heartbeats"] == []
    assert any(w["reason"] == "config_drift" for w in envelope["warnings"])


def test_status_after_init_returns_complete(env, capsys):
    assert main(["db", "init"]) == 0
    capsys.readouterr()
    rc = main(["status"])
    assert rc == 0
    envelope = _read_envelope(capsys)
    assert envelope["status"] == "ok"
    assert envelope["data_completeness"] == "complete"
    assert envelope["data"]["schema_version"] >= 1
    # Pass B: source_health is now part of the status envelope.
    assert "source_health" in envelope["data"]
    assert isinstance(envelope["data"]["source_health"], list)
    assert envelope["data"]["source_health"] == []  # nothing scraped yet


# ---------- scrape (Pass A wires the real handler) ---------------------


def test_scrape_without_init_errors(env, capsys):
    rc = main(["scrape"])
    assert rc == 1
    envelope = _read_envelope(capsys)
    assert envelope["status"] == "error"
    assert "db init" in envelope["error_detail"]


def test_scrape_without_active_themes_errors(env, capsys):
    assert main(["db", "init"]) == 0
    capsys.readouterr()
    rc = main(["scrape"])
    assert rc == 1
    envelope = _read_envelope(capsys)
    assert envelope["status"] == "error"
    assert "no active themes" in envelope["error_detail"]


def test_scrape_after_load_runs_orchestrator_envelope_shape(env, capsys, monkeypatch):
    """End-to-end: scrape runs after themes load, with all sources mocked.

    We patch `build_sources` so no HTTP is exercised at the CLI layer
    either — only the orchestrator's plumbing + envelope rendering.
    """
    from unittest.mock import MagicMock
    from news_watch_daemon.sources.base import FetchedItem, FetchResult, SourcePlugin

    def _fake_build_sources(cfg, themes, http_client):
        src = MagicMock(spec=SourcePlugin)
        src.name = "finnhub:general"
        src.fetch.return_value = FetchResult(
            source="finnhub:general",
            fetched_at_unix=0,
            items=[FetchedItem(
                source_item_id="x",
                headline="Iran tests new missile",
                url="https://example.com/1",
                published_at_unix=0,
                raw_source="TestWire",
            )],
            status="ok",
        )
        return [src]

    monkeypatch.setattr("news_watch_daemon.cli.build_sources", _fake_build_sources)

    assert main(["db", "init"]) == 0
    capsys.readouterr()
    assert main(["themes", "load"]) == 0
    capsys.readouterr()

    rc = main(["scrape"])
    assert rc == 0
    envelope = _read_envelope(capsys)
    assert envelope["status"] == "ok"
    assert envelope["data_completeness"] == "complete"
    data = envelope["data"]
    assert data["sources_attempted"] == 1
    assert data["sources_succeeded"] == 1
    assert data["sources_failed"] == 0
    assert data["headlines_inserted_total"] == 1
    # The canned headline matches the us_iran_escalation primary keyword "Iran".
    # It may also match future themes that include Iran-relevant keywords; the
    # invariant is "at least one tag, and us_iran_escalation is among them".
    assert data["theme_tags_inserted_total"] >= 1
    assert "us_iran_escalation" in data["themes_active"]
    assert data["per_source"][0]["name"] == "finnhub:general"


def test_scrape_with_failing_source_yields_partial(env, capsys, monkeypatch):
    from unittest.mock import MagicMock
    from news_watch_daemon.sources.base import FetchResult, SourcePlugin

    def _fake_build_sources(cfg, themes, http_client):
        src = MagicMock(spec=SourcePlugin)
        src.name = "finnhub:general"
        src.fetch.return_value = FetchResult(
            source="finnhub:general",
            fetched_at_unix=0,
            items=[],
            status="error",
            error_detail="http_5xx: 503",
        )
        return [src]

    monkeypatch.setattr("news_watch_daemon.cli.build_sources", _fake_build_sources)
    assert main(["db", "init"]) == 0
    capsys.readouterr()
    assert main(["themes", "load"]) == 0
    capsys.readouterr()

    rc = main(["scrape"])
    assert rc == 0  # orchestration succeeded; only the source failed
    envelope = _read_envelope(capsys)
    assert envelope["status"] == "ok"
    assert envelope["data_completeness"] == "partial"
    assert envelope["warnings"]
    assert envelope["warnings"][0]["reason"] == "upstream_error"


# ---------- config error path -----------------------------------------


def test_missing_db_path_env_emits_error_envelope(monkeypatch, capsys):
    monkeypatch.delenv("NEWS_WATCH_DB_PATH", raising=False)
    rc = main(["status"])
    assert rc == 1
    envelope = _read_envelope(capsys)
    assert envelope["status"] == "error"
    assert "NEWS_WATCH_DB_PATH" in envelope["error_detail"]


# ---------- doctor (preflight) ----------------------------------------


def _doctor_env(monkeypatch, tmp_path):
    """Point doctor's writable-dir checks at tmp so it never touches ~/.openclaw."""
    monkeypatch.setenv("NEWS_WATCH_BRIEF_ARCHIVE", str(tmp_path / "briefs"))
    monkeypatch.setenv("NEWS_WATCH_TRIGGER_LOG", str(tmp_path / "trigger.jsonl"))
    monkeypatch.setenv("NEWS_WATCH_CROSS_SOURCE_LOG", str(tmp_path / "cross.jsonl"))


def test_doctor_blocks_when_schema_absent(env, monkeypatch, tmp_path, capsys):
    _doctor_env(monkeypatch, tmp_path)
    rc = main(["doctor"])
    assert rc == 1
    envelope = _read_envelope(capsys)
    assert envelope["status"] == "error"
    checks = {c["name"]: c["status"] for c in envelope["data"]["checks"]}
    assert checks["database"] == "error"


def test_doctor_ok_after_init_and_themes_load(env, monkeypatch, tmp_path, capsys):
    _doctor_env(monkeypatch, tmp_path)
    assert main(["db", "init"]) == 0
    capsys.readouterr()
    assert main(["themes", "load"]) == 0
    capsys.readouterr()
    rc = main(["doctor"])
    envelope = _read_envelope(capsys)
    # No BLOCKING errors — warnings (missing secrets / signal-cli) are exit 0.
    assert envelope["data"]["summary"]["error"] == 0
    assert rc == 0
    checks = {c["name"]: c["status"] for c in envelope["data"]["checks"]}
    assert checks["database"] == "ok"
    assert checks["active_themes"] == "ok"
