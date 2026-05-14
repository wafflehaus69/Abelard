"""CLI tests — argparse plumbing, stub envelopes, and real-handler smoke tests.

These cover the boundary contract: stdout/stderr discipline, exit codes,
and the leaf-path dispatch table. The brief mandates a stub envelope on
all not-yet-implemented commands; everything in `_STUB_DETAILS` should
exercise that path.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from news_watch_daemon.cli import (
    HANDLERS,
    _STUB_DETAILS,
    build_parser,
    command_path,
    main,
)


REPO_ROOT = Path(__file__).resolve().parent.parent
SEED_THEME_DIR = REPO_ROOT / "themes"


ALL_LEAVES = {
    "scrape",
    "synthesize",
    "alert-check",
    "status",
    "themes list",
    "themes load",
    "theme show",
    "theme history",
    "headlines recent",
    "alerts recent",
    "db init",
    "db migrate",
    "proposals list",
    "proposals show",
    "proposals approve",
    "proposals reject",
    "briefs list",
    "briefs show",
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
        "scrape", "synthesize", "alert-check", "status",
        "themes", "theme", "headlines", "alerts", "db",
        "proposals", "briefs",
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


def test_theme_show_requires_theme_id():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["theme", "show"])


@pytest.mark.parametrize("argv,expected_leaf", [
    (["scrape"], "scrape"),
    (["synthesize"], "synthesize"),
    (["synthesize", "--theme", "us_iran_escalation"], "synthesize"),
    (["alert-check"], "alert-check"),
    (["status"], "status"),
    (["themes", "list"], "themes list"),
    (["themes", "load"], "themes load"),
    (["theme", "show", "x"], "theme show"),
    (["theme", "history", "x"], "theme history"),
    (["theme", "history", "x", "--days", "60"], "theme history"),
    (["headlines", "recent"], "headlines recent"),
    (["headlines", "recent", "--theme", "x", "--hours", "12"], "headlines recent"),
    (["alerts", "recent"], "alerts recent"),
    (["alerts", "recent", "--days", "30"], "alerts recent"),
    (["db", "init"], "db init"),
    (["db", "migrate"], "db migrate"),
])
def test_all_subcommands_parse_to_expected_leaf(argv, expected_leaf):
    parser = build_parser()
    args = parser.parse_args(argv)
    assert command_path(args) == expected_leaf


def test_every_known_leaf_is_either_real_or_stub():
    """No leaf may silently fall through to a generic 'unmapped' error."""
    for leaf in ALL_LEAVES:
        assert leaf in HANDLERS or leaf in _STUB_DETAILS, (
            f"leaf {leaf!r} is neither in HANDLERS nor _STUB_DETAILS — "
            f"users would see an 'unmapped leaf' error"
        )


# ---------- envelope discipline (stubs) -------------------------------


@pytest.mark.parametrize("argv", [
    ["synthesize"],
    ["synthesize", "--theme", "us_iran_escalation"],
    ["alert-check"],
    ["theme", "show", "us_iran_escalation"],
    ["theme", "history", "us_iran_escalation"],
    ["alerts", "recent"],
])
def test_stub_commands_emit_not_implemented_envelope(env, capsys, argv):
    rc = main(argv)
    assert rc == 1
    envelope = _read_envelope(capsys)
    assert envelope["status"] == "error"
    assert envelope["data_completeness"] == "none"
    assert envelope["data"] is None
    assert envelope["source"] == "internal"
    assert envelope["warnings"], "stubs must carry at least one warning"
    assert envelope["warnings"][0]["reason"] == "not_implemented"


def test_stub_writes_nothing_else_to_stdout(env, capsys):
    main(["synthesize"])
    captured = capsys.readouterr()
    # One trailing newline after the JSON envelope is acceptable.
    out = captured.out.rstrip("\n")
    # No prose before or after — must parse cleanly as a single JSON document.
    json.loads(out)


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
