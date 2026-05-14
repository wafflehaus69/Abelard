"""CLI tests for `proposals list|show|approve|reject`.

Exercises the full leaf-path dispatch + envelope shape contract: each
handler emits exactly one JSON envelope on stdout; nothing else.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from news_watch_daemon.cli import main
from news_watch_daemon.synthesize.brief import DriftProposal
from news_watch_daemon.synthesize.proposals_store import (
    read_pending,
    read_resolved,
    write_pending,
)


REPO_ROOT = Path(__file__).resolve().parent.parent


# ---------- helpers ----------


def _proposal(
    proposal_id: str = "dp-2026-05-13T14-32-08Z-aaaaaaaa",
    theme_id: str = "example",
    proposed_keyword: str = "phrase one",
    suggested_tier: str = "secondary",
    evidence_count: int = 5,
) -> DriftProposal:
    return DriftProposal(
        proposal_id=proposal_id,
        theme_id=theme_id,
        proposed_keyword=proposed_keyword,
        suggested_tier=suggested_tier,
        evidence_count=evidence_count,
        sample_headlines=["a sample headline"],
        notes="n",
        generated_at="2026-05-13T14:32:08Z",
    )


_THEME_YAML = """\
theme_id: example
display_name: Example Theme
status: active
created_at: 2026-05-01

brief: |
  Multi-line brief that must round-trip.

keywords:
  primary:
    - alpha
  secondary:
    - beta
  exclusions: []

tracked_entities:
  tickers:
    - AAPL

alerts:
  velocity_baseline_headlines_per_day: 5.0
"""


@pytest.fixture
def env(monkeypatch, tmp_path):
    """Set up an isolated NEWS_WATCH environment for CLI invocation."""
    themes_dir = tmp_path / "themes"
    themes_dir.mkdir()
    (themes_dir / "example.yaml").write_text(_THEME_YAML, encoding="utf-8")

    proposals_dir = tmp_path / "proposals"

    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.setenv("NEWS_WATCH_THEMES_DIR", str(themes_dir))
    monkeypatch.setenv("NEWS_WATCH_PROPOSALS_PATH", str(proposals_dir))
    monkeypatch.setenv("LOG_LEVEL", "WARNING")
    yield (themes_dir, proposals_dir)


def _read_envelope(capsys) -> dict:
    captured = capsys.readouterr()
    return json.loads(captured.out)


# ---------- proposals list ----------


def test_proposals_list_empty(env, capsys):
    rc = main(["proposals", "list"])
    env_payload = _read_envelope(capsys)
    assert rc == 0
    assert env_payload["status"] == "ok"
    assert env_payload["data"]["count"] == 0
    assert env_payload["data"]["proposals"] == []


def test_proposals_list_returns_pending(env, capsys):
    _, proposals_dir = env
    write_pending(proposals_dir, [
        _proposal(proposal_id="dp-a", proposed_keyword="kw-a"),
        _proposal(proposal_id="dp-b", proposed_keyword="kw-b"),
    ])
    rc = main(["proposals", "list"])
    env_payload = _read_envelope(capsys)
    assert rc == 0
    assert env_payload["data"]["count"] == 2
    ids = {p["proposal_id"] for p in env_payload["data"]["proposals"]}
    assert ids == {"dp-a", "dp-b"}


def test_proposals_list_malformed_store_errors(env, capsys):
    _, proposals_dir = env
    proposals_dir.mkdir(parents=True, exist_ok=True)
    (proposals_dir / "pending.json").write_text("{not json", encoding="utf-8")
    rc = main(["proposals", "list"])
    env_payload = _read_envelope(capsys)
    assert rc == 1
    assert env_payload["status"] == "error"
    assert "unreadable" in env_payload["error_detail"]


# ---------- proposals show ----------


def test_proposals_show_returns_full_proposal(env, capsys):
    _, proposals_dir = env
    p = _proposal(proposal_id="dp-show-me")
    write_pending(proposals_dir, [p])
    rc = main(["proposals", "show", "dp-show-me"])
    env_payload = _read_envelope(capsys)
    assert rc == 0
    assert env_payload["data"]["proposal"]["proposal_id"] == "dp-show-me"
    assert env_payload["data"]["proposal"]["proposed_keyword"] == "phrase one"
    assert env_payload["data"]["proposal"]["evidence_count"] == 5


def test_proposals_show_missing_id_errors(env, capsys):
    rc = main(["proposals", "show", "dp-nope"])
    env_payload = _read_envelope(capsys)
    assert rc == 1
    assert env_payload["status"] == "error"
    assert "not found" in env_payload["error_detail"]


# ---------- proposals approve ----------


def test_proposals_approve_mutates_theme_and_clears_pending(env, capsys):
    themes_dir, proposals_dir = env
    p = _proposal(
        proposal_id="dp-approve",
        proposed_keyword="newly-added",
        suggested_tier="secondary",
    )
    write_pending(proposals_dir, [p])

    rc = main(["proposals", "approve", "dp-approve"])
    env_payload = _read_envelope(capsys)
    assert rc == 0
    assert env_payload["data"]["applied"] is True
    assert env_payload["data"]["mutated_file"].endswith("example.yaml")

    # Theme YAML actually mutated.
    yaml_content = (themes_dir / "example.yaml").read_text(encoding="utf-8")
    assert "newly-added" in yaml_content
    # Multi-line brief block preserved.
    assert "Multi-line brief that must round-trip." in yaml_content

    # Pending now empty.
    assert read_pending(proposals_dir) == []

    # resolved.jsonl carries the audit entry.
    records = read_resolved(proposals_dir)
    assert len(records) == 1
    assert records[0]["proposal_id"] == "dp-approve"
    assert records[0]["action"] == "approve"
    assert records[0]["applied_to_yaml"] is True


def test_proposals_approve_dry_run_changes_nothing(env, capsys):
    themes_dir, proposals_dir = env
    p = _proposal(proposal_id="dp-dryrun", proposed_keyword="kw-dryrun")
    write_pending(proposals_dir, [p])

    rc = main(["proposals", "approve", "dp-dryrun", "--dry-run"])
    env_payload = _read_envelope(capsys)
    assert rc == 0
    assert env_payload["data"]["applied"] is False
    assert env_payload["data"]["dry_run"] is True

    # File unchanged.
    yaml_content = (themes_dir / "example.yaml").read_text(encoding="utf-8")
    assert "kw-dryrun" not in yaml_content
    # Pending unchanged.
    assert len(read_pending(proposals_dir)) == 1
    # No resolved record.
    assert read_resolved(proposals_dir) == []


def test_proposals_approve_missing_id_errors(env, capsys):
    rc = main(["proposals", "approve", "dp-nope"])
    env_payload = _read_envelope(capsys)
    assert rc == 1
    assert env_payload["status"] == "error"
    assert "not found" in env_payload["error_detail"]


def test_proposals_approve_keyword_already_exists_errors(env, capsys):
    """If proposed_keyword somehow ended up in pending despite being in
    the theme YAML already (race condition, manual edit), the mutator
    refuses — pending entry stays so Mando can reject explicitly."""
    themes_dir, proposals_dir = env
    p = _proposal(
        proposal_id="dp-clash",
        proposed_keyword="alpha",   # already in primary
        suggested_tier="secondary",
    )
    write_pending(proposals_dir, [p])

    rc = main(["proposals", "approve", "dp-clash"])
    env_payload = _read_envelope(capsys)
    assert rc == 1
    assert "already exists" in env_payload["error_detail"]
    # Pending still has the proposal — recoverable via reject.
    assert len(read_pending(proposals_dir)) == 1


# ---------- proposals reject ----------


def test_proposals_reject_records_audit(env, capsys):
    themes_dir, proposals_dir = env
    p = _proposal(proposal_id="dp-reject-me", proposed_keyword="kw-rj")
    write_pending(proposals_dir, [p])

    rc = main(["proposals", "reject", "dp-reject-me", "--reason", "cross-theme risk"])
    env_payload = _read_envelope(capsys)
    assert rc == 0
    assert env_payload["data"]["action"] == "reject"
    assert env_payload["data"]["reason"] == "cross-theme risk"

    # Theme YAML untouched.
    yaml_content = (themes_dir / "example.yaml").read_text(encoding="utf-8")
    assert "kw-rj" not in yaml_content
    # Pending now empty.
    assert read_pending(proposals_dir) == []
    # Resolved carries the rejection.
    records = read_resolved(proposals_dir)
    assert len(records) == 1
    assert records[0]["action"] == "reject"
    assert records[0]["reason"] == "cross-theme risk"
    assert records[0]["applied_to_yaml"] is False


def test_proposals_reject_no_reason_ok(env, capsys):
    _, proposals_dir = env
    write_pending(proposals_dir, [_proposal(proposal_id="dp-no-reason")])
    rc = main(["proposals", "reject", "dp-no-reason"])
    env_payload = _read_envelope(capsys)
    assert rc == 0
    records = read_resolved(proposals_dir)
    assert "reason" not in records[0]


def test_proposals_reject_missing_id_errors(env, capsys):
    rc = main(["proposals", "reject", "dp-nope"])
    env_payload = _read_envelope(capsys)
    assert rc == 1
    assert "not found" in env_payload["error_detail"]
