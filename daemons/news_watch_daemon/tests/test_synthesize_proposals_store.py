"""Proposals on-disk store tests."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from news_watch_daemon.synthesize.brief import DriftProposal
from news_watch_daemon.synthesize.proposals_store import (
    ProposalsStoreError,
    append_proposals,
    append_resolved,
    find_proposal,
    read_pending,
    read_resolved,
    remove_proposal,
    write_pending,
)


# ---------- helpers ----------


def _proposal(
    proposal_id: str = "dp-2026-05-13T14-32-08Z-aaaaaaaa",
    theme_id: str = "t1",
    proposed_keyword: str = "phrase",
    suggested_tier: str = "secondary",
    evidence_count: int = 5,
    generated_at: str = "2026-05-13T14:32:08Z",
) -> DriftProposal:
    return DriftProposal(
        proposal_id=proposal_id,
        theme_id=theme_id,
        proposed_keyword=proposed_keyword,
        suggested_tier=suggested_tier,
        evidence_count=evidence_count,
        sample_headlines=["s1"],
        notes="n",
        generated_at=generated_at,
    )


# ---------- read_pending / write_pending ----------


def test_read_pending_missing_file_returns_empty(tmp_path):
    assert read_pending(tmp_path) == []


def test_write_then_read_roundtrip(tmp_path):
    p1 = _proposal()
    write_pending(tmp_path, [p1])
    loaded = read_pending(tmp_path)
    assert len(loaded) == 1
    assert loaded[0].proposal_id == p1.proposal_id
    assert loaded[0].proposed_keyword == "phrase"


def test_read_pending_malformed_json_raises(tmp_path):
    (tmp_path / "pending.json").write_text("{not valid", encoding="utf-8")
    with pytest.raises(ProposalsStoreError, match="malformed"):
        read_pending(tmp_path)


def test_read_pending_non_list_root_raises(tmp_path):
    (tmp_path / "pending.json").write_text('{"key": "value"}', encoding="utf-8")
    with pytest.raises(ProposalsStoreError, match="root must be a list"):
        read_pending(tmp_path)


def test_read_pending_invalid_proposal_raises(tmp_path):
    """Bad shape inside an array element → ProposalsStoreError, not
    a raw Pydantic ValidationError."""
    bad = [{"proposal_id": "x"}]  # missing required fields
    (tmp_path / "pending.json").write_text(
        json.dumps(bad), encoding="utf-8",
    )
    with pytest.raises(ProposalsStoreError, match="invalid proposal"):
        read_pending(tmp_path)


def test_write_pending_creates_directory(tmp_path):
    """Auto-create the proposals_path directory if it doesn't exist."""
    nested = tmp_path / "deep" / "nested" / "proposals"
    write_pending(nested, [_proposal()])
    assert (nested / "pending.json").is_file()


def test_write_pending_empty_list_ok(tmp_path):
    write_pending(tmp_path, [])
    assert read_pending(tmp_path) == []


# ---------- append_proposals ----------


def test_append_proposals_to_empty(tmp_path):
    added = append_proposals(tmp_path, [_proposal()])
    assert added == 1
    assert len(read_pending(tmp_path)) == 1


def test_append_proposals_dedups_by_id(tmp_path):
    p1 = _proposal(proposal_id="dp-1")
    p1_dup = _proposal(proposal_id="dp-1", proposed_keyword="different")
    p2 = _proposal(proposal_id="dp-2")
    append_proposals(tmp_path, [p1])
    added = append_proposals(tmp_path, [p1_dup, p2])
    assert added == 1  # only p2 was new
    loaded = read_pending(tmp_path)
    assert {p.proposal_id for p in loaded} == {"dp-1", "dp-2"}
    # First-wins: the original p1 (not the dup) is preserved.
    p1_loaded = next(p for p in loaded if p.proposal_id == "dp-1")
    assert p1_loaded.proposed_keyword == "phrase"


def test_append_proposals_empty_list_noop(tmp_path):
    write_pending(tmp_path, [_proposal()])
    added = append_proposals(tmp_path, [])
    assert added == 0
    assert len(read_pending(tmp_path)) == 1


# ---------- find_proposal / remove_proposal ----------


def test_find_proposal_present(tmp_path):
    p = _proposal(proposal_id="dp-find-me")
    write_pending(tmp_path, [p])
    result = find_proposal(tmp_path, "dp-find-me")
    assert result is not None
    assert result.proposal_id == "dp-find-me"


def test_find_proposal_missing_returns_none(tmp_path):
    assert find_proposal(tmp_path, "dp-nope") is None


def test_remove_proposal_returns_removed(tmp_path):
    p1 = _proposal(proposal_id="dp-1")
    p2 = _proposal(proposal_id="dp-2")
    write_pending(tmp_path, [p1, p2])
    removed = remove_proposal(tmp_path, "dp-1")
    assert removed is not None
    assert removed.proposal_id == "dp-1"
    remaining = read_pending(tmp_path)
    assert len(remaining) == 1
    assert remaining[0].proposal_id == "dp-2"


def test_remove_proposal_missing_returns_none(tmp_path):
    p1 = _proposal(proposal_id="dp-1")
    write_pending(tmp_path, [p1])
    assert remove_proposal(tmp_path, "dp-nope") is None
    assert len(read_pending(tmp_path)) == 1


def test_remove_proposal_atomic(tmp_path):
    """After remove, the file is rewritten with the survivors only."""
    p1 = _proposal(proposal_id="dp-1")
    p2 = _proposal(proposal_id="dp-2")
    write_pending(tmp_path, [p1, p2])
    remove_proposal(tmp_path, "dp-1")
    raw = json.loads((tmp_path / "pending.json").read_text(encoding="utf-8"))
    assert len(raw) == 1
    assert raw[0]["proposal_id"] == "dp-2"


# ---------- append_resolved / read_resolved ----------


def test_append_resolved_creates_file(tmp_path):
    p = _proposal()
    append_resolved(
        tmp_path, proposal=p, action="approve", applied_to_yaml=True,
    )
    records = read_resolved(tmp_path)
    assert len(records) == 1
    assert records[0]["proposal_id"] == p.proposal_id
    assert records[0]["action"] == "approve"
    assert records[0]["applied_to_yaml"] is True
    assert records[0]["theme_id"] == "t1"


def test_append_resolved_with_reason(tmp_path):
    p = _proposal()
    append_resolved(
        tmp_path, proposal=p, action="reject", applied_to_yaml=False,
        reason="cross-theme contamination risk",
    )
    records = read_resolved(tmp_path)
    assert records[0]["reason"] == "cross-theme contamination risk"
    assert records[0]["action"] == "reject"


def test_append_resolved_appends_not_rewrites(tmp_path):
    """Two appends → two records, in order."""
    p1 = _proposal(proposal_id="dp-1")
    p2 = _proposal(proposal_id="dp-2")
    append_resolved(tmp_path, proposal=p1, action="approve", applied_to_yaml=True)
    append_resolved(tmp_path, proposal=p2, action="reject", applied_to_yaml=False)
    records = read_resolved(tmp_path)
    assert len(records) == 2
    assert records[0]["proposal_id"] == "dp-1"
    assert records[1]["proposal_id"] == "dp-2"


def test_append_resolved_invalid_action_rejected(tmp_path):
    with pytest.raises(ProposalsStoreError, match="must be"):
        append_resolved(
            tmp_path, proposal=_proposal(), action="archive",
            applied_to_yaml=False,
        )


def test_read_resolved_skips_malformed_lines(tmp_path, caplog):
    """A garbage line should not break the whole read."""
    p = tmp_path / "resolved.jsonl"
    p.write_text(
        '{"proposal_id": "dp-1", "action": "approve"}\n'
        "{not valid json\n"
        '{"proposal_id": "dp-2", "action": "reject"}\n',
        encoding="utf-8",
    )
    records = read_resolved(tmp_path)
    assert len(records) == 2
    assert records[0]["proposal_id"] == "dp-1"
    assert records[1]["proposal_id"] == "dp-2"


def test_read_resolved_missing_file_returns_empty(tmp_path):
    assert read_resolved(tmp_path) == []


def test_append_resolved_uses_provided_timestamp(tmp_path):
    p = _proposal()
    when = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    append_resolved(
        tmp_path, proposal=p, action="approve", applied_to_yaml=True, when=when,
    )
    records = read_resolved(tmp_path)
    assert records[0]["resolved_at"] == "2025-01-01T12:00:00Z"
