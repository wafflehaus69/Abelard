"""Drift orchestrator tests — filtering, validation, Brief assembly."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest

from news_watch_daemon.synthesize.brief import DriftProposal
from news_watch_daemon.synthesize.drift import (
    DriftError,
    DriftRunResult,
    propose_drift,
)
from news_watch_daemon.theme_config import ThemeConfig


# ---------- helpers ----------


def _theme(
    theme_id: str = "t1",
    *,
    primary: list[str] | None = None,
    secondary: list[str] | None = None,
    exclusions: list[str] | None = None,
) -> ThemeConfig:
    payload = {
        "theme_id": theme_id,
        "display_name": f"Display {theme_id}",
        "status": "active",
        "created_at": date(2026, 5, 1),
        "brief": "Brief text.",
        "keywords": {
            "primary": primary or ["thing"],
            "secondary": secondary or [],
            "exclusions": exclusions or [],
        },
        "tracked_entities": {"tickers": ["X"]},
        "alerts": {"velocity_baseline_headlines_per_day": 1.0},
    }
    return ThemeConfig.model_validate(payload)


def _text_block(text: str) -> SimpleNamespace:
    return SimpleNamespace(type="text", text=text)


def _make_response(proposals: list[dict]) -> SimpleNamespace:
    return SimpleNamespace(
        content=[_text_block(json.dumps({"proposals": proposals}))],
        model="claude-haiku-4-5-20251029",
        usage=SimpleNamespace(
            input_tokens=1200, output_tokens=300,
            cache_creation_input_tokens=0, cache_read_input_tokens=0,
        ),
    )


class _FakeClient:
    def __init__(self, response):
        self.last_call_kwargs: dict | None = None
        self.messages = SimpleNamespace(create=self._create)
        self._response = response

    def _create(self, **kwargs):
        self.last_call_kwargs = kwargs
        return self._response


def _proposal(
    theme_id: str = "t1",
    proposed_keyword: str = "phrase one",
    suggested_tier: str = "secondary",
    evidence_count: int = 5,
    sample_headlines: list[str] | None = None,
    notes: str | None = "n",
) -> dict:
    return {
        "theme_id": theme_id,
        "proposed_keyword": proposed_keyword,
        "suggested_tier": suggested_tier,
        "evidence_count": evidence_count,
        "sample_headlines": sample_headlines or ["sample one"],
        "notes": notes,
    }


def _call(client, themes, *, min_evidence=3, max_proposals=8, now=None) -> list[DriftProposal]:
    """Wrapper that returns just the proposals list (most tests only
    need that surface). Tests that need telemetry call propose_drift
    directly."""
    return propose_drift(
        client=client,
        model="claude-haiku-4-5",
        max_tokens=1024,
        themes=themes,
        untagged=[(None, "headline", 1764100000)],
        max_proposals_per_batch=max_proposals,
        min_evidence_count=min_evidence,
        now=now,
    ).proposals


# ---------- happy path: Brief assembly ----------


def test_propose_drift_returns_validated_proposals():
    client = _FakeClient(_make_response([_proposal()]))
    now = datetime(2026, 5, 13, 14, 32, 8, tzinfo=timezone.utc)
    results = _call(client, [_theme("t1")], now=now)
    assert len(results) == 1
    p = results[0]
    assert isinstance(p, DriftProposal)
    assert p.theme_id == "t1"
    assert p.proposed_keyword == "phrase one"
    assert p.evidence_count == 5
    assert p.generated_at == "2026-05-13T14:32:08Z"


def test_propose_drift_mints_proposal_id():
    client = _FakeClient(_make_response([_proposal()]))
    now = datetime(2026, 5, 13, 14, 32, 8, tzinfo=timezone.utc)
    results = _call(client, [_theme("t1")], now=now)
    assert results[0].proposal_id.startswith("dp-2026-05-13T14-32-08Z-")


def test_propose_drift_orchestrator_overwrites_id_if_haiku_provided_one():
    """Even if Haiku injects a proposal_id (against the prompt's rules),
    the orchestrator overwrites it. Prevents spoofing existing IDs."""
    payload = _proposal()
    payload["proposal_id"] = "dp-bogus-spoof"
    payload["generated_at"] = "1970-01-01T00:00:00Z"  # spoof
    client = _FakeClient(_make_response([payload]))
    now = datetime(2026, 5, 13, 14, 32, 8, tzinfo=timezone.utc)
    results = _call(client, [_theme("t1")], now=now)
    assert results[0].proposal_id != "dp-bogus-spoof"
    assert results[0].generated_at == "2026-05-13T14:32:08Z"


def test_propose_drift_empty_proposals_returns_empty_list():
    client = _FakeClient(_make_response([]))
    results = _call(client, [_theme("t1")])
    assert results == []


# ---------- defense-in-depth filters ----------


def test_propose_drift_drops_unknown_theme_id():
    """Theme id outside active themes list -> dropped silently."""
    client = _FakeClient(_make_response([_proposal(theme_id="not_a_real_theme")]))
    results = _call(client, [_theme("t1")])
    assert results == []


def test_propose_drift_drops_below_min_evidence():
    client = _FakeClient(_make_response([_proposal(evidence_count=2)]))
    results = _call(client, [_theme("t1")], min_evidence=3)
    assert results == []


def test_propose_drift_keeps_at_min_evidence():
    client = _FakeClient(_make_response([_proposal(evidence_count=3)]))
    results = _call(client, [_theme("t1")], min_evidence=3)
    assert len(results) == 1


def test_propose_drift_drops_existing_primary_keyword():
    client = _FakeClient(_make_response([
        _proposal(proposed_keyword="Iran"),
    ]))
    theme = _theme("t1", primary=["Iran", "Tehran"])
    results = _call(client, [theme])
    assert results == []


def test_propose_drift_drops_existing_secondary_keyword():
    client = _FakeClient(_make_response([
        _proposal(proposed_keyword="Persian Gulf"),
    ]))
    theme = _theme("t1", primary=["X"], secondary=["Persian Gulf"])
    results = _call(client, [theme])
    assert results == []


def test_propose_drift_drops_existing_exclusion_keyword():
    """If Haiku proposes adding a keyword that's already an exclusion,
    drop it — the curator deliberately excluded it, don't re-propose."""
    client = _FakeClient(_make_response([
        _proposal(proposed_keyword="Iran-Contra"),
    ]))
    theme = _theme("t1", primary=["X"], exclusions=["Iran-Contra"])
    results = _call(client, [theme])
    assert results == []


def test_propose_drift_drops_empty_keyword():
    client = _FakeClient(_make_response([_proposal(proposed_keyword="")]))
    results = _call(client, [_theme("t1")])
    assert results == []


def test_propose_drift_drops_whitespace_keyword():
    client = _FakeClient(_make_response([_proposal(proposed_keyword="   ")]))
    results = _call(client, [_theme("t1")])
    assert results == []


def test_propose_drift_caps_at_max_proposals():
    """If Haiku returns more than the cap, keep the top-N by evidence."""
    proposals = [
        _proposal(proposed_keyword=f"kw-{i}", evidence_count=10 - i)
        for i in range(10)
    ]
    client = _FakeClient(_make_response(proposals))
    results = _call(client, [_theme("t1")], max_proposals=3)
    assert len(results) == 3
    # Highest-evidence-count first.
    assert results[0].evidence_count == 10
    assert results[1].evidence_count == 9
    assert results[2].evidence_count == 8


def test_propose_drift_sorts_by_evidence_descending():
    proposals = [
        _proposal(proposed_keyword="kw-low", evidence_count=3),
        _proposal(proposed_keyword="kw-high", evidence_count=8),
        _proposal(proposed_keyword="kw-mid", evidence_count=5),
    ]
    client = _FakeClient(_make_response(proposals))
    results = _call(client, [_theme("t1")])
    assert [p.evidence_count for p in results] == [8, 5, 3]


# ---------- validation errors ----------


def test_propose_drift_invalid_tier_raises():
    """Bad tier value -> aggregated DriftError from Pydantic validation."""
    client = _FakeClient(_make_response([
        _proposal(suggested_tier="not_a_valid_tier"),
    ]))
    with pytest.raises(DriftError, match="proposal validation failed"):
        _call(client, [_theme("t1")])


def test_propose_drift_aggregates_multiple_validation_errors():
    """Multiple bad proposals -> DriftError lists each failure index."""
    client = _FakeClient(_make_response([
        _proposal(proposed_keyword="phrase-a", suggested_tier="bad_tier_1"),
        _proposal(proposed_keyword="phrase-b", suggested_tier="bad_tier_2"),
    ]))
    with pytest.raises(DriftError, match=r"proposals\[1\]"):
        _call(client, [_theme("t1")])


# ---------- prompt / call wiring ----------


def test_propose_drift_passes_themes_into_prompt():
    """The orchestrator must thread themes into the user prompt content."""
    client = _FakeClient(_make_response([]))
    _call(client, [_theme("us_iran_escalation", primary=["Iran"])])
    user_content = client.last_call_kwargs["messages"][0]["content"]
    assert "us_iran_escalation" in user_content
    assert "Iran" in user_content


def test_propose_drift_passes_untagged_into_prompt():
    """Untagged headlines must reach Haiku."""
    client = _FakeClient(_make_response([]))
    result = propose_drift(
        client=client, model="claude-haiku-4-5", max_tokens=1024,
        themes=[_theme("t1")],
        untagged=[("Reuters", "Yemen tanker mine strike", 1764100000)],
        max_proposals_per_batch=8, min_evidence_count=3,
    )
    assert isinstance(result, DriftRunResult)
    user_content = client.last_call_kwargs["messages"][0]["content"]
    assert "Yemen tanker mine strike" in user_content


def test_propose_drift_returns_drift_run_result_with_telemetry():
    """The orchestrator's return type must carry cache telemetry so
    the smoke runner / daemon loop can report Checkpoint metrics."""
    response = SimpleNamespace(
        content=[_text_block(json.dumps({"proposals": [_proposal()]}))],
        model="claude-haiku-4-5-20251029",
        usage=SimpleNamespace(
            input_tokens=2200, output_tokens=550,
            cache_creation_input_tokens=1800, cache_read_input_tokens=0,
        ),
    )
    client = _FakeClient(response)
    result = propose_drift(
        client=client, model="claude-haiku-4-5", max_tokens=1024,
        themes=[_theme("t1")],
        untagged=[(None, "h", 1764100000)],
        max_proposals_per_batch=8, min_evidence_count=3,
    )
    assert isinstance(result, DriftRunResult)
    assert len(result.proposals) == 1
    assert result.model_used == "claude-haiku-4-5-20251029"
    assert result.input_tokens == 2200
    assert result.output_tokens == 550
    assert result.cache_creation_input_tokens == 1800
    assert result.cache_read_input_tokens == 0


def test_propose_drift_uses_haiku_model_id():
    client = _FakeClient(_make_response([]))
    _call(client, [_theme("t1")])
    assert client.last_call_kwargs["model"] == "claude-haiku-4-5"


# ---------- timestamp handling ----------


def test_propose_drift_default_now_used(monkeypatch):
    """now=None uses utcnow under the hood."""
    client = _FakeClient(_make_response([_proposal()]))
    results = _call(client, [_theme("t1")])
    assert len(results) == 1
    # generated_at parses as ISO-8601 with second precision, UTC.
    assert results[0].generated_at.endswith("Z")
