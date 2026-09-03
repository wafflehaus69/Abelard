"""The two gates, tested against REAL policy text.

THE FIXTURES BELOW ARE MEASURED, NOT INVENTED. Every quote in `REAL_POLICIES`
was read from the repository's own CONTRIBUTING/PR-template on 2026-09-02, in
the recon over the 19 projects behind scout's agent-eligible code rows. Invented
fixtures test the classifier against the author's idea of how projects phrase
things; these test it against how they actually do.

That distinction already earned its keep once: the hardest prohibition on the
real list is phrased "we don't accept contributions from autonomous agents",
and the first version of `_AI_PROHIBIT` -- written against imagined wording that
used "do not" -- matched every invented fixture and missed that one.
"""

from __future__ import annotations

import pytest

from builder_daemon import liveness, policy
from builder_daemon.outcomes import GateResult

# ---------------------------------------------------------------------------
# Measured 2026-09-02. repo -> (policy text, expected classification)
# ---------------------------------------------------------------------------

REAL_POLICIES = {
    "zed-industries/zed": (
        "we don't accept contributions from autonomous agents. Pull requests "
        "that appear to violate this may be closed, sometimes without notice.",
        policy.AI_PROHIBITED,
    ),
    "denoland/deno": (
        "If you use AI tools (e.g. Copilot, ChatGPT, Claude, Cursor, etc.) to "
        "help write your contribution, you must disclose this in your PR "
        "description.",
        policy.AI_DISCLOSURE,
    ),
    "godotengine/godot": (
        "Use of AI must be disclosed and should include a description of how "
        "it was used.",
        policy.AI_DISCLOSURE,
    ),
    "go-gitea/gitea": (
        "Contributions made with the assistance of AI tools are welcome, but "
        "contributors must use them responsibly and disclose that use clearly.",
        policy.AI_DISCLOSURE,
    ),
    "storybookjs/storybook": (
        "If AI assisted in creating a pull request, please disclose the tool "
        "used (e.g. Claude, Codex, Copilot).",
        policy.AI_DISCLOSURE,
    ),
    "electron/electron": (
        "Disclosure is mandatory when AI-generated code is accepted largely "
        "as-written.",
        policy.AI_DISCLOSURE,
    ),
    "qtop/qtop": (
        "If AI assistance materially contributed to a change, disclose it "
        "briefly in the pull request description.",
        policy.AI_DISCLOSURE,
    ),
}


@pytest.mark.parametrize("repo", sorted(REAL_POLICIES))
def test_real_ai_policy_text_classifies_correctly(repo) -> None:
    text, expected = REAL_POLICIES[repo]
    finding = policy.analyze(repo, {"https://example/CONTRIBUTING.md": text})
    assert finding.ai_policy == expected, f"{repo}: got {finding.ai_policy}"


def test_zed_is_declined_and_the_quote_is_carried() -> None:
    """The one repository on the queue that forbids this daemon's output.

    Two independent grounds, both verified 2026-09-02: a Contributor License
    Agreement, and an explicit refusal of autonomous-agent contributions.
    """
    text, _ = REAL_POLICIES["zed-industries/zed"]
    verdict = policy.gate(policy.analyze("zed-industries/zed", {"u": text}))
    assert verdict.result is GateResult.DECLINE
    assert "autonomous agents" in verdict.reason


def test_a_cla_declines_even_when_everything_else_is_clean() -> None:
    """Measured: Leantime, secondlife/viewer and zed all gate PRs on a CLA."""
    text = (
        "Developers who wish to contribute code to be considered for inclusion "
        "in Leantime must first complete a Contributor License Agreement (CLA). "
        "We use CLA assistant to manage signatures."
    )
    verdict = policy.gate(policy.analyze("Leantime/leantime", {"u": text}))
    assert verdict.result is GateResult.DECLINE
    assert "Contributor License Agreement" in verdict.reason


def test_disclosure_becomes_an_obligation_not_a_decline() -> None:
    """8 of the 19 repositories require this. If it declined, the queue would
    lose its majority for a duty the human submitter can simply discharge."""
    text, _ = REAL_POLICIES["denoland/deno"]
    finding = policy.analyze("denoland/deno", {"u": text})
    verdict = policy.gate(finding)
    assert verdict.result is GateResult.PASS
    assert verdict.obligations
    assert "isclos" in verdict.obligations[0]


def test_a_ban_outranks_a_disclosure_clause_in_the_same_document() -> None:
    """A project may permit disclosed assistance AND refuse autonomous agents.
    The refusal is the one that binds us."""
    text = (
        "AI tools are welcome if you disclose their use. However, we don't "
        "accept contributions from autonomous agents."
    )
    finding = policy.analyze("x/y", {"u": text})
    assert finding.ai_policy == policy.AI_PROHIBITED


def test_banning_non_disclosure_is_not_banning_ai() -> None:
    """Godot's stricter clause -- 'Agents failing to self-disclose will be
    banned' -- bans a behaviour, not a tool. Reading it as a prohibition would
    decline a repository that in fact permits disclosed contributions.
    """
    text = "Use of AI must be disclosed. Agents failing to self-disclose will be banned."
    finding = policy.analyze("godotengine/godot", {"u": text})
    assert finding.ai_policy == policy.AI_DISCLOSURE


def test_unreadable_policy_escalates_rather_than_declining() -> None:
    """The third answer. Declining on no evidence refuses real work."""
    verdict = policy.gate(policy.analyze("x/y", {}))
    assert verdict.result is GateResult.UNRESOLVED


def test_an_ai_marker_with_no_discernible_intent_escalates() -> None:
    finding = policy.analyze("x/y", {"u": "We have thoughts about artificial intelligence."})
    assert finding.ai_policy == policy.AI_AMBIGUOUS
    assert policy.gate(finding).result is GateResult.UNRESOLVED


def test_no_policy_document_but_files_read_passes() -> None:
    finding = policy.analyze("x/y", {"u": "Run the tests before submitting."})
    assert policy.gate(finding).result is GateResult.PASS


# ---------------------------------------------------------------------------
# Gate two
# ---------------------------------------------------------------------------

class _Item:
    """Minimal stand-in for a WorkItem; the gate reads only these fields."""

    def __init__(self, raw=None):
        self.url = "https://github.com/o/r/issues/1"
        self.host, self.owner, self.repo, self.issue_number = "github.com", "o", "r", 1
        self.raw = raw or {}


def test_platform_claims_are_read_from_the_bounty_payload() -> None:
    """Measured: Opire populates tryingUsers on issues GitHub shows as
    unassigned. Requiring a GitHub assignee would read those as uncontested,
    which is exactly backwards."""
    item = _Item({"tryingUsers": [{"username": "Enrique726"}, {"username": "Eliene-byte"}]})
    finding = liveness.analyze(item, {"state": "open", "assignees": []})
    assert finding.contested
    verdict = liveness.gate(finding)
    assert verdict.result is GateResult.DECLINE
    assert "Enrique726" in verdict.reason


def test_a_github_assignee_alone_is_enough() -> None:
    finding = liveness.analyze(_Item(), {"state": "open", "assignees": [{"login": "someone"}]})
    verdict = liveness.gate(finding)
    assert verdict.result is GateResult.DECLINE
    assert "someone" in verdict.reason


def test_closed_declines_as_closed_not_as_contested() -> None:
    from builder_daemon import runner
    from builder_daemon.outcomes import Outcome

    finding = liveness.analyze(_Item(), {"state": "closed", "assignees": []})
    verdict = liveness.gate(finding)
    assert verdict.result is GateResult.DECLINE
    assert runner._liveness_outcome(verdict, finding) is Outcome.DECLINED_CLOSED


def test_an_unreachable_issue_escalates_rather_than_passing() -> None:
    """Proceeding blind is how you race a contributor by accident."""
    verdict = liveness.gate(liveness.analyze(_Item(), None))
    assert verdict.result is GateResult.UNRESOLVED


def test_a_clean_open_unclaimed_issue_passes() -> None:
    finding = liveness.analyze(_Item(), {"state": "open", "assignees": []})
    assert liveness.gate(finding).result is GateResult.PASS


def test_claim_names_are_carried_so_contested_is_evidence_not_assertion() -> None:
    claims = liveness.platform_claims(
        {"claimerUsers": [{"username": "alice"}], "tryingUsers": ["bob"]}
    )
    assert "claimerUsers:alice" in claims
    assert "tryingUsers:bob" in claims
