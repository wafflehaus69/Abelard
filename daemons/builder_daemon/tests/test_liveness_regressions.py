"""Gate-two regressions, pinned to behaviour measured 2026-09-02 on the live queue.

Every case here was found by rehearsing the real code queue, not by imagining
what a forge might return.
"""

from __future__ import annotations

from dataclasses import dataclass

from builder_daemon import liveness, runner
from builder_daemon.errors import GoneError
from builder_daemon.outcomes import GateResult, Outcome


@dataclass
class FakeItem:
    """Minimal stand-in for a WorkItem; gate two reads only these fields."""

    url: str = "https://github.com/o/r/issues/1"
    host: str = "github.com"
    owner: str = "o"
    repo: str = "r"
    issue_number: int = 1
    raw: dict = None

    def __post_init__(self):
        if self.raw is None:
            self.raw = {}

    @property
    def repo_slug(self):
        return f"{self.owner}/{self.repo}"


class FakeClient:
    """Raises whatever it is told to, so the 410 path can be exercised offline."""

    def __init__(self, exc):
        self.exc = exc

    def get_json(self, url, **kw):
        raise self.exc

    def get_text(self, url, **kw):
        raise self.exc


# ---------------------------------------------------------------------------
# HTTP 410 -- issues disabled on the repository
# ---------------------------------------------------------------------------

def test_a_410_declines_rather_than_aborting_the_run() -> None:
    """Two of the 21 live rows return 410 from the issues API.

    Treated as a transport error, they raised out of the runner and took the
    whole batch with them. 410 is the server stating a fact -- issues are
    disabled -- so it is a determinate decline.
    """
    finding = liveness.check(FakeClient(GoneError("gone (410)")), FakeItem())
    assert finding.state == "gone"
    assert liveness.gate(finding).result is GateResult.DECLINE


def test_gone_is_filed_as_closed_not_contested() -> None:
    """Nobody took the work; the tracker stopped existing.

    Filing it as contested would tell the outcome ladder the queue is
    competitive when the real lesson is that it is stale.
    """
    finding = liveness.check(FakeClient(GoneError("gone (410)")), FakeItem())
    verdict = liveness.gate(finding)
    assert runner._liveness_outcome(verdict, finding) is Outcome.DECLINED_CLOSED


# ---------------------------------------------------------------------------
# Platform claim signals -- the bounty platform knows what the forge does not
# ---------------------------------------------------------------------------

OPIRE_CLAIMED = {
    "tryingUsers": [
        {"id": "01JWWAM9GC4NZXR35MT14FWXP2", "username": "Enrique726"},
        {"id": "01KFEPBF2JV6236VSVN7PA61J8", "username": "Eliene-byte"},
    ],
    "claimerUsers": [],
}


def test_a_platform_claim_contests_an_unassigned_github_issue() -> None:
    """The common case, and the one a forge-only check gets backwards.

    Measured on godotengine/godot#70796: GitHub reports no assignee at all,
    while Opire records multiple contributors already working it. Requiring
    BOTH signals would read that as uncontested.
    """
    item = FakeItem(raw=OPIRE_CLAIMED)
    finding = liveness.analyze(item, {"state": "open", "assignees": []})
    assert finding.contested
    verdict = liveness.gate(finding)
    assert verdict.result is GateResult.DECLINE
    assert "Enrique726" in verdict.reason, "the packet must name who holds it"


def test_an_uncontested_open_issue_passes() -> None:
    finding = liveness.analyze(FakeItem(), {"state": "open", "assignees": []})
    assert not finding.contested
    assert liveness.gate(finding).result is GateResult.PASS


def test_an_unreadable_issue_escalates_rather_than_proceeding_blind() -> None:
    """Proceeding without knowing risks racing a contributor."""
    finding = liveness.analyze(FakeItem(), None)
    assert liveness.gate(finding).result is GateResult.UNRESOLVED


def test_a_decline_always_carries_a_reason() -> None:
    """`Verdict` refuses a reasonless decline -- a shrug is not an outcome."""
    import pytest

    from builder_daemon.outcomes import Verdict

    with pytest.raises(ValueError):
        Verdict(GateResult.DECLINE, reason="   ")
