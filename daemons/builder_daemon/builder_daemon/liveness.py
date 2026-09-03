"""GATE TWO -- issue liveness and assignment state. Runs before any code is written.

LEGITIMACY OVER YIELD. Racing a contributor who claimed an issue first is a
defection: it costs the Tribe standing it cannot rebuild, in exchange for one
bounty. The trade is refused before it is offered, which is why this gate sits
ahead of the work rather than beside it.

TWO INDEPENDENT SOURCES OF CLAIM EVIDENCE, AND THEY DISAGREE USEFULLY.

  * The FORGE knows assignment: `assignees` on the issue is the project's own
    record of who is meant to be working on it.
  * The BOUNTY PLATFORM knows intent: Opire's payload carries `claimerUsers`
    (people who claimed the bounty) and `tryingUsers` (people who pressed
    "I'm working on this"). Measured 2026-09-02 in scout's ledger, these are
    populated on rows where the GitHub issue itself has no assignee at all.

Either is sufficient to establish contest. Requiring both would mean the
common case -- an unassigned GitHub issue that three people are already racing
on Opire -- reads as uncontested, which is exactly backwards.

A CLOSED ISSUE IS A DIFFERENT DECLINE. `declined(closed)` rather than
`declined(contested)`: being late is not the same lesson as being second, and
an outcome ladder that conflates them cannot tell Mando whether the queue is
stale or the competition is thick.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from . import fetch
from .errors import GoneError
from .outcomes import GateResult, Verdict

_API = {
    "github.com": "https://api.github.com/repos/{owner}/{repo}/issues/{number}",
    "gitlab.com": "https://gitlab.com/api/v4/projects/{owner}%2F{repo}/issues/{number}",
    "codeberg.org": "https://codeberg.org/api/v1/repos/{owner}/{repo}/issues/{number}",
}

#: Keys in a scout `raw_json` payload that carry claim intent from the bounty
#: platform. Necessarily incomplete (invariant 6); safe in the narrowing
#: direction only in that a missed key means we do NOT see a claim -- so this
#: list is re-checked whenever a source is added, and `unknown_platform` below
#: makes the gap visible instead of silent.
CLAIM_KEYS = ("claimerUsers", "tryingUsers", "assignees", "claimedBy")


@dataclass(frozen=True)
class LivenessFinding:
    """What gate two established about the issue's availability."""

    url: str
    state: str = "unknown"          # open | closed | unknown
    assignees: tuple[str, ...] = field(default_factory=tuple)
    platform_claims: tuple[str, ...] = field(default_factory=tuple)
    has_linked_pr: bool = False
    sources_read: tuple[str, ...] = field(default_factory=tuple)

    @property
    def contested(self) -> bool:
        return bool(self.assignees or self.platform_claims or self.has_linked_pr)


def platform_claims(raw: dict) -> tuple[str, ...]:
    """Claim signals from the bounty platform's own payload. Pure.

    Names are extracted where present because the packet must be able to say WHO
    already has the issue -- 'contested' without a counterparty is an assertion,
    not evidence.
    """
    names: list[str] = []
    for key in CLAIM_KEYS:
        value = raw.get(key)
        if not value:
            continue
        if isinstance(value, list):
            for entry in value:
                if isinstance(entry, dict):
                    name = entry.get("username") or entry.get("login") or entry.get("name")
                    if name:
                        names.append(f"{key}:{name}")
                elif isinstance(entry, str):
                    names.append(f"{key}:{entry}")
        elif isinstance(value, str):
            names.append(f"{key}:{value}")
    return tuple(names)


def analyze(item, issue: dict | None, *, source_url: str = "") -> LivenessFinding:
    """Combine forge state and platform claims into a finding. Pure -- no network."""
    claims = platform_claims(item.raw if isinstance(item.raw, dict) else {})

    if issue is None:
        return LivenessFinding(
            url=item.url,
            state="unknown",
            platform_claims=claims,
            sources_read=(source_url,) if source_url else (),
        )

    state = str(issue.get("state") or "unknown").lower()
    raw_assignees = issue.get("assignees") or []
    if not raw_assignees and issue.get("assignee"):
        raw_assignees = [issue["assignee"]]
    assignees = tuple(
        a.get("login") or a.get("username") or a.get("name", "")
        for a in raw_assignees
        if isinstance(a, dict)
    )
    assignees = tuple(a for a in assignees if a)

    return LivenessFinding(
        url=item.url,
        state=state,
        assignees=assignees,
        platform_claims=claims,
        has_linked_pr=bool(issue.get("pull_request")),
        sources_read=(source_url,) if source_url else (),
    )


def check(client, item) -> LivenessFinding:
    """Fetch the issue's current state from its forge, then analyze."""
    template = _API.get(item.host)
    if template is None:
        return analyze(item, None)
    url = template.format(owner=item.owner, repo=item.repo, number=item.issue_number)
    try:
        issue = fetch.get_json(client, url, optional=True)
    except GoneError:
        # 410: the repository has issues disabled, so the tracker this row
        # points at no longer exists. That is a determinate answer -- there is
        # nothing to work on -- and it must decline rather than escalate.
        return LivenessFinding(
            url=item.url,
            state="gone",
            platform_claims=platform_claims(item.raw if isinstance(item.raw, dict) else {}),
            sources_read=(url,),
        )
    return analyze(item, issue if isinstance(issue, dict) else None, source_url=url)


def gate(finding: LivenessFinding) -> Verdict:
    """Turn a finding into PASS / DECLINE / UNRESOLVED."""
    if finding.state == "closed":
        return Verdict(
            GateResult.DECLINE,
            reason=f"issue is closed: {finding.url}",
            evidence=finding.sources_read,
        )

    if finding.state == "gone":
        return Verdict(
            GateResult.DECLINE,
            reason=(
                f"the issue tracker for {finding.url} is disabled or removed "
                "(HTTP 410); there is no issue to work on"
            ),
            evidence=finding.sources_read,
        )

    if finding.assignees:
        return Verdict(
            GateResult.DECLINE,
            reason=(
                "issue is assigned to "
                + ", ".join(finding.assignees)
                + " -- taking it would be racing a contributor who has it"
            ),
            evidence=finding.sources_read,
        )

    if finding.has_linked_pr:
        return Verdict(
            GateResult.DECLINE,
            reason=f"an open pull request already addresses {finding.url}",
            evidence=finding.sources_read,
        )

    if finding.platform_claims:
        return Verdict(
            GateResult.DECLINE,
            reason=(
                "the bounty platform records an active claim by "
                + ", ".join(finding.platform_claims[:5])
                + (" (and others)" if len(finding.platform_claims) > 5 else "")
            ),
            evidence=finding.sources_read,
        )

    if finding.state == "unknown":
        return Verdict(
            GateResult.UNRESOLVED,
            reason=(
                f"could not establish whether {finding.url} is open and "
                "unassigned; proceeding blind risks racing a contributor"
            ),
            evidence=finding.sources_read,
        )

    return Verdict(GateResult.PASS, evidence=finding.sources_read)


__all__ = ["CLAIM_KEYS", "LivenessFinding", "platform_claims", "analyze", "check", "gate"]
