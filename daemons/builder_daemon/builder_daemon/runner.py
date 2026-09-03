"""The execution contract: gate one, then gate two, then -- in a later phase -- work.

PHASE 1 SHIPS THE GATES AND NOTHING PAST THEM. There is deliberately no
`draft()` in this module and no `run` verb in the CLI. A work item that clears
both gates in rehearsal returns a packet saying so and stops. The drafting stage
is Phase 2 and waits on an admitted code row, of which there are currently zero.

That absence is the safest possible state to leave the daemon in between phases:
not a disabled execution path, not a feature flag, but no execution path at all.
"""

from __future__ import annotations

from . import liveness as liveness_mod
from . import policy as policy_mod
from .outcomes import GateResult, Outcome
from .packet import Packet

#: Which decline each gate produces. Kept as data so the mapping is inspectable
#: rather than buried in branches -- the outcome ladder needs to be able to read
#: what a gate can conclude without executing it.
_POLICY_OUTCOME = {
    GateResult.DECLINE: Outcome.DECLINED_POLICY,
    GateResult.UNRESOLVED: Outcome.UNRESOLVED_POLICY,
}


def _liveness_outcome(verdict, finding) -> Outcome:
    if verdict.result is GateResult.UNRESOLVED:
        return Outcome.UNRESOLVED_LIVENESS
    # "gone" (HTTP 410, issues disabled) belongs with closed, not contested:
    # nobody took the work, the tracker stopped existing. Filing it as
    # contested would tell the ladder the queue is competitive when the real
    # lesson is that it is stale.
    if finding.state in ("closed", "gone"):
        return Outcome.DECLINED_CLOSED
    return Outcome.DECLINED_CONTESTED


def run_gates(client, item, *, rehearsal: bool = False) -> Packet:
    """Run both gates against one work item and return the packet.

    Gate order is not an implementation detail. Policy runs first because a
    repository that will not accept the contribution makes the issue's
    availability irrelevant -- and because checking availability first would
    mean fetching an issue we have no business working on.
    """
    policy_finding = policy_mod.recon(client, item)
    policy_verdict = policy_mod.gate(policy_finding)

    packet = Packet(
        opportunity_id=item.opportunity_id,
        repo_slug=item.repo_slug,
        issue_url=item.url,
        outcome=Outcome.REHEARSED if rehearsal else Outcome.DRAFTED,
        policy=policy_verdict,
        sources_read=tuple(policy_finding.sources_read),
        obligations=tuple(policy_verdict.obligations),
        rehearsal=rehearsal,
    )

    if policy_verdict.stops:
        packet.outcome = _POLICY_OUTCOME[policy_verdict.result]
        return packet

    live_finding = liveness_mod.check(client, item)
    live_verdict = liveness_mod.gate(live_finding)
    packet.liveness = live_verdict
    packet.sources_read = tuple(packet.sources_read) + tuple(live_finding.sources_read)

    if live_verdict.stops:
        packet.outcome = _liveness_outcome(live_verdict, live_finding)
        return packet

    # Both gates clear. Phase 1 stops here by design: `rehearsal` reports the
    # clearance, and there is no non-rehearsal caller because no execution verb
    # exists yet.
    packet.outcome = Outcome.REHEARSED if rehearsal else Outcome.DRAFTED
    return packet


__all__ = ["run_gates"]
