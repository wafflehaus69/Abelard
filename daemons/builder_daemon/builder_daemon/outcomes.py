"""The outcome vocabulary. Declined-with-reason is a first-class result.

WHY THIS MODULE IS SEPARATE AND SMALL. The outcome ladder is the thing the Tribe
learns from, and a leg that reports only its successes teaches nothing. Every
terminal state here is a *completed* run: a decline is work correctly refused,
recorded with its reason, and it is worth exactly as much to the ladder as a
patch is.

There is deliberately no `FAILED` and no `ERROR` member. A crash is an
exception, which is loud and has a traceback; it is not an outcome. Giving
failure an outcome name is how a taxonomy starts absorbing bugs as if they were
results.

THE THIRD GATE ANSWER. Gates return PASS, DECLINE **or UNRESOLVED**. Unresolved
is scout's YELLOW: the gate could not establish the fact it exists to establish.
It is not a decline -- declining on an unknown would refuse real work on no
evidence -- and it is not a pass. It escalates to Mando, which is the only
correct destination for a question the daemon cannot answer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class GateResult(str, Enum):
    """What a gate concluded. Three answers, not two."""

    PASS = "pass"
    DECLINE = "decline"
    UNRESOLVED = "unresolved"


class Outcome(str, Enum):
    """Terminal states of one work item. Every one of these is a completed run.

    The `DECLINED_*` members are successes of the gate that produced them.
    """

    # Refusals -- first-class, reason-bearing, ladder-feeding.
    DECLINED_POLICY = "declined(policy)"
    DECLINED_CONTESTED = "declined(contested)"
    # Distinct from CONTESTED on purpose: being late is not the same lesson as
    # being second. Collapsing them would leave the ladder unable to say whether
    # the queue is stale or the competition is thick.
    DECLINED_CLOSED = "declined(closed)"
    DECLINED_UNREPRODUCIBLE = "declined(unreproducible)"
    DECLINED_OUT_OF_DEPTH = "declined(out_of_depth)"

    # Escalations -- the daemon could not establish a fact it needs.
    UNRESOLVED_POLICY = "unresolved(policy)"
    UNRESOLVED_LIVENESS = "unresolved(liveness)"

    # Production.
    DRAFTED = "drafted"
    REHEARSED = "rehearsed"


#: Outcomes that mean the Builder correctly declined to do the work. Named as a
#: set so the ladder can count refusals without string-matching a prefix.
DECLINED = frozenset({
    Outcome.DECLINED_POLICY,
    Outcome.DECLINED_CONTESTED,
    Outcome.DECLINED_CLOSED,
    Outcome.DECLINED_UNREPRODUCIBLE,
    Outcome.DECLINED_OUT_OF_DEPTH,
})

#: Outcomes that need Mando to answer something the daemon could not.
ESCALATED = frozenset({
    Outcome.UNRESOLVED_POLICY,
    Outcome.UNRESOLVED_LIVENESS,
})

#: Every outcome is terminal. There is no retry state and no in-progress state:
#: a run either reached a conclusion or raised.
TERMINAL = frozenset(Outcome)


@dataclass(frozen=True)
class Verdict:
    """A gate's answer, with the evidence that produced it.

    `reason` is mandatory on anything but a PASS. A decline without a reason is
    not a first-class outcome, it is a shrug -- so the constructor refuses one.
    """

    result: GateResult
    reason: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    obligations: tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if self.result is not GateResult.PASS and not self.reason.strip():
            raise ValueError(f"{self.result.value} requires a reason")

    @property
    def stops(self) -> bool:
        """True when the pipeline must not proceed to the next stage."""
        return self.result is not GateResult.PASS


__all__ = [
    "GateResult", "Outcome", "Verdict",
    "DECLINED", "ESCALATED", "TERMINAL",
]
