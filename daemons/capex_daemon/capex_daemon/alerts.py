"""CD-DASH1 P2 — the queue sink. The daemon's only path to attention.

**No outward verb here, by construction (E28).** This module enqueues and does
nothing else. It holds no token, opens no socket to a third party, and has no
branch that could send anything anywhere. Abelard consumes the queue, owns the
materiality decision, and is the only component that talks to an external
channel — the GATE 2 ruling of 2026-07-14, encoded in
`abelard_common/alert_queue.py` and generalised by E28.

**The dedupe keys already matched.** `AlertQueue.enqueue` is idempotent on
`dedupe_key`; `phases.Transition.event_key` is content-derived as
`series|quarter|from->to` (E12). The two were designed to the same rule in
different daemons and fit without adaptation, so a transition re-derived by a
later scan cannot double-enqueue.

**The frontier gate lives upstream, and that placement matters.** What reaches
here has already passed `snapshot.alert_lines()`, which drops anything older
than the quarter the panel just reached (E31). So a `--rebuild` that re-derives
thirteen years of history enqueues NOTHING: the queue inherits the gate rather
than enforcing a second, drifting copy of it. `test_a_rebuild_enqueues_nothing`
pins that, because the failure it prevents is a silent flood of an inbox.
"""
from pathlib import Path

QUEUE_SOURCE = "capex_daemon"

# The queue owns its own state home, shared across daemons — it is the boundary
# between every sensor and Abelard, not a per-daemon artifact.
DEFAULT_QUEUE_PATH = Path.home() / ".openclaw" / "abelard_queue" / "queue.db"

KIND_PHASE_TRANSITION = "phase_transition"
KIND_COMMITMENT_MOVE = "commitment_move"


def _open(queue_path):
    # Imported lazily: `abelard_common` is a sibling install rather than a
    # published dependency, so an import failure should surface at the moment
    # the queue is used and be caught by the scan's error path, not take the
    # whole module down at import time.
    from abelard_common.alert_queue import AlertQueue
    return AlertQueue(queue_path or DEFAULT_QUEUE_PATH)


def enqueue_alerts(alerts, queue_path=None, queue=None):
    """Enqueue already-gated transitions. Returns (enqueued, duplicates).

    `alerts` is the output of `snapshot.alert_lines()` — frontier-gated and
    already filtered to the three standing triggers. This function adds no
    policy of its own; deciding what is alertable is the snapshot layer's job
    and duplicating that judgement here would let the two drift.
    """
    if not alerts:
        return 0, 0
    own = queue is None
    q = queue if queue is not None else _open(queue_path)
    enqueued = duplicates = 0
    try:
        for a in alerts:
            _item, created = q.enqueue(
                source=QUEUE_SOURCE,
                kind=KIND_PHASE_TRANSITION,
                # Series-scoped, so a reader can follow one name's history.
                topic_key="series:{}".format(a["series_key"]),
                # THE event key, unchanged. Not a derived variant — if these
                # ever diverge, a re-derived transition alerts twice.
                dedupe_key=a["event_key"],
                payload={
                    "series_key": a["series_key"],
                    "quarter": a["quarter"],
                    "from_state": a["from_state"],
                    "to_state": a["to_state"],
                    "reason": a["reason"],
                },
            )
            if created:
                enqueued += 1
            else:
                duplicates += 1
    finally:
        if own:
            q.close()
    return enqueued, duplicates


def enqueue_commitment_alerts(alerts, queue_path=None, queue=None):
    """Enqueue already-gated commitment moves (P2 C4). Returns (enqueued, duplicates).

    `alerts` is the output of `snapshot.commitment_alerts_and_quarantine()[0]` —
    frontier-gated, above the ratified threshold, basis-checked, and restricted
    to the three buildout classes. As with transitions, this function adds no
    policy of its own: the alertable decision belongs to the snapshot layer, and
    a second copy here would drift from it.

    **This is the sink the B4 work lacked.** `commitment_alert_lines` existed,
    was tested, rendered on the Brief's since-page — and nothing enqueued it, so
    a commitment move could never reach attention. A ratified behaviour is not
    built until its consumer calls it.
    """
    if not alerts:
        return 0, 0
    own = queue is None
    q = queue if queue is not None else _open(queue_path)
    enqueued = duplicates = 0
    try:
        for a in alerts:
            _item, created = q.enqueue(
                source=QUEUE_SOURCE,
                kind=KIND_COMMITMENT_MOVE,
                # Issuer-scoped: a commitment move is one issuer against its own
                # previous disclosure, never a panel series.
                topic_key="issuer:{}".format(a["ticker"]),
                dedupe_key=a["event_key"],
                payload={
                    "ticker": a["ticker"],
                    "bucket": a.get("bucket"),
                    "concept": a.get("concept"),
                    # The class travels with the alert. A commitment figure that
                    # arrives without saying what it contains is how lessor
                    # receipts and prepaid rent reached the buildout read.
                    "basis_class": a.get("basis_class"),
                    # A CONTINGENT figure arrives saying so (ruled 2026-09-28).
                    "qualifiers": a.get("qualifiers") or [],
                    "from_q": a["from_q"], "to_q": a["to_q"],
                    "quarters_between": a.get("quarters_between"),
                    "from_value": a.get("from_value"), "to_value": a.get("to_value"),
                    "delta": a.get("delta"), "multiple": a.get("multiple"),
                    "armed_by": a.get("armed_by"),
                    "basis": a.get("basis"),
                    "reason": a["reason"],
                },
            )
            if created:
                enqueued += 1
            else:
                duplicates += 1
    finally:
        if own:
            q.close()
    return enqueued, duplicates
