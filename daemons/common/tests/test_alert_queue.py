"""AlertQueue tests — the GATE 2 durable queue primitive, hermetic on tmp_path."""

from __future__ import annotations

import pytest

from abelard_common.alert_queue import (
    AlertQueue,
    QueueError,
    SCHEMA_VERSION,
)


@pytest.fixture()
def queue(tmp_path):
    q = AlertQueue(tmp_path / "queue.db")
    yield q
    q.close()


def _enqueue(q: AlertQueue, *, dedupe_key: str = "brief-1",
             topic_key: str = "naval", kind: str = "attention_brief",
             payload: dict | None = None):
    item, created = q.enqueue(
        source="news_watch_daemon", kind=kind, topic_key=topic_key,
        dedupe_key=dedupe_key,
        payload=payload if payload is not None else {"narrative": "n"},
    )
    return item, created


# ---------- enqueue: commit point + idempotency ----------


def test_enqueue_persists_across_connections(tmp_path):
    q1 = AlertQueue(tmp_path / "queue.db")
    item, created = _enqueue(q1)
    q1.close()
    assert created is True
    q2 = AlertQueue(tmp_path / "queue.db")
    try:
        stored = q2.get(item.id)
        assert stored is not None
        assert stored.status == "pending"
        assert stored.payload == {"narrative": "n"}
    finally:
        q2.close()


def test_enqueue_same_dedupe_key_is_idempotent(queue):
    first, created_first = _enqueue(queue, payload={"narrative": "original"})
    second, created_second = _enqueue(queue, payload={"narrative": "DIFFERENT"})
    assert created_first is True
    assert created_second is False
    assert second.id == first.id
    # append-only: the stored payload is never rewritten
    assert second.payload == {"narrative": "original"}
    assert queue.counts()["pending"] == 1


@pytest.mark.parametrize("field", ["source", "kind", "topic_key", "dedupe_key"])
def test_enqueue_refuses_empty_required_field(queue, field):
    kwargs = dict(source="s", kind="k", topic_key="t",
                  dedupe_key="d", payload={})
    kwargs[field] = "  "
    with pytest.raises(QueueError, match=field):
        queue.enqueue(**kwargs)


def test_enqueue_refuses_non_serializable_payload(queue):
    with pytest.raises(QueueError, match="JSON"):
        queue.enqueue(source="s", kind="k", topic_key="t",
                      dedupe_key="d", payload={"bad": object()})


def test_schema_version_recorded(tmp_path):
    q = AlertQueue(tmp_path / "queue.db")
    try:
        row = q._conn.execute(
            "SELECT value FROM queue_meta WHERE key='schema_version'"
        ).fetchone()
        assert row["value"] == str(SCHEMA_VERSION)
    finally:
        q.close()


# ---------- interpretation: status machine + journal ----------


def test_mark_interpreted_push_awaits_dispatch(queue):
    item, _ = _enqueue(queue)
    updated = queue.mark_interpreted(
        item.id, decision="push", decided_by="rule:convergence-push",
        reason="multi-source",
    )
    assert updated.status == "interpreted"
    assert updated.decision == "push"
    assert updated.interpreted_at_unix is not None
    assert [e.decision for e in queue.journal()] == ["push"]


def test_mark_interpreted_suppress_is_terminal(queue):
    item, _ = _enqueue(queue)
    updated = queue.mark_interpreted(
        item.id, decision="suppress", decided_by="rule:cooldown-suppress",
        reason="cooldown",
    )
    assert updated.status == "suppressed"
    assert queue.dispatchable() == []
    entry = queue.journal()[0]
    assert entry.item_id == item.id
    assert entry.decided_by == "rule:cooldown-suppress"


def test_mark_interpreted_twice_raises(queue):
    item, _ = _enqueue(queue)
    queue.mark_interpreted(item.id, decision="push",
                           decided_by="rule:x", reason="r")
    with pytest.raises(QueueError, match="not in 'pending'"):
        queue.mark_interpreted(item.id, decision="suppress",
                               decided_by="rule:x", reason="r")
    # journal only has the one legal decision
    assert len(queue.journal()) == 1


def test_mark_interpreted_rejects_bad_decision(queue):
    item, _ = _enqueue(queue)
    with pytest.raises(QueueError, match="decision"):
        queue.mark_interpreted(item.id, decision="maybe",
                               decided_by="rule:x", reason="r")


def test_mark_interpreted_requires_decided_by_and_reason(queue):
    item, _ = _enqueue(queue)
    with pytest.raises(QueueError, match="required"):
        queue.mark_interpreted(item.id, decision="push",
                               decided_by=" ", reason="r")


# ---------- dispatch: claim discipline (no double-push) ----------


def _pushed_item(queue):
    item, _ = _enqueue(queue)
    return queue.mark_interpreted(item.id, decision="push",
                                  decided_by="rule:x", reason="r")


def test_claim_succeeds_once(queue):
    item = _pushed_item(queue)
    assert queue.claim_for_dispatch(item.id) is True
    assert queue.claim_for_dispatch(item.id) is False  # already claimed
    assert queue.get(item.id).dispatch_attempts == 1


def test_claim_refuses_pending_and_suppressed(queue):
    pending, _ = _enqueue(queue, dedupe_key="p")
    assert queue.claim_for_dispatch(pending.id) is False
    suppressed, _ = _enqueue(queue, dedupe_key="s")
    queue.mark_interpreted(suppressed.id, decision="suppress",
                           decided_by="rule:x", reason="r")
    assert queue.claim_for_dispatch(suppressed.id) is False


def test_mark_dispatched_requires_claim(queue):
    item = _pushed_item(queue)
    with pytest.raises(QueueError, match="claimed"):
        queue.mark_dispatched(item.id, channel="telegram_bot")
    queue.claim_for_dispatch(item.id)
    done = queue.mark_dispatched(item.id, channel="telegram_bot")
    assert done.status == "dispatched"
    assert done.dispatch_channel == "telegram_bot"
    assert done.dispatched_at_unix is not None


def test_known_failure_clears_claim_for_retry(queue):
    item = _pushed_item(queue)
    queue.claim_for_dispatch(item.id)
    failed = queue.record_dispatch_failure(item.id, error="http_500: boom")
    assert failed.status == "interpreted"
    assert failed.claimed_at_unix is None
    assert failed.last_dispatch_error == "http_500: boom"
    # retry is possible: item is dispatchable again
    assert [i.id for i in queue.dispatchable()] == [item.id]
    assert queue.unconfirmed() == []


def test_crash_window_item_is_unconfirmed_and_not_dispatchable(queue):
    item = _pushed_item(queue)
    queue.claim_for_dispatch(item.id)
    # no failure recorded, no dispatch confirmation — crash window
    assert [i.id for i in queue.unconfirmed()] == [item.id]
    assert queue.dispatchable() == []


def test_reset_claim_is_the_manual_recovery_path(queue):
    item = _pushed_item(queue)
    queue.claim_for_dispatch(item.id)
    reset = queue.reset_claim(item.id)
    assert reset.claimed_at_unix is None
    assert [i.id for i in queue.dispatchable()] == [item.id]


def test_reset_claim_refuses_unclaimed(queue):
    item = _pushed_item(queue)
    with pytest.raises(QueueError, match="reset_claim"):
        queue.reset_claim(item.id)


# ---------- cooldown probe ----------


def test_recent_push_exists_inside_window(tmp_path):
    clock = {"now": 1_000_000}
    q = AlertQueue(tmp_path / "queue.db", now_fn=lambda: clock["now"])
    try:
        item, _ = q.enqueue(source="nwd", kind="attention_brief",
                            topic_key="naval", dedupe_key="a", payload={})
        q.mark_interpreted(item.id, decision="push",
                           decided_by="rule:x", reason="r")
        clock["now"] += 3600
        assert q.recent_push_exists(source="nwd", kind="attention_brief",
                                    topic_key="naval", within_s=6 * 3600)
        clock["now"] += 7 * 3600
        assert not q.recent_push_exists(source="nwd", kind="attention_brief",
                                        topic_key="naval", within_s=6 * 3600)
        # different topic never matches
        assert not q.recent_push_exists(source="nwd", kind="attention_brief",
                                        topic_key="china", within_s=10 ** 9)
    finally:
        q.close()


def test_suppress_decision_does_not_arm_cooldown(queue):
    item, _ = _enqueue(queue)
    queue.mark_interpreted(item.id, decision="suppress",
                           decided_by="rule:x", reason="r")
    assert not queue.recent_push_exists(
        source="news_watch_daemon", kind="attention_brief",
        topic_key="naval", within_s=10 ** 9,
    )


# ---------- queries ----------


def test_counts_and_items_filter(queue):
    a, _ = _enqueue(queue, dedupe_key="a")
    b, _ = _enqueue(queue, dedupe_key="b")
    queue.mark_interpreted(a.id, decision="push",
                           decided_by="rule:x", reason="r")
    counts = queue.counts()
    assert counts["pending"] == 1
    assert counts["interpreted"] == 1
    assert [i.id for i in queue.items(status="pending")] == [b.id]
    with pytest.raises(QueueError, match="unknown status"):
        queue.items(status="bogus")
