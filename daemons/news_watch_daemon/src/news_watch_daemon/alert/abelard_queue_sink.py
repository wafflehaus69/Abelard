"""AbelardQueueSink — enqueue-only alert transport (GATE 2, 2026-07-14).

The dumb-daemon invariant made explicit: this sink does NOT deliver to
any external channel. It persists the brief as a structured JSON alert
in abelard_queue (see abelard_common.alert_queue) and returns. Abelard
consumes the queue, owns materiality (push vs suppress), and is the
only component that dispatches externally.

Enqueue is the commit point: DispatchResult.success=True means the
alert row is durable. A duplicate brief_id is SUCCESS (the alert is
already persisted — idempotent by design), not a failure.

Conforms to the AlertSink Protocol: dispatch() never raises; storage
failures surface as DispatchResult(success=False, error=...).
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path

from abelard_common.alert_queue import AlertQueue, QueueError

from ..attention.brief_schema import AttentionBrief
from .sink import DispatchableBrief, DispatchResult

CHANNEL_NAME = "abelard_queue"   # matches Brief.dispatch.channel literal exactly
SOURCE_NAME = "news_watch_daemon"

_LOG = logging.getLogger("news_watch_daemon.alert.abelard_queue")


@dataclass
class AbelardQueueSink:
    """Enqueue-only transport. The queue write IS the delivery."""

    db_path: Path

    @property
    def channel_name(self) -> str:
        return CHANNEL_NAME

    def dispatch(self, brief: DispatchableBrief) -> DispatchResult:
        """Persist the brief as a queue item. Never raises."""
        if isinstance(brief, AttentionBrief):
            kind = "attention_brief"
            topic_key = brief.triggering_term
        else:
            kind = "synthesis_brief"
            topic_key = ",".join(sorted(brief.themes_covered)) or "brief"
        now = int(time.time())
        try:
            payload = brief.model_dump(mode="json")
            queue = AlertQueue(self.db_path)
            try:
                item, created = queue.enqueue(
                    source=SOURCE_NAME,
                    kind=kind,
                    topic_key=topic_key,
                    dedupe_key=brief.brief_id,
                    payload=payload,
                )
            finally:
                queue.close()
        except (QueueError, OSError) as exc:
            return DispatchResult(
                success=False, channel=CHANNEL_NAME,
                error=f"enqueue failed: {exc}", dispatched_at_unix=now,
            )
        if not created:
            _LOG.info("brief %s already enqueued (item %d) — idempotent no-op",
                      brief.brief_id, item.id)
        return DispatchResult(
            success=True, channel=CHANNEL_NAME, dispatched_at_unix=now,
        )


__all__ = ["CHANNEL_NAME", "SOURCE_NAME", "AbelardQueueSink"]
