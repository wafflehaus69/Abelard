"""AbelardQueueSink tests — enqueue-only transport, hermetic on tmp_path.

The GATE 2 sink: dispatch() == durable enqueue, nothing external. Also
covers the factory branch and the config plumbing.
"""

from __future__ import annotations

import pytest

from abelard_common.alert_queue import AlertQueue

from news_watch_daemon.alert.abelard_queue_sink import (
    AbelardQueueSink,
    CHANNEL_NAME,
    SOURCE_NAME,
)
from news_watch_daemon.alert.factory import AlertSinkFactoryError, build_alert_sink
from news_watch_daemon.alert.sink import AlertSink
from news_watch_daemon.attention.brief_schema import AttentionBrief
from news_watch_daemon.synthesize.brief import (
    Brief,
    Dispatch,
    SynthesisMetadata,
    Trigger,
    TriggerWindow,
)
from news_watch_daemon.synthesize.config import AlertSinkConfig


def _brief(brief_id: str = "nwd-2026-07-14T20-00-00Z-aaaaaaaa") -> Brief:
    return Brief(
        brief_id=brief_id,
        generated_at="2026-07-14T20:00:00Z",
        trigger=Trigger(type="event", reason="t",
                        window=TriggerWindow(since="a", until="b")),
        themes_covered=["us_iran_escalation", "energy"],
        narrative="Brief body for test.",
        dispatch=Dispatch(alerted=False),
        synthesis_metadata=SynthesisMetadata(
            model_used="claude-sonnet-4-6", theses_doc_available=False,
        ),
    )


def _attention_brief(
        brief_id: str = "nwd-attn-2026-07-14T20-00-00Z-bbbbbbbb") -> AttentionBrief:
    return AttentionBrief(
        brief_id=brief_id,
        generated_at="2026-07-14T20:00:00Z",
        triggering_term="naval",
        term_frequency_window=12,
        term_frequency_prior=1,
        cluster_size=4,
        narrative="Attention body for test.",
        source_mix={"telegram": 3, "rss": 1},
        attention_shape="narrow_source_spike",
        dispatch=Dispatch(alerted=False),
        synthesis_metadata=SynthesisMetadata(
            model_used="claude-sonnet-4-6", theses_doc_available=False,
        ),
    )


@pytest.fixture()
def db_path(tmp_path):
    return tmp_path / "queue.db"


def test_conforms_to_alert_sink_protocol(db_path):
    sink = AbelardQueueSink(db_path=db_path)
    assert isinstance(sink, AlertSink)
    assert sink.channel_name == CHANNEL_NAME == "abelard_queue"


def test_dispatch_brief_enqueues_structured_item(db_path):
    sink = AbelardQueueSink(db_path=db_path)
    result = sink.dispatch(_brief())
    assert result.success is True
    assert result.channel == "abelard_queue"
    with AlertQueue(db_path) as q:
        items = q.items(status="pending")
        assert len(items) == 1
        item = items[0]
        assert item.source == SOURCE_NAME
        assert item.kind == "synthesis_brief"
        assert item.topic_key == "energy,us_iran_escalation"  # sorted themes
        assert item.dedupe_key == "nwd-2026-07-14T20-00-00Z-aaaaaaaa"
        assert item.payload["narrative"] == "Brief body for test."


def test_dispatch_attention_brief_uses_term_as_topic(db_path):
    sink = AbelardQueueSink(db_path=db_path)
    result = sink.dispatch(_attention_brief())
    assert result.success is True
    with AlertQueue(db_path) as q:
        item = q.items(status="pending")[0]
        assert item.kind == "attention_brief"
        assert item.topic_key == "naval"
        assert item.payload["attention_shape"] == "narrow_source_spike"


def test_dispatch_same_brief_twice_is_idempotent_success(db_path):
    sink = AbelardQueueSink(db_path=db_path)
    first = sink.dispatch(_brief())
    second = sink.dispatch(_brief())
    assert first.success is True
    assert second.success is True  # already durable == delivered
    with AlertQueue(db_path) as q:
        assert q.counts()["pending"] == 1


def test_dispatch_never_raises_on_unwritable_path(tmp_path):
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory")
    # queue.db's parent is a FILE — mkdir fails inside dispatch
    sink = AbelardQueueSink(db_path=blocker / "sub" / "queue.db")
    result = sink.dispatch(_brief())
    assert result.success is False
    assert result.channel == "abelard_queue"
    assert result.error is not None


# ---------- factory branch ----------


def test_factory_builds_abelard_queue_sink(monkeypatch, tmp_path):
    monkeypatch.setenv("ABELARD_QUEUE_DB_PATH", str(tmp_path / "q.db"))
    sink = build_alert_sink(AlertSinkConfig(type="abelard_queue"))
    assert isinstance(sink, AbelardQueueSink)
    assert sink.db_path == tmp_path / "q.db"


def test_factory_falls_back_to_default_path(monkeypatch):
    monkeypatch.delenv("ABELARD_QUEUE_DB_PATH", raising=False)
    sink = build_alert_sink(AlertSinkConfig(type="abelard_queue"))
    assert isinstance(sink, AbelardQueueSink)
    # expanduser applied; no literal tilde survives
    assert "~" not in str(sink.db_path)
    assert str(sink.db_path).endswith("queue.db")


def test_factory_unknown_type_lists_all_three(monkeypatch):
    with pytest.raises(AlertSinkFactoryError, match="abelard_queue"):
        build_alert_sink(AlertSinkConfig(type="bogus"))
