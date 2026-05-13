"""NullSink tests + AlertSink Protocol conformance."""

from __future__ import annotations

import pytest

from news_watch_daemon.alert.null_sink import CHANNEL_NAME, NullSink
from news_watch_daemon.alert.sink import AlertSink, DispatchResult
from news_watch_daemon.synthesize.brief import (
    Brief,
    Dispatch,
    SynthesisMetadata,
    Trigger,
    TriggerWindow,
)


def _brief(narrative: str = "test brief") -> Brief:
    return Brief(
        brief_id="nwd-2026-05-13T14-32-08Z-aaaaaaaa",
        generated_at="2026-05-13T14:32:08Z",
        trigger=Trigger(
            type="event", reason="t",
            window=TriggerWindow(since="a", until="b"),
        ),
        themes_covered=["t1"],
        narrative=narrative,
        dispatch=Dispatch(alerted=False),
        synthesis_metadata=SynthesisMetadata(
            model_used="claude-sonnet-4-7",
            theses_doc_available=False,
        ),
    )


# ---------- protocol conformance ----------


def test_null_sink_is_alert_sink_via_runtime_checkable():
    """Structural typing: NullSink satisfies the AlertSink Protocol."""
    sink = NullSink()
    assert isinstance(sink, AlertSink)


def test_null_sink_channel_name():
    assert NullSink().channel_name == "null"
    assert NullSink().channel_name == CHANNEL_NAME


# ---------- success path ----------


def test_dispatch_records_brief_and_returns_success():
    sink = NullSink()
    b = _brief()
    result = sink.dispatch(b)
    assert isinstance(result, DispatchResult)
    assert result.success is True
    assert result.channel == "null"
    assert result.error is None
    assert sink.dispatched == [b]


def test_dispatch_records_in_order_across_multiple_calls():
    sink = NullSink()
    b1 = _brief("first")
    b2 = _brief("second")
    sink.dispatch(b1)
    sink.dispatch(b2)
    assert [b.narrative for b in sink.dispatched] == ["first", "second"]


def test_dispatched_at_unix_populated():
    sink = NullSink()
    result = sink.dispatch(_brief())
    assert isinstance(result.dispatched_at_unix, int)
    assert result.dispatched_at_unix > 0


# ---------- failure injection ----------


def test_fail_next_returns_failure(monkeypatch):
    sink = NullSink(fail_next=True, fail_error="boom")
    result = sink.dispatch(_brief())
    assert result.success is False
    assert result.channel == "null"
    assert result.error == "boom"
    # Failed dispatch did NOT record the brief
    assert sink.dispatched == []


def test_fail_next_is_one_shot():
    """fail_next auto-resets after firing once."""
    sink = NullSink(fail_next=True)
    sink.dispatch(_brief("first"))   # fails
    sink.dispatch(_brief("second"))  # succeeds
    sink.dispatch(_brief("third"))   # succeeds
    assert [b.narrative for b in sink.dispatched] == ["second", "third"]


def test_fail_next_default_is_false():
    sink = NullSink()
    assert sink.fail_next is False


def test_fail_next_can_be_re_armed():
    """After auto-reset, can re-arm for the next failure simulation."""
    sink = NullSink(fail_next=True, fail_error="first")
    sink.dispatch(_brief())
    sink.fail_next = True
    sink.fail_error = "second"
    result = sink.dispatch(_brief())
    assert result.success is False
    assert result.error == "second"


# ---------- DispatchResult shape ----------


def test_dispatch_result_is_frozen():
    """DispatchResult is immutable to keep call-site discipline."""
    r = DispatchResult(success=True, channel="null", dispatched_at_unix=100)
    with pytest.raises(Exception):
        r.success = False  # type: ignore[misc]


def test_dispatch_result_defaults():
    """error defaults to None; dispatched_at_unix defaults to 0."""
    r = DispatchResult(success=True, channel="x")
    assert r.error is None
    assert r.dispatched_at_unix == 0
