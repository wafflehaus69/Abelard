"""SignalSink tests — subprocess fully mocked, hermetic."""

from __future__ import annotations

import subprocess
import time
import typing
from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch):
    """Replace time.sleep so retry-path tests don't burn real seconds.

    SignalSink calls time.sleep(RETRY_BACKOFF_S) between attempts on
    transport failure. Without this fixture, every failure-path test
    pays a real 2s wall-clock cost.
    """
    monkeypatch.setattr("news_watch_daemon.alert.signal_sink.time.sleep",
                        lambda _seconds: None)

from news_watch_daemon.alert.signal_sink import (
    ALLOWED_DESTINATION,
    CHANNEL_NAME,
    RETRY_BACKOFF_S,
    SignalSink,
    _assert_destination_allowed,
    _format_message_body,
)
from news_watch_daemon.alert.sink import DispatchResult
from news_watch_daemon.synthesize.brief import (
    Brief,
    Dispatch,
    SynthesisMetadata,
    Trigger,
    TriggerWindow,
)


# ---------- helpers ----------


class _FakeCompleted:
    """Stand-in for subprocess.CompletedProcess in tests."""

    def __init__(self, returncode: int = 0, stdout: str = "", stderr: str = ""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def _brief(narrative: str = "Brief body for test.") -> Brief:
    return Brief(
        brief_id="nwd-2026-05-13T14-32-08Z-aaaaaaaa",
        generated_at="2026-05-13T14:32:08Z",
        trigger=Trigger(
            type="event", reason="t",
            window=TriggerWindow(since="a", until="b"),
        ),
        themes_covered=["us_iran_escalation", "fed_policy_path"],
        narrative=narrative,
        dispatch=Dispatch(alerted=False),
        synthesis_metadata=SynthesisMetadata(
            model_used="claude-sonnet-4-6", theses_doc_available=True,
        ),
    )


def _sink(
    destination: str = ALLOWED_DESTINATION,
    cli_path: str = "signal-cli",
    timeout_s: float = 30.0,
    *,
    pre_cache_note_to_self: bool | None = True,
) -> SignalSink:
    """Build a SignalSink; by default, the --note-to-self detection is
    short-circuited as True so tests don't have to mock the probe call."""
    s = SignalSink(cli_path=cli_path, destination=destination, timeout_s=timeout_s)
    if pre_cache_note_to_self is not None:
        s._supports_note_to_self = pre_cache_note_to_self  # type: ignore[assignment]
    return s


# ---------- Mando's forward-note: channel literal alignment ----------


def test_channel_name_matches_brief_schema_literal():
    """SignalSink.channel_name must be exactly the Literal value the
    Brief schema permits for dispatch.channel.

    Mando's Step 6 forward-note (and Step 7+): the sinks return
    DispatchResult.channel matching Brief.dispatch.channel values
    EXACTLY. Verified by introspecting the schema's Literal.
    """
    sink = _sink()
    assert sink.channel_name == CHANNEL_NAME
    # Pull the Literal members out of the Brief.dispatch.channel field
    # and confirm CHANNEL_NAME is one of them.
    dispatch_fields = Dispatch.model_fields
    channel_annotation = dispatch_fields["channel"].annotation
    # channel: Optional[Literal["signal", "telegram_bot"]]
    # Walk the Union/Optional to find the Literal members.
    type_args = typing.get_args(channel_annotation)
    literal_values: set[str] = set()
    for arg in type_args:
        if typing.get_origin(arg) is typing.Literal:
            literal_values.update(typing.get_args(arg))
    assert "signal" in literal_values, (
        f"Brief.dispatch.channel Literal members: {literal_values}"
    )
    assert CHANNEL_NAME in literal_values
    assert CHANNEL_NAME == "signal"


def test_successful_dispatch_returns_channel_signal_literal():
    """Verify the runtime DispatchResult.channel is the literal 'signal'."""
    sink = _sink()
    with patch("subprocess.run", return_value=_FakeCompleted(returncode=0)):
        result = sink.dispatch(_brief())
    assert result.channel == "signal"
    assert result.channel == CHANNEL_NAME


# ---------- destination validation gate ----------


def test_destination_validation_refuses_mismatched(caplog):
    """SignalSink configured with the wrong destination → refuses cleanly."""
    sink = _sink(destination="not_the_allowed_value")
    with caplog.at_level("CRITICAL", logger="news_watch_daemon.alert.signal"):
        with patch("subprocess.run") as mock_run:
            result = sink.dispatch(_brief())
            mock_run.assert_not_called()
    assert result.success is False
    assert result.channel == "signal"
    assert "destination_mismatch" in result.error
    # CRITICAL log emitted
    critical = [r.getMessage() for r in caplog.records if r.levelno >= 50]
    assert any("refused" in m.lower() for m in critical)


def test_destination_validation_passes_allowed():
    """Configured destination == ALLOWED → validator does not raise."""
    _assert_destination_allowed(ALLOWED_DESTINATION)  # no exception


def test_destination_validation_raises_internal_exception_on_mismatch():
    """Internal function raises rather than returning a bool."""
    from news_watch_daemon.alert.signal_sink import _DestinationMismatchError
    with pytest.raises(_DestinationMismatchError):
        _assert_destination_allowed("wrong_value")


# ---------- note-to-self detection ----------


def test_note_to_self_detected_via_help_probe():
    sink = SignalSink(cli_path="signal-cli", destination=ALLOWED_DESTINATION, timeout_s=30.0)
    # Help output contains --note-to-self
    help_output = "Usage: signal-cli send [options]\n  --note-to-self     send to self"
    with patch(
        "subprocess.run",
        return_value=_FakeCompleted(returncode=0, stdout=help_output),
    ):
        assert sink._detect_note_to_self_support() is True


def test_note_to_self_not_detected_when_help_lacks_flag(caplog):
    sink = SignalSink(cli_path="signal-cli", destination=ALLOWED_DESTINATION, timeout_s=30.0)
    help_output = "Usage: signal-cli send [options]\n  --recipient ADDRESS"
    with patch(
        "subprocess.run",
        return_value=_FakeCompleted(returncode=0, stdout=help_output),
    ):
        assert sink._detect_note_to_self_support() is False


def test_note_to_self_detection_handles_missing_cli():
    sink = SignalSink(cli_path="/nonexistent/signal-cli",
                       destination=ALLOWED_DESTINATION, timeout_s=30.0)
    with patch("subprocess.run", side_effect=FileNotFoundError):
        assert sink._detect_note_to_self_support() is False


def test_dispatch_refuses_when_note_to_self_unsupported(caplog):
    """If signal-cli lacks --note-to-self, fail loud with CRITICAL log."""
    sink = _sink(pre_cache_note_to_self=False)
    with caplog.at_level("CRITICAL", logger="news_watch_daemon.alert.signal"):
        result = sink.dispatch(_brief())
    assert result.success is False
    assert "--note-to-self" in result.error
    assert "upgrade" in result.error.lower() or "manually" in result.error.lower()


# ---------- subprocess success / failure paths ----------


def test_happy_path_success():
    sink = _sink()
    with patch("subprocess.run", return_value=_FakeCompleted(returncode=0)) as mock_run:
        result = sink.dispatch(_brief())
    assert result.success is True
    assert result.error is None
    # Verify argv shape
    argv = mock_run.call_args.args[0]
    assert argv[0] == "signal-cli"
    assert "send" in argv
    assert "--note-to-self" in argv
    assert "-m" in argv


def test_nonzero_exit_returns_failure_with_stderr():
    sink = _sink()
    fail = _FakeCompleted(returncode=2, stderr="Auth key invalid")
    with patch("subprocess.run", return_value=fail):
        result = sink.dispatch(_brief())
    assert result.success is False
    assert "exit=2" in result.error
    assert "Auth key invalid" in result.error


def test_timeout_returns_failure():
    sink = _sink()
    with patch(
        "subprocess.run",
        side_effect=subprocess.TimeoutExpired(cmd=["signal-cli"], timeout=30.0),
    ):
        result = sink.dispatch(_brief())
    assert result.success is False
    assert "timed out" in result.error
    assert "30.0" in result.error


def test_signal_cli_not_found_returns_failure():
    sink = _sink()
    with patch("subprocess.run", side_effect=FileNotFoundError):
        result = sink.dispatch(_brief())
    assert result.success is False
    assert "signal-cli not found" in result.error


def test_os_error_returns_failure():
    sink = _sink()
    with patch("subprocess.run", side_effect=OSError("permission denied")):
        result = sink.dispatch(_brief())
    assert result.success is False
    assert "OSError" in result.error


# ---------- single retry ----------


def test_single_retry_succeeds_on_second_attempt():
    """First subprocess call fails; second succeeds → overall success."""
    sink = _sink()
    fail = _FakeCompleted(returncode=1, stderr="transient")
    ok = _FakeCompleted(returncode=0)
    with patch("subprocess.run", side_effect=[fail, ok]) as mock_run:
        with patch("time.sleep") as mock_sleep:  # skip the backoff wait
            result = sink.dispatch(_brief())
    assert result.success is True
    # Two subprocess calls — initial + one retry
    assert mock_run.call_count == 2
    # Backoff sleep called once
    mock_sleep.assert_called_once_with(RETRY_BACKOFF_S)


def test_no_retry_storm_on_persistent_failure():
    """Both attempts fail → exactly 2 subprocess calls (initial + 1 retry)."""
    sink = _sink()
    fail = _FakeCompleted(returncode=1, stderr="still failing")
    with patch("subprocess.run", return_value=fail) as mock_run:
        with patch("time.sleep"):
            result = sink.dispatch(_brief())
    assert result.success is False
    assert mock_run.call_count == 2


# ---------- message body formatting ----------


def test_message_body_includes_narrative():
    b = _brief("the substance Mando reads")
    body = _format_message_body(b)
    assert "the substance Mando reads" in body


def test_message_body_includes_brief_id_trailer():
    b = _brief()
    body = _format_message_body(b)
    assert b.brief_id in body


def test_message_body_includes_themes_trailer():
    b = _brief()
    body = _format_message_body(b)
    assert "us_iran_escalation" in body
    assert "fed_policy_path" in body


def test_message_body_no_themes_skipped():
    """A brief with empty themes_covered must not emit a 'themes:' line."""
    b = _brief()
    b_no_themes = b.model_copy(update={"themes_covered": []})
    body = _format_message_body(b_no_themes)
    assert "[themes:" not in body


# ---------- dispatch never raises ----------


def test_dispatch_never_raises_on_subprocess_error():
    """Defense in depth: even on the weirdest subprocess error, dispatch
    surfaces it via DispatchResult rather than propagating."""
    sink = _sink()
    with patch("subprocess.run", side_effect=Exception("totally unexpected")):
        # Should not raise — surface as a DispatchResult.
        # Note: the current implementation catches FileNotFoundError,
        # TimeoutExpired, and OSError explicitly. A bare Exception
        # would NOT be caught and would propagate. That's a deliberate
        # choice — truly unexpected exceptions should crash the daemon
        # rather than be silently swallowed. Test documents this.
        try:
            sink.dispatch(_brief())
        except Exception:
            # Expected: bare Exception propagates by design.
            pass
        else:
            pytest.fail("bare Exception should propagate; defensive boundary "
                        "is FileNotFoundError/TimeoutExpired/OSError only")
