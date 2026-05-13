"""TelegramBotSink tests — urllib fully mocked, hermetic."""

from __future__ import annotations

import io
import json
import typing
import urllib.error
from unittest.mock import patch

import pytest

from news_watch_daemon.alert.sink import DispatchResult
from news_watch_daemon.alert.telegram_bot_sink import (
    CHANNEL_NAME,
    MAX_MESSAGE_CHARS,
    RETRY_BACKOFF_S,
    TelegramBotSink,
    _assert_credentials_present,
    _format_message_body,
)
from news_watch_daemon.synthesize.brief import (
    Brief,
    Dispatch,
    SynthesisMetadata,
    Trigger,
    TriggerWindow,
)


# ---------- module-scope sleep patch (same pattern as SignalSink) ----------


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch):
    monkeypatch.setattr(
        "news_watch_daemon.alert.telegram_bot_sink.time.sleep",
        lambda _seconds: None,
    )


# ---------- helpers ----------


def _brief(narrative: str = "Brief body for test.") -> Brief:
    return Brief(
        brief_id="nwd-2026-05-13T14-32-08Z-aaaaaaaa",
        generated_at="2026-05-13T14:32:08Z",
        trigger=Trigger(type="event", reason="t",
                        window=TriggerWindow(since="a", until="b")),
        themes_covered=["us_iran_escalation"],
        narrative=narrative,
        dispatch=Dispatch(alerted=False),
        synthesis_metadata=SynthesisMetadata(
            model_used="claude-sonnet-4-7", theses_doc_available=False,
        ),
    )


def _sink(bot_token: str = "fake-token", chat_id: str = "fake-chat",
          timeout_s: float = 30.0, api_base: str | None = None) -> TelegramBotSink:
    return TelegramBotSink(
        bot_token=bot_token, chat_id=chat_id, timeout_s=timeout_s,
        api_base=api_base or "https://api.telegram.org",
    )


class _FakeResponse:
    """Mimics what urlopen returns."""

    def __init__(self, body: str, status: int = 200):
        self._body = body.encode("utf-8")
        self.status = status

    def read(self) -> bytes:
        return self._body

    def getcode(self) -> int:
        return self.status

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, *args) -> bool:
        return False


def _telegram_ok(message_id: int = 1) -> _FakeResponse:
    return _FakeResponse(json.dumps({"ok": True, "result": {"message_id": message_id}}))


# ---------- credentials gate ----------


def test_construction_refuses_empty_bot_token():
    with pytest.raises(Exception, match="bot_token"):
        TelegramBotSink(bot_token="", chat_id="x", timeout_s=30.0)


def test_construction_refuses_whitespace_bot_token():
    with pytest.raises(Exception, match="bot_token"):
        TelegramBotSink(bot_token="   ", chat_id="x", timeout_s=30.0)


def test_construction_refuses_empty_chat_id():
    with pytest.raises(Exception, match="chat_id"):
        TelegramBotSink(bot_token="t", chat_id="", timeout_s=30.0)


def test_credentials_validation_passes_on_both_present():
    _assert_credentials_present("t", "c")  # no exception


# ---------- Mando's forward-note: channel literal alignment ----------


def test_channel_name_matches_brief_schema_literal():
    """DispatchResult.channel == 'telegram_bot' must be a literal member
    of Brief.dispatch.channel's Optional[Literal[...]]."""
    sink = _sink()
    assert sink.channel_name == CHANNEL_NAME
    assert CHANNEL_NAME == "telegram_bot"
    dispatch_fields = Dispatch.model_fields
    channel_annotation = dispatch_fields["channel"].annotation
    type_args = typing.get_args(channel_annotation)
    literal_values: set[str] = set()
    for arg in type_args:
        if typing.get_origin(arg) is typing.Literal:
            literal_values.update(typing.get_args(arg))
    assert CHANNEL_NAME in literal_values, (
        f"Brief.dispatch.channel literal members: {literal_values}"
    )


def test_successful_dispatch_returns_channel_telegram_bot_literal():
    sink = _sink()
    with patch("urllib.request.urlopen", return_value=_telegram_ok()):
        result = sink.dispatch(_brief())
    assert result.channel == "telegram_bot"
    assert result.channel == CHANNEL_NAME


# ---------- happy path ----------


def test_happy_path_returns_success():
    sink = _sink()
    with patch("urllib.request.urlopen", return_value=_telegram_ok()) as mock:
        result = sink.dispatch(_brief())
    assert result.success is True
    assert result.error is None
    assert mock.call_count == 1


def test_post_url_includes_bot_token_path():
    """The bot_token appears in the URL path /bot{token}/sendMessage."""
    sink = _sink(bot_token="my-token-xyz")
    captured = {}

    def _capture(req, timeout):  # noqa: ARG001
        captured["url"] = req.full_url
        return _telegram_ok()

    with patch("urllib.request.urlopen", side_effect=_capture):
        sink.dispatch(_brief())
    assert "/botmy-token-xyz/sendMessage" in captured["url"]


def test_post_body_includes_chat_id_and_text():
    sink = _sink(chat_id="-100123456789")
    captured = {}

    def _capture(req, timeout):  # noqa: ARG001
        captured["body"] = req.data.decode("utf-8")
        return _telegram_ok()

    with patch("urllib.request.urlopen", side_effect=_capture):
        sink.dispatch(_brief("hello signal world"))
    assert "chat_id=-100123456789" in captured["body"]
    assert "hello+signal+world" in captured["body"] or \
           "hello%20signal%20world" in captured["body"]


# ---------- API error paths ----------


def test_api_returns_ok_false_surfaces_description():
    """Bot API returns 200 with ok=false → DispatchResult(success=False)."""
    sink = _sink()
    api_err = _FakeResponse(json.dumps({
        "ok": False,
        "description": "Bad Request: chat not found",
        "error_code": 400,
    }))
    with patch("urllib.request.urlopen", return_value=api_err):
        result = sink.dispatch(_brief())
    assert result.success is False
    assert "chat not found" in result.error


def test_http_4xx_returns_failure():
    """HTTPError for 4xx → DispatchResult(success=False)."""
    sink = _sink()
    err = urllib.error.HTTPError(
        url="x", code=401, msg="Unauthorized",
        hdrs=None, fp=io.BytesIO(b'{"ok":false,"description":"Unauthorized"}'),
    )
    with patch("urllib.request.urlopen", side_effect=err):
        result = sink.dispatch(_brief())
    assert result.success is False
    assert "http_401" in result.error


def test_http_5xx_returns_failure():
    sink = _sink()
    err = urllib.error.HTTPError(
        url="x", code=503, msg="Service Unavailable",
        hdrs=None, fp=io.BytesIO(b""),
    )
    with patch("urllib.request.urlopen", side_effect=err):
        result = sink.dispatch(_brief())
    assert result.success is False
    assert "http_503" in result.error


def test_network_error_returns_failure():
    sink = _sink()
    with patch("urllib.request.urlopen",
               side_effect=urllib.error.URLError("DNS resolution failed")):
        result = sink.dispatch(_brief())
    assert result.success is False
    assert "URLError" in result.error


def test_timeout_returns_failure():
    sink = _sink()
    with patch("urllib.request.urlopen", side_effect=TimeoutError("timed out")):
        result = sink.dispatch(_brief())
    assert result.success is False
    assert "TimeoutError" in result.error


def test_malformed_json_response_returns_failure():
    sink = _sink()
    with patch("urllib.request.urlopen", return_value=_FakeResponse("<html>not json</html>")):
        result = sink.dispatch(_brief())
    assert result.success is False
    assert "malformed JSON" in result.error


# ---------- single retry ----------


def test_single_retry_on_transient_failure_succeeds():
    sink = _sink()
    transient = _FakeResponse(json.dumps({"ok": False, "description": "rate limited"}))
    with patch("urllib.request.urlopen",
               side_effect=[transient, _telegram_ok()]) as mock:
        result = sink.dispatch(_brief())
    assert result.success is True
    assert mock.call_count == 2


def test_no_retry_storm_on_persistent_failure():
    sink = _sink()
    err_resp = _FakeResponse(json.dumps({"ok": False, "description": "persistent"}))
    with patch("urllib.request.urlopen", return_value=err_resp) as mock:
        result = sink.dispatch(_brief())
    assert result.success is False
    assert mock.call_count == 2  # initial + 1 retry only


# ---------- message body formatting ----------


def test_message_body_short_narrative_passes_through():
    body = _format_message_body(_brief("the substance"))
    assert "the substance" in body
    assert "[brief_id:" in body


def test_message_body_truncates_long_narrative():
    long = "x" * (MAX_MESSAGE_CHARS + 1000)
    body = _format_message_body(_brief(long))
    assert len(body) <= MAX_MESSAGE_CHARS
    assert "[truncated]" in body
    assert "[brief_id:" in body  # trailer preserved


def test_message_body_includes_themes_trailer():
    body = _format_message_body(_brief())
    assert "us_iran_escalation" in body


def test_message_body_empty_themes_omits_themes_line():
    b = _brief()
    b_empty = b.model_copy(update={"themes_covered": []})
    body = _format_message_body(b_empty)
    assert "[themes:" not in body


# ---------- isolation from Pass B ----------


def test_module_imports_no_telethon():
    """Sanity check at runtime: telegram_bot_sink doesn't pull in telethon."""
    import news_watch_daemon.alert.telegram_bot_sink as mod
    # No `telethon` attribute reachable; the import statement check is in
    # the readonly grep test.
    assert "telethon" not in dir(mod)
