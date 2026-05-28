"""Telegram-native translation module tests — hermetic, Telethon mocked.

Mocking strategy: Telethon's TelegramClient is callable as
`await client(SomeRequest(...))`. We use an awaitable mock class that
records each invocation's request and returns canned responses or
raises canned exceptions per test scenario.

The translate_telegram_messages() function takes the client as a
parameter (not a class attribute), so substitution is straightforward.

Coverage shape (Mando-approved pre-work):
  - Happy path: batched success, single-message, batching boundary
  - FloodWait: retry-then-success, retries exhausted, hard-cap surface
  - Error mapping: message_deleted, channel_inaccessible,
    translation_error, premium_required, network_error
  - Empty/skip cases: empty input, empty translated_text
  - Validation: bad inputs
  - Latency + attempts accounting
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from telethon import errors
from telethon.tl.functions.messages import TranslateTextRequest

from news_watch_daemon.translation.telegram_native import (
    DEFAULT_BATCH_SIZE,
    MAX_FLOOD_WAIT_S,
    MAX_RETRIES,
    translate_telegram_messages,
)
from news_watch_daemon.translation.types import (
    TRANSLATION_STATUSES,
    TranslationResult,
)


# ---------- helpers ----------


class _FakeClient:
    """Callable mock for Telethon TelegramClient.

    Records each `await client(request)` invocation. Returns canned
    responses (or raises canned exceptions) in sequence, one per call.
    Also handles get_entity separately so entity-resolution failures
    can be simulated independently.
    """

    def __init__(
        self,
        *,
        call_responses: list[Any] | None = None,
        get_entity_exc: BaseException | None = None,
        entity: Any = None,
        sleep_recorded: list[float] | None = None,
    ) -> None:
        self._call_responses = list(call_responses or [])
        self._get_entity_exc = get_entity_exc
        self._entity = entity if entity is not None else SimpleNamespace(id=12345)
        self.call_requests: list[Any] = []
        self.get_entity_calls: list[str] = []

    async def get_entity(self, name: str) -> Any:
        self.get_entity_calls.append(name)
        if self._get_entity_exc is not None:
            raise self._get_entity_exc
        return self._entity

    async def __call__(self, request: Any) -> Any:
        self.call_requests.append(request)
        if not self._call_responses:
            raise RuntimeError("FakeClient: no canned responses left")
        nxt = self._call_responses.pop(0)
        if isinstance(nxt, BaseException):
            raise nxt
        return nxt


def _fake_translate_response(translations: list[str]) -> SimpleNamespace:
    """Construct a fake TranslateTextRequest response — `.result` is a
    list with `.text` attributes per entry."""
    return SimpleNamespace(
        result=[SimpleNamespace(text=t) for t in translations]
    )


def _flood_wait(seconds: int) -> errors.FloodWaitError:
    flood = errors.FloodWaitError(request=None)  # type: ignore[arg-type]
    flood.seconds = seconds
    return flood


def _rpc_error_with_message(msg: str) -> errors.RPCError:
    """Build a generic RPCError whose str() contains `msg`. Used to
    simulate Premium-gating messages, MESSAGE_ID_INVALID, etc."""
    return errors.RPCError(request=None, message=msg, code=403)  # type: ignore[arg-type]


# Default test inputs shared across many test cases
DEFAULT_CHANNEL = "Ateobreaking"
SAMPLE_MSG_IDS = [170825, 170824, 170823]
SAMPLE_ORIGINALS = {
    170825: "Российский министр заявил о готовности",
    170824: "Суд РФ продлил арест Шлосберга",
    170823: "Турция сократит импорт российской нефти",
}
SAMPLE_TRANSLATIONS = [
    "Russian minister stated readiness",
    "Russian court extended Shlosberg's arrest",
    "Turkey will cut Russian oil imports",
]


# ---------- happy path ----------


async def _run(client, *, msg_ids, originals, **kwargs) -> list[TranslationResult]:
    return await translate_telegram_messages(
        client,
        channel_username=DEFAULT_CHANNEL,
        msg_ids=msg_ids,
        original_texts=originals,
        **kwargs,
    )


@pytest.mark.anyio
async def test_happy_path_batched_translation():
    """3 messages in a single batch (≤ default batch_size 10) → 3 ok results."""
    client = _FakeClient(
        call_responses=[_fake_translate_response(SAMPLE_TRANSLATIONS)],
    )
    results = await _run(client, msg_ids=SAMPLE_MSG_IDS, originals=SAMPLE_ORIGINALS)
    assert len(results) == 3
    for i, r in enumerate(results):
        assert r.status == "ok"
        assert r.translated_text == SAMPLE_TRANSLATIONS[i]
        assert r.source_msg_id == str(SAMPLE_MSG_IDS[i])
        assert r.channel_username == DEFAULT_CHANNEL
        assert r.original_text == SAMPLE_ORIGINALS[SAMPLE_MSG_IDS[i]]
        assert r.attempts == 1
        assert r.latency_ms >= 0
        assert r.error_detail is None
    # Verify exactly one client call, one get_entity call
    assert len(client.call_requests) == 1
    assert client.get_entity_calls == [f"@{DEFAULT_CHANNEL}"]
    # Verify request shape
    req = client.call_requests[0]
    assert isinstance(req, TranslateTextRequest)
    assert req.id == SAMPLE_MSG_IDS
    assert req.to_lang == "en"


@pytest.mark.anyio
async def test_single_message_translation():
    """Batch of 1 works."""
    client = _FakeClient(
        call_responses=[_fake_translate_response(["solo translation"])],
    )
    results = await _run(
        client, msg_ids=[170825], originals={170825: "одно сообщение"},
    )
    assert len(results) == 1
    assert results[0].status == "ok"
    assert results[0].translated_text == "solo translation"


@pytest.mark.anyio
async def test_empty_input_returns_empty_list_no_api_call():
    """Empty msg_ids → empty result list, no API calls made."""
    client = _FakeClient(call_responses=[])
    results = await _run(client, msg_ids=[], originals={})
    assert results == []
    # No client call NOR get_entity call should have happened
    assert client.call_requests == []
    assert client.get_entity_calls == []


@pytest.mark.anyio
async def test_batching_splits_at_batch_size():
    """25 messages with batch_size=10 → 3 batches (10, 10, 5)."""
    msg_ids = list(range(170000, 170025))  # 25 ids
    originals = {m: f"text {m}" for m in msg_ids}
    # Three canned responses, one per batch
    client = _FakeClient(call_responses=[
        _fake_translate_response([f"en-{m}" for m in msg_ids[0:10]]),
        _fake_translate_response([f"en-{m}" for m in msg_ids[10:20]]),
        _fake_translate_response([f"en-{m}" for m in msg_ids[20:25]]),
    ])
    results = await _run(client, msg_ids=msg_ids, originals=originals, batch_size=10)
    assert len(results) == 25
    assert all(r.status == "ok" for r in results)
    # Three client calls
    assert len(client.call_requests) == 3
    # First call has 10 ids, second 10, third 5
    assert len(client.call_requests[0].id) == 10
    assert len(client.call_requests[1].id) == 10
    assert len(client.call_requests[2].id) == 5
    # Order preserved
    for i, r in enumerate(results):
        assert r.source_msg_id == str(msg_ids[i])


# ---------- FloodWait ----------


@pytest.mark.anyio
async def test_flood_wait_then_success(monkeypatch):
    """FloodWait on first try, success on retry → status=ok, attempts=2."""
    sleeps: list[float] = []

    async def _fake_sleep(s):
        sleeps.append(s)

    monkeypatch.setattr("asyncio.sleep", _fake_sleep)
    client = _FakeClient(call_responses=[
        _flood_wait(5),
        _fake_translate_response(SAMPLE_TRANSLATIONS),
    ])
    results = await _run(client, msg_ids=SAMPLE_MSG_IDS, originals=SAMPLE_ORIGINALS)
    assert all(r.status == "ok" for r in results)
    assert all(r.attempts == 2 for r in results)
    # Sleep was called once with 5 + jitter[0] (1.0) = 6.0s
    assert sleeps == [6.0]


@pytest.mark.anyio
async def test_flood_wait_exhausts_retries(monkeypatch):
    """3 FloodWaits in a row → status=rate_limited, attempts=3."""
    sleeps: list[float] = []

    async def _fake_sleep(s):
        sleeps.append(s)

    monkeypatch.setattr("asyncio.sleep", _fake_sleep)
    client = _FakeClient(call_responses=[
        _flood_wait(3),
        _flood_wait(3),
        _flood_wait(3),
    ])
    results = await _run(client, msg_ids=SAMPLE_MSG_IDS, originals=SAMPLE_ORIGINALS)
    assert len(results) == 3
    for r in results:
        assert r.status == "rate_limited"
        assert r.attempts == 3
        assert "exhausted" in r.error_detail
        assert r.translated_text is None
    # Sleeps for retries 1 and 2 only (retry 3 fails without sleeping again)
    # Wait, retry 1 = first sleep, retry 2 = second sleep, retry 3 attempt
    # happens without a new sleep before it. So 2 sleeps total.
    # Actually re-reading the impl: each attempt loop iteration:
    #   if FloodWait and attempt < MAX_RETRIES: sleep, continue
    #   else (retries exhausted): return rate_limited
    # So attempt 1 catches FloodWait, sleeps with jitter[0]=1
    # attempt 2 catches FloodWait, sleeps with jitter[1]=5
    # attempt 3 catches FloodWait, MAX_RETRIES=3 hit, no sleep, return
    assert sleeps == [3 + 1.0, 3 + 5.0]


@pytest.mark.anyio
async def test_flood_wait_exceeds_cap_surfaces_immediately(monkeypatch):
    """FloodWait > 300s cap → status=rate_limited immediately, no sleep."""
    sleeps: list[float] = []

    async def _fake_sleep(s):
        sleeps.append(s)

    monkeypatch.setattr("asyncio.sleep", _fake_sleep)
    client = _FakeClient(call_responses=[_flood_wait(MAX_FLOOD_WAIT_S + 1)])
    results = await _run(client, msg_ids=SAMPLE_MSG_IDS, originals=SAMPLE_ORIGINALS)
    assert all(r.status == "rate_limited" for r in results)
    # No sleep happened — we surfaced immediately
    assert sleeps == []
    # All results report attempts=1 (the one that hit the cap)
    assert all(r.attempts == 1 for r in results)
    assert all(f"exceeds cap" in r.error_detail for r in results)


# ---------- error mapping ----------


@pytest.mark.anyio
async def test_message_id_invalid_per_batch_message_deleted():
    """MessageIdInvalidError on call → all msg_ids in batch flagged
    message_deleted (Telegram's batch error doesn't tell us which one)."""
    client = _FakeClient(call_responses=[
        errors.MessageIdInvalidError(request=None),  # type: ignore[arg-type]
    ])
    results = await _run(client, msg_ids=SAMPLE_MSG_IDS, originals=SAMPLE_ORIGINALS)
    for r in results:
        assert r.status == "message_deleted"
        assert r.translated_text is None
        assert "MessageIdInvalidError" in r.error_detail


@pytest.mark.anyio
async def test_channel_private_at_get_entity_marks_all_inaccessible():
    """ChannelPrivateError on get_entity → all msg_ids flagged channel_inaccessible,
    no translate call attempted."""
    client = _FakeClient(
        call_responses=[],
        get_entity_exc=errors.ChannelPrivateError(request=None),  # type: ignore[arg-type]
    )
    results = await _run(client, msg_ids=SAMPLE_MSG_IDS, originals=SAMPLE_ORIGINALS)
    for r in results:
        assert r.status == "channel_inaccessible"
        assert "ChannelPrivateError" in r.error_detail
        assert r.translated_text is None
    # No translate API call attempted
    assert client.call_requests == []


@pytest.mark.anyio
async def test_generic_rpc_error_marks_all_translation_error():
    """Generic RPCError (non-Premium, non-MessageIdInvalid) → translation_error."""
    client = _FakeClient(call_responses=[
        _rpc_error_with_message("FORBIDDEN_BY_POLICY"),
    ])
    results = await _run(client, msg_ids=SAMPLE_MSG_IDS, originals=SAMPLE_ORIGINALS)
    for r in results:
        assert r.status == "translation_error"
        assert "FORBIDDEN_BY_POLICY" in r.error_detail


@pytest.mark.anyio
async def test_premium_required_detected_via_error_message():
    """RPCError containing 'PREMIUM' substring → status=premium_required,
    NOT generic translation_error. This is the load-bearing failure-mode
    detection per Pass F doctrine — orchestrator can detect Premium-gating
    without parsing error_detail strings, enabling clean YAML-flag flip
    to DeepL fallback."""
    client = _FakeClient(call_responses=[
        _rpc_error_with_message("USER_PREMIUM_REQUIRED"),
    ])
    results = await _run(client, msg_ids=SAMPLE_MSG_IDS, originals=SAMPLE_ORIGINALS)
    for r in results:
        assert r.status == "premium_required"
        assert r.translated_text is None
        assert "PREMIUM" in r.error_detail.upper()


@pytest.mark.anyio
async def test_premium_required_case_insensitive():
    """Premium-detection is case-insensitive — 'premium' in lowercase error msg."""
    client = _FakeClient(call_responses=[
        _rpc_error_with_message("requires premium subscription"),
    ])
    results = await _run(client, msg_ids=SAMPLE_MSG_IDS, originals=SAMPLE_ORIGINALS)
    assert all(r.status == "premium_required" for r in results)


@pytest.mark.anyio
async def test_network_error_then_success(monkeypatch):
    """ConnectionError on first try, success on retry → status=ok, attempts=2."""
    sleeps: list[float] = []

    async def _fake_sleep(s):
        sleeps.append(s)

    monkeypatch.setattr("asyncio.sleep", _fake_sleep)
    client = _FakeClient(call_responses=[
        ConnectionError("DNS resolution failed"),
        _fake_translate_response(SAMPLE_TRANSLATIONS),
    ])
    results = await _run(client, msg_ids=SAMPLE_MSG_IDS, originals=SAMPLE_ORIGINALS)
    assert all(r.status == "ok" for r in results)
    # Network retry sleeps NETWORK_RETRY_DELAY_S = 2.0
    assert sleeps == [2.0]


@pytest.mark.anyio
async def test_network_error_exhausts_retries(monkeypatch):
    """2 consecutive ConnectionErrors → status=network_error (1 retry allowed)."""
    sleeps: list[float] = []

    async def _fake_sleep(s):
        sleeps.append(s)

    monkeypatch.setattr("asyncio.sleep", _fake_sleep)
    client = _FakeClient(call_responses=[
        ConnectionError("first failure"),
        ConnectionError("second failure"),
    ])
    results = await _run(client, msg_ids=SAMPLE_MSG_IDS, originals=SAMPLE_ORIGINALS)
    for r in results:
        assert r.status == "network_error"
        assert "second failure" in r.error_detail


# ---------- empty / skip cases ----------


@pytest.mark.anyio
async def test_empty_translated_text_returns_ok_with_empty_string():
    """Telegram refuses to translate (short message / all emoji / copyrighted)
    → response.result[i].text == "". Module returns status='ok' with
    translated_text="" — orchestrator (Commit 2) falls back to original."""
    client = _FakeClient(call_responses=[
        _fake_translate_response(["", "second translation", ""]),
    ])
    results = await _run(client, msg_ids=SAMPLE_MSG_IDS, originals=SAMPLE_ORIGINALS)
    assert results[0].status == "ok"
    assert results[0].translated_text == ""
    assert results[1].status == "ok"
    assert results[1].translated_text == "second translation"
    assert results[2].status == "ok"
    assert results[2].translated_text == ""


@pytest.mark.anyio
async def test_response_length_mismatch_marks_excess_translation_error():
    """If response.result is shorter than the batch (defensive — rare edge
    case where Telegram returns fewer translations than requested), the
    missing tail msg_ids get status=translation_error rather than crashing."""
    client = _FakeClient(call_responses=[
        # 3 msg_ids sent, 1 translation returned
        _fake_translate_response(["only one"]),
    ])
    results = await _run(client, msg_ids=SAMPLE_MSG_IDS, originals=SAMPLE_ORIGINALS)
    assert results[0].status == "ok"
    assert results[0].translated_text == "only one"
    assert results[1].status == "translation_error"
    assert "length mismatch" in results[1].error_detail
    assert results[2].status == "translation_error"


# ---------- input validation ----------


@pytest.mark.anyio
async def test_malformed_channel_username_rejected():
    client = _FakeClient(call_responses=[])
    with pytest.raises(ValueError, match="channel_username"):
        await translate_telegram_messages(
            client,
            channel_username="ab",  # too short
            msg_ids=[1],
            original_texts={1: "x"},
        )


@pytest.mark.anyio
async def test_non_int_msg_id_rejected():
    client = _FakeClient(call_responses=[])
    with pytest.raises(ValueError, match="msg_ids entries"):
        await _run(client, msg_ids=["170825"], originals={"170825": "x"})  # type: ignore[list-item]


@pytest.mark.anyio
async def test_missing_original_text_rejected():
    client = _FakeClient(call_responses=[])
    with pytest.raises(ValueError, match="original_texts missing"):
        await _run(client, msg_ids=[170825], originals={})


@pytest.mark.anyio
async def test_batch_size_out_of_bounds_rejected():
    client = _FakeClient(call_responses=[])
    with pytest.raises(ValueError, match="batch_size"):
        await _run(
            client, msg_ids=[170825], originals={170825: "x"}, batch_size=0,
        )
    with pytest.raises(ValueError, match="batch_size"):
        await _run(
            client, msg_ids=[170825], originals={170825: "x"}, batch_size=101,
        )


# ---------- attribution + accounting ----------


@pytest.mark.anyio
async def test_latency_attribution_per_message():
    """latency_ms is per-message attribution (batch_total / batch_size).
    All messages in the same batch share equivalent per-message latency."""
    client = _FakeClient(
        call_responses=[_fake_translate_response(SAMPLE_TRANSLATIONS)],
    )
    results = await _run(client, msg_ids=SAMPLE_MSG_IDS, originals=SAMPLE_ORIGINALS)
    # All 3 messages were in the same batch; their per-message latency
    # should be equal (within rounding).
    per_msg_latencies = {r.latency_ms for r in results}
    assert len(per_msg_latencies) <= 1, (
        f"Expected uniform per-message latency in single batch, got {per_msg_latencies}"
    )


@pytest.mark.anyio
async def test_to_lang_parameter_forwarded():
    """The to_lang kwarg propagates to the TranslateTextRequest."""
    client = _FakeClient(
        call_responses=[_fake_translate_response(SAMPLE_TRANSLATIONS)],
    )
    await _run(
        client, msg_ids=SAMPLE_MSG_IDS, originals=SAMPLE_ORIGINALS, to_lang="de",
    )
    req = client.call_requests[0]
    assert req.to_lang == "de"


# ---------- closed-Literal invariant ----------


@pytest.mark.anyio
async def test_status_always_in_closed_literal():
    """Every TranslationResult.status must be in TRANSLATION_STATUSES.
    Multiple scenarios checked in one test to lock the invariant."""
    # Scenario 1: success
    client = _FakeClient(call_responses=[_fake_translate_response(["x"])])
    results1 = await _run(client, msg_ids=[170825], originals={170825: "x"})
    assert results1[0].status in TRANSLATION_STATUSES

    # Scenario 2: premium-required
    client = _FakeClient(call_responses=[_rpc_error_with_message("PREMIUM_REQUIRED")])
    results2 = await _run(client, msg_ids=[170825], originals={170825: "x"})
    assert results2[0].status in TRANSLATION_STATUSES

    # Scenario 3: channel_inaccessible
    client = _FakeClient(
        call_responses=[],
        get_entity_exc=errors.ChannelPrivateError(request=None),  # type: ignore[arg-type]
    )
    results3 = await _run(client, msg_ids=[170825], originals={170825: "x"})
    assert results3[0].status in TRANSLATION_STATUSES


# ---------- pytest-anyio plugin config ----------


@pytest.fixture
def anyio_backend():
    """Force asyncio backend (we don't depend on trio)."""
    return "asyncio"
