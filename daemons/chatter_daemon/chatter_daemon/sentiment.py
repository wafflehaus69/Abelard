"""Haiku stance classification for StockTwits message bodies (Order 6/9) — the
daemon's ONE LLM call.

Gated ABOVE the sentiment floor: only tickers whose stream has `messages >= floor`
reach Haiku; thinner tickers fall back to the native tag read (`method="native"`).
One batched call per ticker classifies the full body set (the native tags cover only
~40% of messages; Haiku gives full coverage). The system prompt + schema are stable
and prompt-cached (`cache_control: ephemeral` — a no-op below Haiku's 4096-token
minimum, harmless above it).

Cost telemetry (token usage) is accumulated into the caller's `CostTelemetry`
IMMEDIATELY after the call returns — before any record is built or persisted
(doctrine #8). The cost object is mutated in place, so even when this pass fails
loud the caller still holds the tokens already spent.

Fail loud: any API / parse / truncation failure RAISES `SentimentError`; the StockTwits
source catches it PER TICKER and degrades to that ticker's native tag read (a logged
warning, never a fabricated `neutral`), so one ticker's Haiku failure never sinks the
rest of the scan.

The Haiku model id is pinned by the caller (config), verified live via the
claude-api skill at build time — not from memory. The request shape
(`output_config.format` json_schema, `usage` token capture, ephemeral system cache)
matches the claude-api skill's current Haiku-tier guidance.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from .schema import CostTelemetry

# Bounded output ceiling. A ticker's StockTwits message batch classifies well under
# this; a response that still hits it is caught via stop_reason and fails the batch
# loudly rather than parsing a truncated array and silently dropping classifications.
_MAX_TOKENS = 8192

_STANCE_VALUES = ("bullish", "bearish", "neutral")

_SYSTEM_PROMPT = (
    "You are a stance classifier for retail-investor messages from StockTwits (the "
    "cashtag-driven equity social network). You are given a JSON array of messages. "
    "Each item has a string `post_id`, the message `text`, and a `tickers` array of "
    "US-equity symbols the message is about.\n\n"
    "For every (post, ticker) pair, classify the AUTHOR'S STANCE toward that "
    "specific ticker as exactly one of: bullish, bearish, neutral.\n\n"
    "Rules:\n"
    "- Judge the author's directional sentiment about the company/stock, not the "
    "overall mood of the post.\n"
    "- A post naming two tickers gets one stance PER ticker — attribute correctly; "
    "do not assign one blanket label to both.\n"
    "- bullish = expects the price to rise / positive view. bearish = expects it to "
    "fall / negative view. neutral = mention without a directional view, or "
    "genuinely mixed.\n"
    "- Return one classification object for every (post, ticker) pair you were "
    "given, and only those pairs.\n"
    "- Output strictly matches the provided JSON schema. No prose."
)

_OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "classifications": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "post_id": {"type": "string"},
                    "ticker": {"type": "string"},
                    "stance": {"type": "string", "enum": list(_STANCE_VALUES)},
                },
                "required": ["post_id", "ticker", "stance"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["classifications"],
    "additionalProperties": False,
}


class SentimentError(RuntimeError):
    """A Haiku call / parse / truncation failure. Fails the sentiment pass loudly."""


def build_anthropic_client(api_key: str) -> Any:
    """Construct the production Anthropic client. Lazy-imports the SDK so the test
    path (which injects a fake) never needs `anthropic` installed."""
    if not api_key:
        raise SentimentError("ANTHROPIC_API_KEY is empty; StockTwits sentiment cannot run")
    try:
        import anthropic
    except ImportError as exc:
        raise SentimentError(
            "the `anthropic` package is not installed; StockTwits sentiment cannot run "
            "(`pip install anthropic`)"
        ) from exc
    return anthropic.Anthropic(api_key=api_key)


class AnthropicProvider:
    """Lazily builds + caches an Anthropic client from an API key; returns None when
    there's no key (the caller degrades to native/none). Shared by the Haiku sources
    (StockTwits bodies, /smg/ posts) so the lazy-build-and-degrade lives in one place.
    Inject a ready `client` (a fake) in tests to bypass the build entirely."""

    def __init__(
        self, *, api_key: str | None = None, client: Any | None = None, logger=None
    ) -> None:
        self._client = client
        self._api_key = api_key
        self._log = logger or logging.getLogger("chatter_daemon")

    def get(self) -> Any | None:
        if self._client is not None:
            return self._client
        if not self._api_key:
            return None
        try:
            self._client = build_anthropic_client(self._api_key)
        except SentimentError as exc:
            self._log.warning("anthropic client unavailable: %s", exc)
            return None
        return self._client


def classify_stance(
    *,
    posts: list[dict[str, Any]],
    client: Any,
    model: str,
    cost: CostTelemetry,
) -> dict[str, dict[str, int]]:
    """Classify stance for the (post, ticker) pairs and tally per ticker.

    `posts` is `[{"post_id": str, "text": str, "tickers": [SYMBOL, ...]}, ...]`,
    already restricted by the caller to above-floor tickers. Returns
    `{ticker: {bullish: n, bearish: n, neutral: n}}` for tickers that received at
    least one classification. Accumulates token usage into `cost` (in place) before
    returning. Raises `SentimentError` on any failure — the cost already captured
    survives in the caller's object.
    """
    if not posts:
        return {}

    requested_pairs = {(p["post_id"], t) for p in posts for t in p["tickers"]}
    classifications = _call_haiku(client, model, posts, cost)

    tallies: dict[str, dict[str, int]] = {}
    for item in classifications:
        if not isinstance(item, dict):
            continue
        try:
            post_id = str(item["post_id"])
            ticker = str(item["ticker"]).upper()
            stance = str(item["stance"]).lower()
        except (KeyError, TypeError, ValueError):
            continue
        if stance not in _STANCE_VALUES:
            continue
        if (post_id, ticker) not in requested_pairs:
            continue
        bucket = tallies.setdefault(ticker, {})
        bucket[stance] = bucket.get(stance, 0) + 1
    return tallies


def _call_haiku(
    client: Any, model: str, posts: list[dict[str, Any]], cost: CostTelemetry
) -> list[dict[str, Any]]:
    """One batched classification call. Accumulates usage into `cost` immediately
    after the response returns, BEFORE any validation that might raise — so the
    token spend is captured even when the batch then fails truncation/parse."""
    system = [
        {
            "type": "text",
            "text": _SYSTEM_PROMPT,
            "cache_control": {"type": "ephemeral"},
        }
    ]
    user_text = (
        "Classify the stance for every (post, ticker) pair in this array:\n"
        + json.dumps(posts, ensure_ascii=False, separators=(",", ":"))
    )
    try:
        response = client.messages.create(
            model=model,
            max_tokens=_MAX_TOKENS,
            system=system,
            messages=[{"role": "user", "content": user_text}],
            output_config={"format": {"type": "json_schema", "schema": _OUTPUT_SCHEMA}},
        )
    except Exception as exc:  # SDK / transport error — fail the pass, do not fabricate.
        raise SentimentError(f"Haiku call failed: {exc}") from exc

    # Cost FIRST: capture before any guard below can raise (doctrine #8).
    cost.haiku_calls += 1
    _accumulate_usage(getattr(response, "usage", None), cost)

    if getattr(response, "stop_reason", None) == "max_tokens":
        raise SentimentError(
            "Haiku response truncated (stop_reason=max_tokens); batch failed"
        )

    text = _first_text(response)
    if not text:
        raise SentimentError("Haiku returned no text content")
    try:
        parsed = json.loads(text)
    except ValueError as exc:
        raise SentimentError(f"Haiku returned unparseable JSON: {exc}") from exc

    classifications = parsed.get("classifications")
    if not isinstance(classifications, list):
        raise SentimentError("Haiku response missing `classifications` array")
    return classifications


def _accumulate_usage(usage: Any, cost: CostTelemetry) -> None:
    if usage is None:
        return
    cost.input_tokens += int(getattr(usage, "input_tokens", 0) or 0)
    cost.output_tokens += int(getattr(usage, "output_tokens", 0) or 0)
    cost.cache_read_input_tokens += int(getattr(usage, "cache_read_input_tokens", 0) or 0)
    cost.cache_creation_input_tokens += int(
        getattr(usage, "cache_creation_input_tokens", 0) or 0
    )


def _first_text(response: Any) -> str:
    for block in getattr(response, "content", []) or []:
        if getattr(block, "type", None) == "text":
            return (getattr(block, "text", "") or "").strip()
    return ""


# --- Order 15: named-news one-paragraph summary (factual cause, no stance) -------------

_SUMMARY_SYSTEM = (
    "You summarize company news for a financial-attention sensor. Given a US-equity "
    "ticker, its company name, and recent headlines that name it, write ONE short "
    "paragraph stating factually what the news is about for THIS company.\n"
    "Rules:\n"
    "- Factual and neutral — report what happened or was reported.\n"
    "- NO price prediction, NO buy/sell/hold language, NO investment advice.\n"
    "- Focus on this company specifically; ignore any headline that is really about a "
    "different company.\n"
    "- If the headlines are thin, a brief factual sentence is fine — do not pad.\n"
    "- One paragraph. No preamble, no bullet points, no headers."
)
_SUMMARY_MAX_TOKENS = 512


def summarize_news(*, titles, ticker, company, client, model, cost) -> str:
    """One Haiku call -> a one-paragraph factual summary of what the NAMED news says about
    THIS company. No stance, no price view (Abelard judges). Accumulates token usage into
    `cost` immediately after the response (doctrine #8). Raises SentimentError on a
    transport / empty-response failure — the caller degrades to None + a warning; a
    max_tokens truncation is kept (a clipped paragraph is still usable)."""
    if not titles:
        return ""
    user = (
        f"Ticker: {ticker}\nCompany: {company or ticker}\n\nHeadlines:\n"
        + "\n".join(f"- {t}" for t in titles)
    )
    try:
        response = client.messages.create(
            model=model,
            max_tokens=_SUMMARY_MAX_TOKENS,
            system=[{"type": "text", "text": _SUMMARY_SYSTEM, "cache_control": {"type": "ephemeral"}}],
            messages=[{"role": "user", "content": user}],
        )
    except Exception as exc:  # SDK / transport — degrade to None, never fabricate.
        raise SentimentError(f"news summary call failed: {exc}") from exc
    cost.haiku_calls += 1
    _accumulate_usage(getattr(response, "usage", None), cost)
    text = _first_text(response)
    if not text:
        raise SentimentError("news summary returned no text")
    return text


def summary_cost_usd(cost) -> float:
    """Haiku-4.5 USD estimate from accumulated tokens (~$1/M input, ~$5/M output); all
    input-side counted at the input rate — slightly conservative for the cost-cap guard."""
    in_tok = cost.input_tokens + cost.cache_read_input_tokens + cost.cache_creation_input_tokens
    return in_tok / 1_000_000 * 1.0 + cost.output_tokens / 1_000_000 * 5.0
