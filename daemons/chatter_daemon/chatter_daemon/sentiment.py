"""Haiku stance classification for Reddit (Order 6) — the daemon's ONE LLM call.

Gated ABOVE the mention floor: only tickers with `mention_count >= floor` reach
Haiku; tail tickers never do (their `sentiment.method` stays "none"). One batched
call per scan keeps cost trivial; the system prompt + schema are stable and
prompt-cached (`cache_control: ephemeral` — a no-op below Haiku's 4096-token
minimum, harmless above it).

Cost telemetry (token usage) is accumulated into the caller's `CostTelemetry`
IMMEDIATELY after the call returns — before any record is built or persisted
(doctrine #8). The cost object is mutated in place, so even when this pass fails
loud the caller still holds the tokens already spent.

Fail loud: any API / parse / truncation failure RAISES `SentimentError`; the Reddit
plugin turns it into a `SourceResult(error=...)` so the orchestrator isolates the
source. We never fabricate a `neutral`.

The Haiku model id is pinned by the caller (config), verified live via the
claude-api skill at build time — not from memory. The request shape
(`output_config.format` json_schema, `usage` token capture, ephemeral system cache)
matches the claude-api skill's current Haiku-tier guidance.
"""

from __future__ import annotations

import json
from typing import Any

from .schema import CostTelemetry

# Bounded output ceiling. A batch of Reddit posts classifies well under this; a
# response that still hits it is caught via stop_reason and fails the batch loudly
# rather than parsing a truncated array and silently dropping tickers.
_MAX_TOKENS = 8192

_STANCE_VALUES = ("bullish", "bearish", "neutral")

_SYSTEM_PROMPT = (
    "You are a stance classifier for retail-investor posts from Reddit (subreddits "
    "like r/wallstreetbets, r/stocks, r/investing, r/options). You are given a JSON "
    "array of posts. Each post has a string `post_id`, the post `text`, and a "
    "`tickers` array of US-equity symbols that were detected in that post.\n\n"
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
        raise SentimentError("ANTHROPIC_API_KEY is empty; Reddit sentiment cannot run")
    try:
        import anthropic
    except ImportError as exc:
        raise SentimentError(
            "the `anthropic` package is not installed; Reddit sentiment cannot run "
            "(`pip install anthropic`)"
        ) from exc
    return anthropic.Anthropic(api_key=api_key)


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
