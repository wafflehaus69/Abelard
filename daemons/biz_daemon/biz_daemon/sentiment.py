"""Haiku per-post-per-ticker stance classification + aggregation.

Runs only on ATTENTION-tier tickers (mention_count >= N). Tail tickers never
reach Haiku and carry `sentiment: null`.

One (or few) batched Haiku call per scrape keeps cost trivial. The system
prompt + schema are stable and prompt-cached; only the per-scrape posts vary.

Fail loud: an API/parse error fails the sentiment pass and is surfaced as a
structured error per ticker — we never fabricate a `neutral` or a default read.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

from .config import HAIKU_MODEL_ID

_log = logging.getLogger("biz_daemon.sentiment")

# Safe output ceiling. A bounded batch of posts classifies well under this; the
# headroom (vs the old 1024) plus the per-batch cap below removes the
# truncation cliff. A response that still hits the ceiling is caught via
# stop_reason and fails the batch loudly rather than dropping tickers.
_MAX_TOKENS = 8192

# Max ATTENTION tickers per Haiku call. The attention set is chunked so no
# single call carries an unbounded batch of posts/pairs.
ATTENTION_BATCH_SIZE = 8

_STANCE_VALUES = ("bullish", "bearish", "neutral")

_SYSTEM_PROMPT = (
    "You are a stance classifier for posts from 4chan's /biz/ Stock Market "
    "General. You are given a JSON array of posts. Each post has an integer "
    "`post_id`, the post `text`, and a `tickers` array of US-equity symbols "
    "that were detected in that post.\n\n"
    "For every (post, ticker) pair, classify the AUTHOR'S STANCE toward that "
    "specific ticker as exactly one of: bullish, bearish, neutral.\n\n"
    "Rules:\n"
    "- Judge the author's directional sentiment about the company/stock, not "
    "the overall mood of the post.\n"
    "- A post naming two tickers gets one stance PER ticker — attribute "
    "correctly; do not assign one blanket label to both.\n"
    "- bullish = expects the price to rise / positive view. bearish = expects "
    "it to fall / negative view. neutral = mention without a directional view, "
    "or genuinely mixed.\n"
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
                    "post_id": {"type": "integer"},
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
    """A Haiku call or parse failure. Fails the sentiment pass loudly."""


@dataclass
class Cost:
    haiku_calls: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_input_tokens: int = 0
    cache_creation_input_tokens: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "haiku_calls": self.haiku_calls,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "cache_read_input_tokens": self.cache_read_input_tokens,
            "cache_creation_input_tokens": self.cache_creation_input_tokens,
        }


@dataclass
class SentimentOutcome:
    # ticker -> sentiment dict, or {"error": msg} on a failed pass.
    reads: dict[str, dict[str, Any]] = field(default_factory=dict)
    cost: Cost = field(default_factory=Cost)
    errors: list[str] = field(default_factory=list)


def build_anthropic_client(api_key: str) -> Any:
    """Construct the production Anthropic client. Lazy-imports the SDK."""
    if not api_key:
        raise SentimentError("ANTHROPIC_API_KEY is empty; sentiment cannot run")
    try:
        import anthropic
    except ImportError as exc:
        raise SentimentError(
            "the `anthropic` package is not installed; sentiment cannot run "
            "(`pip install anthropic`)"
        ) from exc
    return anthropic.Anthropic(api_key=api_key)


def _build_request_posts(
    attention_tickers: set[str],
    table: dict[str, Any],
    posts_by_no: dict[int, str],
) -> tuple[list[dict[str, Any]], set[tuple[int, str]]]:
    """Posts payload for Haiku + the set of (post_id, ticker) pairs requested."""
    per_post_tickers: dict[int, set[str]] = {}
    for ticker in attention_tickers:
        for post_no in table[ticker].post_ids:
            per_post_tickers.setdefault(post_no, set()).add(ticker)

    request_posts: list[dict[str, Any]] = []
    requested_pairs: set[tuple[int, str]] = set()
    for post_no in sorted(per_post_tickers):
        tickers = sorted(per_post_tickers[post_no])
        request_posts.append(
            {
                "post_id": post_no,
                "text": posts_by_no.get(post_no, ""),
                "tickers": tickers,
            }
        )
        for ticker in tickers:
            requested_pairs.add((post_no, ticker))
    return request_posts, requested_pairs


def _call_haiku(
    client: Any, model: str, request_posts: list[dict[str, Any]], cost: Cost
) -> list[dict[str, Any]]:
    """One batched classification call. Raises SentimentError on any failure."""
    system = [
        {
            "type": "text",
            "text": _SYSTEM_PROMPT,
            "cache_control": {"type": "ephemeral"},
        }
    ]
    user_text = (
        "Classify the stance for every (post, ticker) pair in this array:\n"
        + json.dumps(request_posts, ensure_ascii=False, separators=(",", ":"))
    )
    try:
        response = client.messages.create(
            model=model,
            max_tokens=_MAX_TOKENS,
            system=system,
            messages=[{"role": "user", "content": user_text}],
            output_config={"format": {"type": "json_schema", "schema": _OUTPUT_SCHEMA}},
        )
    except Exception as exc:  # SDK error — fail the pass, do not fabricate.
        raise SentimentError(f"Haiku call failed: {exc}") from exc

    cost.haiku_calls += 1
    _accumulate_usage(getattr(response, "usage", None), cost)

    # Truncation cliff: a response that hit the output ceiling is incomplete —
    # fail loud rather than parse a partial array and silently drop tickers.
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


def _accumulate_usage(usage: Any, cost: Cost) -> None:
    if usage is None:
        return
    cost.input_tokens += int(getattr(usage, "input_tokens", 0) or 0)
    cost.output_tokens += int(getattr(usage, "output_tokens", 0) or 0)
    cost.cache_read_input_tokens += int(
        getattr(usage, "cache_read_input_tokens", 0) or 0
    )
    cost.cache_creation_input_tokens += int(
        getattr(usage, "cache_creation_input_tokens", 0) or 0
    )


def _first_text(response: Any) -> str:
    for block in getattr(response, "content", []) or []:
        if getattr(block, "type", None) == "text":
            return (getattr(block, "text", "") or "").strip()
    return ""


def _aggregate(
    ticker: str,
    counts: dict[str, int],
    *,
    read_bull_pct: int,
    read_bear_pct: int,
) -> dict[str, Any]:
    bullish = counts.get("bullish", 0)
    bearish = counts.get("bearish", 0)
    neutral = counts.get("neutral", 0)
    directional = bullish + bearish
    if directional == 0:
        return {
            "directional": 0,
            "neutral": neutral,
            "pct_bullish": None,
            "pct_bearish": None,
            "read": "mixed",
        }
    pct_bullish = round(100 * bullish / directional)
    pct_bearish = 100 - pct_bullish
    if pct_bullish > read_bull_pct:
        read = "bullish"
    elif pct_bearish > read_bear_pct:
        read = "bearish"
    else:
        read = "mixed"
    return {
        "directional": directional,
        "neutral": neutral,
        "pct_bullish": pct_bullish,
        "pct_bearish": pct_bearish,
        "read": read,
    }


def _chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def run_sentiment(
    *,
    attention_tickers: set[str],
    table: dict[str, Any],
    posts_by_no: dict[int, str],
    client: Any,
    model: str = HAIKU_MODEL_ID,
    read_bull_pct: int,
    read_bear_pct: int,
    batch_size: int = ATTENTION_BATCH_SIZE,
) -> SentimentOutcome:
    """Classify stance for attention-tier tickers and aggregate per ticker.

    The attention set is chunked into bounded batches; each batch is one Haiku
    call. A batch that errors or truncates fails loud for ITS tickers only
    (structured error) — other batches still produce reads, and we never drop a
    ticker silently or fabricate a neutral.
    """
    outcome = SentimentOutcome()
    if not attention_tickers:
        return outcome

    tallies: dict[str, dict[str, int]] = {t: {} for t in attention_tickers}
    failed: set[str] = set()

    for chunk in _chunks(sorted(attention_tickers), max(1, batch_size)):
        chunk_set = set(chunk)
        request_posts, requested_pairs = _build_request_posts(
            chunk_set, table, posts_by_no
        )
        try:
            classifications = _call_haiku(client, model, request_posts, outcome.cost)
        except SentimentError as exc:
            msg = str(exc)
            outcome.errors.append(f"sentiment: {msg}")
            for ticker in chunk_set:
                outcome.reads[ticker] = {"error": msg}
                failed.add(ticker)
            continue

        for item in classifications:
            if not isinstance(item, dict):
                continue
            try:
                post_id = int(item["post_id"])
                ticker = str(item["ticker"]).upper()
                stance = str(item["stance"]).lower()
            except (KeyError, TypeError, ValueError):
                continue
            if stance not in _STANCE_VALUES:
                continue
            if (post_id, ticker) not in requested_pairs:
                continue
            tallies[ticker][stance] = tallies[ticker].get(stance, 0) + 1

    for ticker in attention_tickers:
        if ticker in failed:
            continue
        outcome.reads[ticker] = _aggregate(
            ticker,
            tallies[ticker],
            read_bull_pct=read_bull_pct,
            read_bear_pct=read_bear_pct,
        )
    return outcome
