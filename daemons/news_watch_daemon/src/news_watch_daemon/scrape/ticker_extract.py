"""Ticker extraction for the scrape orchestrator.

Two sources of tagged tickers:

  1. Tracked-list match: word-boundary case-sensitive regex against the
     union of conviction + watchlist tickers from `tracked_tickers.yaml`.
     The list is hand-curated and edited by Mando; the orchestrator
     loads it once at startup.
  2. Cashtag match: `$TICKER` pattern (e.g. `$AAPL`, `$BRK.B`). Catches
     tickers explicitly cash-tagged in the source text — Telegram in
     particular uses this convention — even when the ticker is not in
     the tracked list.

Output of both passes is union-ed and stored in `headlines.tickers_json`
(an existing-but-unused column from the foundation schema). The Pass B
orchestrator already serializes `FetchedItem.tickers` to this column;
Step 0's enrichment populates it with extracted tickers in addition to
whatever the source plugin pre-tagged (currently always [] in practice).

Word boundaries are critical: `MOST` should not match in `mostly`,
`ETH` should not match in `ETHER`. The `\b...\b` wrapping enforces this.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path

import yaml


_LOG = logging.getLogger("news_watch_daemon.scrape.ticker_extract")


# Cashtag pattern: `$` followed by 1–5 uppercase letters, optional
# share-class suffix (`.A`, `-B`). Word boundary on the right ensures
# `$AAPL` matches but `$AAPLfoo` does not.
_CASHTAG_RE = re.compile(r"\$([A-Z]{1,5}(?:[.\-][A-Z])?)\b")


# Surrounding-text window for tracked-ticker false-positive observation logs.
# 50 chars on each side of the match position gives enough channel-prefix
# context (e.g. "NOW - " breadcrumbs convention) to distinguish a ticker
# reference from an English-word collision.
_TICKER_LOG_CONTEXT_CHARS = 50


class TickerExtractError(RuntimeError):
    """Raised when tracked_tickers.yaml cannot be loaded or validated."""


@dataclass(frozen=True)
class TrackedTickers:
    """Loaded ticker config + pre-compiled match regex."""

    conviction: tuple[str, ...]
    watchlist: tuple[str, ...]
    _regex: re.Pattern[str] | None  # None if both lists are empty

    @property
    def all(self) -> frozenset[str]:
        return frozenset(self.conviction) | frozenset(self.watchlist)

    def extract(self, text: str | None) -> list[str]:
        """Return sorted unique tickers found in `text`.

        Combines tracked-list matches (word-boundary case-sensitive) with
        cashtag matches (`$TICKER` pattern). Returns empty list on None
        or empty input.
        """
        if not text:
            return []
        hits: set[str] = set()
        for ticker, _pos in self.find_tracked_matches(text):
            hits.add(ticker)
        for m in _CASHTAG_RE.finditer(text):
            hits.add(m.group(1))
        return sorted(hits)

    def find_tracked_matches(self, text: str | None) -> list[tuple[str, int]]:
        """Return (ticker, start_pos) for each tracked-list match in `text`.

        Tracked-list only — cashtag matches are excluded. Returns one entry per
        occurrence (no dedup), preserving order. Used by the scrape orchestrator
        for false-positive instrumentation (see `log_tracked_ticker_match`).
        Returns empty list when `text` is None / empty / no tracked tickers
        configured.
        """
        if not text or self._regex is None:
            return []
        return [(m.group(0), m.start()) for m in self._regex.finditer(text)]


def _compile_regex(tickers: frozenset[str]) -> re.Pattern[str] | None:
    if not tickers:
        return None
    # re.escape handles the dot in MOG.A / BRK.B and the hyphen in BF-A.
    # Case-sensitive because real tickers are uppercase; case-insensitive
    # would produce too many false positives ("eth" in "ethics", etc.).
    pattern = r"\b(?:" + "|".join(re.escape(t) for t in sorted(tickers)) + r")\b"
    return re.compile(pattern)


def load_tracked_tickers(path: Path) -> TrackedTickers:
    """Load tracked_tickers.yaml. Fail loud on missing or malformed file."""
    if not isinstance(path, Path):
        path = Path(path)
    if not path.is_file():
        raise TickerExtractError(f"tracked_tickers config not found: {path}")
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise TickerExtractError(f"invalid YAML in {path}: {exc}") from exc
    if raw is None:
        # Empty file: treat as empty config rather than error.
        raw = {}
    if not isinstance(raw, dict):
        raise TickerExtractError(
            f"tracked_tickers root must be a mapping in {path}; got {type(raw).__name__}"
        )

    conviction = _validate_ticker_list(raw.get("conviction") or [], path, "conviction")
    watchlist = _validate_ticker_list(raw.get("watchlist") or [], path, "watchlist")
    combined = frozenset(conviction) | frozenset(watchlist)
    return TrackedTickers(
        conviction=tuple(conviction),
        watchlist=tuple(watchlist),
        _regex=_compile_regex(combined),
    )


def _validate_ticker_list(items: list, path: Path, key: str) -> list[str]:
    if not isinstance(items, list):
        raise TickerExtractError(
            f"{key} must be a list in {path}; got {type(items).__name__}"
        )
    out: list[str] = []
    for item in items:
        if not isinstance(item, str) or not item.strip():
            raise TickerExtractError(
                f"{key} entries must be non-empty strings in {path}; got {item!r}"
            )
        out.append(item.strip())
    return out


def log_tracked_ticker_match(
    *,
    source_channel: str,
    headline_id: str,
    ticker: str,
    headline: str,
    match_position: int,
) -> None:
    """Emit a DEBUG observation for a tracked-list ticker match in a headline.

    Used by the scrape orchestrator for per-channel false-positive measurement.
    Logs the surrounding 50 chars before and 50 chars after the match position
    so downstream audit can distinguish a real ticker mention ("ServiceNow NOW
    reported earnings") from an English-word collision ("NOW - Trump speaks").

    Two-week empirical signal target: per-channel false-positive rates per
    ticker, to inform when to scope a company-name-aware extraction layer
    (Option E from the 2026-05-24 calibration review) reading
    `tracked_entities.companies` across all themes. Until that lands, the
    `NOW` channel-prefix false positive in chainlinkbreadcrumbs is accepted
    as known noise — the cost of removing `NOW` from the tracked list would
    be losing ALL natural-language ServiceNow visibility, which is worse.

    DEBUG level (not INFO): this can fire on every headline tagged with a
    tracked ticker, which is high-volume. Operators must opt in by lowering
    LOG_LEVEL to DEBUG when running calibration measurement.
    """
    context_start = max(0, match_position - _TICKER_LOG_CONTEXT_CHARS)
    context_end = min(
        len(headline),
        match_position + len(ticker) + _TICKER_LOG_CONTEXT_CHARS,
    )
    context = headline[context_start:context_end]
    _LOG.debug(
        "tracked_ticker_match channel=%s headline_id=%s ticker=%s "
        "match_pos=%d context=%r",
        source_channel,
        headline_id,
        ticker,
        match_position,
        context,
    )


__all__ = [
    "TickerExtractError",
    "TrackedTickers",
    "load_tracked_tickers",
    "log_tracked_ticker_match",
]
