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

import re
from dataclasses import dataclass
from pathlib import Path

import yaml


# Cashtag pattern: `$` followed by 1–5 uppercase letters, optional
# share-class suffix (`.A`, `-B`). Word boundary on the right ensures
# `$AAPL` matches but `$AAPLfoo` does not.
_CASHTAG_RE = re.compile(r"\$([A-Z]{1,5}(?:[.\-][A-Z])?)\b")


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
        if self._regex is not None:
            for m in self._regex.finditer(text):
                hits.add(m.group(0))
        for m in _CASHTAG_RE.finditer(text):
            hits.add(m.group(1))
        return sorted(hits)


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


__all__ = [
    "TickerExtractError",
    "TrackedTickers",
    "load_tracked_tickers",
]
