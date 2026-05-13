"""Trigger gate — decides whether the orchestrator should invoke synthesis.

Pure function over (recently-tagged-headlines + config + window). No I/O,
no DB queries. The caller (eventually the scrape orchestrator or the
event handler) is responsible for assembling the input set and acting
on the decision. Every decision is recorded via `trigger_log.write_entry`
for calibration analysis (§14 of the Pass C brief).

Three signals in evaluation order — first to fire wins:

  1. Cross-theme: any headline tagged to ≥2 themes. Brief §3:
     "a single event tagged Iran + Fed simultaneously."
  2. High-signal phrase: any headline contains a phrase from the
     config list. Matched with word boundaries (same discipline as
     Pass C Step 1's theme-keyword regex) so "anew sanctions" doesn't
     fire "new sanctions".
  3. Delta threshold: at least N new tagged headlines for any single
     theme since the last synthesis. Per-theme override or default.

If none fire, the gate decision is "suppress" with reason
`below_thresholds`. The Brief archive does not receive a Brief in
this case — synthesis simply doesn't run.

The pull path (`news-watch-daemon synthesize ...`) bypasses this gate
entirely; this module is only consulted on the event-driven path.
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field

from .config import TriggerGateConfig


@dataclass(frozen=True)
class TriggerHeadline:
    """One recently-tagged headline visible to the gate.

    Decoupled from FetchedItem and from DB-row shape — the gate operates
    on this small projection so it stays a pure function.
    """

    headline_id: str
    headline: str
    themes: tuple[str, ...]  # theme_ids this headline tagged to
    fetched_at_unix: int


@dataclass(frozen=True)
class TriggerDecision:
    """Output of evaluate_gate(). Pure data; logged + returned to caller."""

    fire: bool
    reason: str
    matched_headline_ids: tuple[str, ...] = ()
    themes_in_scope: tuple[str, ...] = ()
    window_since_unix: int = 0
    window_until_unix: int = 0


def _compile_phrase_regex(phrases: list[str]) -> re.Pattern[str] | None:
    if not phrases:
        return None
    pattern = "|".join(rf"\b{re.escape(p)}\b" for p in phrases)
    return re.compile(pattern, re.IGNORECASE)


def evaluate_gate(
    headlines: list[TriggerHeadline],
    *,
    config: TriggerGateConfig,
    window_since_unix: int,
    window_until_unix: int,
) -> TriggerDecision:
    """Decide fire vs suppress for a batch of headlines.

    Evaluation order (first to fire wins): cross-theme → phrase → delta.
    """
    if not headlines:
        return TriggerDecision(
            fire=False,
            reason="no_new_headlines",
            window_since_unix=window_since_unix,
            window_until_unix=window_until_unix,
        )

    # ---- Signal 1: cross-theme overlap ----
    if config.cross_theme_always_triggers:
        for h in headlines:
            if len(h.themes) >= 2:
                return TriggerDecision(
                    fire=True,
                    reason=f"cross_theme:{'+'.join(sorted(h.themes))}",
                    matched_headline_ids=(h.headline_id,),
                    themes_in_scope=tuple(sorted(set(h.themes))),
                    window_since_unix=window_since_unix,
                    window_until_unix=window_until_unix,
                )

    # ---- Signal 2: high-signal phrase ----
    phrase_re = _compile_phrase_regex(config.high_signal_phrases)
    if phrase_re is not None:
        for h in headlines:
            m = phrase_re.search(h.headline)
            if m:
                return TriggerDecision(
                    fire=True,
                    reason=f"high_signal_phrase:{m.group(0).lower()}",
                    matched_headline_ids=(h.headline_id,),
                    themes_in_scope=tuple(sorted(set(h.themes))),
                    window_since_unix=window_since_unix,
                    window_until_unix=window_until_unix,
                )

    # ---- Signal 3: delta threshold per theme ----
    theme_counts: Counter[str] = Counter()
    theme_headlines: dict[str, list[str]] = {}
    for h in headlines:
        for theme in h.themes:
            theme_counts[theme] += 1
            theme_headlines.setdefault(theme, []).append(h.headline_id)

    # Sort themes by count desc, then alphabetical, for deterministic
    # tie-breaking (test-stable, log-stable).
    for theme in sorted(theme_counts, key=lambda t: (-theme_counts[t], t)):
        threshold = config.delta_threshold_overrides.get(
            theme, config.delta_threshold_default,
        )
        if theme_counts[theme] >= threshold:
            return TriggerDecision(
                fire=True,
                reason=f"delta_threshold:{theme}:{theme_counts[theme]}",
                matched_headline_ids=tuple(theme_headlines[theme]),
                themes_in_scope=(theme,),
                window_since_unix=window_since_unix,
                window_until_unix=window_until_unix,
            )

    return TriggerDecision(
        fire=False,
        reason="below_thresholds",
        themes_in_scope=tuple(sorted(theme_counts.keys())),
        window_since_unix=window_since_unix,
        window_until_unix=window_until_unix,
    )


__all__ = [
    "TriggerDecision",
    "TriggerHeadline",
    "evaluate_gate",
]
