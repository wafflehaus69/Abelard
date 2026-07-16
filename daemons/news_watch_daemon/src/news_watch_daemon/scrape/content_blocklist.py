"""Editorial content blocklist — hard-drops headlines with forbidden terms.

Order (Mando, 2026-07-15): certain conspiracy-theory / ideological terms must
never surface in a report. The conspiracy-heavy Telegram feeds (CIG,
Ateobreaking) and some wire proxies carry antisemitic-conspiracy and
"replacement"-theory content that is pure noise for a markets/geopolitics
system and unwanted in the output.

Unlike the sports filter, this is UNCONDITIONAL — there is no markets/geopolitics
guard. If a blocklist term appears, the headline is dropped regardless of what
else it contains (that is the point: "it can't show up in the report"). Drops
run at ingest, upstream of dedup/tag/insert/attention, and are recorded in the
noise-filter audit trail (filtered.jsonl, matched_pattern="blocklist:<term>") so
the suppression is fully reconstructible.

Matching is case-insensitive, word-boundary anchored, with a light inflection
suffix so "Talmud" also catches "Talmudic" and plurals — but tuned NOT to
over-match adjective usage: `\bGreater Israel\b`-style anchoring means "Greater
Israel" (the expansionist ideology) is caught while "greater Israeli role"
(ordinary adjective) is NOT.

The list is a plain module constant — extend it inline. Terms below the
"---- added beyond the explicit request ----" line are same-family terms added
under Mando's "shit like that" instruction; prune any that overreach.
"""

from __future__ import annotations

import re


_BLOCKLIST_TERMS = [
    # GUIDING RULE (Mando 2026-07-15): block ONLY fabricated antisemitic
    # conspiracy theories and slurs — the invented-plot / hoax content that
    # never appears as genuine reporting. REAL geopolitical terms are NOT
    # blocked even when they sound loaded: actual named frameworks (Abraham
    # Accords, Isaac Accords) and geopolitical concepts (Greater Israel,
    # Greater Judea) are legitimate news, incl. when heads of state invoke them.
    #
    # REMOVED as real terms (do NOT re-add): "Isaac Accords", "Greater Israel",
    # "Greater Judea". "Abraham Accords" was never listed.
    "Kalergi Plan",                     # debunked "replacement" conspiracy — not a real plan
    "Kalergi",                          # bare, for variants ("the Kalergi agenda")
    "Protocols of the Elders of Zion",  # antisemitic forgery
    "Protocols of Zion",
    "white genocide",                   # replacement-conspiracy slur
    "ZOG",                              # "Zionist Occupied Government" slur (exact-bounded; not Zogby)
    # Talmud: a REAL religious text, kept ONLY because in these conspiracy-heavy
    # feeds it appears almost exclusively as an antisemitic dog-whistle ("what
    # the Talmud commands..."), never as markets/geo signal. It is the one
    # real-but-blocked term — flagged to Mando; trivial to pull if the
    # dog-whistle-noise justification doesn't hold.
    "Talmud",                           # + Talmudic via the inflection suffix
]

# Inflection suffix: plural / past / gerund / -ian(s) / -ic (Talmudic). The
# trailing \b still blocks adjective over-match: "Greater Israel" + no suffix +
# \b matches "Greater Israel <space>" but NOT "Greater Israeli" (the "i" is a
# word char with no valid suffix consumed, so the boundary fails).
_INFLECT = r"(?:s|es|ed|ing|ians?|ic)?"

_BLOCKLIST_RE = re.compile(
    "|".join(rf"\b{re.escape(t)}{_INFLECT}\b" for t in _BLOCKLIST_TERMS),
    re.IGNORECASE,
)


def classify_blocklist(headline: str | None) -> str | None:
    """Return the matched blocklist term if `headline` contains one, else None.

    Unconditional: any match drops the headline. The returned string is the
    matched text, for the audit trail's `matched_pattern`.
    """
    if not headline:
        return None
    m = _BLOCKLIST_RE.search(headline)
    return m.group(0) if m else None


__all__ = ["classify_blocklist"]
