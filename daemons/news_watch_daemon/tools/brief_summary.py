"""One-line 'highest volume of activity' summary of a full-brief JSON artifact —
the flavor line for the morning-briefs email (Mando 2026-07-15).

Reads the news_watch full-brief artifact and prints ONE sentence naming the
dominant news flow: the top material event (highest materiality) framed by the
material-event count. Stdlib only — no daemon import needed (pure JSON read),
so it runs under any python; the morning-briefs wrapper runs it under the
news_watch venv for consistency.

Fail-soft by contract: any problem -> print nothing to stdout, exit 0, so a
malformed or missing artifact omits the news line rather than blocking the
email. Diagnostics go to stderr.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_HEADLINE_MAX = 100


def summarize(path: Path) -> str:
    d = json.loads(path.read_text())
    execs = d.get("executive_summary", {}) or {}
    events = (d.get("theme_synthesis", {}) or {}).get("events", []) or []
    n_events = execs.get("material_event_count", len(events))
    themes = execs.get("dominant_themes") or []
    lead_theme = themes[0] if themes else None

    if not events:
        base = f"News watch logged no material events in the {_window_h(d)} window"
        return f"{base} on {lead_theme}." if lead_theme else f"{base}."

    top = max(events, key=lambda e: e.get("materiality_score") or 0.0)
    headline = str(top.get("headline_summary", "")).strip()
    if len(headline) > _HEADLINE_MAX:
        headline = headline[: _HEADLINE_MAX - 1].rstrip() + "…"
    score = top.get("materiality_score")
    srcs = top.get("source_count")
    tail = []
    if isinstance(score, (int, float)):
        tail.append(f"materiality {score:.2f}")
    if isinstance(srcs, int) and srcs:
        tail.append(f"{srcs} sources")
    qualifier = f" ({', '.join(tail)})" if tail else ""
    count_clause = (f"{n_events} material events led by"
                    if isinstance(n_events, int) and n_events > 1
                    else "The window's material event:")
    lead = f"News flow ({lead_theme}) — " if lead_theme else "News flow — "
    return f"{lead}{count_clause} “{headline}”{qualifier}."


def _window_h(d: dict) -> str:
    h = (d.get("window", {}) or {}).get("duration_hours")
    return f"{h}h" if h else "reporting"


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if not argv:
        print("brief_summary: usage: brief_summary.py <brief.json>", file=sys.stderr)
        return 0
    try:
        line = summarize(Path(argv[0]).expanduser())
    except Exception as exc:  # fail-soft: never block the email
        print(f"brief_summary: skipped ({type(exc).__name__}: {exc})", file=sys.stderr)
        return 0
    print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
