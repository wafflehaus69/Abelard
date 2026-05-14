"""Synthesis smoke runner — Step 9g / Checkpoint 4 deliverable.

Three sequential synthesis calls against canned-but-realistic headlines.
Prints cache_creation_input_tokens (call 1) and cache_read_input_tokens
(calls 2 and 3) to verify the two-breakpoint cache shape works against
the live Anthropic API. Also prints the final Brief JSON so Mando can
inspect the real-data-shape output.

USAGE:
    export ANTHROPIC_API_KEY=sk-ant-...
    python tools/synthesis_smoke.py

Optional flags:
    --theses-path PATH    Pass a theses doc through the second cache block.
                          Without this, run uses the no-theses variant
                          (single cache breakpoint, no thesis_links emitted).
    --model MODEL         Override the model id. Default: claude-sonnet-4-6.
    --calls N             Number of sequential calls. Default: 3.

Headlines are hand-crafted real-world examples — the Pass B smoke
corpus has aged out of the temp DB by Step 9 time. This script is
NOT a daemon path; it's a manual operator tool for Step 9 calibration
and the Step 16 live-smoke procedure.

Cost note: each call is ~2-4K input tokens + ~500-1500 output tokens.
On Sonnet 4.6 ($3/M input, $15/M output), 3 calls cost on the order
of $0.05. After the first call, cached prefix reads bill at 1/10 the
normal input price.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Add src/ to path so we can run as `python tools/synthesis_smoke.py`
# without installing first. Editable install (`pip install -e .`) also
# works.
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

from news_watch_daemon.synthesize.brief import Trigger, TriggerWindow  # noqa: E402
from news_watch_daemon.synthesize.cluster import Cluster, ClusterInput  # noqa: E402
from news_watch_daemon.synthesize.synthesize import (  # noqa: E402
    build_anthropic_client,
    synthesize_brief,
)


# Canned-but-realistic clustered headlines for the smoke run.
# Three clusters across two themes (us_iran_escalation + fed_policy_path)
# to exercise themes_in_scope passing, cluster rendering, and
# materiality variance.
_NOW = int(datetime.now(timezone.utc).timestamp())

_SMOKE_CLUSTERS: list[Cluster] = [
    Cluster(
        headline_ids=("h-1", "h-2", "h-3"),
        members=(
            ClusterInput(
                headline_id="h-1",
                headline="Iran's foreign minister rejects U.S. ceasefire framework, calls terms 'unacceptable'",
                url="https://reuters.com/world/middle-east/iran-rejects-us-ceasefire/",
                publisher="Reuters",
                published_at_unix=_NOW - 1800,
            ),
            ClusterInput(
                headline_id="h-2",
                headline="Tehran says no to U.S. peace proposal, citing sanctions demands",
                url="https://apnews.com/article/iran-us-peace-rejected/",
                publisher="AP",
                published_at_unix=_NOW - 1700,
            ),
            ClusterInput(
                headline_id="h-3",
                headline="Iranian leadership turns down latest U.S. terms in negotiation",
                url=None,
                publisher="CNBC",
                published_at_unix=_NOW - 1600,
            ),
        ),
    ),
    Cluster(
        headline_ids=("h-4", "h-5"),
        members=(
            ClusterInput(
                headline_id="h-4",
                headline="Strait of Hormuz tanker traffic down 35% week-over-week amid heightened tensions",
                url="https://reuters.com/markets/commodities/hormuz-tanker-traffic/",
                publisher="Reuters",
                published_at_unix=_NOW - 3600,
            ),
            ClusterInput(
                headline_id="h-5",
                headline="Hormuz shipping activity drops sharply as Iran-U.S. standoff persists",
                url="https://bloomberg.com/news/articles/hormuz-shipping/",
                publisher="Bloomberg",
                published_at_unix=_NOW - 3500,
            ),
        ),
    ),
    Cluster(
        headline_ids=("h-6",),
        members=(
            ClusterInput(
                headline_id="h-6",
                headline="Fed officials signal cuts may come faster if labor market softens further",
                url="https://wsj.com/economy/fed-cuts-faster-labor/",
                publisher="WSJ",
                published_at_unix=_NOW - 5400,
            ),
        ),
    ),
]

# Theme briefs (canned excerpts; production uses ThemeConfig.brief).
_THEME_BRIEFS: dict[str, str] = {
    "us_iran_escalation": (
        "Tracks the trajectory of U.S.-Iran conflict and its cascade effects "
        "on commodities (oil, fertilizer feedstock, grains), defense procurement, "
        "and emerging-market risk. Material signals: military movements, "
        "sanctions changes, diplomatic breakdowns or progress, oil infrastructure "
        "attacks, statements from named principals, Strait of Hormuz tanker activity. "
        "Noise: routine diplomatic statements, anniversary commentary, op-eds."
    ),
    "fed_policy_path": (
        "Tracks U.S. Federal Reserve policy trajectory — rate decisions, FOMC "
        "language shifts, dual-mandate balance. Material signals: FOMC meeting "
        "language changes, named Fed officials' policy statements, emergency "
        "moves, dissent records. Noise: market-color commentary, retail "
        "sentiment, generic 'rates are high' framing."
    ),
}

_TRIGGER = Trigger(
    type="event",
    reason="us_iran_escalation delta threshold exceeded; fed_policy_path phrase signal",
    window=TriggerWindow(
        since=datetime.fromtimestamp(_NOW - 3600 * 4, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        until=datetime.fromtimestamp(_NOW, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    ),
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Synthesis smoke runner.")
    parser.add_argument("--theses-path", type=Path, default=None,
                        help="Optional theses doc path (else no-theses variant).")
    parser.add_argument("--model", default="claude-sonnet-4-6",
                        help="Anthropic model id. Default: claude-sonnet-4-6.")
    parser.add_argument("--calls", type=int, default=3,
                        help="Number of sequential calls (cache verification). Default: 3.")
    parser.add_argument("--max-tokens", type=int, default=2048,
                        help="Output cap per call. Default: 2048.")
    parser.add_argument("--max-events", type=int, default=8,
                        help="max_events_per_brief constraint. Default: 8.")
    args = parser.parse_args()

    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY is not set. Export it and re-run.",
              file=sys.stderr)
        return 1

    client = build_anthropic_client(api_key)

    print(f"--- synthesis smoke ({args.calls} calls, model={args.model}) ---")
    print(f"theses_path: {args.theses_path or '(none — no-theses variant)'}")
    print(f"clusters: {len(_SMOKE_CLUSTERS)}, themes_in_scope: 2")
    print()

    final_brief = None
    for i in range(1, args.calls + 1):
        print(f"--- call {i}/{args.calls} ---")
        brief = synthesize_brief(
            client=client,
            model=args.model,
            max_tokens=args.max_tokens,
            trigger=_TRIGGER,
            themes_in_scope=["us_iran_escalation", "fed_policy_path"],
            theme_briefs=_THEME_BRIEFS,
            clusters=_SMOKE_CLUSTERS,
            max_events_per_brief=args.max_events,
            theses_path=args.theses_path,
        )
        md = brief.synthesis_metadata
        print(f"  model_used:                        {md.model_used}")
        print(f"  input_tokens:                      {md.input_tokens}")
        print(f"  output_tokens:                     {md.output_tokens}")
        print(f"  cache_creation_input_tokens:       {md.cache_creation_input_tokens}")
        print(f"  cache_read_input_tokens:           {md.cache_read_input_tokens}")
        print(f"  events:                            {len(brief.events)}")
        final_brief = brief
        print()

    if final_brief is not None:
        print("--- final brief (call {}) ---".format(args.calls))
        print(json.dumps(final_brief.model_dump(mode="json"), indent=2, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    sys.exit(main())
