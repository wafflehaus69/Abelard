"""Drift smoke runner — Step 10 deliverable.

Two sequential drift calls against canned untagged headlines + the
six bundled themes. Prints cache_creation_input_tokens (call 1) and
cache_read_input_tokens (call 2) to verify the single-breakpoint
cache shape works against Haiku 4.5. Prints the final proposals JSON
so Mando can inspect what Haiku is suggesting.

USAGE:
    export ANTHROPIC_API_KEY=sk-ant-...
    python tools/drift_smoke.py

Optional flags:
    --themes-dir DIR       Override themes directory. Default: bundled themes/.
    --model MODEL          Override model id. Default: claude-haiku-4-5.
    --calls N              Number of sequential calls. Default: 2.
    --max-proposals N      max_proposals_per_batch. Default: 8.
    --min-evidence N       min_evidence_count. Default: 3.

Headlines are hand-crafted real-world-shape examples that recur in
public coverage but don't match the bundled themes' current keyword
sets — designed to give Haiku a real signal to detect.

Cost note: Haiku 4.5 at $1/M input, $5/M output. A typical drift
call burns ~3-5K input + ~1K output tokens. Two calls cost on the
order of $0.01.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

# Add src/ to path so this runs without an editable install.
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

from news_watch_daemon.synthesize.drift import propose_drift  # noqa: E402
from news_watch_daemon.synthesize.synthesize import build_anthropic_client  # noqa: E402
from news_watch_daemon.theme_config import load_all_themes  # noqa: E402


# Canned untagged headlines designed to give Haiku real drift signal
# against the bundled themes. Each cluster of related headlines is a
# pattern Haiku should be able to detect.
#
# (publisher, headline, published_at_unix)
_UNTAGGED: list[tuple[str | None, str, int]] = [
    # Cluster A: "rare earth" supply-chain stories — likely candidate
    # for china_us_decoupling secondary.
    ("Reuters", "Beijing tightens rare earth export licensing for samarium", 1764000000),
    ("Bloomberg", "Rare earth mine in Mountain Pass restarts amid export curbs", 1764003600),
    ("WSJ", "Pentagon stockpiles rare earth oxides ahead of expected curbs", 1764007200),
    ("FT", "China hints at rare earth quotas for defense buyers", 1764010800),
    # Cluster B: Houthi shipping disruption — likely us_iran_escalation
    # secondary (already has Houthi as primary, but specific phrase
    # "Bab el-Mandeb" may not be covered).
    ("Reuters", "Bab el-Mandeb transit volumes fall sharply after attack", 1764014400),
    ("AP", "Container ships reroute around Bab el-Mandeb chokepoint", 1764018000),
    ("CNBC", "Insurance war risk premiums jump on Bab el-Mandeb closures", 1764021600),
    # Cluster C: "carbon credit" related — possible
    # tokenized_finance_infrastructure candidate (registry / tokenization).
    ("Reuters", "Voluntary carbon credit registry sees record issuance volume", 1764025200),
    ("Bloomberg", "Tokenized carbon credit pilot launches on private chain", 1764028800),
    ("FT", "Carbon credit market hit by double-counting scandal", 1764032400),
    # Cluster D: "labor market" deceleration — likely fed_policy_path
    # secondary; not unique enough to be primary.
    ("WSJ", "Job openings dip in latest JOLTS release as labor market cools", 1764036000),
    ("Bloomberg", "Labor market cooling signals priced into rate path", 1764039600),
    ("Reuters", "Unemployment claims rise as labor market loses momentum", 1764043200),
    # Cluster E: AI energy demand — possible ai_capex_cycle secondary.
    ("FT", "Hyperscaler power demand outpaces grid expansion in Virginia", 1764046800),
    ("Reuters", "Microsoft signs nuclear PPA to backstop AI training load", 1764050400),
    ("Bloomberg", "Constellation Energy data-center capacity sells out for 2027", 1764054000),
    # Noise (singletons, shouldn't propose):
    ("AP", "Local school board approves new curriculum", 1764057600),
    ("Reuters", "Sports league announces expansion franchise", 1764061200),
]


def main() -> int:
    parser = argparse.ArgumentParser(description="Drift smoke runner.")
    parser.add_argument(
        "--themes-dir", type=Path,
        default=_REPO_ROOT / "themes",
        help="Themes directory. Default: bundled themes/.",
    )
    parser.add_argument(
        "--model", default="claude-haiku-4-5",
        help="Anthropic model id. Default: claude-haiku-4-5.",
    )
    parser.add_argument(
        "--calls", type=int, default=2,
        help="Number of sequential calls (cache verification). Default: 2.",
    )
    parser.add_argument(
        "--max-tokens", type=int, default=2048,
        help="Output cap per call. Default: 2048.",
    )
    parser.add_argument(
        "--max-proposals", type=int, default=8,
        help="max_proposals_per_batch. Default: 8.",
    )
    parser.add_argument(
        "--min-evidence", type=int, default=3,
        help="min_evidence_count floor. Default: 3.",
    )
    args = parser.parse_args()

    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY is not set. Export it and re-run.",
              file=sys.stderr)
        return 1

    themes = load_all_themes(args.themes_dir)
    client = build_anthropic_client(api_key)

    print(f"--- drift smoke ({args.calls} calls, model={args.model}) ---")
    print(f"themes_dir: {args.themes_dir}")
    print(f"themes loaded: {len(themes)} ({', '.join(t.theme_id for t in themes)})")
    print(f"untagged headlines: {len(_UNTAGGED)}")
    print()

    final_result = None
    for i in range(1, args.calls + 1):
        print(f"--- call {i}/{args.calls} ---")
        result = propose_drift(
            client=client,
            model=args.model,
            max_tokens=args.max_tokens,
            themes=themes,
            untagged=_UNTAGGED,
            max_proposals_per_batch=args.max_proposals,
            min_evidence_count=args.min_evidence,
        )
        print(f"  model_used:                        {result.model_used}")
        print(f"  input_tokens:                      {result.input_tokens}")
        print(f"  output_tokens:                     {result.output_tokens}")
        print(f"  cache_creation_input_tokens:       {result.cache_creation_input_tokens}")
        print(f"  cache_read_input_tokens:           {result.cache_read_input_tokens}")
        print(f"  proposals returned:                {len(result.proposals)}")
        for p in result.proposals:
            print(f"    - [{p.theme_id}] {p.proposed_keyword!r} "
                  f"({p.suggested_tier}, evidence={p.evidence_count})")
        final_result = result
        print()

    if final_result is not None:
        print(f"--- final proposals (call {args.calls}) ---")
        print(json.dumps(
            [p.model_dump(mode="json") for p in final_result.proposals],
            indent=2, ensure_ascii=False,
        ))

    return 0


if __name__ == "__main__":
    sys.exit(main())
