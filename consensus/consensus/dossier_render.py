"""Dossier renderer — M10-D §3.3. Turns a stored footprint row into a human-readable
artifact (Markdown default; PDF via the hoisted abelard_common.render if available).

The renderer is where several honesty constraints are ENFORCED at render time, over
whatever the store captured wide:
  - §4.5 CEX confidence: a low-confidence class renders as ``unclassified`` — a
    provisional guess never hardens into an implied allegation.
  - §4.2 mesh-collapse DEFLATES: raw wallet count AND post-collapse actor count are
    both shown; a 20-wallet mesh that collapses to one actor is stated as n=1 evidence.
  - §4.4 contested vs headline notional shown SEPARATELY (the carry-trade confound).
  - §4.6 tiers latch: current tier + peak (high-water) shown as trajectory, not retraction.
  - Rule 1: missing data renders as an explicit em-dash/"unavailable", never imputed.
  - §4.1 no EV, no take-the-bet affordance; mandatory footer on every artifact.
"""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

# A CEX class at or below this confidence renders as "unclassified" (§4.5 / v1.8 §4.3).
CEX_CONFIDENCE_FLOOR = 0.5

FOOTER = (
    "*This is anomaly detection over public on-chain data. It is not a validated trade "
    "signal, not an allegation about any person, and no expected value is implied or "
    "computed.*"
)


def _d(ts: Any) -> str:
    try:
        return dt.datetime.fromtimestamp(int(ts), dt.timezone.utc).strftime("%Y-%m-%d")
    except Exception:
        return "—"


def _money(x: Any) -> str:
    try:
        return f"${float(x):,.0f}"
    except Exception:
        return "—"


def _num(x: Any, fmt: str = "{:.2f}") -> str:
    try:
        return fmt.format(float(x))
    except Exception:
        return "—"


def _loads(x: Any, default):
    if x is None:
        return default
    try:
        return json.loads(x) if isinstance(x, str) else x
    except Exception:
        return default


def render_cex(cex_class: Any, confidence: Any, *, floor: float = CEX_CONFIDENCE_FLOOR) -> str:
    """Honesty gate: below-floor confidence => 'unclassified', regardless of the raw
    class the store holds. Returns the display string."""
    if not cex_class:
        return "unclassified"
    try:
        c = float(confidence)
    except Exception:
        c = 0.0
    if c < floor:
        return "unclassified"
    return str(cex_class)


def render_markdown(row: dict[str, Any], *, cex_floor: float = CEX_CONFIDENCE_FLOOR) -> str:
    wallets = _loads(row.get("cluster_wallets"), [row.get("wallet")])
    n_raw = len(wallets) if wallets else 1
    actors = row.get("actor_count_post_collapse")   # None = UNRESOLVED, never imputed
    tier = row.get("tier") or "—"
    peak = row.get("tier_peak") or "—"
    L: list[str] = []
    L.append(f"# Dossier `{row.get('dossier_id', '—')}`")
    cat = row.get("market_category")
    L.append(f"**Market:** {row.get('market_question') or '—'}" + (f"  ({cat})" if cat else ""))
    traj = "" if tier == peak else f" — trajectory: peaked at **{peak}** ({_d(row.get('tier_peak_ts'))}), now {tier}"
    L.append(f"**Tier:** {tier} · peak **{peak}**{traj}")
    L.append("")

    L.append("## Footprint")
    fs = row.get("first_seen_ts")
    fss = row.get("first_seen_source")
    fresh = f"{_d(fs)} (via {fss})" if fs else f"**unavailable** ({fss or 'not resolved'}) — F not scored (Rule 1)"
    L += [
        f"- Wallet: `{row.get('wallet')}`",
        f"- Side: {row.get('side') or '—'} · entry VWAP {_num(row.get('entry_vwap'), '{:.3f}')} · "
        f"price at detection {_num(row.get('price_at_detection'), '{:.3f}')}",
        f"- **Contested notional:** {_money(row.get('contested_notional'))}  _(the informed slice)_",
        f"- **Headline notional:** {_money(row.get('headline_notional'))}  _(total position; carry-trade context)_",
        f"- Detected: {_d(row.get('detection_ts'))} · wallet first-seen: {fresh}",
        "",
    ]

    L.append("## Factors")
    L.append("| F | S | D | C | latency | composite |")
    L.append("|---|---|---|---|---|---|")
    L.append(f"| {_num(row.get('f_factor'))} | {_num(row.get('s_factor'))} | {_num(row.get('d_factor'))} "
             f"| {_num(row.get('c_factor'))} | {_num(row.get('latency_factor'))} | {_num(row.get('composite'))} |")
    L.append("")

    L.append("## Cluster / actor")
    if actors is None:
        # §4.2 / Rule 1: the funding mesh could not be computed — the EXPECTED outcome
        # of keeping the enrichment cap (a cluster member was never enriched). Declare
        # it. Imputing the raw count here would render an uncollapsed mesh as
        # "N coordinated actors", indistinguishable from a verified N-actor cluster —
        # the exact Mojtaba overstatement this invariant exists to prevent.
        L.append(f"- Raw wallets: **{n_raw}** · post-collapse actors: **UNRESOLVED** "
                 f"_(cluster size unavailable)_")
        if n_raw > 1:
            L.append(f"- ⚠ funding-mesh collapse NOT COMPUTED — this is **NOT n={n_raw}** "
                     f"evidence. {n_raw} wallets may be a single actor in {n_raw} masks; "
                     f"unresolved is not {n_raw}.")
        else:
            L.append("- solo footprint (funding unresolved).")
    else:
        L.append(f"- Raw wallets: **{n_raw}** · post-collapse actors: **{actors}** _(cluster size)_")
        if actors < n_raw:
            L.append(f"- ⚠ funding-mesh collapse: {n_raw} wallets → **{actors} actor(s)** — this is "
                     f"**n={actors}** evidence, not {n_raw} (the inversion).")
        elif n_raw == 1:
            L.append("- solo footprint.")
    xmc = _loads(row.get("cross_market_cluster"), None)
    if xmc:
        L.append(f"- cross-market cluster: {xmc}")
    L.append("")

    L.append("## Funding / venue")
    fund = _loads(row.get("funding_summary"), None)
    L.append(f"- Funding trail: {fund if fund else '—'}")
    disp = render_cex(row.get("cex_class"), row.get("cex_confidence"), floor=cex_floor)
    conf = row.get("cex_confidence")
    note = ""
    if row.get("cex_class") and disp == "unclassified" and str(row.get("cex_class")) != "unclassified":
        note = f" _(raw class held at confidence {_num(conf)} < {cex_floor} → rendered unclassified)_"
    L.append(f"- CEX class: **{disp}**{note}")
    L.append("")

    L.append("## Resolution")
    if row.get("resolved"):
        won = row.get("outcome_for_side")
        verdict = "WON" if won == 1 else ("LOST" if won == 0 else "—")
        L.append(f"- Market resolved {_d(row.get('resolution_ts'))}; flagged side **{verdict}**.")
    else:
        L.append("- Unresolved.")
    L.append("")

    prov = _loads(row.get("provenance"), None)
    L.append("## Provenance (Rule 1)")
    L.append(f"- {prov if prov else '—'}")
    if row.get("label"):
        L.append(f"\n**Label:** {row['label']}")
    L.append("")
    L.append("---")
    L.append(FOOTER)
    return "\n".join(L)


def render_pdf(row: dict[str, Any], out_path: str, *, cex_floor: float = CEX_CONFIDENCE_FLOOR) -> str:
    """Best-effort PDF via the hoisted abelard_common.render toolkit. Raises a clear
    error if that toolkit is not importable (Markdown is the default surface)."""
    try:
        from abelard_common.render import build_pdf, default_styles, section_box  # type: ignore
    except Exception as e:  # pragma: no cover - depends on the render hoist landing
        raise RuntimeError(
            "PDF rendering needs abelard_common.render (the hoisted ReportLab toolkit); "
            f"it is not importable here ({e}). Use render_markdown, or land the render hoist."
        ) from e
    from reportlab.platypus import Paragraph  # type: ignore
    styles = default_styles()
    md = render_markdown(row, cex_floor=cex_floor)
    story = [section_box("Dossier", [Paragraph(line, styles["body"]) for line in md.splitlines() if line])]
    build_pdf(out_path, story, title=f"Dossier {row.get('dossier_id', '')}")
    return out_path
