"""M10 — live UNUSUAL_ACTIVITY scan (Detector B).

An on-command scan over the L2 tape that surfaces fresh-wallet informed-money
footprints as DOSSIERS for human review (spec §M10 + docs/m10_build_plan.md).

Detector B is kept strictly separate from Detector A (consensus). Non-negotiables
carried from the spec, enforced here:
  - NO EV, ever. No staging path. A dossier is an intelligence product, not a
    trade signal (permanently excluded from M9).
  - Fill-factors first (free, from the tape); chain enrichment is GATED to the
    handful of wallets past the fill-factor bar (v1.6 §3.3), bounded per scan.
  - Latency ELEVATES, never gates (v1.5 §3): it can only add lift to a wallet
    already past the bar; absent/errored/slow latency leaves the score intact.
  - Cluster membership is recorded as evidence, never scored (v1.3 §3.2).
  - Every datum traces to a cached raw record; missing/failed enrichment is
    declared, never imputed (Rule 1).

Read-only over the tape: this opens TapeStore for reads only and never writes.

v1 scope (documented follow-ups, per the build plan §7):
  - The F (freshness) fill-factor needs a LIVE wallet first-seen; m0f.enrich_wallet
    sources it from the FROZEN L1 subgraph, so it is unusable for a live scan. The
    free bar here scores on S/D/C only, and the funded->bet latency elevator is
    the informed-timing signal. A live first-seen source (data-api /activity or
    chain age) is the follow-up that re-activates F.
  - Tier latching is computed in-memory per scan (no cross-scan persistence yet,
    which would require a latch store; kept out to preserve read-only). The
    dossier's latch therefore reflects this scan's high-water mark.
"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

from .errors import DataLayerError
from .m0f import (
    Fill,
    apply_cluster_amplifier,
    assign_tiers,
    latch_tiers,
    score_candidates_as_of,
    trailing_volumes,
)
from .m5 import classify_funder, wallet_funding_latency

_DAEMON = "consensus_m10"
_SCHEMA = 1
_SKEW_S = 300  # bound the window's upper edge above wall-clock skew / corrupt ts
_TRAILING_DAYS = 7

_CAVEAT = (
    "Anomaly detection over public on-chain data; NOT a validated trade signal; "
    "NOT an allegation about any person. Calibration is n=1 event (Feb-28), n=6 "
    "labels — thresholds calibrated once, not validated. No EV is estimated."
)


def _now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def _build_scoring_cfg(loaded: Any, m10: Any) -> SimpleNamespace:
    """A scoring cfg for score_candidates_as_of: every M0-F field (the scorer
    reads a flat cfg) with M10's overrides. The scorer filters fills only by
    ``as_of`` (never the M0-F study window), so copying the M0-F config is safe."""
    m0f_cfg = loaded.config.m0f
    attrs = {f: getattr(m0f_cfg, f) for f in type(m0f_cfg).model_fields}
    attrs["size_floor_usdc"] = m10.size_floor_usdc
    attrs["tier_thresholds"] = m10.tier_thresholds
    attrs["cluster_window_hours"] = m10.cluster_window_hours
    attrs["cross_market_enabled"] = True     # record cross-market membership...
    attrs["cluster_boosts_score"] = False    # ...but never let it move the score (v1.3)
    if m10.factor_weights:
        attrs["factor_weights"] = m10.factor_weights
    return SimpleNamespace(**attrs)


def _row_to_fill(r: dict[str, Any]) -> Fill | None:
    """Adapt an l2_trades row to an M0-F Fill. usdc is not stored -> size*price.
    Rows missing an identifying field are skipped (never fabricated)."""
    if not (r.get("proxy_wallet") and r.get("condition_id") and r.get("asset")
            and r.get("side") and r.get("timestamp") is not None
            and r.get("size") is not None and r.get("price") is not None):
        return None
    return Fill(
        wallet=r["proxy_wallet"],
        condition_id=r["condition_id"],
        token_id=r["asset"],
        side=r["side"],
        usdc=r["size"] * r["price"],
        tokens=r["size"],
        price=r["price"],
        timestamp=r["timestamp"],
        event_id=r.get("transaction_hash") or "",
    )


def _in_scope(market: dict[str, Any], m10: Any) -> bool:
    tags = market.get("tags") or ""
    return tags not in set(m10.excluded_categories)


def _latency_boost(wf: Any, m10: Any) -> float:
    """Elevator (v1.5 §3): multiplicative lift, never suppression. None/error/slow
    -> 1.0 (score intact). A CEX-funded fast bet is far less discriminating, so
    its lift is discounted."""
    if wf is None or wf.error is not None or wf.latency_s is None:
        return 1.0
    if wf.latency_s > m10.latency_tight_minutes * 60:
        return 1.0
    boost = m10.latency_elevator_boost
    if wf.funder_kind == "cex":
        boost = 1.0 + (boost - 1.0) * 0.5
    return boost


def _gap_index(window_gaps: list[dict[str, Any]]) -> tuple[dict[str, list[str]], list[str]]:
    """Split overlapping declared gaps into per-market and global-lane reasons."""
    per_market: dict[str, list[str]] = {}
    global_reasons: list[str] = []
    for g in window_gaps:
        if g.get("condition_id") is None:
            global_reasons.append(g["reason"])
        else:
            per_market.setdefault(g["condition_id"], []).append(g["reason"])
    return per_market, global_reasons


def _dossier(c: Any, per_market_gaps: dict[str, list[str]], global_gaps: list[str]) -> dict[str, Any]:
    fund = c.notes.get("funding") or {}
    caveats = list(global_gaps) + per_market_gaps.get(c.condition_id, [])
    return {
        "wallet": c.wallet,
        "market": c.condition_id,
        "token_id": c.token_id,
        "tier": c.tier,
        "composite": round(c.composite, 4),
        "composite_pre_elevator": round(c.notes.get("composite_pre_elevator", c.composite), 4),
        "latency_boost": round(c.notes.get("latency_boost", 1.0), 3),
        "factors": {k: round(v, 4) for k, v in (c.factors or {}).items()},
        "factors_active": list(c.factors_active or []),
        "net_stake_usdc": round(c.net_stake_usdc, 2),
        "vwap_entry": round(c.vwap_entry, 4),
        "first_bet_ts": c.first_bet_ts,
        "last_bet_ts": c.last_bet_ts,
        "enriched": bool(c.notes.get("funding") is not None and "funding" in c.notes),
        "funding": {
            "latency_s": fund.get("latency_s"),
            "funder": fund.get("funder"),
            "funder_kind": fund.get("funder_kind"),
            "enrichment_error": fund.get("error"),
        },
        "clusters": list(c.cluster_ids or []),
        "latch": c.notes.get("latch"),
        "data_incomplete": c.data_incomplete,
        "coverage_caveats": caveats,   # declared gaps overlapping this window/market
        "caveat": _CAVEAT,
        # NO EV — Detector B never estimates expected value.
    }


def run_scan(
    dl: Any, loaded: Any, *, lookback_hours: int | None = None, max_wallets: int | None = None,
) -> dict[str, Any]:
    """One on-command M10 scan of the recent L2 window. Returns an
    orchestrator-facing envelope (``result.dossiers`` is the human payload)."""
    from .tape import TapeStore

    m10 = loaded.config.m10
    # Explicit None checks (not `or`): an override of 0 means 0, not the default.
    lookback = lookback_hours if lookback_hours is not None else m10.unusual_lookback_hours
    max_w = max_wallets if max_wallets is not None else m10.enrichment_max_wallets_per_scan
    scoring_cfg = _build_scoring_cfg(loaded, m10)
    started = _now_ts()
    errors: list[str] = []

    tape = TapeStore(loaded.tape_path)
    try:
        newest = tape.newest_fill_ts()
        as_of = min(started, newest) if isinstance(newest, int) else started
        hi = started + _SKEW_S
        lo = as_of - lookback * 3600
        tracked = {m["condition_id"] for m in tape.markets(active_only=False)
                   if _in_scope(m, m10)}
        rows = tape.fills_in_window(lo_ts=lo, hi_ts=hi, condition_ids=tracked, parsed_only=True)
        window_gaps = tape.gaps_overlapping(lo_ts=lo, hi_ts=hi, condition_ids=tracked)
    finally:
        tape.close()

    per_market_gaps, global_gaps = _gap_index(window_gaps)
    fills = [f for f in (_row_to_fill(r) for r in rows) if f is not None]

    def _envelope(candidates: list[Any], enriched: int) -> dict[str, Any]:
        surfaced = sorted(
            (c for c in candidates if c.tier != "NONE"),
            key=lambda c: c.composite, reverse=True,
        )
        status = "degraded" if (errors or window_gaps) else "ok"
        return {
            "daemon": _DAEMON,
            "schema": _SCHEMA,
            "status": status,
            "started_ts": started,
            "finished_ts": _now_ts(),
            "result": {
                "window": {"lookback_hours": lookback, "lo_ts": lo, "hi_ts": hi, "as_of": as_of},
                "fills_scanned": len(fills),
                "candidates_scored": len(candidates),
                "enriched": enriched,
                "dossiers": [_dossier(c, per_market_gaps, global_gaps) for c in surfaced],
                "tier_counts": {
                    t: sum(1 for c in candidates if c.tier == t)
                    for t in ("CRITICAL", "ELEVATED", "WATCH", "INSUFFICIENT_DATA")
                },
                "declared_gaps": window_gaps,
            },
            "errors": errors,
            "caveat": _CAVEAT,
        }

    if not fills:
        return _envelope([], 0)

    trailing = trailing_volumes(fills, as_of=as_of, days=_TRAILING_DAYS)
    candidates = score_candidates_as_of(
        as_of=as_of, fills=fills, crossing_usdc={}, wallet_info={},
        market_trailing_vol=trailing, cfg=scoring_cfg,
    )

    # v1.6 §3.3 enrichment gate: only wallets past the fill-factor bar, bounded.
    bar = float(m10.tier_thresholds.get("ELEVATED", 0.0))
    to_enrich = sorted(
        (c for c in candidates if c.composite >= bar),
        key=lambda c: c.composite, reverse=True,
    )[:max_w]

    funder_cache: dict[str, Any] = {}
    enriched = 0
    for c in to_enrich:
        try:
            wf = wallet_funding_latency(dl, c.wallet, first_bet_ts=c.first_bet_ts)
        except DataLayerError as exc:
            # A hard fetch failure is a declared gap, not an imputed latency.
            errors.append(exc.to_error())
            c.notes["funding"] = {"latency_s": None, "funder": None,
                                  "funder_kind": None, "error": exc.to_error()}
            enriched += 1
            continue
        if wf.error is None and wf.funder and wf.funder_kind is None:
            fk = funder_cache.get(wf.funder)
            if fk is None:
                try:
                    fk = classify_funder(
                        dl, wf.funder, cex_fanout_threshold=loaded.config.m5.cex_fanout_threshold)
                except DataLayerError as exc:
                    errors.append(exc.to_error())
                    fk = None
                funder_cache[wf.funder] = fk
            if fk is not None:
                wf = replace(wf, funder_kind=fk.kind)
        c.notes["funding"] = {
            "latency_s": wf.latency_s, "funder": wf.funder,
            "funder_kind": wf.funder_kind, "error": wf.error,
        }
        if wf.error:
            errors.append(wf.error)
        # Elevator: lift only; absent/errored/slow -> 1.0 (fill-factor score stands).
        c.notes["composite_pre_elevator"] = c.composite
        boost = _latency_boost(wf, m10)
        c.composite *= boost
        c.notes["latency_boost"] = boost
        enriched += 1

    # Cluster membership as evidence (never scores — cluster_boosts_score False).
    apply_cluster_amplifier(candidates, cfg=scoring_cfg, elevated_floor=bar)
    # Tiers on the (elevator-lifted) composite; cluster never elevates a tier.
    assign_tiers(candidates, m10.tier_thresholds, cluster_elevates=False)
    # Latch high-water mark (in-memory; cross-scan persistence is a follow-up).
    latch = latch_tiers({}, candidates, as_of=as_of)
    for c in candidates:
        entry = latch.get((c.wallet, c.condition_id))
        if entry:
            c.notes["latch"] = entry

    return _envelope(candidates, enriched)


def render_dossier_human(summary: dict[str, Any]) -> str:
    r = summary.get("result", {})
    w = r.get("window", {})
    tc = r.get("tier_counts", {})
    lines = [
        f"CONSENSUS M10 UNUSUAL_ACTIVITY scan (Detector B)  [{summary.get('status')}]",
        f"  window : last {w.get('lookback_hours')}h  as_of={w.get('as_of')}  "
        f"({r.get('fills_scanned')} fills, {r.get('candidates_scored')} candidates, "
        f"{r.get('enriched')} enriched)",
        f"  tiers  : CRITICAL {tc.get('CRITICAL', 0)} / ELEVATED {tc.get('ELEVATED', 0)} / "
        f"WATCH {tc.get('WATCH', 0)} / insufficient {tc.get('INSUFFICIENT_DATA', 0)}",
    ]
    if r.get("declared_gaps"):
        lines.append(f"  COVERAGE: {len(r['declared_gaps'])} declared gap(s) overlap the window "
                     "— dossiers below are annotated; scan is not complete across them.")
    if not r.get("dossiers"):
        lines.append("  (no footprints surfaced — expected in a normal week; value shows on an event)")
    for d in r.get("dossiers", []):
        fund = d.get("funding", {})
        lat = fund.get("latency_s")
        lat_txt = (f"{lat}s" if lat is not None
                   else ("enrich-error" if fund.get("enrichment_error") else "no-funding"))
        lines.append(
            f"  [{d['tier']}] {d['wallet'][:10]}.. mkt={d['market'][:12]}.. "
            f"score={d['composite']} (pre-elev {d['composite_pre_elevator']}, x{d['latency_boost']}) "
            f"net=${d['net_stake_usdc']:.0f} funder={fund.get('funder_kind')} latency={lat_txt}"
            + (f" clusters={d['clusters']}" if d['clusters'] else "")
            + (f"  ⚠ {d['coverage_caveats']}" if d['coverage_caveats'] else "")
        )
    lines.append(f"  NOTE: {_CAVEAT}")
    return "\n".join(lines)
