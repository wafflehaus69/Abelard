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

Scope notes:
  - Factor F (freshness) is RESTORED for the live scan (M10-D §3.1): the fill-factor
    bar is still scored on S/D/C only — that is the v1.5 §4 enrichment gate — and only
    the wallets past it get a live first-seen pull (consensus.firstseen), after which
    the scorer re-runs with F active for them. A failed lookup declares F unavailable
    for that wallet, never imputed.
  - Tier latching is computed in-memory per scan; the DURABLE cross-scan high-water
    mark lives in the Dossier Store's ``tier_peak`` column (M10-D §3.2), written when
    ``store_path`` is set.
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
    attrs["cross_market_scope_id"] = "m10-target-set"  # not the M0-F 'iran-cluster'
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


def _is_fast_funded(wf: Any, m10: Any) -> bool | None:
    """v1.16 §1: the funded->bet latency as a REPORTED FLAG, not a multiplier.

    True  = funded and bet inside ``latency_tight_minutes`` by a purpose-built
            (dedicated) funder — the discriminating case.
    False = measured, but not that shape (slow, or a CEX/infra/unclassified funder,
            which v1.7 §1 says must never be treated as dedicated).
    None  = NOT MEASURED (absent or errored lookup) — declared, never imputed to False,
            because "no evidence of fast funding" and "we could not look" are different
            facts and only one of them is informative.
    """
    if wf is None or wf.error is not None or wf.latency_s is None:
        return None
    if wf.latency_s > m10.latency_tight_minutes * 60:
        return False
    return wf.funder_kind == "dedicated"


def collapse_actors(member_funding: dict[str, dict[str, Any] | None]) -> int | None:
    """Funding-mesh collapse (v1.9 §0.3 / v1.15 §4.2 / v1.16 §2): how many ACTORS are
    behind a set of co-trading wallets?

    ``member_funding`` maps wallet -> its funding record (``funder``/``funder_kind``),
    or None if that wallet was never enriched. Returns the post-collapse actor count,
    or **None = UNRESOLVED** when any member's funding is unknown — because a partial
    collapse could only ever UNDER-count actors, i.e. overstate coordination, which is
    the one direction this invariant exists to prevent. Unresolved is never "20".

    Collapse rule: wallets sharing a DEDICATED (purpose-built) funder are one actor. A
    ``cex`` funder is a shared hot wallet used by thousands of unrelated people and must
    NOT link anyone; ``nonpersonal`` (infra) likewise; ``unknown`` means the classifier
    failed, and per v1.8 §4.3 a low-confidence verdict never hardens into a link. Every
    wallet in those three cases counts as its own actor.
    """
    if not member_funding:
        return None
    dedicated_funders: set[str] = set()
    standalone = 0
    for _wallet, fund in member_funding.items():
        if not fund or fund.get("error") or not fund.get("funder_kind"):
            return None                      # incomplete -> UNRESOLVED (Rule 1)
        if fund["funder_kind"] == "dedicated" and fund.get("funder"):
            dedicated_funders.add(fund["funder"])
        else:
            standalone += 1                  # cex / nonpersonal / unknown: links no one
    return len(dedicated_funders) + standalone


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
        # Fill-factor composite ONLY — latency never moves it (v1.16 §1).
        "composite": round(c.composite, 4),
        "fast_funded": c.notes.get("fast_funded"),      # True / False / None=not measured
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
    firstseen: Any = True, store_path: str | None = None, as_of_ts: int | None = None,
) -> dict[str, Any]:
    """One on-command M10 scan of the recent L2 window. Returns an
    orchestrator-facing envelope (``result.dossiers`` is the human payload).

    ``firstseen``: True (default) restores factor F on the gated set via the live
    resolver (M10-D §3.1); a callable injects a resolver (tests); False disables it
    (the pre-M10-D S/D/C-only behaviour).
    ``store_path``: when set, every candidate above the capture floor is persisted to
    the Dossier Store (M10-D §3.2). CAPTURE WIDE: persistence is deliberately wider
    than the envelope's surfaced (tier != NONE) set — narrowing is a query/render
    concern, never a capture one."""
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
        if as_of_ts is not None:
            # Replay a HISTORICAL window (alert-rate calibration / backtest). Nothing
            # after as_of is loaded, so the scan sees exactly what a scheduled run at
            # that moment would have seen — no lookahead.
            as_of = int(as_of_ts)
        hi = (as_of if as_of_ts is not None else started) + _SKEW_S
        scan_lo = as_of - lookback * 3600
        # Load the WIDER of the scan window and the factor-S trailing baseline so
        # trailing volume is a true _TRAILING_DAYS window, not truncated to the
        # (often shorter) lookback — a short baseline inflates S and the composite
        # (review 2026-07-20). Candidate extraction stays gated to the scan window.
        load_lo = min(scan_lo, as_of - _TRAILING_DAYS * 86400)
        in_scope = [m for m in tape.markets(active_only=False) if _in_scope(m, m10)]
        tracked = {m["condition_id"] for m in in_scope}
        # Market metadata for the dossier store (question/category/slug/resolution).
        market_meta = {
            m["condition_id"]: {
                "question": m.get("question"), "category": m.get("tags"),
                "slug": m.get("slug"), "resolution": m.get("resolution"),
            }
            for m in in_scope
        }
        rows = tape.fills_in_window(lo_ts=load_lo, hi_ts=hi, condition_ids=tracked, parsed_only=True)
        window_gaps = tape.gaps_overlapping(lo_ts=scan_lo, hi_ts=hi, condition_ids=tracked)
    finally:
        tape.close()

    per_market_gaps, global_gaps = _gap_index(window_gaps)
    loaded_fills = [f for f in (_row_to_fill(r) for r in rows) if f is not None]
    # Candidates from the scan window; factor-S trailing baseline from the full
    # loaded (>= _TRAILING_DAYS) span (trailing_volumes filters to its own window).
    fills = [f for f in loaded_fills if f.timestamp >= scan_lo]

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
                "window": {"lookback_hours": lookback, "lo_ts": scan_lo, "hi_ts": hi, "as_of": as_of},
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

    # trailing baseline over the full loaded span (>= _TRAILING_DAYS); it filters
    # to [as_of - days, as_of] internally, so the S denominator is a true 7-day vol.
    trailing = trailing_volumes(loaded_fills, as_of=as_of, days=_TRAILING_DAYS)
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

    # --- M10-D §3.1: restore factor F (freshness) on the gated set --------------
    # The bar above is scored WITHOUT F (S/D/C only), exactly as v1.5 §4 requires:
    # enrichment is gated behind the fill-factor bar. Only those wallets get a live
    # first-seen pull; the scorer then re-runs with F active for them. A failed
    # lookup leaves that wallet's F unavailable (Rule 1) — never imputed.
    firstseen_meta: dict[str, dict[str, Any]] = {}
    if firstseen and to_enrich:
        from .firstseen import resolve_first_seen

        resolver = firstseen if callable(firstseen) else resolve_first_seen
        wallet_info: dict[str, dict[str, Any]] = {}
        for c in {c.wallet: c for c in to_enrich}.values():
            try:
                # before_ts lets the resolver MEASURE prior activity during the same
                # walk, so the prior-fills freshness discount is driven by data rather
                # than by a hardcoded 0 (which read as "brand new" and disabled it).
                fsr = resolver(dl, c.wallet, before_ts=c.first_bet_ts)
            except Exception as exc:  # noqa: BLE001 - declared, never imputed
                errors.append(f"first-seen {c.wallet[:10]}..: {exc}")
                firstseen_meta[c.wallet] = {"ts": None, "source": "unavailable"}
                continue
            firstseen_meta[c.wallet] = {"ts": fsr.ts, "source": fsr.source,
                                        "min_age_days": fsr.min_age_days}
            if fsr.available:
                info: dict[str, Any] = {"first_seen_ts": fsr.ts}
                if fsr.prior_fills is not None:
                    info["prior_fills"] = fsr.prior_fills   # measured, not assumed
                wallet_info[c.wallet] = info
            elif fsr.min_age_days:
                # CAPPED = the wallet's history exceeds the data-api reach, which is
                # itself positive evidence that it is ESTABLISHED. Place F/T from that
                # lower bound on age instead of omitting them: omitting makes m0f
                # renormalise the geometric mean over S/D/C, which IMPUTES freshness
                # (m0f forbids exactly this) and inflates the least-verifiable wallets
                # to CRITICAL. True age >= min_age, and F falls with age, so scoring at
                # min_age is a conservative upper bound on F, not an invention.
                wallet_info[c.wallet] = {
                    "first_seen_ts": int(c.first_bet_ts - fsr.min_age_days * 86400)}
            else:
                # Nothing known: declare it. m0f's data_incomplete guard then assigns
                # INSUFFICIENT_DATA and bars a real tier (Rule 1) — never CRITICAL by
                # imputation.
                wallet_info[c.wallet] = {"error": f"first_seen unavailable ({fsr.source})"}
        if wallet_info:
            # Re-score once with F active for the gated wallets; others unchanged.
            candidates = score_candidates_as_of(
                as_of=as_of, fills=fills, crossing_usdc={}, wallet_info=wallet_info,
                market_trailing_vol=trailing, cfg=scoring_cfg,
            )
            keys = {(c.wallet, c.condition_id, c.token_id) for c in to_enrich}
            to_enrich = sorted(
                (c for c in candidates if (c.wallet, c.condition_id, c.token_id) in keys),
                key=lambda c: c.composite, reverse=True,
            )

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
        # v1.16 §1: latency is a REPORTED FACT, never a score multiplier. It is NOT
        # multiplied into the composite and NEVER affects the tier. Multiplying it in
        # (a) broke the [0,1] scale the tiers are defined on (composites reached 1.27,
        # making every threshold in the 0.75-0.99 gap inert) and (b) turned the elevator
        # into a gate — a wallet cleared CRITICAL on latency almost regardless of its
        # fill factors. That inverts v1.5 §3 and is adversarially backwards: latency is
        # the CHEAPEST factor to fake (fund early, wait), while the fill factors cost
        # real capital. The human weighs the flag; the score does not move.
        c.notes["fast_funded"] = _is_fast_funded(wf, m10)
        c.notes["latency_s"] = None if wf is None else wf.latency_s
        enriched += 1

    # Cluster membership as evidence (never scores — cluster_boosts_score False).
    apply_cluster_amplifier(candidates, cfg=scoring_cfg, elevated_floor=bar)

    # v1.16 §2: funding-mesh COLLAPSE. Uses the funder data the latency pull already
    # fetched — no extra network. Only wallets past the enrichment gate have funding,
    # so a cluster with any unenriched member resolves to None (UNRESOLVED), which is
    # the declared outcome of keeping the enrichment cap (v1.16 owner ruling (a)).
    # The actor count MUST be computed over exactly the roster that gets persisted.
    # (Earlier this took the MIN across a wallet's clusters while persisting the UNION
    # of their wallets — so a resolved sibling cluster's count could be stamped onto an
    # unresolved 20-wallet roster, manufacturing a "20 -> 1 collapse" never computed.)
    # One roster, one number: collapse over the union, and if any member of that union
    # is unenriched the whole thing is UNRESOLVED.
    members_by_cluster: dict[str, set[str]] = {}
    for c in candidates:
        for cid_ in (c.cluster_ids or []):
            members_by_cluster.setdefault(cid_, set()).add(c.wallet)
    funding_by_wallet = {c.wallet: c.notes.get("funding") for c in candidates}
    for c in candidates:
        if not c.cluster_ids:
            c.notes["actor_count"] = None
            continue
        union = {c.wallet}
        for cid_ in c.cluster_ids:
            union |= members_by_cluster.get(cid_, set())
        c.notes["cluster_roster"] = sorted(union)
        c.notes["actor_count"] = collapse_actors(
            {w: funding_by_wallet.get(w) for w in union})
    # Tiers on the (elevator-lifted) composite; cluster never elevates a tier.
    assign_tiers(candidates, m10.tier_thresholds, cluster_elevates=False)
    # Latch high-water mark (in-memory per scan; the Dossier Store latches ACROSS
    # scans at the tier_peak column — that is where the durable high-water lives).
    latch = latch_tiers({}, candidates, as_of=as_of)
    for c in candidates:
        entry = latch.get((c.wallet, c.condition_id))
        if entry:
            c.notes["latch"] = entry

    envelope = _envelope(candidates, enriched)
    if store_path:
        try:
            written = _persist(candidates, store_path, market_meta=market_meta,
                               firstseen_meta=firstseen_meta, scan_ts=started, m10=m10,
                               tape_path=str(loaded.tape_path))
            envelope["result"]["stored"] = written
        except Exception as exc:  # noqa: BLE001 - a store failure must not lose the scan
            errors.append(f"dossier store write failed: {exc}")
            envelope["status"] = "degraded"
    return envelope


def _persist(candidates: list[Any], store_path: str, *, market_meta: dict[str, Any],
             firstseen_meta: dict[str, Any], scan_ts: int, m10: Any,
             tape_path: str = "") -> dict[str, int]:
    """Write the scan into the Dossier Store (M10-D §3.2). Capture floor is the
    scan's own size floor — wide by design; the render/query layers narrow."""
    from . import dossier_store as ds

    con = ds.connect(store_path)
    try:
        # §4.2: recover the cluster ROSTER the scan computed. apply_cluster_amplifier
        # tags each candidate with the cluster ids it belongs to; invert that to get
        # co-members. Hardcoding [self] threw this away and made the cluster surface
        # (and the cluster alert arm) structurally dead.
        members: dict[str, set[str]] = {}
        for c in candidates:
            for cid_ in (c.cluster_ids or []):
                members.setdefault(cid_, set()).add(c.wallet)
        recs = []
        for c in candidates:
            if c.net_stake_usdc < float(m10.size_floor_usdc):
                continue
            meta = market_meta.get(c.condition_id, {})
            fs = firstseen_meta.get(c.wallet, {})
            fund = c.notes.get("funding") or {}
            recs.append({
                "wallet": c.wallet, "condition_id": c.condition_id, "token_id": c.token_id,
                "side": "BUY",  # candidates are net-long footprints
                "market_question": meta.get("question"), "market_category": meta.get("category"),
                "event_slug": meta.get("slug"),
                "first_seen_ts": fs.get("ts"),
                "first_seen_source": fs.get("source") or ("not_gated" if not fs else None),
                "detection_ts": c.first_bet_ts, "entry_vwap": c.vwap_entry,
                # NOT a copy of entry_vwap: the market price at the detection instant is
                # a different observation and this scan does not measure it. Declared
                # unknown (Rule 1) rather than duplicated — the renderer presents the two
                # as independent facts, so a silent copy would fabricate corroboration.
                "price_at_detection": None,
                # §4.4 — these are TWO DIFFERENT FACTS and must never be the same
                # number. The scan aggregates fills with NO price filter (m0f filters
                # on size/directionality only), so net_stake is the HEADLINE position
                # over the scan window. The contested (0.10-0.90) slice is NOT measured
                # here, so it is DECLARED UNKNOWN rather than imputed from the headline
                # (Rule 1) — an earlier comment claimed an upstream gate that does not
                # exist, which collapsed the carry-trade confound the product exists to
                # keep visible.
                "contested_notional": None,
                "headline_notional": c.net_stake_usdc,
                "f_factor": (c.factors or {}).get("F"), "s_factor": (c.factors or {}).get("S"),
                "d_factor": (c.factors or {}).get("D"), "c_factor": (c.factors or {}).get("C"),
                # v1.16 §1: latency is no longer a score factor at all. The column now
                # carries the measured funded->bet seconds (a fact), NULL when the
                # lookup failed or was never made — never a neutral stand-in.
                "latency_factor": (c.notes.get("latency_s")
                                   if fund and not fund.get("error") else None),
                "composite": c.composite, "tier": c.tier,
                "cluster_id": (sorted(c.cluster_ids)[0] if c.cluster_ids else None),
                # Raw roster (§4.2 reports raw count AND post-collapse count). This is
                # the SAME union the actor count was computed over — the two must
                # describe one wallet set or the pair is a fabrication.
                "cluster_wallets": c.notes.get("cluster_roster") or [c.wallet],
                # Post-collapse ACTOR count from the funding-mesh collapse (v1.16 §2).
                # None = UNRESOLVED (a cluster member was never enriched, so the mesh
                # could not be computed) — never silently 1, and never the raw wallet
                # count. A solo footprint with resolved funding is genuinely 1 actor.
                "actor_count_post_collapse": (
                    c.notes.get("actor_count")
                    if c.cluster_ids
                    else (1 if (fund and fund.get("funder_kind") and not fund.get("error"))
                          else None)),
                "cross_market_cluster": (sorted(c.cluster_ids) if c.cluster_ids else None),
                "funding_summary": ({"funder": fund.get("funder"),
                                     "latency_s": fund.get("latency_s"),
                                     "error": fund.get("error")} if fund else None),
                "cex_class": fund.get("funder_kind"),
                # Confidence is not yet emitted per-classification; absent => the
                # renderer's floor makes it 'unclassified' (§4.5 fail-safe).
                "cex_confidence": None,
                "provenance": {"scan_ts": scan_ts, "source": "m10_scan",
                               "data_incomplete": bool(c.data_incomplete),
                               # Rule 1: the raw records this row was derived from.
                               "tape_path": tape_path,
                               "condition_id": c.condition_id, "token_id": c.token_id,
                               "wallet": c.wallet,
                               "fill_window": [c.first_bet_ts, c.last_bet_ts],
                               "first_seen_source": fs.get("source")},
            })
        return ds.write_scan(con, recs, scan_ts=scan_ts)
    finally:
        con.close()


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
        ff = d.get("fast_funded")
        ff_txt = " FAST-FUNDED" if ff else ("" if ff is False else " latency-unmeasured")
        lines.append(
            f"  [{d['tier']}]{ff_txt} {d['wallet'][:10]}.. mkt={d['market'][:12]}.. "
            f"score={d['composite']} "
            f"net=${d['net_stake_usdc']:.0f} funder={fund.get('funder_kind')} latency={lat_txt}"
            + (f" clusters={d['clusters']}" if d['clusters'] else "")
            + (f"  ⚠ {d['coverage_caveats']}" if d['coverage_caveats'] else "")
        )
    lines.append(f"  NOTE: {_CAVEAT}")
    return "\n".join(lines)
