"""M5 — funding provenance, corrected to funded→bet LATENCY (addendum v1.4).

The Feb-28 funding experiment (v1.3 §4.3) returned a load-bearing correction:

  - The naive coordination signal FAILED. The three confirmed insiders share a
    funder, but that funder is a CEX hot wallet fanning out to ~1,700 addresses;
    shared-CEX-funding carries almost no linkage information. So funding-linkage
    is NOT the strongest de-dup edge in the common case (v1.4 §2).
  - What survived is a TIMING signal: funded→first-bet latency of 20s / 80s /
    7min across the insiders — a purpose-built-wallet signature (capital
    arrives, bet fires immediately). This is the true, chain-derived form of
    factor T; the M0-F proxy (first-seen→bet) was ~0 for all fresh wallets and
    did not discriminate.

This module therefore builds two things:

  1. A CEX/hot-wallet classifier (v1.4 §2.1): fan-out degree (distinct
     outbound recipients) + a known-CEX address list. A funding EDGE (for
     de-dup or attribution) may only be drawn on a NON-exchange funder; a
     shared CEX funder draws no edge.
  2. The funded→bet-latency factor: per wallet, the last inbound stablecoin
     transfer before its first bet in the target market, and the latency.

The milestone deliverable is NOT the three positives — it is the FALSE-POSITIVE
CURVE (v1.4 §3.3): at the latency threshold that catches the confirmed insiders,
how many non-insider war-week wallets also fire? That base rate is the whole
result, and it decides whether latency is a real detector or merely correlates
with "new user in an exciting week". Every chain response is cached (Rule 1).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .config import LoadedConfig
from .errors import DataLayerError
from .fetching import DataLayer
from .sources_polygon import get_erc20_transfers

# Stablecoin symbols that count as "funding" a Polymarket wallet.
_STABLES = {"USDC", "USDC.E", "USDT", "DAI", "PUSD"}

# Known Polymarket infrastructure whose stablecoin sends are trade SETTLEMENT,
# not personal funding. Seed only — the robust exclusion is behavioural (any
# address the wallet also sent tokens TO is a bidirectional trading counterparty,
# not a one-directional deposit source), so an unseeded settlement route is
# still caught. Addresses observed 2026-07 on Polygon.
_KNOWN_NONPERSONAL = {
    "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e",  # CTFExchange-era counterparty
    "0x4d97dcd97ec945f40cf65f87097ace5ea0476045",  # ConditionalTokens (CTF)
    "0xe111180000d2663c0091e4f400237545b87b996b",  # NegRisk exchange (post-Apr migration)
    "0xe2222d279d744050d28e00520010520000310f59",  # NegRisk adapter/exchange
    "0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb",  # pUSD-era settlement token route
}

# Below this USDC amount an inbound transfer is dust, not "the funding" — a
# dust transfer arriving just before a bet must not fabricate a short latency.
_FUNDING_DUST_FLOOR_USDC = 10.0


@dataclass(frozen=True)
class FunderClass:
    address: str
    kind: str          # 'cex' | 'dedicated' | 'nonpersonal' | 'unknown'
    fanout: int | None  # distinct outbound recipients observed (None if not probed)


@dataclass(frozen=True)
class WalletFunding:
    """Funded→bet result for one wallet. ``latency_s`` is None when no external
    inbound funding precedes the first bet — a declared gap, never imputed.
    ``error`` distinguishes a FETCH FAILURE (unknown, excluded from the FP
    curve) from a genuine no-funding result (a confident non-firer)."""

    wallet: str
    first_bet_ts: int
    funded_ts: int | None
    latency_s: int | None
    funder: str | None
    funder_kind: str | None
    inbound_count: int
    error: str | None = None


def classify_funder(
    dl: DataLayer, address: str, *, cex_fanout_threshold: int = 400
) -> FunderClass:
    """Classify a funding source. Cheap first discriminant is fan-out degree —
    an exchange hot wallet pays out to hundreds/thousands of distinct addresses;
    a personal wallet does not. A known-nonpersonal address short-circuits."""
    addr = address.lower()
    if addr in _KNOWN_NONPERSONAL:
        return FunderClass(address=addr, kind="nonpersonal", fanout=None)
    try:
        tx = get_erc20_transfers(dl, addr)
    except DataLayerError:
        return FunderClass(address=addr, kind="unknown", fanout=None)
    # Distinct EXTERNAL recipients: exclude self-sends and known infra so the
    # fan-out is real payout breadth, not artifacts.
    recipients = {t.to_addr for t in tx
                  if t.from_addr == addr and t.to_addr != addr
                  and t.to_addr not in _KNOWN_NONPERSONAL}
    fanout = len(recipients)
    # Fan-out is a lower bound (the tokentx pull is capped), so once it already
    # exceeds the threshold the CEX verdict is certain; below it, "dedicated"
    # is provisional on the observed window.
    kind = "cex" if fanout >= cex_fanout_threshold else "dedicated"
    return FunderClass(address=addr, kind=kind, fanout=fanout)


def wallet_funding_latency(
    dl: DataLayer, wallet: str, *, first_bet_ts: int
) -> WalletFunding:
    """Last EXTERNAL inbound funding transfer strictly before the wallet's first
    bet, and the funded→bet latency.

    Three review-hardened rules make "funding" mean a deposit, not trade
    proceeds:
      - ``sort='desc'`` so the transfers NEAREST the first bet are retained even
        when the wallet's history exceeds the provider record cap (an ``asc``
        pull would keep only the oldest and truncate the real funding away).
      - A funder must be ONE-DIRECTIONAL: any address the wallet also SENT
        tokens to is a bidirectional trading/settlement counterparty (the CTF /
        neg-risk exchange), not a deposit source — excluded behaviourally so an
        unseeded settlement route is still caught. Seeded infra is excluded too.
      - A dust inbound (< ``_FUNDING_DUST_FLOOR_USDC``) cannot be "the funding".

    ``error`` is set on a FETCH FAILURE (rate limit / transport / replay miss) —
    distinct from a genuine no-funding result, so the caller can exclude it from
    the base-rate curve rather than miscount it as a confident non-firer."""
    w = wallet.lower()
    try:
        tx = get_erc20_transfers(dl, w, sort="desc")
    except DataLayerError as exc:
        return WalletFunding(wallet=w, first_bet_ts=first_bet_ts, funded_ts=None,
                             latency_s=None, funder=None, funder_kind=None,
                             inbound_count=0, error=exc.to_error())
    # Addresses this wallet SENT to — bidirectional partners are trading
    # counterparties (settlement), never funders.
    sent_to = {t.to_addr for t in tx if t.from_addr == w}
    inbound = [
        t for t in tx
        if t.to_addr == w
        and t.timestamp is not None
        and t.timestamp < first_bet_ts
        and (t.token_symbol or "").upper() in _STABLES
        and t.from_addr not in _KNOWN_NONPERSONAL
        and t.from_addr not in sent_to
        and (t.value_normalized or 0.0) >= _FUNDING_DUST_FLOOR_USDC
    ]
    if not inbound:
        return WalletFunding(wallet=w, first_bet_ts=first_bet_ts, funded_ts=None,
                             latency_s=None, funder=None, funder_kind=None,
                             inbound_count=0)
    last = max(inbound, key=lambda t: t.timestamp)
    return WalletFunding(
        wallet=w, first_bet_ts=first_bet_ts, funded_ts=last.timestamp,
        latency_s=first_bet_ts - last.timestamp, funder=last.from_addr,
        funder_kind=None,  # filled lazily by the caller via classify_funder
        inbound_count=len(inbound),
    )


def latency_score(latency_s: int | None, breakpoints_min: list[int], scores: list[float]) -> float:
    """Piecewise latency→[0,1] score. None (no funding found) → 0.0, declared
    upstream — never imputed to a middling value."""
    if latency_s is None:
        return 0.0
    minutes = latency_s / 60.0
    for bp, s in zip(breakpoints_min, scores):
        if minutes < bp:
            return s
    return scores[len(breakpoints_min)]


# ---------------------------------------------------------------------------
# batch: the Feb-28 latency re-scan (the milestone deliverable)
# ---------------------------------------------------------------------------


def _candidate_wallets(dl: DataLayer, loaded: LoadedConfig) -> tuple[dict[str, int], set[str]]:
    """Return ({wallet: first_bet_ts}, labeled_wallets) for the M0-F Feb-28
    cluster — reuses the M0-F pipeline (all cached) so the candidate set and
    labels are identical to the footprint backtest."""
    import time as _time

    from .m0f import (_load_universe, pull_market_events, normalize_fills,
                      score_candidates_as_of, match_hypotheses, trailing_volumes)

    cfg = loaded.config.m0f
    markets = _load_universe(loaded, None)
    token_to_cid = {t: m["condition_id"] for m in markets for t in m["token_ids"]}
    all_fills = []
    for m in markets:
        events, _p = pull_market_events(dl, loaded, m)
        fills, _c, _d = normalize_fills(events, token_to_cid)
        all_fills.extend(fills)

    # Candidate set = the M0-F stage-1 candidates at the news-break as-of (net
    # directional >= floor). first_bet_ts = earliest visible fill per wallet.
    news = cfg.news_break_ts
    cands = score_candidates_as_of(
        as_of=news, fills=all_fills, crossing_usdc={}, wallet_info={},
        market_trailing_vol=trailing_volumes(all_fills, as_of=news), cfg=cfg,
    )
    cand_wallets = {c.wallet for c in cands}
    first_bet: dict[str, int] = {}
    for f in all_fills:
        if f.wallet in cand_wallets and f.timestamp <= news:
            prev = first_bet.get(f.wallet)
            if prev is None or f.timestamp < prev:
                first_bet[f.wallet] = f.timestamp

    matches = match_hypotheses(cands, cfg.labeled_hypotheses)
    labeled = {h["wallet"] for m in matches.values() for h in m["hits"]}
    return first_bet, labeled


def run_latency_scan(
    dl: DataLayer, loaded: LoadedConfig, *, limit_wallets: int | None = None
) -> dict[str, Any]:
    """Compute funded→bet latency for every M0-F candidate wallet and build the
    false-positive curve (labeled vs unlabeled wallets firing at each latency
    threshold). This is the v1.4 §3 deliverable.

    Candidate derivation reads the L1 tape (already cached by ``m0f pull``) in
    replay, regardless of the caller's mode — re-walking 114 markets live would
    be wasteful and risks subgraph timeouts. The Etherscan batch then runs in
    the caller's mode (live to populate the cache, replay to reproduce)."""
    import time as _time

    cfg = loaded.config.m5
    caller_replay = dl.replay
    dl.replay = True  # candidate derivation from the cached tape
    try:
        first_bet, labeled = _candidate_wallets(dl, loaded)
    finally:
        dl.replay = caller_replay
    wallets = sorted(first_bet)
    if limit_wallets is not None:
        # Keep all labeled wallets + fill up to the cap with others (so the
        # recall side of the curve is never silently truncated).
        others = [w for w in wallets if w not in labeled]
        wallets = sorted(set(list(labeled) + others[:max(0, limit_wallets - len(labeled))]))

    fundings: list[WalletFunding] = []
    funder_cache: dict[str, FunderClass] = {}
    for w in wallets:
        wf = wallet_funding_latency(dl, w, first_bet_ts=first_bet[w])
        if wf.funder and wf.funder not in funder_cache:
            funder_cache[wf.funder] = classify_funder(
                dl, wf.funder, cex_fanout_threshold=cfg.cex_fanout_threshold
            )
            if cfg.request_spacing_ms and not dl.replay:
                _time.sleep(cfg.request_spacing_ms / 1000.0)
        kind = funder_cache[wf.funder].kind if wf.funder else None
        fundings.append(WalletFunding(**{**wf.__dict__, "funder_kind": kind}))
        if cfg.request_spacing_ms and not dl.replay:
            _time.sleep(cfg.request_spacing_ms / 1000.0)

    # Partition (Rule 1): a FETCH FAILURE is 'unknown', excluded from the base
    # rate — it is NOT a confident non-firer. Only successfully-fetched wallets
    # form the FP-curve denominator.
    fetch_errors = [f for f in fundings if f.error]
    resolved = [f for f in fundings if not f.error]
    labeled_resolved = [f for f in resolved if f.wallet in labeled]
    unlabeled_resolved = [f for f in resolved if f.wallet not in labeled]

    curve = []
    for thr in cfg.fp_curve_thresholds_min:
        thr_s = thr * 60
        lab_fired = [f for f in labeled_resolved
                     if f.latency_s is not None and f.latency_s <= thr_s]
        unlab_fired = [f for f in unlabeled_resolved
                       if f.latency_s is not None and f.latency_s <= thr_s]
        fired_total = len(lab_fired) + len(unlab_fired)
        unlab_nonCEX = sum(1 for f in unlab_fired if f.funder_kind == "dedicated")
        curve.append({
            "threshold_min": thr,
            "labeled_fired": len(lab_fired), "labeled_total": len(labeled_resolved),
            "unlabeled_fired": len(unlab_fired),
            "unlabeled_total": len(unlabeled_resolved),
            # base rate: P(fires | non-insider, resolved) — the deliverable
            "unlabeled_fire_rate": round(len(unlab_fired) / len(unlabeled_resolved), 4)
                                   if unlabeled_resolved else None,
            "unlabeled_fired_nonCEX": unlab_nonCEX,
            "precision": round(len(lab_fired) / fired_total, 3) if fired_total else None,
        })

    kinds = [c.kind for c in funder_cache.values()]
    latencies = {f.wallet: f.latency_s for f in fundings}
    return {
        "kind": "m5.latency_scan",
        "wallets_scanned": len(wallets),
        "capped": limit_wallets is not None,   # unlabeled cohort truncated if True
        "labeled_in_scan": len(labeled_resolved),
        "resolved": len(resolved),
        "fetch_errors": len(fetch_errors),     # excluded from the curve, declared
        "no_funding_found": sum(1 for f in resolved if f.latency_s is None),
        "distinct_funders": len(funder_cache),
        "funder_kinds": {k: kinds.count(k) for k in ("cex", "dedicated", "nonpersonal", "unknown")},
        "fp_curve": curve,
        "labeled_latencies": {f.wallet: f.latency_s for f in labeled_resolved},
        "rate_limit_hits": dl.rate_limits.count_429 if dl.rate_limits else 0,
    }
