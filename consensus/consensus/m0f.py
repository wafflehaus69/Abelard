"""M0-F — Feb-28 footprint backtest (spec v1.0 §M0-F, addendum v1.2 §3).

A historical study on the L1 archival tape: reconstruct the Feb-2026
Iran-related market cluster's trade tape, identify the fresh-wallet cluster
independently on-chain, run the M10 seven-factor detection rules AS-OF
timestamps before the news break, and report precision/recall + calibration.
No live scanning, no alerting, nothing touches M9.

Pipeline stages (each a CLI command, each resumable from the response cache):

  universe  gamma search-term sweep -> markets whose life intersects the
            detection window -> artifact (data/m0f/universe.json)
  pull      L1 subgraph walk of every universe market's fills over
            [window_start - baseline, window_end] -> response cache
  identify  signature-match reported wallet hypotheses against the tape
            (entries are HYPOTHESES until confirmed on-chain — spec §7)
  score     seven-factor detection replay at each as-of ladder timestamp;
            emits the detection report artifact

Wallet-attribution semantics (verified on-chain 2026-07-13 against the one
fully-known hypothesis wallet, which reproduced its reported position
digit-for-digit): a wallet's OWN fills are the events where it is ``maker``;
events where it appears as ``taker`` are the counterparty perspective of its
marketable crossings (used for the aggression factor, never double-counted
into volume).

As-of discipline: every aggregate at time T uses only events with
``timestamp <= T``. Wallet first-seen/prior-history lookups are inherently
lookahead-safe (they observe only the past relative to any T in the ladder).

Factor availability (run (a), per v1.2: chain-funding data needs the owner's
Etherscan key): P (funding provenance) is EXCLUDED, T is a stated proxy
(first-seen -> first-bet latency, not funded -> first-bet). Absent factors are
excluded from the weighted geometric mean and listed in the report — never
imputed (Rule 1).
"""

from __future__ import annotations

import json
import math
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from .config import LoadedConfig
from .errors import DataLayerError
from .fetching import DataLayer
from .models import OrderFilledEvent
from .sources_subgraph import paginate_order_filled

_GAMMA = "polymarket_gamma"
_USDC_SCALE = 1_000_000  # 6-decimal raw amounts

TIER_ORDER = ["NONE", "WATCH", "ELEVATED", "CRITICAL"]


# ---------------------------------------------------------------------------
# universe
# ---------------------------------------------------------------------------


def _iso_to_ts(iso: str | None) -> int | None:
    if not iso:
        return None
    from datetime import datetime
    try:
        return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp())
    except ValueError:
        return None


def build_universe(dl: DataLayer, loaded: LoadedConfig) -> dict[str, Any]:
    """Sweep the configured search terms for candidate events, then pull each
    event's FULL market list (``/events?slug=`` — ``public-search`` embeds only
    still-active markets, which silently drops resolved anchors like the Feb-28
    strikes contract). Keep every market that could have traded in the window.

    Window rule: keep unless the market demonstrably started at/after the
    window end. The ``endDate`` field is an unreliable event-level date (it read
    2026-01-31 for a market that actually resolved Feb 28), so it is NOT used to
    exclude — over-inclusion is safe (the L1 pull returns nothing for a market
    with no in-window fills), while excluding the anchor is fatal."""
    cfg = loaded.config.m0f
    slugs: list[str] = []
    seen_slugs: set[str] = set()
    for term in cfg.search_terms:
        body = dl.fetch(
            source=_GAMMA, base_url=dl.endpoints.polymarket_gamma_api,
            endpoint="/public-search", request_params={"q": term},
        )
        if not isinstance(body, dict):
            raise DataLayerError(
                f"{_GAMMA}/public-search: expected an object, got {type(body).__name__}",
                source=_GAMMA,
            )
        for ev in body.get("events") or []:
            slug = ev.get("slug")
            if slug and slug not in seen_slugs:
                seen_slugs.add(slug)
                slugs.append(slug)

    markets: list[dict[str, Any]] = []
    dropped_no_tokens = 0
    dropped_after_window = 0
    events_resolved = 0
    for slug in slugs:
        ev_body = dl.fetch(
            source=_GAMMA, base_url=dl.endpoints.polymarket_gamma_api,
            endpoint="/events", request_params={"slug": slug},
        )
        if not isinstance(ev_body, list) or not ev_body:
            continue
        events_resolved += 1
        ev = ev_body[0]
        for m in ev.get("markets") or []:
            cid = m.get("conditionId")
            if not cid:
                continue
            start = _iso_to_ts(m.get("startDate"))
            if start is not None and start >= cfg.window_end_ts:
                dropped_after_window += 1
                continue
            try:
                token_ids = [str(t) for t in json.loads(m.get("clobTokenIds") or "[]")]
            except (ValueError, TypeError):
                token_ids = []
            if not token_ids:
                dropped_no_tokens += 1
                continue
            markets.append({
                "condition_id": cid,
                "question": m.get("question"),
                "slug": m.get("slug"),
                "event_slug": slug,
                "event_title": ev.get("title"),
                "token_ids": token_ids,
                "start_date": m.get("startDate"),
                "end_date": m.get("endDate"),
                "closed": m.get("closed"),
                "volume": m.get("volume"),
            })
    by_cid = {m["condition_id"]: m for m in markets}
    return {
        "kind": "m0f.universe",
        "window": [cfg.window_start_ts, cfg.window_end_ts],
        "search_terms": cfg.search_terms,
        "events_seen": len(slugs),
        "events_resolved": events_resolved,
        "markets": sorted(by_cid.values(), key=lambda m: m["condition_id"]),
        "dropped_no_clob_tokens": dropped_no_tokens,
        "dropped_started_after_window": dropped_after_window,
    }


# ---------------------------------------------------------------------------
# pull + normalized fills
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Fill:
    """One own-order fill, normalized from a maker-side event."""

    wallet: str
    condition_id: str
    token_id: str
    side: str          # BUY (paid USDC) | SELL (received USDC)
    usdc: float        # notional in USDC
    tokens: float
    price: float
    timestamp: int
    event_id: str


def pull_market_events(
    dl: DataLayer, loaded: LoadedConfig, market: dict[str, Any], *, retries: int = 2
) -> tuple[list[OrderFilledEvent], dict[str, Any]]:
    """Walk one market's window slice. Retries a transient subgraph timeout
    (the server occasionally returns a 200 'Query timed out' under load); a
    persistent failure raises so the caller can declare the market failed."""
    cfg = loaded.config.m0f
    since = cfg.window_start_ts - cfg.baseline_days * 86400
    last_exc: DataLayerError | None = None
    for attempt in range(retries + 1):
        try:
            return paginate_order_filled(
                dl, asset_ids=market["token_ids"], ts_gte=since, ts_lt=cfg.window_end_ts
            )
        except DataLayerError as exc:
            last_exc = exc
            msg = str(exc).lower()
            transient = "timed out" in msg or "timeout" in msg
            if dl.replay or not transient or attempt == retries:
                raise
            dl.logger.warning("m0f pull %s: subgraph timeout, retry %d/%d",
                              market["condition_id"][:14], attempt + 1, retries)
    raise last_exc  # unreachable, for the type checker


def normalize_fills(
    events: list[OrderFilledEvent], token_to_cid: dict[str, str]
) -> tuple[list[Fill], dict[str, float], dict[str, int]]:
    """Maker-side events -> own-order fills; taker-side crossing volume is
    returned separately per wallet (aggression input), never added to volume.

    Returns ``(fills, crossing_usdc, drops)``. ``drops`` counts the events not
    turned into priced fills, by reason — token-for-token legs (merge/convert),
    events on a token outside the universe, and non-positive amounts — so the
    caller can declare them rather than let a silent ``continue`` hide data
    (Rule 1)."""
    fills: list[Fill] = []
    crossing_usdc: dict[str, float] = defaultdict(float)
    drops = {"token_for_token": 0, "unknown_token": 0, "nonpositive": 0}
    for e in events:
        if e.maker_asset_id == "0":
            side, token = "BUY", e.taker_asset_id
            usdc, tokens = e.maker_amount_filled, e.taker_amount_filled
        elif e.taker_asset_id == "0":
            side, token = "SELL", e.maker_asset_id
            usdc, tokens = e.taker_amount_filled, e.maker_amount_filled
        else:
            drops["token_for_token"] += 1
            continue  # token-for-token (merge/convert paths) — not a priced fill
        cid = token_to_cid.get(token)
        if cid is None:
            drops["unknown_token"] += 1
            continue
        if tokens <= 0:
            drops["nonpositive"] += 1
            continue
        fills.append(Fill(
            wallet=e.maker,
            condition_id=cid,
            token_id=token,
            side=side,
            usdc=usdc / _USDC_SCALE,
            tokens=tokens / _USDC_SCALE,
            price=usdc / tokens,
            timestamp=e.timestamp,
            event_id=e.event_id,
        ))
        # The taker on a maker-order event is the crossing counterparty (or the
        # exchange for its own order-event); attribute crossing volume to real
        # wallet takers only.
        crossing_usdc[e.taker] += usdc / _USDC_SCALE
    return fills, dict(crossing_usdc), drops


# ---------------------------------------------------------------------------
# wallet enrichment (lookahead-safe)
# ---------------------------------------------------------------------------


def _wallet_query(
    dl: DataLayer, wallet: str, *, extra: str, first: int, retries: int = 2
) -> list[dict[str, Any]]:
    query = (
        f'{{ orderFilledEvents(first: {first}, orderBy: timestamp, orderDirection: asc, '
        f'where: {{ maker: "{wallet}"{extra} }}) {{ id timestamp }} }}'
    )
    for attempt in range(retries + 1):
        try:
            data = dl.fetch_graphql(
                source="goldsky_subgraph", url=dl.endpoints.goldsky_subgraph, query=query
            )
            raw = data.get("orderFilledEvents")
            return raw if isinstance(raw, list) else []
        except DataLayerError as exc:
            if dl.replay or "timed out" not in str(exc).lower() or attempt == retries:
                raise
            dl.logger.warning("enrich %s: subgraph timeout, retry %d/%d",
                              wallet[:12], attempt + 1, retries)
    return []  # unreachable


def enrich_wallet(dl: DataLayer, wallet: str, *, before_ts: int) -> dict[str, Any]:
    """First-seen timestamp + prior-fill count strictly before ``before_ts``.
    Attribution is maker-side (a wallet's own orders each emit an event with
    maker=<owner>; validated exact against the one fully-known wallet). The
    count is capped at 1000 ('1000+') — far past every freshness breakpoint, so
    the cap cannot change a score; it is still reported. ``error`` is set (and
    F/T excluded for that wallet) if the wallet cannot be enriched — never
    imputed."""
    try:
        first = _wallet_query(dl, wallet, extra="", first=1)
        prior = _wallet_query(dl, wallet, extra=f', timestamp_lt: "{before_ts}"', first=1000)
    except DataLayerError as exc:
        return {"wallet": wallet, "first_seen_ts": None, "prior_fills": None,
                "prior_fills_capped": None, "error": exc.to_error()}
    return {
        "wallet": wallet,
        "first_seen_ts": int(first[0]["timestamp"]) if first else None,
        "prior_fills": len(prior),
        "prior_fills_capped": len(prior) >= 1000,
    }


# ---------------------------------------------------------------------------
# factors
# ---------------------------------------------------------------------------


@dataclass
class CandidateScore:
    wallet: str
    condition_id: str
    token_id: str
    net_stake_usdc: float
    vwap_entry: float
    first_bet_ts: int
    last_bet_ts: int
    buy_tokens: float = 0.0
    sell_tokens: float = 0.0
    factors: dict[str, float] = field(default_factory=dict)
    factors_active: list[str] = field(default_factory=list)
    composite: float = 0.0
    cluster_ids: list[str] = field(default_factory=list)
    tier: str = "NONE"
    data_incomplete: bool = False
    notes: dict[str, Any] = field(default_factory=dict)


def _piecewise(value: float, breakpoints: list[int], scores: list[float]) -> float:
    for bp, s in zip(breakpoints, scores):
        if value < bp:
            return s
    return scores[len(breakpoints)]


def score_candidates_as_of(
    *,
    as_of: int,
    fills: list[Fill],
    crossing_usdc: dict[str, float],
    wallet_info: dict[str, dict[str, Any]],
    market_trailing_vol: dict[str, float],
    cfg: Any,  # M0FConfig
) -> list[CandidateScore]:
    """Stage 1 + Stage 2 of the M10 profile, as-of ``as_of`` (only fills with
    timestamp <= as_of participate)."""
    visible = [f for f in fills if f.timestamp <= as_of]

    # Stage 1: net-directional stakes per (wallet, market, token).
    agg: dict[tuple[str, str, str], dict[str, float]] = defaultdict(
        lambda: {"buy_usdc": 0.0, "sell_usdc": 0.0, "buy_tokens": 0.0, "sell_tokens": 0.0,
                 "first_ts": float("inf"), "last_ts": 0.0}
    )
    wallet_gross: dict[str, float] = defaultdict(float)
    for f in visible:
        key = (f.wallet, f.condition_id, f.token_id)
        a = agg[key]
        if f.side == "BUY":
            a["buy_usdc"] += f.usdc
            a["buy_tokens"] += f.tokens
        else:
            a["sell_usdc"] += f.usdc
            a["sell_tokens"] += f.tokens
        a["first_ts"] = min(a["first_ts"], f.timestamp)
        a["last_ts"] = max(a["last_ts"], f.timestamp)
        wallet_gross[f.wallet] += f.usdc

    out: list[CandidateScore] = []
    for (wallet, cid, token), a in agg.items():
        net = a["buy_usdc"] - a["sell_usdc"]
        gross = a["buy_usdc"] + a["sell_usdc"]
        if net < cfg.size_floor_usdc or gross <= 0:
            continue
        if net / gross < cfg.directional_min:
            continue
        vwap = a["buy_usdc"] / a["buy_tokens"] if a["buy_tokens"] > 0 else 0.0

        info = wallet_info.get(wallet) or {}
        first_seen = info.get("first_seen_ts")
        # F/T are dropped when first_seen is missing. Distinguish the two causes:
        # an ENRICHMENT ERROR is a data gap (renormalizing over survivors would
        # impute freshness — Rule 1 forbids that), whereas the intentional A/P
        # exclusions are design. A data-incomplete candidate is scored but
        # flagged and barred from a tier, never silently boosted.
        data_incomplete = bool(info.get("error"))
        factors: dict[str, float] = {}
        active: list[str] = []
        notes: dict[str, Any] = {}
        if data_incomplete:
            notes["data_incomplete"] = info["error"][:120]

        # F — freshness (source (a): first Polymarket order; chain age needs key)
        if first_seen is not None:
            age_days = max(0.0, (a["first_ts"] - first_seen) / 86400)
            f_score = _piecewise(age_days, cfg.fresh_day_breakpoints, cfg.fresh_scores)
            if info.get("prior_fills", 0) >= cfg.prior_fills_discount_threshold:
                f_score *= cfg.prior_fills_discount
            factors["F"] = f_score
            active.append("F")
            notes["age_days_at_first_bet"] = round(age_days, 3)
            notes["prior_fills"] = info.get("prior_fills")
        else:
            notes["F_unavailable"] = "wallet has no maker event in L1 (unexpected)"

        # S — relative size vs trailing market volume
        trailing = market_trailing_vol.get(cid, 0.0)
        if trailing > 0:
            factors["S"] = min(1.0, (net / trailing) / cfg.s_full_scale_frac)
            active.append("S")
            notes["trailing_vol_7d"] = round(trailing, 2)
        else:
            # A stake into a market with ~zero trailing volume is maximal
            # relative size by construction.
            factors["S"] = 1.0
            active.append("S")
            notes["trailing_vol_7d"] = 0.0

        gross_all = wallet_gross.get(wallet, 0.0)

        # A — aggression: EXCLUDED on run (a). The spec defines A via order-book
        # levels swept + realized price impact (CLOB data, deferred to M8). The
        # subgraph's two-events-per-match model exposes each order's owner as
        # `maker`, so a fill-only crossing metric is not a faithful aggression
        # signal — honestly excluded rather than computed muddled (Rule 1).
        notes["A_excluded"] = "needs CLOB order-book depth/impact (M8); not on L1 fills"

        # D — contrarian depth
        factors["D"] = max(0.0, 1.0 - vwap)
        active.append("D")

        # C — commitment (proxy: this stake / wallet's gross visible volume;
        # true funded-balance needs chain data)
        factors["C"] = min(1.0, net / gross_all) if gross_all > 0 else 0.0
        active.append("C")

        # T — timing (PROXY: first-seen -> first-bet latency; funded -> bet
        # latency needs the funding graph)
        if first_seen is not None:
            latency_min = (a["first_ts"] - first_seen) / 60
            factors["T"] = _piecewise(latency_min, cfg.t_latency_breakpoints_min, cfg.t_scores)
            active.append("T")

        # P — funding provenance: requires M5 + Etherscan key. EXCLUDED.
        notes["P_excluded"] = "funding graph not built (run (a)); see report caveats"

        # Weighted geometric mean over ACTIVE factors only.
        wsum = sum(cfg.factor_weights.get(k, 0.0) for k in active)
        if wsum > 0:
            log_sum = 0.0
            for k in active:
                w = cfg.factor_weights.get(k, 0.0)
                log_sum += w * math.log(max(factors[k], 1e-9))
            composite = math.exp(log_sum / wsum)
        else:
            composite = 0.0

        out.append(CandidateScore(
            wallet=wallet, condition_id=cid, token_id=token,
            net_stake_usdc=round(net, 2), vwap_entry=round(vwap, 4),
            first_bet_ts=int(a["first_ts"]), last_bet_ts=int(a["last_ts"]),
            buy_tokens=round(a["buy_tokens"], 2), sell_tokens=round(a["sell_tokens"], 2),
            factors={k: round(v, 4) for k, v in factors.items()},
            factors_active=active, composite=round(composite, 4),
            data_incomplete=data_incomplete, notes=notes,
        ))
    return out


def apply_cluster_amplifier(
    candidates: list[CandidateScore], *, cfg: Any, elevated_floor: float
) -> list[dict[str, Any]]:
    """Stage 3: coordinated bursts of distinct high-score wallets on the same
    side, per-market and cross-market within the sibling set (v1.2 §3)."""
    strong = [c for c in candidates if c.composite >= elevated_floor]
    window_s = cfg.cluster_window_hours * 3600
    clusters: list[dict[str, Any]] = []

    def _find(group: list[CandidateScore], scope: str, scope_id: str) -> None:
        group = sorted(group, key=lambda c: c.first_bet_ts)
        i = 0
        while i < len(group):
            burst = [group[i]]
            j = i + 1
            while j < len(group) and group[j].first_bet_ts - burst[0].first_bet_ts <= window_s:
                burst.append(group[j])
                j += 1
            wallets = {c.wallet for c in burst}
            if len(wallets) >= cfg.cluster_min:
                cluster_id = f"{scope}:{scope_id}:{burst[0].first_bet_ts}"
                # Record membership only; the boost is applied ONCE below, so a
                # wallet in both a per-market and a cross-market cluster is not
                # amplified twice (membership is the trigger, not scope count).
                for c in burst:
                    c.cluster_ids.append(cluster_id)
                clusters.append({
                    "cluster_id": cluster_id, "scope": scope, "scope_id": scope_id,
                    "wallets": sorted(wallets), "members": len(burst),
                    "first_bet_ts": burst[0].first_bet_ts,
                    "last_bet_ts": max(c.last_bet_ts for c in burst),
                })
                i = j
            else:
                i += 1

    by_market: dict[str, list[CandidateScore]] = defaultdict(list)
    for c in strong:
        by_market[c.condition_id].append(c)
    for cid, group in by_market.items():
        _find(group, "market", cid)
    # Cross-market: the whole sibling set is one scope (informed actors spreading
    # across correlated contracts is itself the detection feature). M0-F showed
    # this over-fires in a high-activity window (docs/m0f_report.md §4), so it is
    # config-gated (default off pending a same-wallet or P-gated rule).
    if getattr(cfg, "cross_market_enabled", True):
        # scope_id is the M0-F study's cluster by default; a live caller (M10)
        # overrides it so dossiers don't carry the stale 'iran-cluster' label.
        _find(strong, "cross-market", getattr(cfg, "cross_market_scope_id", "iran-cluster"))

    # v1.3 §3.2: membership is recorded above (cluster_ids) as dossier evidence.
    # The composite boost is applied ONLY when cluster_boosts_score is enabled;
    # by default cluster membership does not move the score (it over-fires in a
    # saturated-attention regime). Boost is once-per-wallet regardless of scope.
    if getattr(cfg, "cluster_boosts_score", False):
        for c in candidates:
            if c.cluster_ids:
                c.composite = round(min(1.0, c.composite * cfg.cluster_boost), 4)
    return clusters


def latch_tiers(
    history: dict[tuple[str, str], dict[str, Any]],
    candidates: list[CandidateScore],
    *,
    as_of: int,
) -> dict[tuple[str, str], dict[str, Any]]:
    """Tier high-water-mark latching for the LIVE M10 scan (v1.3 §3.3).

    A live alert that fires CRITICAL at −30h and then "un-fires" as trailing
    volume dilutes the relative-size factor invites a "it cleared itself"
    misread. So per (wallet, market-family) the tier latches at its highest
    level reached, with the crossing timestamp; decay is shown as trajectory in
    the dossier, never as an alert retraction. (Early peaking is desirable — the
    detector is loudest when the information is freshest.)

    ``history`` is the running latch state (mutated and returned); key is
    (wallet, condition_id). Backtests report the raw per-as-of tier and do not
    latch — this helper is for the live scan and is unit-tested here so it ships
    ready.
    """
    for c in candidates:
        if c.tier in ("NONE", "INSUFFICIENT_DATA"):
            continue
        key = (c.wallet, c.condition_id)
        rank = TIER_ORDER.index(c.tier)
        prev = history.get(key)
        if prev is None or rank > TIER_ORDER.index(prev["peak_tier"]):
            history[key] = {"peak_tier": c.tier, "crossed_ts": as_of,
                            "peak_composite": c.composite}
    return history


def assign_tiers(
    candidates: list[CandidateScore],
    thresholds: dict[str, float],
    *,
    cluster_elevates: bool = False,
) -> None:
    for c in candidates:
        if c.data_incomplete:
            # Enrichment failed -> F/T unknown. Never assign a real tier from an
            # imputed composite; surface it as its own state for the report.
            c.tier = "INSUFFICIENT_DATA"
            continue
        tier = "NONE"
        for name in ("WATCH", "ELEVATED", "CRITICAL"):
            if c.composite >= thresholds[name]:
                tier = name
        # v1.3 §3.2: cluster membership does not move the tier by default (it is
        # dossier evidence). Auto-elevation is available only when explicitly
        # enabled alongside cluster_boosts_score.
        if cluster_elevates and c.cluster_ids and tier != "NONE":
            tier = TIER_ORDER[min(TIER_ORDER.index(tier) + 1, len(TIER_ORDER) - 1)]
        c.tier = tier


# ---------------------------------------------------------------------------
# market trailing volume (factor S input)
# ---------------------------------------------------------------------------


def trailing_volumes(fills: list[Fill], *, as_of: int, days: int = 7) -> dict[str, float]:
    lo = as_of - days * 86400
    vol: dict[str, float] = defaultdict(float)
    for f in fills:
        if lo <= f.timestamp <= as_of:
            vol[f.condition_id] += f.usdc
    return dict(vol)


# ---------------------------------------------------------------------------
# hypothesis matching (labels are hypotheses until matched on-chain — spec §7)
# ---------------------------------------------------------------------------


def match_hypotheses(
    candidates: list[CandidateScore], hypotheses: list[Any]
) -> dict[str, Any]:
    """Match press-reported entries against on-chain candidates: by address when
    known, else by (net-shares-held, entry-price) signature within tolerance.

    Two review-fixed subtleties:
      - Shares are the candidate's actual NET SHARES HELD (buy_tokens -
        sell_tokens), not net_usd/vwap — the latter mis-states the count once a
        wallet takes any profit (directional_min admits ~11% sell churn), which
        silently dropped true matches from recall.
      - A hypothesis with neither an address nor an approx_shares signature is
        structurally UNMATCHABLE; that is reported explicitly (``matchable:
        false``) so "can't be searched" is never conflated with "searched and
        absent on-chain" (Rule 1: declare the gap)."""
    out: dict[str, Any] = {}
    for h in hypotheses:
        matchable = bool(h.address) or (h.approx_shares is not None)
        hits: list[dict[str, Any]] = []
        for c in candidates:
            if h.address:
                if c.wallet == h.address.lower():
                    hits.append({"wallet": c.wallet, "match": "address",
                                 "market": c.condition_id, "vwap": c.vwap_entry})
                continue
            if h.approx_shares is None:
                continue  # unmatchable — recorded below, never a silent []
            net_shares = c.buy_tokens - c.sell_tokens
            share_ok = abs(net_shares - h.approx_shares) / h.approx_shares <= 0.03
            price_ok = (h.approx_price is None or
                        (c.vwap_entry > 0 and
                         abs(c.vwap_entry - h.approx_price) / h.approx_price <= 0.15))
            if share_ok and price_ok:
                hits.append({"wallet": c.wallet, "match": "signature",
                             "net_shares": round(net_shares), "vwap": c.vwap_entry,
                             "market": c.condition_id})
        out[h.name] = {
            "matchable": matchable,
            "reason": None if matchable else "no address and no approx_shares signature",
            "hits": hits,
        }
    return out


# ---------------------------------------------------------------------------
# orchestration (CLI-facing)
# ---------------------------------------------------------------------------


def _artifact_dir(loaded: LoadedConfig):
    d = loaded.config_dir / "data" / "m0f"
    d.mkdir(parents=True, exist_ok=True)
    return d


def run_universe(dl: DataLayer, loaded: LoadedConfig) -> dict[str, Any]:
    art = build_universe(dl, loaded)
    path = _artifact_dir(loaded) / "universe.json"
    path.write_text(json.dumps(art, indent=2, ensure_ascii=False), encoding="utf-8")
    return {
        "kind": "m0f.universe",
        "artifact": str(path),
        "events_seen": art["events_seen"],
        "events_resolved": art["events_resolved"],
        "markets": len(art["markets"]),
        "dropped_no_clob_tokens": art["dropped_no_clob_tokens"],
        "dropped_started_after_window": art["dropped_started_after_window"],
    }


def _load_universe(loaded: LoadedConfig, limit_markets: int | None) -> list[dict[str, Any]]:
    path = _artifact_dir(loaded) / "universe.json"
    if not path.is_file():
        raise DataLayerError(
            "m0f universe artifact missing — run `consensus m0f universe` first",
            source="m0f",
        )
    markets = json.loads(path.read_text(encoding="utf-8"))["markets"]
    markets.sort(key=lambda m: -(float(m.get("volume") or 0)))
    if limit_markets is not None:
        markets = markets[:limit_markets]
    return markets


def run_pull(
    dl: DataLayer, loaded: LoadedConfig, *, limit_markets: int | None = None
) -> dict[str, Any]:
    markets = _load_universe(loaded, limit_markets)
    per_market: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    total_events = 0
    for m in markets:
        try:
            events, prov = pull_market_events(dl, loaded, m)
        except DataLayerError as exc:
            # Per-market fault isolation: one market's persistent failure is a
            # DECLARED gap in the pull, not a silent hole and not an aborted run
            # (Rule 1). The market is reported; scoring will note it missing.
            failed.append({"condition_id": m["condition_id"],
                           "question": (m.get("question") or "")[:70],
                           "error": exc.to_error()})
            dl.logger.error("m0f pull %s FAILED: %s", m["condition_id"][:14], exc.to_error())
            continue
        total_events += len(events)
        per_market.append({
            "condition_id": m["condition_id"],
            "question": (m.get("question") or "")[:70],
            "events": len(events),
            "pages": prov["pages"],
            "forced_skips": prov["forced_skips_ts"],
        })
        dl.logger.info("m0f pull %s: %d events (%d pages)",
                       m["condition_id"][:14], len(events), prov["pages"])
    return {
        "kind": "m0f.pull",
        "markets_pulled": len(per_market),
        "markets_failed": len(failed),
        "failed": failed,
        "limit_markets": limit_markets,   # explicit — a bounded pull is reported
        "total_events": total_events,
        "rate_limit_hits": dl.rate_limits.count_429 if dl.rate_limits else 0,
        "per_market": sorted(per_market, key=lambda m: -m["events"]),
    }


def run_score(
    dl: DataLayer,
    loaded: LoadedConfig,
    *,
    as_of_override: int | None = None,
    limit_markets: int | None = None,
) -> dict[str, Any]:
    cfg = loaded.config.m0f
    markets = _load_universe(loaded, limit_markets)
    token_to_cid = {t: m["condition_id"] for m in markets for t in m["token_ids"]}

    all_fills: list[Fill] = []
    crossing: dict[str, float] = defaultdict(float)
    failed_markets: list[dict[str, Any]] = []
    drops_total = {"token_for_token": 0, "unknown_token": 0, "nonpositive": 0}
    forced_skips_total: list[int] = []
    for m in markets:
        try:
            events, prov = pull_market_events(dl, loaded, m)
        except DataLayerError as exc:
            failed_markets.append({"condition_id": m["condition_id"],
                                   "error": exc.to_error()})
            dl.logger.error("m0f score: market %s unavailable: %s",
                            m["condition_id"][:14], exc.to_error())
            continue
        forced_skips_total.extend(prov.get("forced_skips_ts") or [])
        fills, cross, drops = normalize_fills(events, token_to_cid)
        for k in drops_total:
            drops_total[k] += drops[k]
        all_fills.extend(fills)
        for w, v in cross.items():
            crossing[w] += v

    ladder = [as_of_override] if as_of_override is not None else list(cfg.as_of_ladder)
    # Lookahead guard: prior-history is anchored at window_start, which is only
    # safe for as-of points at/after it. Reject an earlier probe rather than
    # silently fold post-T baseline fills into a pre-T freshness discount.
    bad = [t for t in ladder if t < cfg.window_start_ts]
    if bad:
        raise DataLayerError(
            f"m0f score: as-of {bad} precedes window_start_ts {cfg.window_start_ts}; "
            "prior-history anchoring would look ahead. Widen window_start or pick a later as-of.",
            source="m0f",
        )

    # Candidate-wallet union across the ladder (cheap pre-pass), then one
    # enrichment lookup per wallet (each cached -> replay-stable).
    union: set[str] = set()
    for t in ladder:
        pre = score_candidates_as_of(
            as_of=t, fills=all_fills, crossing_usdc=dict(crossing),
            wallet_info={}, market_trailing_vol=trailing_volumes(all_fills, as_of=t),
            cfg=cfg,
        )
        union.update(c.wallet for c in pre)
    wallet_info = {w: enrich_wallet(dl, w, before_ts=cfg.window_start_ts) for w in sorted(union)}
    enrich_failures = [w for w, i in wallet_info.items() if i.get("error")]

    per_asof: list[dict[str, Any]] = []
    for t in ladder:
        cands = score_candidates_as_of(
            as_of=t, fills=all_fills, crossing_usdc=dict(crossing),
            wallet_info=wallet_info,
            market_trailing_vol=trailing_volumes(all_fills, as_of=t), cfg=cfg,
        )
        clusters = apply_cluster_amplifier(
            cands, cfg=cfg, elevated_floor=cfg.tier_thresholds["ELEVATED"]
        )
        assign_tiers(cands, cfg.tier_thresholds,
                     cluster_elevates=cfg.cluster_boosts_score)
        matches = match_hypotheses(cands, cfg.labeled_hypotheses)

        labeled_wallets = {h["wallet"] for m in matches.values() for h in m["hits"]}
        critical = [c for c in cands if c.tier == "CRITICAL"]
        critical_wallets = {c.wallet for c in critical}
        per_asof.append({
            "as_of": t,
            "pre_news_s": cfg.news_break_ts - t,
            "candidates": len(cands),
            "tiers": {tier: sum(1 for c in cands if c.tier == tier)
                      for tier in (*TIER_ORDER, "INSUFFICIENT_DATA")},
            "clusters": clusters,
            "hypothesis_matches": matches,
            "labeled_wallets_found": sorted(labeled_wallets),
            "labeled_flagged_critical": sorted(labeled_wallets & critical_wallets),
            "unlabeled_critical": sorted(critical_wallets - labeled_wallets),
            "top_candidates": [
                {"wallet": c.wallet, "market": c.condition_id[:14], "tier": c.tier,
                 "composite": c.composite, "net_stake": c.net_stake_usdc,
                 "vwap": c.vwap_entry, "factors": c.factors,
                 "clusters": c.cluster_ids}
                for c in sorted(cands, key=lambda c: -c.composite)[:15]
            ],
        })

    report = {
        "kind": "m0f.score",
        "news_break_ts": cfg.news_break_ts,
        "markets_scored": len(markets) - len(failed_markets),
        "markets_failed": failed_markets,   # declared, never silently skipped
        "limit_markets": limit_markets,
        "fills": len(all_fills),
        "events_dropped": drops_total,   # declared, not silently skipped (Rule 1)
        "l1_forced_skips_ts": forced_skips_total,   # subgraph >page/sec skips, if any
        "wallets_enriched": len(wallet_info),
        "enrichment_failures": enrich_failures,   # F/T excluded for these, declared
        "factors_excluded": {
            "A": "aggression needs CLOB order-book depth/impact (deferred to M8)",
            "P": "funding provenance needs the M5 funding graph (Etherscan key)",
            "T": "PROXY only (first-seen -> first-bet latency; funded->bet needs chain data)",
        },
        "ladder": per_asof,
    }
    path = _artifact_dir(loaded) / (
        f"score_{ladder[0]}.json" if len(ladder) == 1 else "score_ladder.json"
    )
    path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    report["artifact"] = str(path)
    return report
