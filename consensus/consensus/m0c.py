"""M0-C — consensus historical replay (spec v1.0 §M0-C + addendum v1.2 §4).

Replays the CONSENSUS mechanic (Detector A) over the L1 archival tape with
**zero lookahead**, sweeps its parameters, and returns a GO/NO-GO on whether
the "trust proven wallets + require agreement" signal ever had measurable edge
at an owner-realistic entry lag. Historical study on cached data only.

The cardinal rule here is the cardinal backtest sin's inverse: every quantity
computed as-of time T uses ONLY information observable at T.
  - A wallet's SKILL as-of T aggregates its edge on markets that RESOLVED at or
    before T (outcome known) — never a market resolving after T.
  - The ROSTER as-of T is built from skill-as-of-T alone.
  - A consensus SIGNAL at T reads roster wallets' positions from fills at or
    before T, on markets still unresolved at T.
  - The OUTCOME of a signal is measured at the realistic entry price
    (signal_ts + entry_lag, from the tape) against the eventual resolution —
    the only place post-T information legitimately enters, because it is the
    thing being predicted, never an input to the decision.

Pieces (all pure functions over parsed inputs; data plumbing is in run_*):
  resolve_market   gamma closed-market -> ResolvedMarket (winning token + ts)
  wallet_edges     L1 fills + resolution -> per (wallet, market) realized edge
  score_wallets    as-of skill per wallet (decayed, significance-gated, MM screen)
  build_roster     as-of top-K roster
  scan_consensus   as-of signals (participation + agreement + remaining-edge gate)
  evaluate_signal  realistic-entry price + resolution -> captured edge
  sweep            parameter grid -> expectancy per cell -> GO/NO-GO
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from .m0f import Fill  # normalized own-order fill (maker-side, validated)

_DAY = 86400


# ---------------------------------------------------------------------------
# market resolution
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ResolvedMarket:
    condition_id: str
    resolution_ts: int              # when the outcome became known (closedTime)
    winning_token: str | None       # token id priced 1 at resolution; None if void/unknown
    token_ids: tuple[str, ...]
    category: str | None = None


def resolve_market(m: dict[str, Any]) -> ResolvedMarket | None:
    """Parse a gamma closed-market dict into a :class:`ResolvedMarket`.

    Returns None (a declared gap, not a guess) if the market is not cleanly
    resolved: missing token ids/prices, unresolved UMA status, or a non-binary
    price vector we can't map to a single winner.
    """
    import json

    cid = m.get("conditionId")
    if not cid:
        return None
    if not m.get("closed"):
        return None
    status = (m.get("umaResolutionStatus") or "").lower()
    if status and status != "resolved":
        return None
    try:
        tokens = [str(t) for t in json.loads(m.get("clobTokenIds") or "[]")]
        prices = [float(p) for p in json.loads(m.get("outcomePrices") or "[]")]
    except (ValueError, TypeError):
        return None
    if not tokens or len(tokens) != len(prices):
        return None
    # Resolution timestamp: prefer closedTime, then umaEndDate, then endDate.
    res_ts = _parse_ts(m.get("closedTime")) or _parse_ts(m.get("umaEndDate")) \
        or _parse_ts(m.get("endDate"))
    if res_ts is None:
        return None
    # Winning token = the one priced ~1. A clean binary resolution has exactly
    # one winner at ~1 and the rest at ~0; anything else (split/void) is unusable.
    winners = [tok for tok, p in zip(tokens, prices) if p >= 0.99]
    losers = [p for p in prices if p <= 0.01]
    if len(winners) == 1 and len(losers) == len(prices) - 1:
        winning = winners[0]
    else:
        winning = None  # void / not cleanly resolved -> edge is undefined, declared
    return ResolvedMarket(
        condition_id=cid, resolution_ts=res_ts, winning_token=winning,
        token_ids=tuple(tokens), category=(m.get("category") or None),
    )


def _parse_ts(value: Any) -> int | None:
    if not value:
        return None
    from datetime import datetime
    s = str(value).strip().replace(" ", "T", 1)
    # tolerate "+00" and "Z" and space-separated forms
    s = s.replace("Z", "+00:00")
    if s.endswith("+00"):
        s = s + ":00"
    try:
        return int(datetime.fromisoformat(s).timestamp())
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# per-(wallet, market) realized edge  (M3 core)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WalletMarketEdge:
    wallet: str
    condition_id: str
    resolution_ts: int
    side_token: str                 # the token the wallet was net-long
    net_tokens: float
    capital: float                  # USDC deployed net-long on that token
    vwap: float
    outcome: float                  # 1.0 if side_token won, else 0.0
    edge: float                     # outcome - vwap  (per-share edge from their side)
    mm_suspect: bool


def wallet_edges(
    fills: list[Fill], market: ResolvedMarket, *, mm_two_sided_frac: float
) -> list[WalletMarketEdge]:
    """Per-wallet realized edge on one resolved market.

    A wallet's position per token = buy_tokens − sell_tokens; its capital = net
    buy USDC. We score the token it was **net long** (the directional bet). Edge
    = outcome(0/1) − vwap. Wallets trading both tokens two-sided beyond
    ``mm_two_sided_frac`` (of gross) are flagged MM_SUSPECT (market-maker/wash).
    Void markets (no clean winner) yield no edges — declared, not guessed.
    """
    if market.winning_token is None:
        return []
    per: dict[tuple[str, str], dict[str, float]] = defaultdict(
        lambda: {"buy_usdc": 0.0, "buy_tok": 0.0, "sell_usdc": 0.0, "sell_tok": 0.0}
    )
    for f in fills:
        if f.token_id not in market.token_ids:
            continue
        a = per[(f.wallet, f.token_id)]
        if f.side == "BUY":
            a["buy_usdc"] += f.usdc
            a["buy_tok"] += f.tokens
        else:
            a["sell_usdc"] += f.usdc
            a["sell_tok"] += f.tokens

    # gross per wallet (both tokens) for the MM screen
    gross: dict[str, float] = defaultdict(float)
    net_long_usdc: dict[str, float] = defaultdict(float)
    for (w, _tok), a in per.items():
        gross[w] += a["buy_usdc"] + a["sell_usdc"]

    out: list[WalletMarketEdge] = []
    # choose each wallet's net-long token (largest positive net token position)
    best: dict[str, tuple[str, dict[str, float]]] = {}
    for (w, tok), a in per.items():
        net_tok = a["buy_tok"] - a["sell_tok"]
        if net_tok <= 0:
            continue
        if w not in best or net_tok > (best[w][1]["buy_tok"] - best[w][1]["sell_tok"]):
            best[w] = (tok, a)
    for w, (tok, a) in best.items():
        net_tok = a["buy_tok"] - a["sell_tok"]
        capital = a["buy_usdc"] - a["sell_usdc"]
        if net_tok <= 0 or a["buy_tok"] <= 0 or capital <= 0:
            continue
        vwap = a["buy_usdc"] / a["buy_tok"]
        # two-sided share: how much of the wallet's gross was on the OTHER token
        other = gross[w] - (a["buy_usdc"] + a["sell_usdc"])
        mm = gross[w] > 0 and (other / gross[w]) > mm_two_sided_frac
        outcome = 1.0 if tok == market.winning_token else 0.0
        out.append(WalletMarketEdge(
            wallet=w, condition_id=market.condition_id,
            resolution_ts=market.resolution_ts, side_token=tok,
            net_tokens=round(net_tok, 4), capital=round(capital, 2),
            vwap=round(vwap, 6), outcome=outcome, edge=round(outcome - vwap, 6),
            mm_suspect=mm,
        ))
    return out


# ---------------------------------------------------------------------------
# as-of wallet skill  (M3)  +  roster  (M4)
# ---------------------------------------------------------------------------


@dataclass
class WalletScore:
    wallet: str
    n_resolved: int
    size_wtd_edge: float            # decayed, size-weighted mean edge (the core skill metric)
    capital_deployed: float
    realized_pnl: float             # sum(edge_i * capital_i)   (report, not ranked directly)
    win_rate: float                 # diagnostic only, NEVER ranked on
    mm_suspect: bool
    eligible: bool
    reason: str = ""


def score_wallets(
    edges: list[WalletMarketEdge], *, as_of: int, cfg: Any
) -> dict[str, WalletScore]:
    """Score every wallet's skill using only markets RESOLVED at/before ``as_of``.

    The core metric is size-weighted, time-decayed **edge over entry price** —
    the calibration measure that defeats the favorite-buyer win-rate trap. Win
    rate is computed for diagnostics but never ranked on. Wallets below the
    resolved-trade floor are ``INSUFFICIENT_DATA`` (eligible=False), not scored
    low; MM-suspect wallets are excluded.
    """
    hl = cfg.decay_half_life_days * _DAY
    by_wallet: dict[str, list[WalletMarketEdge]] = defaultdict(list)
    for e in edges:
        if e.resolution_ts <= as_of:            # AS-OF: outcome known by now
            by_wallet[e.wallet].append(e)

    scores: dict[str, WalletScore] = {}
    for w, es in by_wallet.items():
        mm = any(e.mm_suspect for e in es)
        n = len(es)
        wins = sum(1 for e in es if e.outcome == 1.0)
        # time-decay weight: recent resolutions dominate; old ones still count
        # toward the sample but with less weight (half-life).
        num = den = 0.0
        pnl = 0.0
        cap = 0.0
        for e in es:
            age = max(0, as_of - e.resolution_ts)
            decay = math.exp(-age * math.log(2) / hl)
            w_i = e.capital * decay
            num += w_i * e.edge
            den += w_i
            pnl += e.edge * e.capital
            cap += e.capital
        swe = num / den if den > 0 else 0.0
        eligible = (n >= cfg.min_resolved_trades) and not mm
        reason = "" if eligible else ("MM_SUSPECT" if mm else "INSUFFICIENT_DATA")
        scores[w] = WalletScore(
            wallet=w, n_resolved=n, size_wtd_edge=round(swe, 6),
            capital_deployed=round(cap, 2), realized_pnl=round(pnl, 2),
            win_rate=round(wins / n, 4) if n else 0.0, mm_suspect=mm,
            eligible=eligible, reason=reason,
        )
    return scores


def build_roster(scores: dict[str, WalletScore], *, k: int) -> list[str]:
    """Top-K eligible wallets by size-weighted edge (the roster as-of). Ties
    broken by resolved-trade count then wallet id for determinism."""
    ranked = sorted(
        (s for s in scores.values() if s.eligible),
        key=lambda s: (-s.size_wtd_edge, -s.n_resolved, s.wallet),
    )
    return [s.wallet for s in ranked[:k]]


# ---------------------------------------------------------------------------
# consensus scan  (M6)
# ---------------------------------------------------------------------------


@dataclass
class ConsensusSignal:
    condition_id: str
    signal_ts: int
    side_token: str
    n_participants: int
    agreement: float
    entry_band_lo: float
    entry_band_hi: float
    current_price: float
    remaining_edge: float
    exhausted: bool
    participants: list[str]


def _positions_as_of(
    fills: list[Fill], roster: set[str] | None, *, as_of: int, min_usdc: float
) -> dict[str, dict[str, dict[str, float]]]:
    """wallet -> token -> {net_tokens, capital, vwap} from fills <= as_of.

    ``roster=None`` aggregates ALL wallets (used by the sweep's per-(market,date)
    precompute, which is then filtered to each cell's roster — the min_usdc gate
    is per-position and roster-independent, so precompute-then-filter is
    identical to computing a roster directly)."""
    agg: dict[tuple[str, str], dict[str, float]] = defaultdict(
        lambda: {"buy_usdc": 0.0, "buy_tok": 0.0, "sell_usdc": 0.0, "sell_tok": 0.0,
                 "last_ts": 0.0}
    )
    for f in fills:
        if (roster is not None and f.wallet not in roster) or f.timestamp > as_of:
            continue
        a = agg[(f.wallet, f.token_id)]
        if f.side == "BUY":
            a["buy_usdc"] += f.usdc
            a["buy_tok"] += f.tokens
        else:
            a["sell_usdc"] += f.usdc
            a["sell_tok"] += f.tokens
        a["last_ts"] = max(a["last_ts"], f.timestamp)
    out: dict[str, dict[str, dict[str, float]]] = defaultdict(dict)
    for (w, tok), a in agg.items():
        net_tok = a["buy_tok"] - a["sell_tok"]
        capital = a["buy_usdc"] - a["sell_usdc"]
        if net_tok <= 0 or capital < min_usdc or a["buy_tok"] <= 0:
            continue
        out[w][tok] = {"net_tokens": net_tok, "capital": capital,
                       "vwap": a["buy_usdc"] / a["buy_tok"], "last_ts": a["last_ts"]}
    return out


def scan_consensus_market(
    fills: list[Fill],
    roster: list[str],
    *,
    as_of: int,
    token_ids: tuple[str, ...],
    current_price_by_token: dict[str, float],
    cfg: Any,
) -> ConsensusSignal | None:
    """One market's consensus signal as-of ``as_of``. Returns a signal (possibly
    EXHAUSTED) or None if the participation/agreement gates fail.

    Each roster wallet votes once, for the token it is net-long. Participation
    floor + agreement threshold gate the signal; the remaining-edge gate marks
    it EXHAUSTED if the current price has moved past the participants' entry
    band by more than ``max_edge_paid``."""
    roster_set = set(roster)
    pos = _positions_as_of(fills, roster_set, as_of=as_of, min_usdc=cfg.min_position_usdc)
    return _signal_from_positions(pos, as_of=as_of, token_ids=token_ids,
                                  current_price_by_token=current_price_by_token, cfg=cfg)


def _signal_from_positions(
    pos: dict[str, dict[str, dict[str, float]]],
    *,
    as_of: int,
    token_ids: tuple[str, ...],
    current_price_by_token: dict[str, float],
    cfg: Any,
) -> ConsensusSignal | None:
    """The vote + gate logic, given already-aggregated roster positions. Split
    out of :func:`scan_consensus_market` so the sweep can feed it precomputed,
    roster-filtered positions instead of re-scanning fills for every param cell."""
    # each wallet votes for its largest net-long token in THIS market
    votes: dict[str, list[str]] = defaultdict(list)   # token -> wallets
    entries: dict[str, list[float]] = defaultdict(list)
    for w, toks in pos.items():
        in_market = {t: d for t, d in toks.items() if t in token_ids}
        if not in_market:
            continue
        top = max(in_market.items(), key=lambda kv: kv[1]["capital"])
        votes[top[0]].append(w)
        entries[top[0]].append(top[1]["vwap"])

    n_part = sum(len(ws) for ws in votes.values())
    if n_part < cfg.participation_floor:
        return None
    # majority side
    side_token, side_wallets = max(votes.items(), key=lambda kv: len(kv[1]))
    agreement = len(side_wallets) / n_part
    if agreement < cfg.agreement_threshold:
        return None

    band = sorted(entries[side_token])
    lo, hi = band[0], band[-1]
    cur = current_price_by_token.get(side_token, float("nan"))
    remaining = (hi - cur) if cur == cur else float("nan")  # edge still available vs band top
    # EXHAUSTED if price ran past the band top by more than max_edge_paid
    exhausted = (cur == cur) and (cur - hi > cfg.max_edge_paid)
    # signal ts = the latest participant entry (the moment consensus was complete)
    sig_ts = int(max(pos[w][t]["last_ts"] for t in [side_token] for w in side_wallets))
    # Freshness gate (M6.5): a consensus completed long before the scan is stale
    # by construction — pilot finding: 6-week-old consensus on resolved-favorite
    # positions. No signal.
    freshness_s = getattr(cfg, "freshness_window_days", 14) * _DAY
    if as_of - sig_ts > freshness_s:
        return None
    # Absolute payoff-room ceiling (deviation-flagged in config): at 0.999 there
    # is nothing left to win regardless of band run-up.
    ceiling = getattr(cfg, "price_ceiling", 1.0)
    if cur == cur and cur > ceiling:
        return None
    return ConsensusSignal(
        condition_id="", signal_ts=sig_ts, side_token=side_token,
        n_participants=n_part, agreement=round(agreement, 4),
        entry_band_lo=round(lo, 6), entry_band_hi=round(hi, 6),
        current_price=round(cur, 6) if cur == cur else float("nan"),
        remaining_edge=round(remaining, 6) if remaining == remaining else float("nan"),
        exhausted=exhausted, participants=sorted(side_wallets),
    )


# ---------------------------------------------------------------------------
# outcome evaluation  (owner-realistic entry lag)
# ---------------------------------------------------------------------------


def price_at(fills: list[Fill], token: str, *, at_ts: int, window_s: int = 6 * 3600) -> float | None:
    """Realistic fill price for ``token`` at ``at_ts``: the VWAP of fills on that
    token in ``[at_ts, at_ts + window_s]`` (the owner enters AFTER the signal,
    never at the whales' price). None if no fills in the window — a declared gap.
    """
    num = den = 0.0
    for f in fills:
        if f.token_id == token and at_ts <= f.timestamp <= at_ts + window_s:
            num += f.usdc
            den += f.tokens
    return (num / den) if den > 0 else None


@dataclass
class SignalOutcome:
    condition_id: str
    side_token: str
    entry_price: float | None
    outcome: float                  # 1.0 if side won
    realized_edge: float | None     # outcome - entry_price at realistic lag
    won: bool
    tradeable: bool                 # had a realistic entry price


def evaluate_signal(
    signal: ConsensusSignal,
    fills: list[Fill],
    market: ResolvedMarket,
    *,
    entry_lag_minutes: int,
    entry_anchor_ts: int | None = None,
) -> SignalOutcome:
    """Measure a signal at the OWNER's realistic entry against the eventual
    resolution. Post-resolution info enters here only as the thing predicted,
    never as a decision input.

    ``entry_anchor_ts`` is the owner's INFORMATION time — the scan that surfaced
    the signal. Entry = anchor + lag. Anchoring to signal_ts (when the whales'
    consensus completed, possibly days earlier) would price the owner into a
    moment he could not have known about."""
    entry_ts = (entry_anchor_ts if entry_anchor_ts is not None else signal.signal_ts) \
        + entry_lag_minutes * 60
    entry = price_at(fills, signal.side_token, at_ts=entry_ts)
    outcome = 1.0 if signal.side_token == market.winning_token else 0.0
    realized = (outcome - entry) if entry is not None else None
    return SignalOutcome(
        condition_id=market.condition_id, side_token=signal.side_token,
        entry_price=round(entry, 6) if entry is not None else None,
        outcome=outcome, realized_edge=round(realized, 6) if realized is not None else None,
        won=outcome == 1.0, tradeable=entry is not None,
    )


# ---------------------------------------------------------------------------
# aggregate metrics for a parameter cell
# ---------------------------------------------------------------------------


def summarize_outcomes(outcomes: list[SignalOutcome]) -> dict[str, Any]:
    tradeable = [o for o in outcomes if o.tradeable and o.realized_edge is not None]
    n = len(tradeable)
    edges = [o.realized_edge for o in tradeable]
    wins = sum(1 for o in tradeable if o.realized_edge > 0)
    mean_edge = sum(edges) / n if n else 0.0
    # naive fixed-stake equity path -> max drawdown on realized edge per signal
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for e in edges:
        equity += e
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    return {
        "signals": len(outcomes),
        "tradeable": n,
        "not_tradeable": len(outcomes) - n,   # declared: no realistic entry price
        "hit_rate": round(wins / n, 4) if n else None,
        "mean_realized_edge": round(mean_edge, 6) if n else None,
        "total_realized_edge": round(sum(edges), 4) if n else 0.0,
        "max_drawdown": round(max_dd, 4),
        "positive_expectancy": (mean_edge > 0) if n else None,
    }


# ---------------------------------------------------------------------------
# orchestration + parameter sweep  (CLI-facing)
# ---------------------------------------------------------------------------

import itertools
import json as _json
from pathlib import Path

from .config import LoadedConfig
from .errors import DataLayerError
from .fetching import DataLayer
from .m0f import normalize_fills

_GAMMA = "polymarket_gamma"


def _to_float(v: Any) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _artifact_dir(loaded: LoadedConfig) -> Path:
    d = loaded.config_dir / "data" / "m0c"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _month_windows(start_ts: int, end_ts: int) -> list[tuple[str, str]]:
    """ISO (lo, hi) endDate bounds, one per calendar month spanning the window,
    to keep each gamma query under its offset cap."""
    from datetime import datetime, timezone, timedelta
    out: list[tuple[str, str]] = []
    cur = datetime.fromtimestamp(start_ts, timezone.utc).replace(
        day=1, hour=0, minute=0, second=0, microsecond=0)
    end = datetime.fromtimestamp(end_ts, timezone.utc)
    while cur <= end:
        nxt = (cur + timedelta(days=32)).replace(day=1)
        out.append((cur.strftime("%Y-%m-%dT%H:%M:%SZ"), nxt.strftime("%Y-%m-%dT%H:%M:%SZ")))
        cur = nxt
    return out


def build_universe(dl: DataLayer, loaded: LoadedConfig) -> dict[str, Any]:
    """Target-category markets that RESOLVED inside the replay window (gamma tag
    endpoint, closed=true, paginated per category)."""
    cfg = loaded.config.m0c
    seen: dict[str, dict[str, Any]] = {}
    # /markets?tag_slug does NOT filter by category (returns everything). The
    # /events?tag_slug=<cat>&closed=true endpoint DOES filter correctly and
    # paginates by offset; each event carries its (resolved) markets. This is
    # the M0-F enumeration pattern.
    truncated_chunks = 0
    for tag in cfg.categories:
        offset = 0
        while offset <= 900:  # /events also caps offset (~1000); order by volume
            try:
                body = dl.fetch(
                    source=_GAMMA, base_url=dl.endpoints.polymarket_gamma_api,
                    endpoint="/events",
                    request_params={"tag_slug": tag, "closed": "true", "limit": 100,
                                    "offset": offset, "order": "volume", "ascending": "false"},
                )
            except DataLayerError as exc:
                if "offset too large" in str(exc):
                    truncated_chunks += 1
                    break
                raise
            if not isinstance(body, list) or not body:
                break
            for ev in body:
                for m in (ev.get("markets") or []):
                    rm = resolve_market(m)
                    if rm is None or rm.winning_token is None:
                        continue
                    if not (cfg.replay_start_ts <= rm.resolution_ts <= cfg.replay_end_ts):
                        continue
                    if rm.condition_id not in seen:
                        seen[rm.condition_id] = {
                            "condition_id": rm.condition_id, "question": m.get("question"),
                            "tag": tag, "event_slug": ev.get("slug"),
                            "token_ids": list(rm.token_ids),
                            "winning_token": rm.winning_token, "resolution_ts": rm.resolution_ts,
                            "start_ts": _parse_ts(m.get("startDate")),
                            "volume": _to_float(m.get("volume")),
                        }
            if len(body) < 100:
                break
            offset += 100
        else:
            truncated_chunks += 1  # hit the offset ceiling; low-volume tail dropped
    markets = sorted(seen.values(), key=lambda m: -(m["volume"] or 0))
    art = {"kind": "m0c.universe", "window": [cfg.replay_start_ts, cfg.replay_end_ts],
           "categories": cfg.categories, "markets": markets,
           "truncated_chunks": truncated_chunks}
    (_artifact_dir(loaded) / "universe.json").write_text(
        _json.dumps(art, indent=2, ensure_ascii=False), encoding="utf-8")
    return {"kind": "m0c.universe", "markets": len(markets),
            "truncated_chunks": truncated_chunks,   # declared: low-volume tail dropped
            "artifact": str(_artifact_dir(loaded) / "universe.json")}


def _load_universe(
    loaded: LoadedConfig, limit: int | None, *,
    min_volume: float = 0.0, max_volume: float | None = None,
) -> list[dict[str, Any]]:
    """Universe markets in a volume band, highest-volume first. The band bounds
    pull cost: mega-markets ($100M+) have millions of fills over the 16-month
    window and are individually infeasible to walk via the frozen subgraph, so a
    first replay runs on a mid-volume band (declared) where the pull is
    tractable and skilled-wallet activity is still ample."""
    path = _artifact_dir(loaded) / "universe.json"
    if not path.is_file():
        raise DataLayerError("m0c universe missing — run `consensus m0c universe` first",
                             source="m0c")
    ms = _json.loads(path.read_text(encoding="utf-8"))["markets"]
    band = [m for m in ms if (m.get("volume") or 0) >= min_volume
            and (max_volume is None or (m.get("volume") or 0) <= max_volume)]
    return band[:limit] if limit else band


@dataclass
class MarketData:
    market: ResolvedMarket
    fills: list[Fill]
    question: str | None


def _pull_full(dl: DataLayer, token_ids: list[str], until_ts: int, *, since_ts: int,
               retries: int = 2):
    """Walk a market's fills over [since_ts, until_ts], ONE outcome token at a
    time and merged (deduped by event id).

    Walking both tokens in a single 4-branch ``or`` times the subgraph out on
    mega-markets; a single token's 2-branch ``or`` returns in ~1.6s even on a
    $269M market. Bounded below by the replay window start so full multi-month
    histories stay tractable. Retries a transient timeout per token."""
    from .sources_subgraph import paginate_order_filled
    merged: dict[str, Any] = {}
    prov_pages = 0
    for token in token_ids:
        for attempt in range(retries + 1):
            try:
                evs, prov = paginate_order_filled(
                    dl, asset_ids=[token], ts_gte=since_ts, ts_lt=until_ts + _DAY)
                break
            except DataLayerError as exc:
                if dl.replay or "timed out" not in str(exc).lower() or attempt == retries:
                    raise
                dl.logger.warning("m0c pull token ..%s: timeout, retry %d/%d",
                                  token[-8:], attempt + 1, retries)
        prov_pages += prov["pages"]
        for e in evs:
            merged[e.event_id] = e   # dedup: a fill touching both tokens appears once
    return list(merged.values()), {"pages": prov_pages, "events": len(merged)}


def load_market_data(
    dl: DataLayer, loaded: LoadedConfig, *, limit: int | None,
    min_volume: float = 0.0, max_volume: float | None = None,
) -> tuple[list["MarketData"], dict[str, Any]]:
    """Pull each universe market's full L1 fills (cache-through) and pair with
    its resolution. Per-market fault isolation; failures declared."""
    universe = _load_universe(loaded, limit, min_volume=min_volume, max_volume=max_volume)
    token_to_cid = {t: u["condition_id"] for u in universe for t in u["token_ids"]}
    data: list[MarketData] = []
    failed: list[dict[str, Any]] = []
    for u in universe:
        rm = ResolvedMarket(condition_id=u["condition_id"], resolution_ts=u["resolution_ts"],
                            winning_token=u["winning_token"], token_ids=tuple(u["token_ids"]),
                            category=u.get("tag"))
        try:
            events, _prov = _pull_full(dl, u["token_ids"], u["resolution_ts"],
                                       since_ts=loaded.config.m0c.replay_start_ts)
        except DataLayerError as exc:
            failed.append({"condition_id": u["condition_id"], "error": exc.to_error()})
            continue
        fills, _c, _d = normalize_fills(events, token_to_cid)
        data.append(MarketData(market=rm, fills=fills, question=u.get("question")))
    return data, {"markets": len(data), "failed": failed}


def _rescan_dates(cfg: Any) -> list[int]:
    step = cfg.rescan_cadence_days * _DAY
    return list(range(cfg.replay_start_ts, cfg.replay_end_ts, step))


def _last_price_before(fills: list[Fill], as_of: int) -> dict[str, float]:
    last: dict[str, tuple[int, float]] = {}
    for f in fills:
        if f.timestamp <= as_of and (f.token_id not in last or f.timestamp >= last[f.token_id][0]):
            last[f.token_id] = (f.timestamp, f.price)
    return {tok: px for tok, (_ts, px) in last.items()}


@dataclass
class SweepPrecompute:
    """Param-independent inputs computed ONCE and reused across every sweep cell:
    per-date wallet scores, and per-(market_index, date) all-wallet positions +
    last-price. Turns the sweep's dominant cost from O(cells x dates x fills) into
    O(dates x fills) precompute + O(cells x dates x roster) cheap filtering."""

    scores_by_date: dict[int, dict[str, "WalletScore"]]
    positions: dict[tuple[int, int], dict[str, dict[str, dict[str, float]]]]
    price: dict[tuple[int, int], dict[str, float]]


def build_sweep_precompute(
    market_data: list["MarketData"], *, cfg: Any, edges: list[WalletMarketEdge]
) -> SweepPrecompute:
    """One O(dates x fills) pass: per-date scores + per-(market,date) all-wallet
    positions and last-price. A market contributes to a date only while unresolved
    and with fills before it (same skips as the scan loop)."""
    score_cfg = type("SkC", (), {"decay_half_life_days": cfg.decay_half_life_days,
                                 "min_resolved_trades": cfg.min_resolved_trades})()
    dates = _rescan_dates(cfg)
    scores_by_date = {d: score_wallets(edges, as_of=d, cfg=score_cfg) for d in dates}
    positions: dict[tuple[int, int], dict[str, dict[str, dict[str, float]]]] = {}
    price: dict[tuple[int, int], dict[str, float]] = {}
    for mi, md in enumerate(market_data):
        for d in dates:
            if md.market.resolution_ts <= d:
                continue
            pbt = _last_price_before(md.fills, d)
            if not pbt:
                continue
            price[(mi, d)] = pbt
            positions[(mi, d)] = _positions_as_of(
                md.fills, None, as_of=d, min_usdc=cfg.min_position_usdc)
    return SweepPrecompute(scores_by_date=scores_by_date, positions=positions, price=price)


def replay(
    market_data: list["MarketData"],
    *,
    cfg: Any,
    k: int,
    participation_floor: int,
    agreement_threshold: float,
    max_edge_paid: float,
    precomputed_edges: list[WalletMarketEdge] | None = None,
    precomputed: "SweepPrecompute | None" = None,
) -> list[SignalOutcome]:
    """One parameter cell: as-of roster + consensus scan across the rescan ladder,
    first-signal-per-market dedup, realistic-entry outcome eval. Zero lookahead.

    Realized edges (param-independent) can be precomputed once and reused across
    sweep cells."""
    edges = precomputed_edges
    if edges is None:
        edges = []
        for md in market_data:
            edges.extend(wallet_edges(md.fills, md.market, mm_two_sided_frac=cfg.mm_two_sided_frac))

    score_cfg = type("SkC", (), {"decay_half_life_days": cfg.decay_half_life_days,
                                 "min_resolved_trades": cfg.min_resolved_trades})()
    scan_cfg = type("ScC", (), {"participation_floor": participation_floor,
                                "agreement_threshold": agreement_threshold,
                                "max_edge_paid": max_edge_paid,
                                "min_position_usdc": cfg.min_position_usdc,
                                "freshness_window_days": cfg.freshness_window_days,
                                "price_ceiling": cfg.price_ceiling})()

    outcomes: list[SignalOutcome] = []
    signalled: set[str] = set()
    dates = _rescan_dates(cfg)
    for date in dates:
        # Param-independent per date: wallet scores. Reused across cells when a
        # ``precomputed`` bundle is supplied (the sweep path).
        scores = (precomputed.scores_by_date[date] if precomputed
                  else score_wallets(edges, as_of=date, cfg=score_cfg))
        roster = build_roster(scores, k=k)
        if not roster:
            continue
        roster_set = set(roster)
        for mi, md in enumerate(market_data):
            cid = md.market.condition_id
            if cid in signalled or md.market.resolution_ts <= date:
                continue
            if precomputed:
                price_by_token = precomputed.price.get((mi, date))
                if not price_by_token:   # market absent from precompute = no price/resolved
                    continue
                allpos = precomputed.positions[(mi, date)]
                pos = {w: allpos[w] for w in roster_set if w in allpos}
                sig = _signal_from_positions(
                    pos, as_of=date, token_ids=md.market.token_ids,
                    current_price_by_token=price_by_token, cfg=scan_cfg)
            else:
                price_by_token = _last_price_before(md.fills, date)
                if not price_by_token:
                    continue
                sig = scan_consensus_market(
                    md.fills, roster, as_of=date, token_ids=md.market.token_ids,
                    current_price_by_token=price_by_token, cfg=scan_cfg)
            if sig is None or sig.exhausted:
                continue
            sig.condition_id = cid
            signalled.add(cid)
            outcomes.append(evaluate_signal(sig, md.fills, md.market,
                                            entry_lag_minutes=cfg.entry_lag_minutes,
                                            entry_anchor_ts=date))
    return outcomes


def run_universe(dl: DataLayer, loaded: LoadedConfig) -> dict[str, Any]:
    return build_universe(dl, loaded)


def _sweep_decision(
    cells: list[dict[str, Any]], regimes: list[dict[str, Any]], *, entry_lag_minutes: int
) -> tuple[bool, bool, int, str]:
    """GO / NO-GO with a regime-decay guard (v1.2 §4).

    A cell is 'positive' with >=10 tradeable signals at positive mean edge. But
    an aggregate positive is a STALE ARTIFACT if the signals live only in old
    regimes and the most-recent slice (last in config order — closest to the
    live platform) is empty or negative. Aggregating across regimes masks decay,
    so a naive 'any positive cell -> GO' mislabels a mechanic whose edge has
    already vanished. Returns (go, regime_decay, n_positive, basis)."""
    positive = [c for c in cells if (c.get("tradeable") or 0) >= 10
                and (c.get("mean_realized_edge") or 0) > 0]
    latest = regimes[-1] if regimes else None
    latest_fires = bool(latest and (latest.get("tradeable") or 0) >= 1
                        and (latest.get("mean_realized_edge") or 0) > 0)
    regime_decay = bool(positive) and not latest_fires
    go = bool(positive) and not regime_decay
    if go:
        basis = (f"{len(positive)} sweep cell(s) show positive expectancy at >=10 tradeable "
                 f"signals and realistic +{entry_lag_minutes}min entry, and the most-recent "
                 f"regime slice ({latest['name']}) also fires positive")
    elif regime_decay:
        basis = (f"{len(positive)} cell(s) show aggregate positive expectancy, but the signals "
                 f"are confined to older regimes — the most-recent slice ({latest['name']}) is "
                 f"empty/negative. The edge has DECAYED; the aggregate is a stale artifact "
                 f"(v1.2 §4). Not tradeable on the current regime.")
    else:
        basis = ("no sweep cell shows positive expectancy at a non-trivial sample; on this "
                 "data/window the edge is not demonstrable (necessary-not-sufficient)")
    return go, regime_decay, len(positive), basis


def run_sweep(
    dl: DataLayer, loaded: LoadedConfig, *, limit_markets: int | None = None,
    min_volume: float = 0.0, max_volume: float | None = None,
) -> dict[str, Any]:
    """M0-C deliverable: sweep participation_floor x agreement x K x max_edge_paid,
    report expectancy per cell + regime slices + a GO/NO-GO."""
    cfg = loaded.config.m0c
    market_data, load_info = load_market_data(dl, loaded, limit=limit_markets,
                                               min_volume=min_volume, max_volume=max_volume)
    # realized edges are param-independent -> compute once.
    edges: list[WalletMarketEdge] = []
    for md in market_data:
        edges.extend(wallet_edges(md.fills, md.market, mm_two_sided_frac=cfg.mm_two_sided_frac))

    # Precompute the param-independent scan inputs ONCE (per-date scores +
    # per-(market,date) positions/prices), reused across all sweep cells.
    precomp = build_sweep_precompute(market_data, cfg=cfg, edges=edges)

    sw = cfg.sweep
    cells: list[dict[str, Any]] = []
    best: dict[str, Any] | None = None
    for pf, ag, k, mep in itertools.product(
        sw.participation_floor, sw.agreement_threshold, sw.circle_size_k, sw.max_edge_paid
    ):
        outs = replay(market_data, cfg=cfg, k=k, participation_floor=pf,
                      agreement_threshold=ag, max_edge_paid=mep,
                      precomputed_edges=edges, precomputed=precomp)
        summ = summarize_outcomes(outs)
        cell = {"participation_floor": pf, "agreement_threshold": ag, "circle_size_k": k,
                "max_edge_paid": mep, **summ}
        cells.append(cell)
        if summ["tradeable"] and summ["mean_realized_edge"] is not None:
            if best is None or summ["mean_realized_edge"] > (best["mean_realized_edge"] or -9):
                best = cell

    regimes = []
    for rs in cfg.regime_slices:
        md_slice = [m for m in market_data if rs.start <= m.market.resolution_ts < rs.end]
        outs = replay(md_slice, cfg=cfg, k=cfg.circle_size_k,
                      participation_floor=cfg.participation_floor,
                      agreement_threshold=cfg.agreement_threshold,
                      max_edge_paid=cfg.max_edge_paid)
        regimes.append({"name": rs.name, "markets": len(md_slice), **summarize_outcomes(outs)})

    go, regime_decay, n_positive, basis = _sweep_decision(
        cells, regimes, entry_lag_minutes=cfg.entry_lag_minutes)

    report = {
        "kind": "m0c.sweep",
        "markets_loaded": load_info["markets"], "markets_failed": load_info["failed"],
        "limit_markets": limit_markets, "entry_lag_minutes": cfg.entry_lag_minutes,
        "total_realized_edges": len(edges),
        "decision": "GO" if go else "NO-GO",
        "regime_decay": regime_decay,
        "positive_cells": n_positive,
        "decision_basis": basis,
        "best_cell": best,
        "cells": sorted(cells, key=lambda c: -(c["mean_realized_edge"] or -9)),
        "regime_slices": regimes,
        "caveat": ("Necessary-not-sufficient (v1.2 s4): replay is almost entirely "
                   "pre-June-2026-split regime; L2 forward archive runs the confirmation pass."),
    }
    (_artifact_dir(loaded) / "sweep.json").write_text(
        _json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    report["artifact"] = str(_artifact_dir(loaded) / "sweep.json")
    return report
