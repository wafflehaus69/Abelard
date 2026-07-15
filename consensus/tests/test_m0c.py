"""M0-C consensus replay engine: resolution parsing, realized edge, as-of skill
(zero lookahead), roster, consensus scan gates, realistic-entry outcome eval."""

from __future__ import annotations

import json

import pytest

from consensus.m0c import (
    ResolvedMarket,
    build_roster,
    evaluate_signal,
    price_at,
    resolve_market,
    scan_consensus_market,
    score_wallets,
    summarize_outcomes,
    wallet_edges,
)
from consensus.m0f import Fill


class _Cfg:
    """Minimal cfg stand-in for the pure functions."""
    decay_half_life_days = 90
    min_resolved_trades = 3
    mm_two_sided_frac = 0.35
    participation_floor = 3
    agreement_threshold = 0.75
    max_edge_paid = 0.10
    min_position_usdc = 500
    entry_lag_minutes = 30
    freshness_window_days = 14
    price_ceiling = 0.95


CFG = _Cfg()
YES, NO = "TOKYES", "TOKNO"


def _fill(wallet, token, side, usdc, tokens, ts):
    return Fill(wallet=wallet, condition_id="0xM", token_id=token, side=side,
                usdc=usdc, tokens=tokens, price=usdc / tokens, timestamp=ts, event_id="e")


def _market(win=YES, res_ts=1_000_000):
    return ResolvedMarket(condition_id="0xM", resolution_ts=res_ts, winning_token=win,
                          token_ids=(YES, NO), category="geopolitics")


def test_sweep_precompute_matches_naive_replay():
    """The optimized (precomputed) replay must produce OUTCOME-IDENTICAL results
    to the naive per-cell path — the sweep's speedup must not change a number."""
    from consensus.m0c import (MarketData, ResolvedMarket, replay, wallet_edges,
                               build_sweep_precompute)

    class Cfg:
        decay_half_life_days = 90
        min_resolved_trades = 1
        mm_two_sided_frac = 0.35
        min_position_usdc = 100
        entry_lag_minutes = 30
        freshness_window_days = 3650   # don't gate on freshness in this fixture
        price_ceiling = 0.999
        replay_start_ts = 1_000_000
        replay_end_ts = 1_000_000 + 30 * 86400
        rescan_cadence_days = 7

    # Two resolved markets; several wallets build converging positions over time.
    def mk(cid, win, res, fills):
        return MarketData(market=ResolvedMarket(condition_id=cid, resolution_ts=res,
                          winning_token=win, token_ids=(YES, NO), category="geopolitics"),
                          fills=fills, question=None)
    base = 1_000_000 + 3 * 86400
    m1 = mk("0xA", YES, 1_000_000 + 40 * 86400, [
        _fill("w1", YES, "BUY", 5000, 20000, base),
        _fill("w2", YES, "BUY", 6000, 24000, base + 3600),
        _fill("w3", YES, "BUY", 7000, 28000, base + 7200),
        # a later fill to create price history
        _fill("w4", YES, "BUY", 1000, 3000, base + 10 * 86400),
    ])
    m2 = mk("0xB", NO, 1_000_000 + 40 * 86400, [
        _fill("w1", NO, "BUY", 5000, 12000, base + 8 * 86400),
        _fill("w2", NO, "BUY", 5000, 12000, base + 8 * 86400 + 3600),
        _fill("w5", NO, "BUY", 5000, 12000, base + 8 * 86400 + 7200),
    ])
    md = [m1, m2]
    cfg = Cfg()
    edges = []
    for m in md:
        edges.extend(wallet_edges(m.fills, m.market, mm_two_sided_frac=cfg.mm_two_sided_frac))
    pre = build_sweep_precompute(md, cfg=cfg, edges=edges)

    for k, pf, ag, mep in [(15, 3, 0.75, 0.10), (10, 2, 0.6, 0.05), (25, 4, 0.8, 0.2)]:
        naive = replay(md, cfg=cfg, k=k, participation_floor=pf, agreement_threshold=ag,
                       max_edge_paid=mep, precomputed_edges=edges)
        fast = replay(md, cfg=cfg, k=k, participation_floor=pf, agreement_threshold=ag,
                      max_edge_paid=mep, precomputed_edges=edges, precomputed=pre)
        assert [o.__dict__ for o in naive] == [o.__dict__ for o in fast], (
            f"precompute diverged from naive at k={k} pf={pf} ag={ag} mep={mep}")


# -- resolution parsing --------------------------------------------------------


def test_resolve_market_picks_winner_and_ts():
    m = resolve_market({
        "conditionId": "0xM", "closed": True, "umaResolutionStatus": "resolved",
        "clobTokenIds": json.dumps([YES, NO]), "outcomePrices": json.dumps(["0", "1"]),
        "closedTime": "2025-07-01 07:59:05+00", "category": "geopolitics",
    })
    assert m is not None
    assert m.winning_token == NO           # priced "1"
    assert m.resolution_ts == 1751356745   # 2025-07-01T07:59:05Z
    assert m.category == "geopolitics"


def test_resolve_market_rejects_unresolved_and_void():
    base = {"conditionId": "0xM", "closed": True, "clobTokenIds": json.dumps([YES, NO])}
    # UMA not resolved
    assert resolve_market({**base, "umaResolutionStatus": "proposed",
                           "outcomePrices": json.dumps(["0", "1"]), "closedTime": "2025-07-01T00:00:00Z"}) is None
    # split/void (both ~0.5) -> winner undefined -> parsed but winning_token None
    m = resolve_market({**base, "umaResolutionStatus": "resolved",
                        "outcomePrices": json.dumps(["0.5", "0.5"]), "closedTime": "2025-07-01T00:00:00Z"})
    assert m is not None and m.winning_token is None
    # not closed
    assert resolve_market({**base, "closed": False,
                           "outcomePrices": json.dumps(["0", "1"]), "closedTime": "2025-07-01T00:00:00Z"}) is None


# -- realized edge -------------------------------------------------------------


def test_wallet_edge_is_outcome_minus_vwap():
    # wallet bought YES @ 0.20; YES won -> edge = 1 - 0.20 = 0.80
    fills = [_fill("0xw", YES, "BUY", 200, 1000, 500_000)]
    edges = wallet_edges(fills, _market(win=YES), mm_two_sided_frac=0.35)
    assert len(edges) == 1
    e = edges[0]
    assert e.side_token == YES and e.vwap == pytest.approx(0.20)
    assert e.outcome == 1.0 and e.edge == pytest.approx(0.80)


def test_wallet_edge_favorite_buyer_has_low_edge():
    # bought YES @ 0.95, YES won -> 92% "win" but edge only 0.05 (the trap)
    fills = [_fill("0xw", YES, "BUY", 950, 1000, 500_000)]
    e = wallet_edges(fills, _market(win=YES), mm_two_sided_frac=0.35)[0]
    assert e.edge == pytest.approx(0.05)


def test_wallet_edge_mm_suspect_flagged():
    # heavy two-sided trading -> MM_SUSPECT
    fills = [_fill("0xw", YES, "BUY", 1000, 5000, 1), _fill("0xw", NO, "BUY", 1000, 5000, 2),
             _fill("0xw", NO, "SELL", 900, 4500, 3)]
    e = wallet_edges(fills, _market(win=YES), mm_two_sided_frac=0.35)
    assert e and e[0].mm_suspect is True


def test_void_market_yields_no_edges():
    fills = [_fill("0xw", YES, "BUY", 200, 1000, 1)]
    assert wallet_edges(fills, _market(win=None), mm_two_sided_frac=0.35) == []


# -- as-of skill (ZERO LOOKAHEAD) ----------------------------------------------


def _edges_for(wallet, specs):
    """specs: list of (win_bool, vwap, capital, res_ts) -> WalletMarketEdge list."""
    from consensus.m0c import WalletMarketEdge
    out = []
    for i, (won, vwap, cap, res_ts) in enumerate(specs):
        out.append(WalletMarketEdge(
            wallet=wallet, condition_id=f"0xM{i}", resolution_ts=res_ts,
            side_token="T", net_tokens=cap / vwap, capital=cap, vwap=vwap,
            outcome=1.0 if won else 0.0, edge=(1.0 if won else 0.0) - vwap, mm_suspect=False))
    return out


def test_score_excludes_markets_resolved_after_as_of():
    """The cardinal rule: a market resolving AFTER as_of contributes nothing to
    skill as-of that time (its outcome is unknowable then)."""
    edges = _edges_for("0xw", [
        (True, 0.3, 1000, 500_000),   # resolved before as_of
        (True, 0.3, 1000, 900_000),   # resolved before as_of
        (True, 0.3, 1000, 2_000_000), # resolves AFTER as_of -> must be ignored
    ])
    s = score_wallets(edges, as_of=1_000_000, cfg=CFG)["0xw"]
    assert s.n_resolved == 2                    # only the two known-by-now
    assert s.eligible is False                  # below min_resolved_trades=3 as-of now
    # later, all three are known
    s2 = score_wallets(edges, as_of=3_000_000, cfg=CFG)["0xw"]
    assert s2.n_resolved == 3 and s2.eligible is True


def test_score_edge_over_entry_beats_win_rate():
    """Two wallets, both 100% win rate; the one buying cheap has real edge, the
    favorite-buyer ~none. Ranking must prefer the cheap buyer."""
    cheap = _edges_for("0xcheap", [(True, 0.2, 1000, t) for t in (1, 2, 3)])
    fav = _edges_for("0xfav", [(True, 0.95, 1000, t) for t in (1, 2, 3)])
    scores = score_wallets(cheap + fav, as_of=1_000_000, cfg=CFG)
    assert scores["0xcheap"].win_rate == 1.0 and scores["0xfav"].win_rate == 1.0
    assert scores["0xcheap"].size_wtd_edge > scores["0xfav"].size_wtd_edge + 0.5
    assert build_roster(scores, k=1) == ["0xcheap"]   # ranked on edge, not win rate


def test_score_decay_weights_recent_more():
    recent = _edges_for("0xr", [(True, 0.4, 1000, 990_000)] * 1 + [(False, 0.4, 1000, 1)] * 3)
    # recent win (edge +0.6) should pull the decayed mean up vs the old losses
    s = score_wallets(recent, as_of=1_000_000, cfg=CFG)["0xr"]
    naive = (0.6 + (-0.4) * 3) / 4
    assert s.size_wtd_edge > naive     # decay favors the recent win


def test_insufficient_and_mm_are_declared_not_low():
    thin = _edges_for("0xthin", [(True, 0.3, 1000, 1)])
    s = score_wallets(thin, as_of=1_000_000, cfg=CFG)["0xthin"]
    assert s.eligible is False and s.reason == "INSUFFICIENT_DATA"
    assert build_roster(score_wallets(thin, as_of=1_000_000, cfg=CFG), k=10) == []


# -- consensus scan gates ------------------------------------------------------


def _pos_fills(wallets_side, ts=500_000):
    """wallets_side: list of (wallet, token, vwap). Each buys $1000 of token."""
    return [_fill(w, tok, "BUY", 1000, 1000 / vwap, ts) for w, tok, vwap in wallets_side]


def test_scan_participation_floor():
    fills = _pos_fills([("a", YES, 0.3), ("b", YES, 0.3)])   # only 2 < floor 3
    sig = scan_consensus_market(fills, ["a", "b", "c"], as_of=1_000_000,
                                token_ids=(YES, NO), current_price_by_token={YES: 0.35}, cfg=CFG)
    assert sig is None


def test_scan_agreement_threshold():
    fills = _pos_fills([("a", YES, 0.3), ("b", YES, 0.3), ("c", NO, 0.3), ("d", NO, 0.3)])
    # 2 YES / 2 NO -> agreement 0.5 < 0.75
    sig = scan_consensus_market(fills, ["a", "b", "c", "d"], as_of=1_000_000,
                                token_ids=(YES, NO), current_price_by_token={YES: 0.35}, cfg=CFG)
    assert sig is None


def test_scan_emits_signal_on_agreement():
    fills = _pos_fills([("a", YES, 0.30), ("b", YES, 0.32), ("c", YES, 0.34), ("d", NO, 0.3)])
    sig = scan_consensus_market(fills, ["a", "b", "c", "d"], as_of=1_000_000,
                                token_ids=(YES, NO), current_price_by_token={YES: 0.36}, cfg=CFG)
    assert sig is not None
    assert sig.side_token == YES and sig.n_participants == 4
    assert sig.agreement == pytest.approx(0.75)
    assert sig.entry_band_lo == pytest.approx(0.30) and sig.entry_band_hi == pytest.approx(0.34)
    assert sig.exhausted is False           # 0.36 - 0.34 = 0.02 < max_edge_paid 0.10


def test_scan_remaining_edge_gate_exhausted():
    fills = _pos_fills([("a", YES, 0.30), ("b", YES, 0.31), ("c", YES, 0.32)])
    # current price 0.50 -> ran 0.18 past band top 0.32 > max_edge_paid 0.10
    sig = scan_consensus_market(fills, ["a", "b", "c"], as_of=1_000_000,
                                token_ids=(YES, NO), current_price_by_token={YES: 0.50}, cfg=CFG)
    assert sig is not None and sig.exhausted is True


def test_scan_one_vote_per_wallet_largest_position():
    # wallet 'a' holds both; its larger capital is on NO -> votes NO
    fills = [_fill("a", YES, "BUY", 600, 2000, 1), _fill("a", NO, "BUY", 1400, 4000, 2),
             *_pos_fills([("b", NO, 0.35), ("c", NO, 0.35)], ts=3)]
    sig = scan_consensus_market(fills, ["a", "b", "c"], as_of=1_000_000,
                                token_ids=(YES, NO), current_price_by_token={NO: 0.36}, cfg=CFG)
    assert sig is not None and sig.side_token == NO and sig.n_participants == 3


def test_scan_freshness_gate_kills_stale_consensus():
    """Pilot finding: a consensus completed weeks before the scan is stale by
    construction (M6.5) — no signal, even if all other gates pass."""
    scan_ts = 1_800_000_000
    stale_ts = scan_ts - 20 * 86400   # 20 days before the scan (window 14d)
    fills = _pos_fills([("a", YES, 0.30), ("b", YES, 0.31), ("c", YES, 0.32)], ts=stale_ts)
    sig = scan_consensus_market(fills, ["a", "b", "c"], as_of=scan_ts,
                                token_ids=(YES, NO), current_price_by_token={YES: 0.35}, cfg=CFG)
    assert sig is None
    # same consensus, formed 2 days before the scan -> fresh -> signal
    fresh = _pos_fills([("a", YES, 0.30), ("b", YES, 0.31), ("c", YES, 0.32)],
                       ts=scan_ts - 2 * 86400)
    sig2 = scan_consensus_market(fresh, ["a", "b", "c"], as_of=scan_ts,
                                 token_ids=(YES, NO), current_price_by_token={YES: 0.35}, cfg=CFG)
    assert sig2 is not None


def test_scan_price_ceiling_kills_no_room_signals():
    """Pilot finding: a 0.99 'signal' has no payoff room regardless of band —
    gated by the (deviation-flagged) absolute ceiling."""
    fills = _pos_fills([("a", YES, 0.97), ("b", YES, 0.98), ("c", YES, 0.98)],
                       ts=1_000_000 - 3600)
    sig = scan_consensus_market(fills, ["a", "b", "c"], as_of=1_000_000,
                                token_ids=(YES, NO), current_price_by_token={YES: 0.99}, cfg=CFG)
    assert sig is None


def test_evaluate_anchors_entry_to_scan_information_time():
    """The owner learns of a signal at the SCAN; entry must be scan+lag, not the
    (possibly much earlier) consensus-completion moment."""
    from consensus.m0c import ConsensusSignal
    sig = ConsensusSignal(condition_id="0xM", signal_ts=900_000, side_token=YES,
                          n_participants=3, agreement=1.0, entry_band_lo=0.3,
                          entry_band_hi=0.34, current_price=0.4, remaining_edge=0.0,
                          exhausted=False, participants=["a"])
    fills = [
        _fill("x", YES, "BUY", 35, 100, 900_000 + 30 * 60 + 10),   # 0.35 near signal_ts+lag
        _fill("y", YES, "BUY", 60, 100, 1_000_000 + 30 * 60 + 10), # 0.60 near scan+lag
    ]
    o = evaluate_signal(sig, fills, _market(win=YES), entry_lag_minutes=30,
                        entry_anchor_ts=1_000_000)
    assert o.entry_price == pytest.approx(0.60)   # scan-anchored, not signal-anchored


def test_scan_positions_are_as_of():
    """A roster wallet's position must reflect only fills <= as_of."""
    fills = _pos_fills([("a", YES, 0.3), ("b", YES, 0.3), ("c", YES, 0.3)], ts=2_000_000)
    sig = scan_consensus_market(fills, ["a", "b", "c"], as_of=1_000_000,  # before the fills
                                token_ids=(YES, NO), current_price_by_token={YES: 0.35}, cfg=CFG)
    assert sig is None   # no positions visible as-of 1,000,000


# -- outcome eval --------------------------------------------------------------


def test_price_at_uses_post_signal_window():
    fills = [_fill("x", YES, "BUY", 40, 100, 1_000_000),      # at signal
             _fill("y", YES, "BUY", 90, 200, 1_000_000 + 600)]  # +10min, within window
    px = price_at(fills, YES, at_ts=1_000_000, window_s=3600)
    assert px == pytest.approx((40 + 90) / (100 + 200))       # VWAP over the window


def test_evaluate_signal_realistic_entry_and_outcome():
    from consensus.m0c import ConsensusSignal
    sig = ConsensusSignal(condition_id="0xM", signal_ts=1_000_000, side_token=YES,
                          n_participants=4, agreement=0.8, entry_band_lo=0.30,
                          entry_band_hi=0.34, current_price=0.36, remaining_edge=0.0,
                          exhausted=False, participants=["a"])
    # owner enters +30min at 0.40; YES resolves YES(1) -> realized edge 0.60
    entry = [_fill("z", YES, "BUY", 40, 100, 1_000_000 + 30 * 60 + 5)]
    o = evaluate_signal(sig, entry, _market(win=YES), entry_lag_minutes=30)
    assert o.entry_price == pytest.approx(0.40) and o.won is True
    assert o.realized_edge == pytest.approx(0.60) and o.tradeable is True


def test_evaluate_signal_not_tradeable_when_no_fills():
    from consensus.m0c import ConsensusSignal
    sig = ConsensusSignal(condition_id="0xM", signal_ts=1_000_000, side_token=YES,
                          n_participants=4, agreement=0.8, entry_band_lo=0.3, entry_band_hi=0.34,
                          current_price=0.36, remaining_edge=0.0, exhausted=False, participants=["a"])
    o = evaluate_signal(sig, [], _market(win=YES), entry_lag_minutes=30)
    assert o.tradeable is False and o.realized_edge is None   # declared, not a fake 0


# -- aggregation ---------------------------------------------------------------


def test_summarize_expectancy_and_drawdown():
    from consensus.m0c import SignalOutcome
    outs = [
        SignalOutcome("0xM1", YES, 0.4, 1.0, 0.6, True, True),
        SignalOutcome("0xM2", YES, 0.7, 0.0, -0.7, False, True),
        SignalOutcome("0xM3", YES, 0.5, 1.0, 0.5, True, True),
        SignalOutcome("0xM4", NO, None, 0.0, None, False, False),  # not tradeable
    ]
    s = summarize_outcomes(outs)
    assert s["signals"] == 4 and s["tradeable"] == 3 and s["not_tradeable"] == 1
    assert s["hit_rate"] == pytest.approx(2 / 3, abs=1e-3)
    assert s["mean_realized_edge"] == pytest.approx((0.6 - 0.7 + 0.5) / 3, abs=1e-3)
    assert s["max_drawdown"] == pytest.approx(0.7)  # peak 0.6 -> -0.1 trough
    assert s["positive_expectancy"] is True
