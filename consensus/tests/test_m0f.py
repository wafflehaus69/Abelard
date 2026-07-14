"""M0-F engine: fill normalization, as-of discipline, the seven factors,
cluster amplifier, tiers, hypothesis matching."""

from __future__ import annotations

import pytest

from consensus.m0f import (
    CandidateScore,
    Fill,
    apply_cluster_amplifier,
    assign_tiers,
    match_hypotheses,
    normalize_fills,
    score_candidates_as_of,
    trailing_volumes,
)
from consensus.models import OrderFilledEvent


def _cfg(loaded):
    return loaded.config.m0f


def _event(i, *, maker="0xw1", maker_asset="0", taker_asset="TOK", ts=1_772_000_000,
           maker_amt=10_850_000, taker_amt=100_000_000, taker="0xe4bfb"):
    return OrderFilledEvent(
        event_id=f"0xtx{i}_0xoh{i}", timestamp=ts, maker=maker, taker=taker,
        maker_asset_id=maker_asset, taker_asset_id=taker_asset,
        maker_amount_filled=maker_amt, taker_amount_filled=taker_amt, fee=0,
    )


TOKENS = {"TOK": "0xMKT", "TOK2": "0xMKT2"}


# -- normalization -------------------------------------------------------------


def test_normalize_buy_sell_and_price():
    buy = _event(1)                                            # 10.85 USDC -> 100 TOK
    sell = _event(2, maker_asset="TOK", taker_asset="0",
                  maker_amt=50_000_000, taker_amt=10_000_000)  # 50 TOK -> 10 USDC
    fills, crossing, drops = normalize_fills([buy, sell], TOKENS)
    assert len(fills) == 2
    b, s = fills
    assert b.side == "BUY" and b.usdc == pytest.approx(10.85) and b.tokens == 100
    assert b.price == pytest.approx(0.1085)
    assert s.side == "SELL" and s.usdc == pytest.approx(10.0) and s.price == pytest.approx(0.2)
    # taker (counterparty) crossing volume attributed, never added to fills
    assert crossing["0xe4bfb"] == pytest.approx(20.85)
    assert drops == {"token_for_token": 0, "unknown_token": 0, "nonpositive": 0}


def test_normalize_drops_are_counted_not_silent():
    swap = _event(3, maker_asset="TOK", taker_asset="TOK2")       # token-for-token
    unknown = _event(4, taker_asset="UNKNOWN")                    # not in universe
    zero = _event(5, taker_amt=0)                                 # nonpositive tokens
    fills, _c, drops = normalize_fills([swap, unknown, zero], TOKENS)
    assert fills == []
    # Every dropped event is declared by reason (Rule 1) — never a silent continue.
    assert drops == {"token_for_token": 1, "unknown_token": 1, "nonpositive": 1}


# -- as-of discipline + stage 1 ---------------------------------------------------


def _fill(wallet="0xw1", cid="0xMKT", token="TOK", side="BUY", usdc=6000.0,
          tokens=60000.0, ts=1_772_000_000, eid="e1"):
    return Fill(wallet=wallet, condition_id=cid, token_id=token, side=side,
                usdc=usdc, tokens=tokens, price=usdc / tokens, timestamp=ts,
                event_id=eid)


def _info(wallet="0xw1", first_seen=1_771_900_000, prior=0):
    return {wallet: {"wallet": wallet, "first_seen_ts": first_seen,
                     "prior_fills": prior, "prior_fills_capped": False}}


def test_as_of_excludes_future_fills(loaded):
    fills = [_fill(ts=1_772_000_000), _fill(ts=1_772_100_000, eid="e2", usdc=6000, tokens=60000)]
    cands = score_candidates_as_of(
        as_of=1_772_050_000, fills=fills, crossing_usdc={}, wallet_info=_info(),
        market_trailing_vol={"0xMKT": 100_000}, cfg=_cfg(loaded),
    )
    assert len(cands) == 1
    assert cands[0].net_stake_usdc == 6000  # only the visible fill


def test_as_of_boundary_is_inclusive(loaded):
    """A fill exactly at the as-of instant must be visible (<=, not <)."""
    fills = [_fill(ts=1_772_000_000)]
    cands = score_candidates_as_of(
        as_of=1_772_000_000, fills=fills, crossing_usdc={}, wallet_info=_info(),
        market_trailing_vol={"0xMKT": 100_000}, cfg=_cfg(loaded),
    )
    assert len(cands) == 1


def test_enrichment_failure_is_insufficient_data_not_imputed(loaded):
    """A wallet whose enrichment ERRORED must not get a real tier from a
    renormalized (imputed-freshness) composite — it is INSUFFICIENT_DATA."""
    from consensus.m0f import assign_tiers
    fills = [_fill(usdc=60_000, tokens=555_000)]
    info = {"0xw1": {"wallet": "0xw1", "first_seen_ts": None, "prior_fills": None,
                     "error": "goldsky timeout"}}
    cands = score_candidates_as_of(
        as_of=2_000_000_000, fills=fills, crossing_usdc={}, wallet_info=info,
        market_trailing_vol={"0xMKT": 500_000}, cfg=_cfg(loaded),
    )
    assert cands[0].data_incomplete is True
    assert "F" not in cands[0].factors and "T" not in cands[0].factors
    assign_tiers(cands, _cfg(loaded).tier_thresholds)
    assert cands[0].tier == "INSUFFICIENT_DATA"  # never CRITICAL from imputation


def test_stage1_floors(loaded):
    small = [_fill(usdc=1000, tokens=10000)]
    assert score_candidates_as_of(as_of=2_000_000_000, fills=small, crossing_usdc={},
                                  wallet_info=_info(), market_trailing_vol={},
                                  cfg=_cfg(loaded)) == []
    # Two-sided churn (net/gross < 0.8) is excluded — MM shape, not conviction.
    churn = [_fill(usdc=20_000, tokens=100_000),
             _fill(side="SELL", usdc=15_000, tokens=80_000, eid="e2")]
    assert score_candidates_as_of(as_of=2_000_000_000, fills=churn, crossing_usdc={},
                                  wallet_info=_info(), market_trailing_vol={},
                                  cfg=_cfg(loaded)) == []


# -- factors ------------------------------------------------------------------------


def test_fresh_wallet_deep_price_scores_high(loaded):
    """The Feb-28 signature: 3-day-old wallet, all-in, 10.8c entry, aggressive."""
    fills = [_fill(usdc=60_828, tokens=560_680, ts=1_772_000_000)]
    cands = score_candidates_as_of(
        as_of=1_772_100_000, fills=fills,
        crossing_usdc={"0xw1": 60_828},                      # fully marketable
        wallet_info=_info(first_seen=1_771_800_000),         # ~2.3 days old
        market_trailing_vol={"0xMKT": 500_000}, cfg=_cfg(loaded),
    )
    assert len(cands) == 1
    c = cands[0]
    assert c.factors["F"] == 1.0            # < 7 days
    assert c.factors["S"] == 1.0            # 12% of trailing >> 5% full scale
    assert c.factors["D"] == pytest.approx(1 - 0.1085, abs=0.001)
    assert c.factors["C"] == 1.0
    assert c.composite > 0.8
    # A and P are honestly excluded on run (a), not fabricated.
    assert "A" not in c.factors and "A_excluded" in c.notes
    assert "P" not in c.factors and "P_excluded" in c.notes
    assert set(c.factors_active) == {"F", "S", "D", "C", "T"}


def test_old_whale_suppressed_by_geometric_mean(loaded):
    """A 3-year-old whale making the same bet must NOT alert: F~=0.02 drags the
    geometric composite down — the spec's stated reason for geometric."""
    fills = [_fill(usdc=60_828, tokens=560_680, ts=1_772_000_000)]
    cands = score_candidates_as_of(
        as_of=1_772_100_000, fills=fills, crossing_usdc={"0xw1": 60_828},
        wallet_info=_info(first_seen=1_772_000_000 - 3 * 365 * 86400, prior=0),
        market_trailing_vol={"0xMKT": 500_000}, cfg=_cfg(loaded),
    )
    c = cands[0]
    assert c.factors["F"] == 0.02
    assert c.composite < 0.35


def test_favorite_buyer_scores_below_deep_conviction(loaded):
    """Fresh wallet buying the 95c favorite vs the same wallet buying at 10.8c:
    contrarian depth (D) must separate them — the favorite buyer scores clearly
    lower (anti win-rate-trap). The absolute threshold is a calibration output
    of the backtest, so the robust invariant is the SEPARATION, not a number."""
    cfg = _cfg(loaded)
    common = dict(as_of=1_772_100_000, crossing_usdc={"0xw1": 57_000},
                  wallet_info=_info(first_seen=1_771_900_000),
                  market_trailing_vol={"0xMKT": 500_000}, cfg=cfg)
    favorite = score_candidates_as_of(
        fills=[_fill(usdc=57_000, tokens=60_000)], **common)[0]           # px 0.95
    deep = score_candidates_as_of(
        fills=[_fill(usdc=57_000, tokens=527_777, ts=1_772_000_000)], **common)[0]  # px 0.108
    assert favorite.factors["D"] == pytest.approx(0.05)
    assert deep.factors["D"] == pytest.approx(0.892, abs=0.01)
    assert favorite.composite < deep.composite - 0.25  # clear separation


def test_prior_history_discounts_freshness(loaded):
    fills = [_fill(usdc=20_000, tokens=100_000)]
    cands = score_candidates_as_of(
        as_of=2_000_000_000, fills=fills, crossing_usdc={},
        wallet_info=_info(first_seen=1_771_990_000, prior=60),  # young but busy
        market_trailing_vol={"0xMKT": 500_000}, cfg=_cfg(loaded),
    )
    assert cands[0].factors["F"] == pytest.approx(1.0 * 0.3)


def test_zero_trailing_volume_is_max_relative_size(loaded):
    fills = [_fill(usdc=20_000, tokens=100_000)]
    cands = score_candidates_as_of(
        as_of=2_000_000_000, fills=fills, crossing_usdc={},
        wallet_info=_info(), market_trailing_vol={}, cfg=_cfg(loaded),
    )
    assert cands[0].factors["S"] == 1.0
    assert cands[0].notes["trailing_vol_7d"] == 0.0


# -- clusters + tiers ------------------------------------------------------------------


def _cand(wallet, cid="0xMKT", composite=0.5, first_bet=1_772_000_000):
    return CandidateScore(wallet=wallet, condition_id=cid, token_id="TOK",
                          net_stake_usdc=10_000, vwap_entry=0.1,
                          first_bet_ts=first_bet, last_bet_ts=first_bet,
                          composite=composite)


def test_cluster_amplifier_boosts_and_records(loaded):
    cfg = _cfg(loaded)
    cands = [_cand("0xa", first_bet=1_772_000_000),
             _cand("0xb", first_bet=1_772_010_000),
             _cand("0xc", first_bet=1_772_020_000)]
    clusters = apply_cluster_amplifier(cands, cfg=cfg, elevated_floor=0.30)
    assert clusters and clusters[0]["members"] == 3
    assert all(c.composite == pytest.approx(0.75) for c in cands)  # 0.5 * 1.5
    assert all(c.cluster_ids for c in cands)


def test_cluster_needs_min_distinct_wallets(loaded):
    cands = [_cand("0xa"), _cand("0xb")]
    assert apply_cluster_amplifier(cands, cfg=_cfg(loaded), elevated_floor=0.30) == []
    assert all(not c.cluster_ids for c in cands)


def test_cluster_counts_distinct_wallets_not_rows(loaded):
    """3 candidate ROWS but only 2 distinct WALLETS (one wallet in two markets
    in the burst) must NOT form a cluster — cluster_min is on distinct wallets."""
    cands = [_cand("0xa", cid="0xM1", first_bet=1_772_000_000),
             _cand("0xa", cid="0xM2", first_bet=1_772_000_050),
             _cand("0xb", cid="0xM1", first_bet=1_772_000_100)]
    assert apply_cluster_amplifier(cands, cfg=_cfg(loaded), elevated_floor=0.30) == []
    assert all(not c.cluster_ids for c in cands)


def test_cluster_last_bet_ts_spans_activity(loaded):
    """Reported last_bet_ts must reflect members' LAST bets, not first."""
    a = _cand("0xa", first_bet=1_772_000_000); a.last_bet_ts = 1_772_030_000
    b = _cand("0xb", first_bet=1_772_000_100); b.last_bet_ts = 1_772_040_000
    c = _cand("0xc", first_bet=1_772_000_200); c.last_bet_ts = 1_772_005_000
    cl = apply_cluster_amplifier([a, b, c], cfg=_cfg(loaded), elevated_floor=0.30)
    assert cl[0]["last_bet_ts"] == 1_772_040_000  # max of last_bet_ts, not first


def test_cross_market_cluster_detected(loaded):
    """Same-window high scorers spread across SIBLING markets — the v1.2 §3
    detection feature (the Feb-28 wallets did exactly this)."""
    cands = [_cand("0xa", cid="0xMKT"),
             _cand("0xb", cid="0xMKT2"),
             _cand("0xc", cid="0xMKT3")]
    clusters = apply_cluster_amplifier(cands, cfg=_cfg(loaded), elevated_floor=0.30)
    scopes = {c["scope"] for c in clusters}
    assert scopes == {"cross-market"}  # no single market has 3; the set does


def test_cross_market_gate_disables_scope(loaded, monkeypatch):
    """M0-F calibration: cross_market_enabled=False suppresses the cross-market
    scope (the false-positive driver) while per-market clustering still fires."""
    import copy
    cfg = copy.copy(_cfg(loaded))
    object.__setattr__(cfg, "cross_market_enabled", False)
    spread = [_cand("0xa", cid="0xM1"), _cand("0xb", cid="0xM2"), _cand("0xc", cid="0xM3")]
    assert apply_cluster_amplifier(spread, cfg=cfg, elevated_floor=0.30) == []
    # per-market still works with the gate off
    same = [_cand("0xa", cid="0xM"), _cand("0xb", cid="0xM"), _cand("0xc", cid="0xM")]
    cl = apply_cluster_amplifier(same, cfg=cfg, elevated_floor=0.30)
    assert cl and cl[0]["scope"] == "market"


def test_tier_assignment_and_cluster_elevation(loaded):
    cfg = _cfg(loaded)
    plain = _cand("0xa", composite=0.35)
    clustered = _cand("0xb", composite=0.35)
    clustered.cluster_ids = ["market:0xMKT:1"]
    assign_tiers([plain, clustered], cfg.tier_thresholds)
    assert plain.tier == "ELEVATED"
    assert clustered.tier == "CRITICAL"  # auto-elevated one tier


# -- hypothesis matching ------------------------------------------------------------------


def _cand_pos(wallet, buy_tokens, sell_tokens, vwap):
    c = _cand(wallet)
    c.buy_tokens, c.sell_tokens, c.vwap_entry = buy_tokens, sell_tokens, vwap
    c.net_stake_usdc = (buy_tokens - sell_tokens) * vwap
    return c


def test_match_by_address_and_signature(loaded):
    cfg = _cfg(loaded)
    known = _cand_pos("0xknown", 560_680, 0, 0.1085)
    sig = _cand_pos("0xsig", 200_747, 0, 0.132)             # net shares 200,747 @ 13.2c
    other = _cand_pos("0xother", 18_000, 0, 0.5)
    m = match_hypotheses([known, sig, other], cfg.labeled_hypotheses)
    assert m["known"]["hits"][0]["wallet"] == "0xknown"
    assert m["known"]["hits"][0]["match"] == "address"
    assert m["sig-only"]["hits"][0]["wallet"] == "0xsig"
    assert m["sig-only"]["hits"][0]["match"] == "signature"


def test_match_uses_net_shares_held_not_usd_over_vwap(loaded):
    """Review fix: a wallet that took profit must still match on net shares held.
    Bought 220,747 @ 0.12, sold 20,000 -> net 200,747; net_usd/vwap would give a
    different (wrong) share count and drop the true match."""
    cfg = _cfg(loaded)
    profit_taker = _cand_pos("0xpt", 220_747, 20_000, 0.132)
    m = match_hypotheses([profit_taker], cfg.labeled_hypotheses)
    assert m["sig-only"]["hits"] and m["sig-only"]["hits"][0]["wallet"] == "0xpt"
    assert m["sig-only"]["hits"][0]["net_shares"] == 200_747


def test_price_only_hypothesis_declared_unmatchable(loaded):
    """A hypothesis with no address and no approx_shares can't be matched — that
    must be DECLARED, never conflated with searched-and-absent (empty hits)."""
    from consensus.config import LabeledHypothesis
    h = LabeledHypothesis(name="price-only", approx_price=0.19)
    m = match_hypotheses([_cand_pos("0xa", 100_000, 0, 0.19)], [h])
    assert m["price-only"]["matchable"] is False
    assert m["price-only"]["hits"] == []
    assert "no address" in m["price-only"]["reason"]


def test_signature_share_tolerance_boundary(loaded):
    cfg = _cfg(loaded)
    # 200,747 target, 3% tol -> 194,725..206,769 matches; just outside misses.
    inside = _cand_pos("0xin", 206_000, 0, 0.132)
    outside = _cand_pos("0xout", 215_000, 0, 0.132)
    m = match_hypotheses([inside, outside], cfg.labeled_hypotheses)
    hit_wallets = {h["wallet"] for h in m["sig-only"]["hits"]}
    assert "0xin" in hit_wallets and "0xout" not in hit_wallets


def test_pull_market_retries_transient_timeout(loaded, requests_mock):
    from consensus.fetching import build_data_layer
    from consensus.m0f import pull_market_events
    from tests.conftest import subgraph_meta_body, subgraph_event, SUBGRAPH_URL

    dl = build_data_layer(loaded)
    try:
        market = {"condition_id": "0xM", "token_ids": ["TOK"]}
        requests_mock.post(SUBGRAPH_URL, [
            {"json": {"data": subgraph_meta_body()}},
            {"json": {"errors": [{"message": "Query timed out"}]}},   # transient
            {"json": {"data": subgraph_meta_body()}},
            {"json": {"data": {"orderFilledEvents": [subgraph_event(1)]}}},
        ])
        events, prov = pull_market_events(dl, loaded, market, retries=2)
        assert len(events) == 1  # succeeded on retry, not aborted
    finally:
        dl.cache.close()


def test_enrich_wallet_isolates_persistent_failure(loaded, requests_mock):
    from consensus.fetching import build_data_layer
    from consensus.m0f import enrich_wallet
    from tests.conftest import SUBGRAPH_URL

    dl = build_data_layer(loaded)
    try:
        requests_mock.post(SUBGRAPH_URL, json={"errors": [{"message": "Query timed out"}]})
        info = enrich_wallet(dl, "0xw", before_ts=1_771_545_600)
        # A wallet that can't be enriched is DECLARED (error set), not imputed:
        # first_seen None -> F and T excluded downstream, never fabricated.
        assert info["error"] and info["first_seen_ts"] is None
    finally:
        dl.cache.close()


def test_trailing_volumes_window():
    fills = [_fill(ts=1_000_000, usdc=100, tokens=1000),
             _fill(ts=1_000_000 - 8 * 86400, usdc=999, tokens=9990, eid="old")]
    vols = trailing_volumes(fills, as_of=1_000_000, days=7)
    assert vols["0xMKT"] == pytest.approx(100)  # 8-day-old fill outside window
