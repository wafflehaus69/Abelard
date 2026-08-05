"""Alerting tests (M10-D §3.5 / §5.4): quiet-week dedupe, the mesh-collapse guard,
and the no-recommendation constraint."""

import json

import consensus.dossier_alert as da
import consensus.dossier_store as ds


def _rec(wallet="0xW1", composite=0.95, actors=1, wallets=None, vwap=0.42, cat="geopolitics"):
    return dict(
        wallet=wallet, condition_id="0xM" + wallet, token_id="0xT", side="BUY",
        market_question="Will X happen?", market_category=cat, event_slug="x",
        first_seen_ts=1_000_000, first_seen_source="activity", detection_ts=1_100_000,
        entry_vwap=vwap, price_at_detection=vwap, contested_notional=25_000.0,
        headline_notional=25_000.0, f_factor=1.0, s_factor=0.6, d_factor=0.6, c_factor=0.7,
        latency_factor=None, composite=composite, tier="CRITICAL",
        cluster_id=None, cluster_wallets=(wallets or [wallet]),
        actor_count_post_collapse=actors, cross_market_cluster=None,
        funding_summary=None, cex_class=None, cex_confidence=None,
        provenance={"scan_id": "s1"},
    )


def _con(*recs):
    con = ds.connect(":memory:")
    for r in recs:
        ds.upsert(con, r, scan_ts=1_200_000)
    return con


def test_alert_fires_above_bar_and_is_a_pointer_not_advice():
    con = _con(_rec(composite=0.95))
    alerts = da.evaluate(con, now=1_300_000)
    assert len(alerts) == 1
    a = alerts[0]
    assert a["open_with"].startswith("consensus dossier show")
    assert "composite" in a["why"][0]
    # Advice-language check over the alert BODY. The trailing disclaimer is excluded
    # because it necessarily names what is absent ("no expected value is implied").
    body = "\n".join(l for l in da.render_alerts(alerts).lower().splitlines()
                     if "not trade signals" not in l)
    for banned in ("buy", "sell", "recommend", "expected value", "position size", "you should"):
        assert banned not in body


def test_below_bar_is_silent():
    # a CRITICAL-tier dossier (0.70) must NOT page: the alert bar is separate/higher
    con = _con(_rec(composite=0.72))
    assert da.evaluate(con, now=1_300_000) == []
    assert "none" in da.render_alerts([]).lower()


def test_dedupe_pages_once_across_scans():
    con = _con(_rec(composite=0.95))
    assert len(da.evaluate(con, now=1_300_000)) == 1
    # the footprint persists into the next daily scan -> must not page again
    ds.upsert(con, _rec(composite=0.96), scan_ts=1_400_000)
    assert da.evaluate(con, now=1_400_000) == []


def test_escalation_repages_but_jitter_does_not():
    """One marginal crossing must not silence a footprint forever, but scan-to-scan
    jitter must not re-page either (quiet-week discipline)."""
    con = _con(_rec(composite=0.91))
    assert len(da.evaluate(con, now=1_000)) == 1
    ds.upsert(con, _rec(composite=0.93), scan_ts=1_100)      # +0.02 jitter
    assert da.evaluate(con, now=1_100) == []
    ds.upsert(con, _rec(composite=0.99), scan_ts=1_200)      # +0.08 real escalation
    again = da.evaluate(con, now=1_200)
    assert len(again) == 1 and again[0]["composite"] == 0.99


def test_stray_prefixed_category_still_matches_a_watch_list():
    con = _con(_rec(composite=0.95, cat="stray:geopolitics"))
    rule = da.AlertRule(categories=("geopolitics",))
    assert len(da.evaluate(con, rule=rule, now=1_000)) == 1


def test_cluster_facts_never_page_now_that_the_arm_is_closed():
    """The cluster/coordination arm is CLOSED permanently (owner ruling 2026-08-05):
    the measurement showed nothing to calibrate against. No cluster shape may page —
    not a collapsed mesh, not a genuine multi-actor cluster, not an unresolved one.
    Mesh collapse itself still runs; it informs the DOSSIER, never the alert."""
    for actors, wallets in ((1, [f"0x{i}" for i in range(20)]),      # sybil mesh -> 1
                            (4, [f"0x{i}" for i in range(4)]),       # genuine 4 actors
                            (None, [f"0x{i}" for i in range(20)])):  # unresolved
        con = _con(_rec(composite=0.10, actors=actors, wallets=wallets))
        assert da.evaluate(con, now=1_300_000) == []
    # and the rule no longer carries a cluster knob at all
    assert not hasattr(da.AlertRule(), "cluster_arm_enabled")
    assert not hasattr(da.AlertRule(), "cluster_min_actors")


def test_contested_gate_and_category_filter():
    # outside the standing 0.10-0.90 band -> not a signal-surface footprint
    con = _con(_rec(composite=0.95, vwap=0.97))
    assert da.evaluate(con, now=1_300_000) == []
    # category restriction
    con2 = _con(_rec(composite=0.95, cat="sports"))
    rule = da.AlertRule(categories=("geopolitics",))
    assert da.evaluate(con2, rule=rule, now=1_300_000) == []


def test_mark_false_does_not_consume_the_alert():
    con = _con(_rec(composite=0.95))
    assert len(da.evaluate(con, now=1_300_000, mark=False)) == 1
    assert len(da.evaluate(con, now=1_300_000)) == 1     # still pending


def test_incomplete_score_never_pages_and_never_sets_the_high_water_mark():
    """Live false positive, 2026-08-05: a wallet whose freshness could not be resolved
    was barred from a real tier by m0f (INSUFFICIENT_DATA, tier_peak NONE) yet paged on
    a composite_peak of 0.842 whose own frozen composite was 0.194. A score the detector
    refused to tier must neither set the peak nor reach the owner."""
    con = ds.connect(":memory:")
    ds.upsert(con, _rec(composite=0.194), scan_ts=1_000)
    # a later scan scores high but is data-incomplete
    ds.upsert(con, dict(_rec(composite=0.842), tier="INSUFFICIENT_DATA"), scan_ts=2_000)
    row = ds.query(con)[0]
    assert row["composite_peak"] != 0.842, "an incomplete score set the high-water mark"
    assert da.evaluate(con, now=3_000) == [], "an untierable score paged the owner"
