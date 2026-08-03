"""Dossier Store acceptance tests (M10-D §3.2 / §5.2): a scan persists richly-tagged
records; re-scan is idempotent on dossier_id; backfill stamps outcomes."""

import json

import consensus.dossier_store as ds


def _con():
    return ds.connect(":memory:")


def _rec(**over):
    r = dict(
        wallet="0xW1", condition_id="0xMKT", token_id="0xTOK", side="BUY",
        market_question="Will X happen?", market_category="Politics", event_slug="x",
        first_seen_ts=1_000_000, first_seen_source="activity", detection_ts=1_100_000,
        entry_vwap=0.42, price_at_detection=0.44, contested_notional=25_000.0,
        headline_notional=90_000.0, f_factor=1.0, s_factor=0.6, d_factor=0.58, c_factor=0.7,
        latency_factor=None, composite=0.71, tier="ELEVATED", cluster_id=None,
        cluster_wallets=["0xW1"], actor_count_post_collapse=1, cross_market_cluster=None,
        funding_summary={"funder": "0xF", "hops": 1}, cex_class="unclassified",
        cex_confidence=0.3, provenance={"scan_id": "s1", "fills_ref": "cache#123"},
    )
    r.update(over)
    return r


def test_capture_is_rich_and_json_roundtrips():
    con = _con()
    ds.upsert(con, _rec(), scan_ts=1_200_000)
    row = ds.query(con)[0]
    assert row["contested_notional"] == 25_000.0 and row["headline_notional"] == 90_000.0
    assert json.loads(row["provenance"])["scan_id"] == "s1"      # Rule-1 provenance stored
    assert row["cex_class"] == "unclassified" and row["cex_confidence"] == 0.3
    assert row["label"] is None                                  # unset until reviewed


def test_rescan_is_idempotent_on_dossier_id():
    con = _con()
    assert ds.upsert(con, _rec(), scan_ts=1_000) == "inserted"
    assert ds.upsert(con, _rec(composite=0.80), scan_ts=2_000) == "updated"
    rows = ds.query(con)
    assert len(rows) == 1                       # no duplicate
    assert rows[0]["n_scans"] == 2
    assert rows[0]["first_scan_ts"] == 1_000 and rows[0]["last_scan_ts"] == 2_000
    # §6 frozen-as-scored: the detection-time factor vector is NOT rewritten by a later
    # scan (otherwise the accumulating labeled dataset would describe today's scoring,
    # not the scoring that actually fired). Movement lives in the *_peak columns.
    assert rows[0]["composite"] == 0.71
    assert rows[0]["composite_peak"] == 0.80


def test_never_wipe_a_later_scan_cannot_erase_captured_enrichment():
    """A re-scan in which the wallet is not gated supplies first_seen/F/funding as
    NULL. That must fill gaps, never erase what the gated scan paid to capture."""
    con = _con()
    ds.upsert(con, _rec(), scan_ts=1_000)
    ds.upsert(con, _rec(first_seen_ts=None, first_seen_source=None, f_factor=None,
                        funding_summary=None, cex_class=None), scan_ts=2_000)
    row = ds.query(con)[0]
    assert row["first_seen_ts"] == 1_000_000 and row["first_seen_source"] == "activity"
    assert row["f_factor"] == 1.0 and row["cex_class"] == "unclassified"


def test_tier_latches_at_high_water_mark():
    con = _con()
    ds.upsert(con, _rec(tier="WATCH"), scan_ts=1_000)
    ds.upsert(con, _rec(tier="CRITICAL"), scan_ts=2_000)
    ds.upsert(con, _rec(tier="WATCH"), scan_ts=3_000)   # decays
    row = ds.query(con)[0]
    assert row["tier"] == "WATCH"               # current = latest (trajectory)
    assert row["tier_peak"] == "CRITICAL"       # latched, never retracts
    assert row["tier_peak_ts"] == 2_000


def test_label_survives_rescan():
    con = _con()
    ds.upsert(con, _rec(), scan_ts=1_000)
    did = ds.make_dossier_id("0xMKT", "0xTOK", "0xW1")
    ds.set_label(con, did, "coordinated")
    ds.upsert(con, _rec(composite=0.9), scan_ts=2_000)   # a scan must not clobber the label
    assert ds.query(con)[0]["label"] == "coordinated"


def test_backfill_stamps_outcome_and_survives_rescan():
    con = _con()
    ds.upsert(con, _rec(token_id="0xTOK"), scan_ts=1_000)
    stats = ds.backfill_resolutions(con, lambda cid: ("0xTOK", 5_000_000))
    assert stats["stamped"] == 1
    row = ds.query(con, resolved=True)[0]
    assert row["resolved"] == 1 and row["winning_token"] == "0xTOK"
    assert row["outcome_for_side"] == 1                  # held token won
    ds.upsert(con, _rec(composite=0.5), scan_ts=2_000)   # re-scan must not wipe resolution
    assert ds.query(con)[0]["resolved"] == 1


def test_outcome_for_side_zero_when_flagged_side_loses():
    con = _con()
    ds.upsert(con, _rec(token_id="0xTOK"), scan_ts=1_000)
    ds.backfill_resolutions(con, lambda cid: ("0xOTHER", 5_000_000))
    assert ds.query(con)[0]["outcome_for_side"] == 0


def test_query_time_tags():
    assert ds.price_band(0.42) == "contested" and ds.price_band(0.97) == "favorite"
    assert ds.notional_bucket(25_000) == "10-50k" and ds.notional_bucket(None) == "none"
    assert ds.freshness_tag(1_000_000, 1_000_000 + 3 * 86400) == "fresh<=7d"
    assert ds.freshness_tag(1_000_000, 1_000_000 + 100 * 86400) == "established"
    assert ds.is_mesh_collapsed({"cluster_wallets": json.dumps(["a", "b", "c"]),
                                 "actor_count_post_collapse": 1}) is True
    assert ds.is_mesh_collapsed({"cluster_wallets": json.dumps(["a"]),
                                 "actor_count_post_collapse": 1}) is False


def test_missing_required_field_raises():
    con = _con()
    try:
        ds.upsert(con, _rec(wallet=None))
    except ValueError:
        return
    raise AssertionError("expected ValueError on missing wallet")
