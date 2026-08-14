"""Static-render acceptance (addendum v1.18).

The prior acceptance check verified the EXPORT had the right keys and passed a page that
rendered nothing, because it never checked that the page contains content. These assert
the rendered markup, which is the only check that catches an empty dashboard.
"""
import consensus.dossier_export as dx
import consensus.dossier_html as dh
import consensus.dossier_store as ds


def _store(**over):
    con = ds.connect(":memory:")
    rec = dict(wallet="0xW", condition_id="0xM", token_id="0xT", side="BUY",
               market_question="Will X happen?", market_category="politics", event_slug="e",
               first_seen_ts=1_000_000, first_seen_source="activity", detection_ts=1_100_000,
               entry_vwap=0.42, price_at_detection=None, contested_notional=None,
               headline_notional=25_000.0, f_factor=None, s_factor=0.6, d_factor=0.6,
               c_factor=0.7, latency_factor=None, composite=0.9, tier="CRITICAL",
               cluster_id=None, cluster_wallets=["0xW"], actor_count_post_collapse=1,
               cross_market_cluster=None, funding_summary=None, cex_class=None,
               cex_confidence=None, provenance={"s": 1})
    rec.update(over)
    ds.upsert(con, rec, scan_ts=1)
    return con


def test_page_needs_no_javascript_to_show_its_data():
    """The bug: the page rendered client-side, and the surface it is VIEWED through does
    not execute scripts, so every panel was blank while the prose looked authoritative."""
    page = dh.render_page(dx.build_export(_store(), now_ts=2))
    assert "<script" not in page.lower(), "page still depends on JS to render"


def test_every_panel_renders_real_values_not_just_headers():
    con = _store()
    ds.backfill_resolutions(con, lambda cid: ("0xT", 1_500_000))
    data = dx.build_export(con, now_ts=2_000_000)
    page = dh.render_page(data)
    # B: the block count and the power targets
    assert str(data["totals"]["resolved_blocks"]) in page
    assert "155" in page and "618" in page
    # A: a card with the market name
    assert "Will X happen?" in page
    # C: restraint numbers and the closed arm
    assert "alerts ever raised" in page and "coordination alerting" in page
    # D: a distribution
    assert "by price band" in page
    # framing
    assert "intelligence record, not a trading signal" in page
    assert "Detector A" in page and "Cross-venue lead-lag" in page


def test_missing_payload_renders_a_loud_error_not_silence():
    """§3: a blank panel is a bug, not a blank."""
    for bad in (None, {}, "not a dict"):
        page = dh.render_page(bad)
        assert "failed to load" in page
        assert "dossier record" in page          # still a page, not a crash


def test_a_failing_panel_shows_its_error_and_does_not_abort_the_others():
    def boom():
        raise RuntimeError("synthetic failure")
    out = dh.panel("T", "s", boom)
    assert "panel failed to build" in out and "synthetic failure" in out
    assert "error, not an empty result" in out


def test_true_zero_is_visually_distinct_from_failure():
    """Panel C's whole point: restraint and breakage must not look the same."""
    data = dx.build_export(_store(), now_ts=2)
    data["totals"]["alerted"] = 0
    page = dh.render_page(data)
    assert "No alert has ever fired" in page and "true zero" in page
    assert "failed to load" not in page


def test_empty_store_reads_as_empty_not_broken():
    page = dh.render_page(dx.build_export(ds.connect(":memory:"), now_ts=2))
    assert "true empty state" in page
    assert "failed to build" not in page
